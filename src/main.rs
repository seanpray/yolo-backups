use async_stream::stream;
use chrono::{DateTime, Utc};
use chrono::{Datelike, Local, TimeZone, Timelike};
use clap::Parser;
use fs_extra::dir::get_size;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use reqwest::header::{HeaderName, HeaderValue};
use serde::Deserialize;
use serde_json::json;
use std::fs::{create_dir_all, remove_dir_all, remove_file};
use std::io::Error as ioError;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::{park_timeout, sleep};
use std::time::{Duration, SystemTime};
use std::{error::Error, fs::read_to_string, path::Path, process::Stdio};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// trigger backup now
    #[arg(short, long)]
    backup: bool,
}

pub fn exec_stream<P: AsRef<Path>>(cmd: P, args: &[&str]) -> impl Stream<Item = String> {
    let mut cmd = Command::new(cmd.as_ref())
        .args(args)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();

    stream! {
        let stdout = cmd.stdout.as_mut().unwrap();
        let stdout_reader = BufReader::new(stdout);
        let mut stdout_lines = stdout_reader.lines();
        while let Ok(line) = stdout_lines.next_line().await {
            if line.is_none() {
                break;
            }
            yield line.unwrap().clone();
        }
    }
}

#[derive(Deserialize)]
struct Client {
    alias: String,
    remote_user: String,
    remote_host: String,
    remote_folder: String,
    rename_result_ts: Option<bool>,
    // max folder_size for those backups
    max_size_gb: usize,
    // number of backups to keep
    backup_count: usize,
    // mb/s limit
    bandwidth_limit_mb: usize,
    // if not specified, take latest updated file/folder
    backup_name: Option<String>,
    // suffix that is required when trying to find latest folder
    required_suffix: Option<String>,
    // prefix that is required when trying to find latest folder
    required_prefix: Option<String>,
    // default true
    archive: Option<bool>,
    // default 3
    compression_level: Option<u8>,
    local_path: String,
    port: Option<u16>,
    interval_sec: Option<usize>,
    timeout: Option<usize>,
}

#[derive(Debug)]
struct RemoteFileEntry {
    name: String,
    last_update: Option<DateTime<Utc>>,
}

impl Client {
    async fn get_latest_entry(&self) -> Option<String> {
        let s = exec_stream(
            "rsync",
            &[
                "-e",
                &format!("ssh -p {}", self.port.unwrap_or(22)),
                "--list-only",
                &format!("--timeout={}", self.timeout.unwrap_or(10)),
                &format!("--bwlimit={}", self.bandwidth_limit_mb * 1024),
                &format!(
                    "{}@{}:{}",
                    self.remote_user, self.remote_host, self.remote_folder
                ),
            ],
        );
        pin_mut!(s);
        let mut directories = Vec::new();
        while let Some(value) = s.next().await {
            let value_split: Vec<&str> = value.split_whitespace().collect();
            if value_split.len() < 5 {
                dbg!("{value}");
                continue;
            }
            let dt_str = &value_split[2..4].join(" ");
            directories.push(RemoteFileEntry {
                name: value_split[4..].join(" "),
                last_update: Utc.datetime_from_str(dt_str, "%Y/%m/%d %H:%M:%S").ok(),
            });
        }
        let mut directories: Vec<(String, i64)> = directories
            .into_iter()
            .filter_map(|x| {
                if let Some(v) = x.last_update {
                    let mut filter = false;
                    if let Some(v) = &self.required_suffix {
                        if !x.name.ends_with(v) {
                            filter = true;
                        }
                    }
                    if let Some(v) = &self.required_prefix {
                        if !x.name.starts_with(v) {
                            filter = true;
                        }
                    }
                    if !filter {
                        Some((x.name, v.timestamp()))
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();
        directories.sort_by_key(|(_n, ts)| *ts);
        directories.reverse();
        directories.into_iter().map(|(n, _ts)| n).nth(0)
    }

    async fn delete_old_backups(&self) -> Result<String, ioError> {
        let dir = PathBuf::from(format!("{}{}", self.local_path, self.alias));
        if !dir.exists() {
            return Ok(format!("Directory {} does not exist", self.local_path));
        }
        let backups = match dir.read_dir() {
            Ok(v) => v,
            Err(_) => {
                return Ok(format!(
                    "Failed to read directory entries {}",
                    self.local_path
                ))
            }
        };
        let mut backups_list = Vec::new();
        for backup in backups.flatten() {
            let fname = backup.file_name().to_string_lossy().to_string();
            if let Ok(m) = backup.metadata() {
                let creation = match m.created() {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let elapsed = match SystemTime::now().duration_since(creation) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if let Some(v) = &self.required_prefix {
                    if !fname.starts_with(v) {
                        continue;
                    }
                }
                if let Some(v) = &self.required_suffix {
                    if !fname.ends_with(v) {
                        continue;
                    }
                }
                backups_list.push((fname, elapsed, m.is_dir()));
            }
        }
        if self.backup_count <= backups_list.len() {
            return Ok(format!(
                "No backups to remove! [{}/{}]",
                backups_list.len(),
                self.backup_count
            ));
        }
        backups_list.sort_by_key(|x| x.1);

        let new_backups_count = backups_list.len() - self.backup_count;
        let mut removed = Vec::with_capacity(new_backups_count);
        for _ in 0..new_backups_count {
            if let Some((backup, time, dir)) = &backups_list.pop() {
                removed.push(format!(
                    "{} \t\tcreation elapsed (seconds): {}",
                    backup,
                    time.as_secs()
                ));
                if *dir {
                    remove_dir_all(backup)?;
                } else {
                    remove_file(backup)?;
                }
            }
        }
        if new_backups_count > 0 {
            Ok(format!(
                "```Removed:\n{}\nRemaining:\n{}```",
                removed.join("\n"),
                backups_list
                    .iter()
                    .map(|(path, age, _)| format!(
                        "{path} \t\t creation elapsed (seconds): {}",
                        age.as_secs()
                    ))
                    .collect::<Vec<String>>()
                    .join("\n")
            ))
        } else {
            unreachable!()
        }
    }

    fn exec_rsync(&self, backup_name: &str) -> impl Stream<Item = String> {
        exec_stream(
            "rsync",
            &[
                "-e",
                &format!("ssh -p {}", self.port.unwrap_or(22)),
                "-avzh",
                &format!("--timeout={}", self.timeout.unwrap_or(10)),
                &format!("--bwlimit={}", self.bandwidth_limit_mb * 1024),
                &format!(
                    "{}@{}:{}{}",
                    self.remote_user, self.remote_host, self.remote_folder, backup_name,
                ),
                &format!("{}{}/", self.local_path, self.alias),
            ],
        )
    }

    async fn backup(&self) -> Result<String, String> {
        let path = format!("{}{}", self.local_path, self.alias);
        let Ok(size) = get_size(path) else {
            return Err(String::from("Failed to check backup folder size"));
        };
        if size as usize > self.max_size_gb * 1_000_000_000 {
            return Err(format!(
                "max size exceeded for backup folder for {}",
                self.alias
            ));
        }
        // TODO compress-level
        if let Some(backup_name) = &self.backup_name {
            let s = self.exec_rsync(backup_name);
            dbg!("test2");
            let s = s.peekable();
            pin_mut!(s);
            let mut res = String::new();
            while let Some(value) = &s.next().await {
                if s.as_mut().peek().await.is_none() {
                    dbg!(res);
                    res = value.to_owned();
                }
            }
            // if self.rename_result_ts.unwrap_or_default() {
            //     let now: DateTime<Local> = Local::now();
            //     let backup_name = &format!(
            //         "{}_{:0>4}-{:0>2}-{:0>2}_{:0>2}_{:0>2}_{:0>2}",
            //         self.alias,
            //         now.year(),
            //         now.month(),
            //         now.day(),
            //         now.hour(),
            //         now.minute(),
            //         now.second()
            //     );
            //     let s = exec_stream(
            //         "mv",
            //         &[
            //             &format!("{}{}/{v} {}{}/{backup_name}", self.local_path, self.alias, self.local_path, self.alias)
            //         ],
            //     );
            //     pin_mut!(s);
            //     let _ = s;
            // }
            return Ok(res);
        }

        if let Some(latest) = self.get_latest_entry().await {
            let s = self.exec_rsync(&latest);
            dbg!("test3");
            let s = s.peekable();
            pin_mut!(s);
            let mut res = String::new();
            while let Some(value) = &s.next().await {
                if s.as_mut().peek().await.is_none() {
                    dbg!(res);
                    res = value.to_owned();
                }
            }
            if self.rename_result_ts.unwrap_or_default() {
                let now: DateTime<Local> = Local::now();
                let backup_name = &format!(
                    "{}_{:0>4}-{:0>2}-{:0>2}_{:0>2}_{:0>2}_{:0>2}",
                    self.alias,
                    now.year(),
                    now.month(),
                    now.day(),
                    now.hour(),
                    now.minute(),
                    now.second()
                );
                let s = exec_stream(
                    "mv",
                    &[&format!(
                        "{}{}/{latest} {}{}/{backup_name}",
                        self.local_path, self.alias, self.local_path, self.alias
                    )],
                );
                pin_mut!(s);
                let _ = s;
            }
            return Ok(res);
        }
        Ok(String::from(
            "no backup method was set for scheduled backup",
        ))
    }
}

#[derive(Deserialize)]
struct Config {
    alias: String,
    notification_webhook: Vec<String>,
    notify_on_start: Option<bool>,
    notify_on_end: Option<bool>,
    notify_on_fail: Option<bool>,
    notify_on_delete: Option<bool>,
    connections: Vec<Client>,
}

/* would be better to take &self, but ownership issues with aysnc move */
pub async fn send_webhook(
    webhooks: &[String],
    message: &str,
    alias: &str,
) -> Result<(), Box<dyn Error>> {
    for webhook in webhooks {
        let client = reqwest::ClientBuilder::new()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_default();
        let _ = client
            .post(webhook)
            .header(
                HeaderName::from_str("Content-type").unwrap(),
                HeaderValue::from_str("application/json").unwrap(),
            )
            .body(serde_json::to_string(
                &json!({ "content": format!("[{} Node] {message}", alias) }),
            )?)
            .send()
            .await;
    }
    Ok(())
}

include!(concat!(env!("OUT_DIR"), "/version.rs"));

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("starting backup system, {VERSION}");
    println!("reading config");
    let mut config: Config = serde_yaml::from_str(&read_to_string("./config.yml")?)?;
    for client in &config.connections {
        println!(
            "checking if local_path backup directory exists for {}...",
            client.alias
        );
        if !PathBuf::from(&client.local_path).exists() {
            println!("local_path backup directory doesn't exists, attempting to create");
            create_dir_all(&client.local_path).expect("failed to create backup folder");
        }
        println!("creating alias directory inside local_path");
        create_dir_all(format!("{}{}", client.local_path, client.alias))
            .expect("failed to alias backup folder");
        dbg!(client.get_latest_entry().await);
        dbg!("finished getting latest file");
    }
    let webhooks = Arc::new(config.notification_webhook);
    while !config.connections.is_empty() {
        if let Some(client) = config.connections.pop() {
            let webhooks = webhooks.clone();
            tokio::spawn(async move {
                if let Some(backup_interval) = client.interval_sec {
                    loop {
                        dbg!("iter");
                        if config.notify_on_start.unwrap_or_default() {
                            let _ = send_webhook(
                                &webhooks,
                                &format!("Starting scheduled backed for {}", client.alias),
                                &client.alias,
                            )
                            .await;
                        }
                        if let Ok(v) = client.backup().await {
                            if config.notify_on_end.unwrap_or_default() {
                                let _ = send_webhook(
                                    &webhooks,
                                    &format!(
                                        "Successfully backed up {}, message: {v}",
                                        client.alias
                                    ),
                                    &client.alias,
                                )
                                .await;
                                continue;
                            }
                        }
                        if config.notify_on_fail.unwrap_or_default() {
                            let _ = send_webhook(
                                &webhooks,
                                &format!("Failed: {}", client.alias),
                                &client.alias,
                            )
                            .await;
                        }
                        let deletion_message = client.delete_old_backups().await;
                        if let Ok(v) = deletion_message {}
                        park_timeout(Duration::from_secs(backup_interval as u64))
                    }
                }
            });
        }
    }
    Ok(())
}
