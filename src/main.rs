use async_stream::stream;
use serde_json::json;
use chrono::{TimeZone, Local, Datelike, Timelike};
use chrono::{DateTime, Utc};
use fs_extra::dir::get_size;
use futures_core::stream::Stream;
use futures_util::pin_mut;
use futures_util::stream::StreamExt;
use reqwest::header::{HeaderName, HeaderValue};
use serde::Deserialize;
use std::fs::create_dir_all;
use std::path::PathBuf;
use std::str::FromStr;
use std::thread::sleep;
use std::time::{Duration, SystemTime};
use std::{error::Error, fs::read_to_string, path::Path, process::Stdio};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::Command,
};
use clap::Parser;

/// Simple program to greet a person
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
    // detault true
    archive: Option<bool>,
    // detaul true
    compress: Option<bool>,
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

    async fn delete_old_backups(&self) {
        let dir = PathBuf::from(format!("{}{}", self.local_path, self.alias));
        if !dir.exists() {
            return;
        }
        let backups = match dir.read_dir() {
            Ok(v) => v,
            Err(_) => return,
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
                backups_list.push((fname, elapsed));
            }
        }
        if self.backup_count > backups_list.len() {
            
        }
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
        if let Some(v) = &self.backup_name {
            let s = exec_stream(
                "rsync",
                &[
                    "-e",
                    &format!("ssh -p {}", self.port.unwrap_or(22)),
                    "-avzh",
                    &format!("--timeout={}", self.timeout.unwrap_or(10)),
                    &format!("--bwlimit={}", self.bandwidth_limit_mb * 1024),
                    &format!(
                        "{}@{}:{}{}",
                        self.remote_user, self.remote_host, self.remote_folder, v
                    ),
                    &format!("{}{}/", self.local_path, self.alias),
                ],
            );
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

        if let Some(v) = self.get_latest_entry().await {
            let s = exec_stream(
                "rsync",
                &[
                    "-e",
                    &format!("ssh -p {}", self.port.unwrap_or(22)),
                    "-avzh",
                    &format!("--timeout={}", self.timeout.unwrap_or(10)),
                    &format!(
                        "{}@{}:{}{}",
                        self.remote_user, self.remote_host, self.remote_folder, v
                    ),
                    &format!("{}{}/", self.local_path, self.alias),
                ],
            );
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
                    &[
                        &format!("{}{}/{v} {}{}/{backup_name}", self.local_path, self.alias, self.local_path, self.alias)
                    ],
                );
                pin_mut!(s);
                let _ = s;
            }
            return Ok(res);

        }
        Ok(String::from("no backup method was set for scheduled backup"))
    }
}

#[derive(Deserialize)]
struct Config {
    alias: String,
    notification_webhook: Vec<String>,
    notify_on_start: Option<bool>,
    notify_on_end: Option<bool>,
    notify_on_fail: Option<bool>,
    connections: Vec<Client>,
}

impl Config {
    pub async fn send_webhook(&self, message: &str) -> Result<(), Box<dyn Error>> {
        for webhook in &self.notification_webhook {
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
                .body(
                    serde_json::to_string(&json!({ "content": format!("[{} Node] {message}", self.alias) }))?,
                )
                .send()
                .await;
        }
        Ok(())
    }
}

include!(concat!(env!("OUT_DIR"), "/version.rs"));

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("starting backup system, {VERSION}");
    println!("reading config");
    let config: Config = serde_yaml::from_str(&read_to_string("./config.yml")?)?;
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
    // tokio::spawn(async move {
    // });
    let mut counter: usize = 0;
    dbg!("iter start");
    loop {
        // tokio::time::sleep(Duration::from_secs(1)).await;
        sleep(Duration::from_secs(1));
        counter = counter.wrapping_add(1);
        dbg!("iter");
        for client in &config.connections {
            if let Some(v) = client.interval_sec {
                if counter % v == 0 {
                    if config.notify_on_start.unwrap_or_default() {
                        let _ = config.send_webhook(&format!("Starting scheduled backed for {}", client.alias)).await;
                    }
                    if let Ok(v) = client.backup().await {
                        if config.notify_on_end.unwrap_or_default() {
                            let _ = config.send_webhook(&format!("Successfully backed up {}, message: {v}", client.alias)).await;
                            continue;
                        }
                    }
                    if config.notify_on_fail.unwrap_or_default() {
                        let _ = config.send_webhook(&format!("Failed: {v}")).await;
                    }
                }
            }
        }
    }
    // TODO future rest api or smth to measure backup system health
    // park();
}