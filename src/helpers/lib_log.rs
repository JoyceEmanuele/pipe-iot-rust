use std::io::Write;
use std::ops::Sub;

pub fn create_log_dir() -> std::io::Result<()> {
    std::fs::create_dir_all("./log")
}

pub struct AppLog {
    pub app_name: &'static str,
}

impl AppLog {
    pub fn log_file_name_for_day(&self, day: &str) -> String {
        format!("./log/{}_{}.txt", self.app_name, day)
    }

    pub fn stats_file_name_for_day(&self, day: &str) -> String {
        format!("./log/stats_{}_{}.txt", self.app_name, day)
    }

    pub fn append_statistics(&self, json_str: &str) {
        // (payload: &[u8]) -> Result<(), String>
        let now = chrono::Utc::now().sub(chrono::Duration::hours(3));
        let result = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(self.stats_file_name_for_day(&now.to_rfc3339()[0..10]))
            .and_then(|mut file| {
                file.write_all(
                    format!(
                        "{:?}-0300 ",
                        &now.to_rfc3339_opts(chrono::SecondsFormat::Secs, false)[0..19]
                    )
                    .as_bytes(),
                )?;
                file.write_all(json_str.as_bytes())?;
                file.write_all(b"\n")
            }); // .map_err(|err| err.to_string())
        if let Err(err) = result {
            self.append_log_tag_msg("ERROR", &format!("Error writing to file: {}", err));
        }
    }

    pub fn append_log_tag_msg(&self, tag: &str, msg: &str) {
        self.append_log_tag_msg_v2(tag, msg, true);
    }

    pub fn append_log_tag_msg_v2(&self, tag: &str, msg: &str, to_stdout: bool) {
        if to_stdout {
            // Print to stdout
            println!("{}: {}", tag, msg);
        }

        // Insert into log file
        let now = chrono::Utc::now().sub(chrono::Duration::hours(3));
        let ts = &now.to_rfc3339_opts(chrono::SecondsFormat::Millis, false)[0..23];
        let json_str = serde_json::json!({ "tag":tag, "msg":msg }).to_string();
        let result = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(self.log_file_name_for_day(&ts[0..10]))
            .and_then(|mut file| {
                file.write_all(br#"{"tslog":""#)?;
                file.write_all(ts.as_bytes())?;
                file.write_all(br#"-0300","#)?;
                file.write_all(json_str[1..].as_bytes())?;
                file.write_all(b"\n")
            });
        if let Err(err) = result {
            println!("Error writing to log file: {}", err);
        }
    }

    fn append_log_tab(&self, line: &[&str]) {
        if line.len() == 1 {
            println!("{}", line[0]);
        } else if line.len() == 2 {
            println!("{}\t{}", line[0], line[1]);
        } else if line.len() > 0 {
            print!("{}", line[0]);
            for i in 1..line.len() {
                print!("\t");
                print!("{}", line[i]);
            }
            println!("");
        }
        let now = chrono::Utc::now().sub(chrono::Duration::hours(3));
        let ts = &now.to_rfc3339_opts(chrono::SecondsFormat::Millis, false)[0..23];
        let result = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(self.log_file_name_for_day(&ts[0..10]))
            .and_then(|mut file| {
                file.write_all(ts.as_bytes())?;
                file.write_all(b"-0300")?;
                for part in line {
                    file.write_all(b"\t")?;
                    file.write_all(part.as_bytes())?;
                }
                file.write_all(b"\n")
            });
        if let Err(err) = result {
            println!("Error writing to log file: {}", err);
        }
    }
}
