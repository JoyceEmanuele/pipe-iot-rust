use std::path::Path;

use crate::lib_http::{
    response::{respond_http_json, respond_http_plain_text},
    types::HttpResponse,
};

pub fn create_parts_dir() -> std::io::Result<()> {
    std::fs::create_dir_all("./parts")
}

pub fn build_part_file_name(dev_id: &str, ts_ini: &str, offset: &str) -> String {
    format!("./parts/{}.{}.{}.part", ts_ini, dev_id, offset)
}

pub fn process_clear_cache(parsed: serde_json::Value) -> Result<HttpResponse, String> {
    let before = parsed["before"].as_str().unwrap_or("");
    let mut folder_path = match std::env::current_dir() {
        Ok(v) => v,
        Err(err) => {
            return Ok(respond_http_plain_text(
                500,
                &format!("Could not get current directory: {}", err),
            ))
        }
    };
    folder_path.push(Path::new("./parts/"));
    crate::LOG.append_log_tag_msg(
        "INFO",
        &format!(
            "Deleting .part files from the current directory: {}",
            folder_path.as_path().to_str().unwrap() // .to_str() falha se Path não é UTF-8 válido. Não trabalhamos com caminhos que não sejam UTF-8.
        ),
    );

    // 2021-03-12T00:00:00.DAC210191053.part
    let rgx_filename = regex::Regex::new(r"^(.+/)?(\d\d\d\d-\d\d-\d\d.*\.\w+\.part)$").unwrap();
    // let root_path = std::path::Path::new(folder_path);
    let paths = match std::fs::read_dir(&folder_path) {
        Ok(v) => v,
        Err(err) => {
            return Ok(respond_http_plain_text(
                500,
                &format!("Could not get read directory: {}", err),
            ))
        }
    };
    let mut n = 0;
    for entry in paths {
        let path = match entry {
            Ok(v) => v.path(),
            Err(err) => {
                return Err(format!("Error gettng file info: {}", err));
            }
        };
        let filename = match rgx_filename.captures(path.to_str().unwrap()) {
            Some(cap) => cap[2].to_owned(),
            None => continue,
        };
        if (!before.is_empty()) && (&filename[..] > before) {
            continue;
        }
        match std::fs::remove_file(path) {
            Ok(_) => {}
            Err(err) => {
                return Ok(respond_http_plain_text(
                    500,
                    &format!("Could not get remove file: {}", err),
                ))
            }
        }
        n += 1;
    }

    crate::LOG.append_log_tag_msg("INFO", &format!("Part files removed: {}", n));

    let resp = serde_json::json!({
      "success": true,
    });
    return Ok(respond_http_json(200, &resp.to_string()));
}
