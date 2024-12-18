use super::global_vars::DevLastMessage;
use crate::GlobalVars;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;

/*

function onDeviceMessage(devId: string) {
  let devLastMessages = lastMessages[devId];
  const prevTS = devLastMessages?.ts;

  if (devLastMessages) {
    devLastMessages.ts = Date.now();
  } else {
    devLastMessages = lastMessages[devId] = {
      tsBefore: null,
      ts: Date.now(),
    };
  }
  deviceLastTs[devId] = devLastMessages.ts;

  const becameOnline = !!(prevTS && ((devLastMessages.ts - prevTS) > TIMEOUT_LATE));
  const wasOffline = !!(prevTS && ((devLastMessages.ts - prevTS) > TIMEOUT_OFFLINE));
  const tsBefore = prevTS || null;
  if (becameOnline) {
    // Avisa o front através do websocket
    listenerForStatusChange?.(devId, 'ONLINE');
  }

  return { devLastMessages, wasOffline, tsBefore };
}

*/

pub fn process_payload(packet: rumqttc::Publish, globs: &Arc<GlobalVars>) {
    let mut topic = &packet.topic[..];
    if topic.starts_with("iotrelay/") {
        topic = &topic[9..];
    }

    let is_data = topic.starts_with("data/");

    if !is_data && !topic.starts_with("control/") {
        // Ignore
        return;
    }

    tokio::spawn(process_payload_on_valid_topic(
        globs.clone(),
        packet,
        is_data,
    ));
}

async fn process_payload_on_valid_topic(
    globs: Arc<GlobalVars>,
    packet: rumqttc::Publish,
    is_data: bool,
) {
    let (_payload_str, payload_json, dev_id) = match parse_payload_json(&packet) {
        ResultJsonParse::Ok(x) => x,
        ResultJsonParse::Ignore => {
            return;
        }
        ResultJsonParse::Err(err) => {
            let message = format!("[76] {err}");
            crate::LOG.append_log_tag_msg_v2("ERROR", &message, false);
            return;
        }
    };

    let now_millis: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
        .try_into()
        .expect("timestamp too large");

    // Atualiza o last_timestamp
    {
        let mut need_insert = false;
        match globs.last_timestamp.read().await.get(&dev_id) {
            Some(dev_info) => {
                dev_info.store(now_millis, Ordering::Relaxed);
            }
            None => {
                need_insert = true;
            }
        };
        if need_insert {
            globs
                .last_timestamp
                .write()
                .await
                .insert(dev_id.to_owned(), AtomicU64::new(now_millis));
        }
    }

    // Atualiza o last_telemetry
    if is_data {
        let need_insert;
        match globs.last_telemetry.read().await.get(&dev_id) {
            Some(dev_info) => {
                let mut dev_info = dev_info.write().await;
                dev_info.ts = now_millis;
                dev_info.telemetry = payload_json;
                return;
            }
            None => {
                need_insert = true;
            }
        };
        if need_insert {
            let dev_info = DevLastMessage {
                ts: now_millis,
                telemetry: payload_json,
            };
            globs
                .last_telemetry
                .write()
                .await
                .insert(dev_id.to_owned(), RwLock::new(dev_info));
        }
    }
}

enum ResultJsonParse<T> {
    Ok(T),
    Err(String),
    Ignore,
}

fn parse_payload_json<'a>(
    packet: &'a rumqttc::Publish,
) -> ResultJsonParse<(&'a str, serde_json::Value, String)> {
    let payload_str = match std::str::from_utf8(&packet.payload) {
        Ok(v) => v,
        Err(err) => {
            return ResultJsonParse::Err(format!("Invalid payload: {}", err));
        }
    };

    // Ignore invalid payload
    if !payload_str.starts_with('{') {
        // For example: "Current RMT state:..."
        return ResultJsonParse::Ignore;
    }

    // Parse payload string to JSON object
    let payload_json: serde_json::Value = match serde_json::from_str(payload_str) {
        Ok(v) => v,
        Err(err) => {
            // let message = format!("{} {} {}", packet.topic, err, payload_str);
            // crate::LOG.append_log_tag_msg("ERROR", &message);
            return ResultJsonParse::Err(format!(
                "Invalid payload [197]: {}\n  {}",
                err, payload_str
            ));
        }
    };

    let dev_id = match payload_json["dev_id"].as_str() {
        Some(v) => v.to_owned(),
        None => {
            return ResultJsonParse::Err(format!("Invalid payload [208]: {}", payload_str));
        }
    };

    ResultJsonParse::Ok((payload_str, payload_json, dev_id))
}
