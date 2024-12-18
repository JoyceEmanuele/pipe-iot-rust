use super::on_control_message;
use super::on_data_message;
use crate::GlobalVars;
use serde_json::json;
use std::sync::Arc;

pub async fn process_payload(topic: &str, payload_str: &str, globs: &Arc<GlobalVars>) {
    // println!("Salvando: {} {}", msg.topic(), &payload);
    // let topic = msg.topic();
    // let payload_str = match std::str::from_utf8(msg.payload()) {
    // 	Ok(v) => v,
    // 	Err(err) => { println!("{} {}", topic, err); return; },
    // };
    if topic.starts_with("data/") {
        on_data_message::process_telemetry_message(payload_str, topic, globs).await;
        return;
    } else if topic.starts_with("control/") {
        let payload: serde_json::Value = match serde_json::from_str(payload_str) {
            Ok(v) => v,
            Err(err) => {
                println!("{} {} {}", topic, err, payload_str);
                return;
            }
        };
        let dev_id = match payload["dev_id"].as_str() {
            None => {
                return;
            }
            Some(v) => v.to_owned(),
        };
        let mut payload_obj: serde_json::Value = json!({});
        payload_obj["payload"] = payload.into();
        on_control_message::process_control_message(
            payload_obj,
            topic,
            "log_dev_ctrl",
            dev_id,
            globs,
        )
        .await;
        return;
    } else if topic.starts_with("commands/sync/") {
        let dev_id = topic["commands/sync/".len()..].to_owned();
        let mut payload_obj: serde_json::Value = json!({});
        payload_obj["payload"] = payload_str.into();
        on_control_message::process_control_message(
            payload_obj,
            topic,
            "log_dev_cmd",
            dev_id,
            globs,
        )
        .await;
        return;
    } else if topic.starts_with("commands/") {
        let payload: serde_json::Value = match serde_json::from_str(payload_str) {
            Ok(v) => v,
            Err(err) => {
                println!("{} {} {}", topic, err, payload_str);
                return;
            }
        };
        let dev_id = topic["commands/".len()..].to_owned();
        let mut payload_obj: serde_json::Value = json!({});
        payload_obj["payload"] = payload.into();
        on_control_message::process_control_message(
            payload_obj,
            topic,
            "log_dev_cmd",
            dev_id,
            globs,
        )
        .await;
        return;
    } else if (topic == "sync")
        && (payload_str.starts_with("SYNC ") || payload_str.starts_with("TIME "))
    {
        let dev_id = payload_str["SYNC ".len()..].to_owned();
        let mut payload_obj: serde_json::Value = json!({});
        payload_obj["payload"] = payload_str.into();
        on_control_message::process_control_message(
            payload_obj,
            topic,
            "log_dev_ctrl",
            dev_id,
            globs,
        )
        .await;
        return;
    }
    println!("ERROR89: Ignoring unknown topic: {}", topic);
}
