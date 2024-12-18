use crate::{save_to_bigquery, save_to_dynamodb, GlobalVars};
use chrono::NaiveDateTime;
use std::sync::Arc;

pub async fn process_telemetry_message(payload_str: &str, topic: &str, globs: &Arc<GlobalVars>) {
    let mut payload: serde_json::Value = match serde_json::from_str(payload_str) {
        Ok(v) => v,
        Err(err) => {
            crate::LOG.append_log_tag_msg("ERROR", &format!("{} {} {}", topic, err, payload_str));
            return;
        }
    };

    let dev_id = match payload["dev_id"].as_str() {
        None => {
            globs
                .log_info
                .lock()
                .await
                .topicError("Could not find dev ID", topic, &payload, "");
            return;
        }
        Some(v) => v.to_owned(),
    };

    let gmt = match payload["GMT"].as_i64() {
        Some(x) => x,
        None => {
            payload["GMT"] = Into::<serde_json::Value>::into(-3);
            -3
        }
    };

    let timestamp = match payload["timestamp"].as_str() {
        Some(v) => v,
        None => {
            globs
                .log_info
                .lock()
                .await
                .topicError("No timestamp", &topic, &payload, "");
            return;
        }
    };

    let pack_ts = match NaiveDateTime::parse_from_str(&timestamp, "%Y-%m-%dT%H:%M:%S") {
        Ok(date) => date.and_utc().timestamp(),
        Err(err) => {
            globs.log_info.lock().await.topicError(
                "Invalid timestamp",
                &topic,
                &payload,
                &err.to_string(),
            );
            return;
        }
    };

    // match payload["saved_data"].as_bool() {
    // 	Some(v) => v,
    // 	None => false,
    // };

    let enable_dynamodb = globs.configfile.aws_config.is_some();
    let enable_bigquery = globs.configfile.gcp_config.is_some();

    if enable_dynamodb {
        let topic = topic.to_owned();
        let globs = globs.clone();
        let payload = payload.clone();
        tokio::spawn(async move {
            save_to_dynamodb::save_telemetry_to_dynamodb(&topic, payload, &globs).await
        });
    }

    if enable_bigquery {
        save_to_bigquery::save_telemetry_to_bigquery(topic, payload, dev_id, pack_ts, gmt, globs)
            .await;
    }
}
