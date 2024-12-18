use crate::GlobalVars;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub async fn process_control_message(
    mut payload: serde_json::Value,
    topic: &str,
    table_name: &'static str,
    dev_id: String,
    globs: &Arc<GlobalVars>,
) {
    use chrono::Duration;
    if dev_id.len() >= 3 {
    }
    // OK
    else {
        globs
            .log_info
            .lock()
            .await
            .devError("dev_id.length < 3", &dev_id, &topic, &payload, "");
        globs.stats.dev_id_missing.fetch_add(1, Ordering::Relaxed);
        return;
    }

    payload["devId"] = (&dev_id[..]).into();
    payload["ts"] = (chrono::Utc::now() - Duration::hours(3))
        .format("%Y-%m-%dT%H:%M:%S%.3f")
        .to_string()
        .into();
    payload["topic"] = topic.into();

    let topic = topic.to_owned();
    let globs = globs.clone();
    tokio::task::spawn(async move {
        let result = globs
            .client_dynamo
            .as_ref()
            .expect("DynamoDB client is null")
            .insert_telemetry(table_name, &payload, &globs)
            .await;
        match result {
            Ok(_v) => {
                // v.consumed_capacity;
                globs.log_info.lock().await.telemetrySaved(
                    &dev_id,
                    "",
                    payload["ts"].as_str().unwrap_or(""),
                    table_name,
                );
                if table_name == "log_dev_cmd" {
                    globs.stats.saved_command.fetch_add(1, Ordering::Relaxed);
                } else {
                    globs.stats.saved_control.fetch_add(1, Ordering::Relaxed);
                };
            }
            Err(err) => {
                globs.log_info.lock().await.devError(
                    "Error saving to DynamoDB",
                    &dev_id,
                    &topic,
                    &payload,
                    &format!("{}", err),
                );
                crate::LOG.append_log_tag_msg(
                    "ERRDYNDB",
                    &format!("{};{}", table_name, payload.to_string()),
                );
                globs.stats.dynamodb_error.fetch_add(1, Ordering::Relaxed);
            }
        };
    });
}
