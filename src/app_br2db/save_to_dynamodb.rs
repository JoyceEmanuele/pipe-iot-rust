use crate::diel_hist_tables::TopicRulesUtils;
use crate::GlobalVars;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub async fn save_telemetry_to_dynamodb(
    topic: &str,
    payload: serde_json::Value,
    globs: &Arc<GlobalVars>,
) {
    let (table_name, dev_id) = match find_dynamodb_table_name(&globs, &topic, &payload).await {
        Some(x) => x,
        None => match propose_table_name(&globs, &topic, &payload) {
            Some(x) => x,
            None => {
                return;
            }
        },
    };

    let result = globs
        .client_dynamo
        .as_ref()
        .expect("DynamoDB client is null")
        .insert_telemetry(&table_name, &payload, &globs)
        .await;
    match result {
        Ok(_v) => {
            // v.consumed_capacity;
            globs.log_info.lock().await.telemetrySaved(
                &dev_id,
                payload["GMT"].as_str().unwrap_or(""),
                payload["timestamp"].as_str().unwrap_or(""),
                &table_name,
            );
            globs.stats.saved_telemetry.fetch_add(1, Ordering::Relaxed);
        }
        Err(err) => {
            globs.log_info.lock().await.devError(
                "Error saving to DynamoDB",
                &dev_id,
                &topic,
                &payload,
                &format!("{}", err),
            );
            crate::LOG.append_log_tag_msg("ERRDYNDB", &format!("{};{:?}", table_name, payload));
            globs.stats.dynamodb_error.fetch_add(1, Ordering::Relaxed);
        }
    };
}

async fn find_dynamodb_table_name(
    globs: &Arc<GlobalVars>,
    topic: &str,
    payload: &serde_json::Value,
) -> Option<(String, String)> {
    let topic_tables = match globs.tables.find_matching_topic_rule(topic) {
        Some(v) => v,
        None => {
            // globs.log_info.lock().unwrap().topicError("Unexpected topic", topic, &payload, "");
            // globs.stats.unknown_topic.fetch_add(1, Ordering::Relaxed);
            return None; //("Unknown table - unknown topic".to_owned());
        }
    };
    for prop_rule in &topic_tables.topic_props {
        let dev_id = match payload[&prop_rule.prop_name].as_str() {
            None => {
                continue;
            }
            Some(v) => v.to_owned(),
        };

        if dev_id.len() != 12 {
            if (dev_id.len() > 4) && (dev_id.as_bytes()[4] == b'-') {
            }
            // Temporary ID, just ignore
            else {
                globs.log_info.lock().await.devError(
                    "dev_id.length !== 12",
                    &dev_id,
                    &topic,
                    &payload,
                    "",
                );
            }
            return None; //("Unknown table - dev_id.len() != 12".to_owned());
        }

        for (prefix, table_name) in &prop_rule.tables_list {
            if dev_id.starts_with(prefix) {
                return Some((table_name.to_owned(), dev_id));
            }
        }

        // globs.log_info.lock().unwrap().devError("No definition for this dev ID", &dev_id, topic, &payload, "");
        // {
        // 	let stats_sender = globs.stats.sender.clone();
        // 	let topic = topic.to_owned();
        // 	let dev_id = dev_id.to_owned();
        // 	task::spawn(async move {
        // 		stats_sender.send(StatsEvent::unknown_table(dev_id, topic)).await.expect("Erro ao gerar estatísticas");
        // 	});
        // }

        return None; //("Unknown table - No definition for this dev ID".to_owned());
    }

    // globs.log_info.lock().unwrap().topicError("Could not find dev ID", topic, &payload, "");
    // globs.stats.dev_id_missing.fetch_add(1, Ordering::Relaxed);
    return None; //("Unknown table - Could not find dev ID".to_owned());
}

fn propose_table_name(
    globs: &Arc<GlobalVars>,
    topic: &str,
    payload: &serde_json::Value,
) -> Option<(String, String)> {
    if !topic.starts_with("data/") {
        return None;
    }

    let dev_id = match payload["dev_id"].as_str() {
        Some(v) => v.to_owned(),
        None => {
            return None;
        }
    };

    if let Some(table_name) = &globs.configfile.default_aws_table_name {
        return Some((table_name.to_owned(), dev_id));
    }

    let is_valid = globs.valid_dev_id_checker.is_match(&dev_id);
    if !is_valid {
        return None;
    }

    let proposed_table_name = get_prod_dynamo_table_for_dev(&dev_id);

    return Some((proposed_table_name, dev_id));
}

fn get_prod_dynamo_table_for_dev(dev_id: &str) -> String {
    let prefix = &dev_id[0..8];
    format!("{prefix}XXXX_RAW")
    // let prefix = &dev_id[..8];
    // let proposed_table_name = format!("{}XXXX_RAW", prefix);
}
