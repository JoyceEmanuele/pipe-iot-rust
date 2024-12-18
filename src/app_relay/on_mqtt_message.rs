use super::commands_sender::MsgToBroker;
use super::payload_conversions::{
    convert_control_payload, convert_data_payload, PayloadConversionResult,
};
use crate::GlobalVars;
use chrono::NaiveDateTime;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub async fn process_payload(topic: &str, payload_str: &str, globs: &Arc<GlobalVars>) {
    // Statistics counters
    globs.stats.mqtt_recv.fetch_add(1, Ordering::Relaxed);
    if topic.starts_with("data/") {
        globs.stats.topic_data.fetch_add(1, Ordering::Relaxed);
    } else if topic.starts_with("control/") {
        globs.stats.topic_ctrl.fetch_add(1, Ordering::Relaxed);
    }

    // Ignore invalid payload
    if !payload_str.starts_with('{') {
        // For example: "Current RMT state:..."
        return;
    }

    // Parse payload string to JSON object
    let (payload_json, dev_id) = match parse_packet(&payload_str) {
        Ok(v) => v,
        Err(err) => {
            let message = format!("{} {} {}", topic, err, payload_str);
            crate::LOG.append_log_tag_msg("ERROR", &message);
            return;
        }
    };

    if topic.starts_with("data/") {
        process_payload_on_data(globs, payload_json, dev_id, topic, payload_str).await;
    } else if topic.starts_with("control/") {
        process_payload_on_control(globs, payload_json, dev_id, topic, payload_str);
    } else {
        process_payload_on_others(globs, payload_json, dev_id, topic);
    }
}

pub fn check_and_forward_payload(
    processing_result: PayloadConversionResult,
    topic: &str,
    payload_str: &str,
    dev_id: &str,
    globs: &Arc<GlobalVars>,
    enable_forward: bool,
) -> Option<serde_json::Value> {
    match processing_result {
        PayloadConversionResult::WithoutConversion => {
            if enable_forward {
                let topic = build_topic(&dev_id, topic);
                let payload_str = payload_str.to_owned();
                let globs = globs.clone();
                tokio::spawn(async move {
                    globs
                        .to_broker
                        .send(MsgToBroker::MessageToTopic(topic, payload_str))
                        .await
                        .unwrap();
                });
            }
            None
        }
        PayloadConversionResult::Converted(payload_json) => {
            if enable_forward {
                let topic = build_topic(&dev_id, topic);
                let payload_str = payload_json.to_string();
                let globs = globs.clone();
                tokio::spawn(async move {
                    globs
                        .to_broker
                        .send(MsgToBroker::MessageToTopic(topic, payload_str))
                        .await
                        .unwrap();
                });
            }
            Some(payload_json)
        }
        PayloadConversionResult::Error(err) => {
            crate::LOG.append_log_tag_msg_v2(
                "ERROR[152]",
                &format!(
                    "Ignoring invalid payload: {} {} {}",
                    &err, topic, payload_str
                ),
                false,
            );
            None
        }
        PayloadConversionResult::IgnorePayload => None,
    }
}

async fn process_payload_on_data(
    globs: &Arc<GlobalVars>,
    mut payload_json: serde_json::Value,
    dev_id: String,
    topic: &str,
    payload_str: &str,
) {
    globs.stats.topic_data.fetch_add(1, Ordering::Relaxed);

    let pack_ts = match payload_json["timestamp"].as_str() {
        Some(timestamp_str) => {
            match NaiveDateTime::parse_from_str(&timestamp_str, "%Y-%m-%dT%H:%M:%S") {
                Ok(date) => date.and_utc().timestamp(),
                Err(err) => {
                    let message = format!("{} {} {}", topic, err, payload_json.to_string());
                    crate::LOG.append_log_tag_msg("ERROR", &message);
                    return;
                }
            }
        }
        None => {
            let message = format!("{} {} {}", topic, "No timestamp", payload_json.to_string());
            crate::LOG.append_log_tag_msg("ERROR", &message);
            return;
        }
    };

    let gmt = match payload_json["GMT"].as_i64() {
        Some(x) => x,
        None => {
            payload_json["GMT"] = Into::<serde_json::Value>::into(-3);
            -3
        }
    };

    let processing_result = convert_data_payload(payload_json, payload_str, &dev_id, &globs).await;
    check_and_forward_payload(processing_result, topic, payload_str, &dev_id, globs, true);
}

fn process_payload_on_control(
    globs: &Arc<GlobalVars>,
    payload_json: serde_json::Value,
    dev_id: String,
    topic: &str,
    payload_str: &str,
) {
    // Tratamento do iotrelay feito para o tempo real
    let processing_result =
        convert_control_payload(payload_json.clone(), topic, payload_str, &dev_id, &globs);
    check_and_forward_payload(processing_result, topic, payload_str, &dev_id, globs, true);
}

fn process_payload_on_others(
    globs: &Arc<GlobalVars>,
    payload_json: serde_json::Value,
    dev_id: String,
    topic: &str,
) {
    if topic == "apiserver/hwcfg-change" {
        // Houve mudança de config de hardware, marca o booleano para solicitar versão atualizada
        globs.need_update_configs.store(true, Ordering::Relaxed);
    } else {
        // Ignorar, tópico desconhecido
        println!("ERROR89: Ignoring unknown topic: {}", topic);
    }
}

pub fn build_topic(dev_id: &str, in_topic: &str) -> String {
    // montar iotrelay/data/dal/DAL123
    if dev_id.len() >= 3 {
        if in_topic.starts_with("data/") {
            return format!("iotrelay/data/{}/{}", dev_id[0..3].to_lowercase(), dev_id);
        }
        if in_topic.starts_with("control/") {
            return format!(
                "iotrelay/control/{}/{}",
                dev_id[0..3].to_lowercase(),
                dev_id
            );
        }
    };

    // default
    return format!("iotrelay/{}", in_topic);
}

pub fn parse_packet(payload_str: &str) -> Result<(serde_json::Value, String), String> {
    if !payload_str.starts_with('{') {
        return Err(format!("Invalid payload [191]: {}", payload_str));
    }

    let payload_json: serde_json::Value = match serde_json::from_str(payload_str) {
        Err(err) => {
            return Err(format!("Invalid payload [197]: {}\n  {}", err, payload_str));
        }
        Ok(v) => v,
    };

    let dev_id = match payload_json["dev_id"].as_str() {
        Some(v) => v.to_owned(),
        None => {
            return Err(format!("Invalid payload [208]: {}", payload_str));
        }
    };

    Ok((payload_json, dev_id))
}
