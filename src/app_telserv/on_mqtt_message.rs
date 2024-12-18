use super::merge_calculated_values::merge_processed_values;
use crate::app_relay::on_mqtt_message::check_and_forward_payload;
use crate::app_relay::on_mqtt_message::parse_packet;
use crate::app_relay::payload_conversions::convert_control_payload;
use crate::app_relay::payload_conversions::convert_data_payload;
use crate::save_to_bigquery;
use crate::save_to_dynamodb;
use crate::GlobalVars;
use chrono::NaiveDateTime;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub fn process_payload(packet: rumqttc::Publish, globs: &Arc<GlobalVars>, configs_ready: bool) {
    let topic = &packet.topic;

    globs.stats.mqtt_recv.fetch_add(1, Ordering::Relaxed);

    if topic.starts_with("data/") {
        globs.stats.topic_data.fetch_add(1, Ordering::Relaxed);
        tokio::spawn(process_payload_on_data(
            globs.clone(),
            packet,
            configs_ready,
        ));
    } else if topic.starts_with("control/") {
        process_payload_on_control(globs, packet);
        globs.stats.topic_ctrl.fetch_add(1, Ordering::Relaxed);
    } else {
        process_payload_on_others(globs, packet);
    }
}

fn parse_payload_json<'a>(
    packet: &'a rumqttc::Publish,
) -> Option<(&'a str, serde_json::Value, String)> {
    let payload_str = match std::str::from_utf8(&packet.payload) {
        Ok(v) => v,
        Err(err) => {
            crate::LOG.append_log_tag_msg("ERROR", &format!("Invalid payload: {}", err));
            return None;
        }
    };

    // Ignore invalid payload
    if !payload_str.starts_with('{') {
        // For example: "Current RMT state:..."
        return None;
    }

    // Parse payload string to JSON object
    let (payload_json, dev_id) = match parse_packet(&payload_str) {
        Ok(v) => v,
        Err(err) => {
            let message = format!("{} {} {}", packet.topic, err, payload_str);
            crate::LOG.append_log_tag_msg_v2("ERROR", &message, false);
            return None;
        }
    };

    Some((payload_str, payload_json, dev_id))
}

async fn process_payload_on_data(
    globs: Arc<GlobalVars>,
    packet: rumqttc::Publish,
    configs_ready: bool,
) {
    let topic = &packet.topic;
    let Some((payload_str, mut payload_json, dev_id)) = parse_payload_json(&packet) else {
        return;
    };

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

    globs
        .stats
        .payloads_received
        .fetch_add(1, Ordering::Relaxed);

    if pack_ts < 946684800 {
        // Ignore invalid timestamp
        // 946684800 is the timestamp for 2000-01-01
        globs
            .stats
            .payloads_discarded
            .fetch_add(1, Ordering::Relaxed);
        return;
    }

    let gmt = match payload_json["GMT"].as_i64() {
        Some(x) => x,
        None => {
            payload_json["GMT"] = Into::<serde_json::Value>::into(-3);
            -3
        }
    };

    // match payload["saved_data"].as_bool() {
    // 	Some(v) => v,
    // 	None => false,
    // };

    // Salvar no DynamoDB o payload sem os cálculos do iotrelay
    let enable_dynamodb =
        globs.configfile.enable_save_to_dynamodb && globs.configfile.aws_config.is_some();
    if enable_dynamodb {
        if topic.starts_with("data/") {
            let topic = topic.to_owned();
            let globs = globs.clone();
            let payload = payload_json.clone();
            tokio::spawn(async move {
                save_to_dynamodb::save_telemetry_to_dynamodb(&topic, payload, &globs).await
            });
        }
    }

    // Tratamento do iotrelay feito para o tempo real
    let processed_payload;
    if configs_ready {
        let processing_result =
            convert_data_payload(payload_json.clone(), payload_str, &dev_id, &globs).await;
        processed_payload = check_and_forward_payload(
            processing_result,
            topic,
            payload_str,
            &dev_id,
            &globs,
            globs.configfile.enable_forward_to_broker,
        );
    } else {
        processed_payload = None;
    }

    let enable_bigquery =
        globs.configfile.enable_save_to_bigquery && globs.configfile.gcp_config.is_some();
    if enable_bigquery {
        merge_processed_values(&mut payload_json, processed_payload, &dev_id);
        save_to_bigquery::save_telemetry_to_bigquery(
            topic,
            payload_json,
            dev_id,
            pack_ts,
            gmt,
            &globs,
        )
        .await;
    }
}

fn process_payload_on_control(globs: &Arc<GlobalVars>, packet: rumqttc::Publish) {
    let topic = &packet.topic;
    let Some((payload_str, payload_json, dev_id)) = parse_payload_json(&packet) else {
        return;
    };

    // Tratamento do iotrelay feito para o tempo real
    let processing_result =
        convert_control_payload(payload_json.clone(), topic, payload_str, &dev_id, &globs);
    check_and_forward_payload(
        processing_result,
        topic,
        payload_str,
        &dev_id,
        globs,
        globs.configfile.enable_forward_to_broker,
    );
}

fn process_payload_on_others(globs: &Arc<GlobalVars>, packet: rumqttc::Publish) {
    let topic = &packet.topic;
    if topic == "apiserver/hwcfg-change" {
        // Houve mudança de config de hardware, marca o booleano para solicitar versão atualizada
        globs.need_update_configs.store(true, Ordering::Relaxed);
    } else {
        // Ignorar, tópico desconhecido
        println!("ERROR89: Ignoring unknown topic: {}", topic);
    }
}
