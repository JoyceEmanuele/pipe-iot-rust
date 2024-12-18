use crate::lib_rumqtt::{abrir_conexao_broker_rumqtt, next_mqtt_message_rumqtt};
use crate::on_mqtt_message;
use crate::GlobalVars;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn task_mqtt_client_broker(globs: Arc<GlobalVars>) {
    let broker_config = &globs.configfile.broker_config;
    loop {
        let result_msg = task_mqtt_client_broker_rumqtt(&globs).await;
        crate::LOG.append_log_tag_msg(
            "error",
            &format!(
                "task_mqtt_client_broker interrupted, will restart: {}:{} {:?}",
                broker_config.host, broker_config.port, result_msg
            ),
        );
        tokio::time::sleep(std::time::Duration::from_secs(3)).await;
    }
}

pub async fn connect_to_mqtt_broker(globs: &Arc<GlobalVars>) -> Result<rumqttc::EventLoop, String> {
    let broker_config = &globs.configfile.broker_config;
    // Create the client. Use an ID. A real system should try harder to use a unique ID.
    let pseudo_random = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_millis()
        % 100000;
    let client_id = format!("broker2db-{}", pseudo_random);

    // Abre a conexão com o broker (vernemq)
    let (eventloop, client_mqtt) = abrir_conexao_broker_rumqtt(broker_config, &client_id).await?;

    // Faz subscribe nos tópicos de interesse
    for topic in &globs.configfile.topics {
        // rumqttc
        client_mqtt
            .subscribe(topic, rumqttc::QoS::ExactlyOnce)
            .await
            .map_err(|e| e.to_string())?;
    }

    // Just loop on incoming messages.
    crate::LOG.append_log_tag_msg(
        "info",
        &format!(
            "Awaiting events from: {}:{}",
            broker_config.host, broker_config.port
        ),
    );

    // Note that we're not providing a way to cleanly shut down and
    // disconnect. Therefore, when you kill this app (with a ^C or
    // whatever) the server will get an unexpected drop.

    Ok(eventloop)
}

async fn task_mqtt_client_broker_rumqtt(globs: &Arc<GlobalVars>) -> Result<String, String> {
    let broker_config = &globs.configfile.broker_config;
    let mut eventloop = connect_to_mqtt_broker(globs).await?;
    loop {
        let packet = next_mqtt_message_rumqtt(&mut eventloop, broker_config).await?;

        let payload_str = match std::str::from_utf8(&packet.payload) {
            Ok(v) => v,
            Err(err) => {
                crate::LOG.append_log_tag_msg("ERROR", &format!("Invalid payload: {}", err));
                continue;
            }
        };

        on_mqtt_message::process_payload(&packet.topic, payload_str, globs).await;
    }
}
