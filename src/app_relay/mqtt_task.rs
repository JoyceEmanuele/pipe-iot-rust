use super::on_mqtt_message;
use crate::lib_rumqtt::abrir_conexao_broker_rumqtt;
use crate::lib_rumqtt::next_mqtt_message_rumqtt;
use crate::GlobalVars;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub async fn task_mqtt_client_broker(globs: Arc<GlobalVars>) {
    loop {
        {
            if !*globs.configs_ready.lock().await {
                crate::LOG.append_log_tag_msg(
                    "warn",
                    &format!(
                        "task_mqtt_client_broker aguardando configs de hardware para poder iniciar"
                    ),
                );
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                continue;
            }
        }
        let result_msg = task_mqtt_client_broker_rumqtt(&globs).await;
        crate::LOG.append_log_tag_msg(
            "error",
            &format!(
                "task_mqtt_client_broker interrupted, will restart: {:?}",
                result_msg
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
    let client_id = format!("iotrelay-{}", pseudo_random);

    // Abre a conexão com o broker (vernemq)
    let (eventloop, client_mqtt) = abrir_conexao_broker_rumqtt(broker_config, &client_id).await?;

    // Faz subscribe nos tópicos de interesse
    client_mqtt
        .subscribe(r"$share/iotrelay/data/#", rumqttc::QoS::AtLeastOnce)
        .await
        .map_err(|e| e.to_string())?;
    client_mqtt
        .subscribe(r"$share/iotrelay/control/#", rumqttc::QoS::AtLeastOnce)
        .await
        .map_err(|e| e.to_string())?;
    client_mqtt
        .subscribe("apiserver/#", rumqttc::QoS::AtLeastOnce)
        .await
        .map_err(|e| e.to_string())?;

    {
        *(globs.broker_client.write().await) = Some(Arc::new(client_mqtt));
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
