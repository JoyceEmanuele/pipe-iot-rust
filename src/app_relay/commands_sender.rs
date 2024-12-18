use crate::GlobalVars;
use futures::StreamExt;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tokio::sync::mpsc;
use MsgToBroker::*;

pub enum MsgToBroker {
    MessageToTopic(String, String),
}
impl std::fmt::Debug for MsgToBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        match self {
            MessageToTopic(..) => write!(f, "MsgToBroker::MessageToTopic"),
        }
    }
}

pub async fn task_mqtt_broker_writer(
    receiver: mpsc::Receiver<MsgToBroker>,
    globs: Arc<GlobalVars>,
) {
    let stream = tokio_stream::wrappers::ReceiverStream::new(receiver);
    stream
        .for_each_concurrent(None, |msg| async {
            let MsgToBroker::MessageToTopic(topic, packet_payload) = msg;
            let mut tentativa = 1;
            loop {
                let broker = {
                    let client = globs
                        .broker_client
                        .read()
                        .await
                        .as_ref()
                        .map(|client| client.clone());
                    match client {
                        None => {
                            crate::LOG.append_log_tag_msg(
                                "ERR_FWBRKR",
                                &format!(
                                    "[E1][T{}] {:?} {:?} {}",
                                    tentativa, &topic, packet_payload, "No connection"
                                ),
                            );
                            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                            tentativa += 1;
                            continue;
                        }
                        Some(client) => client,
                    }
                };

                let fut = tokio::time::timeout(
                    std::time::Duration::from_secs(3),
                    broker.publish(
                        &topic,
                        rumqttc::QoS::AtLeastOnce,
                        false,
                        packet_payload.as_bytes(),
                    ), // rumqttc::QoS::AtLeastOnce
                );
                let result = fut
                    .await
                    .map_err(|_e| "Publish to broker operation timed out".to_owned())
                    .and_then(|v| v.map_err(|err| format!("MQTT Error: {}", err)));
                match result {
                    Err(err) => {
                        // ("Erro ao encaminhar a mensagem para o broker");
                        crate::LOG.append_log_tag_msg(
                            "ERR_FWBRKR",
                            &format!("[E2][T{}] {} {} {}", tentativa, &topic, packet_payload, err),
                        );
                        globs.stats.fwbroker_error.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        tentativa += 1;
                        continue;
                    }
                    Ok(()) => {
                        if tentativa > 1 {
                            crate::LOG.append_log_tag_msg(
                                "FWBRKR_OK",
                                &format!("[T{}] {} {}", tentativa, &topic, packet_payload),
                            );
                        }
                        globs.stats.fwbroker_sent.fetch_add(1, Ordering::Relaxed);
                    }
                };
                break;
            }
        })
        .await;
}
