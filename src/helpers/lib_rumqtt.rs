use crate::tls_socket_rustls;
use std::sync::Arc;

pub struct BrokerConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub use_tls: bool,
    pub ca_cert: Option<String>,
}

pub async fn abrir_conexao_broker_rumqtt(
    config: &BrokerConfig,
    client_id: &str,
) -> Result<(rumqttc::EventLoop, rumqttc::AsyncClient), String> {
    // Define the set of options for the connection
    let mut mqttoptions = rumqttc::MqttOptions::new(client_id, &config.host, config.port);
    mqttoptions.set_keep_alive(std::time::Duration::from_secs(10));
    mqttoptions.set_clean_session(true);
    mqttoptions.set_request_channel_capacity(10000);
    // mqttoptions.set_connection_timeout(7);
    mqttoptions.set_credentials(config.username.to_owned(), config.password.to_owned());
    if config.use_tls {
        // Build the TLS (rustls) config
        let ca_path = config
            .ca_cert
            .as_ref()
            .expect("Invalid TLS config, missing ca_cert");
        let tls_client_config = tls_socket_rustls::create_client_config(ca_path)?;

        mqttoptions.set_transport(rumqttc::Transport::Tls(rumqttc::TlsConfiguration::Rustls(
            Arc::new(tls_client_config),
        )));
    } else {
        mqttoptions.set_transport(rumqttc::Transport::Tcp);
    }

    // Make the connection to the broker
    // crate::LOG.append_log_tag_msg("info", &format!("Connecting to the MQTT server..."));
    let (client, eventloop) = rumqttc::AsyncClient::new(mqttoptions, 10000);

    return Ok((eventloop, client));
}

pub async fn next_mqtt_message_rumqtt(
    eventloop: &mut rumqttc::EventLoop,
    config: &BrokerConfig,
) -> Result<rumqttc::Publish, String> {
    loop {
        let event = match eventloop.poll().await {
            Ok(event) => event,
            Err(err) => {
                return Err(format!(
                    "Broker disconnected: {}:{} {:?}",
                    config.host, config.port, err
                ));
            }
        };

        let packet = match event {
            rumqttc::Event::Incoming(packet) => packet,
            rumqttc::Event::Outgoing(packet) => {
                match packet {
                    rumqttc::Outgoing::Publish(_) => {}
                    rumqttc::Outgoing::PubAck(_) => {}
                    rumqttc::Outgoing::PubRel(_) => {}
                    rumqttc::Outgoing::PubRec(_) => {}
                    rumqttc::Outgoing::PubComp(_) => {}
                    rumqttc::Outgoing::PingReq => {}
                    rumqttc::Outgoing::PingResp => {}
                    other => {
                        crate::LOG
                            .append_log_tag_msg("info", &format!("Outgoing packet: {:?}", other));
                    }
                };
                continue;
            }
        };

        let packet = match packet {
            rumqttc::Packet::Publish(packet) => packet,
            rumqttc::Packet::PingReq => {
                continue;
            }
            rumqttc::Packet::PingResp => {
                continue;
            }
            rumqttc::Packet::PubRec(_) => {
                continue;
            }
            rumqttc::Packet::PubRel(_) => {
                continue;
            }
            rumqttc::Packet::PubComp(_) => {
                continue;
            }
            rumqttc::Packet::PubAck(_) => {
                continue;
            }
            other => {
                crate::LOG.append_log_tag_msg("info", &format!("Incoming packet: {:?}", other));
                continue;
            }
        };

        // crate::LOG.append_log_tag_msg("debug", &format!("{} {}", msg.topic(), msg.payload_str()));

        // let payload = match std::str::from_utf8(&packet.payload) {
        // 	Ok(v) => v,
        // 	Err(err) => {
        // 		crate::LOG.append_log_tag_msg("ERROR", &format!("Invalid payload: {}", err));
        // 		continue;
        // 	}
        // };

        return Ok(packet);
    }
}
