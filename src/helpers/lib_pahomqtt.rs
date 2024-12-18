use crate::GlobalVars;
use futures::stream::StreamExt;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

async fn abrir_conexao_broker_paho(
    globs: Arc<GlobalVars>,
    client_id: &str,
) -> Result<
    (
        paho_mqtt::AsyncReceiver<Option<paho_mqtt::Message>>,
        paho_mqtt::AsyncClient,
    ),
    String,
> {
    let broker_host = &globs.configfile.broker_config.host;
    let broker_port = globs.configfile.broker_config.port;

    // "tcp://127.0.0.1:1884"
    // "ssl://127.0.0.1:8883"
    let broker_uri = format!("tcp://{}:{}", broker_host, broker_port);
    let create_opts = paho_mqtt::CreateOptionsBuilder::new()
        .server_uri(&broker_uri)
        .client_id(client_id)
        .max_buffered_messages(10000)
        .persistence(None)
        .finalize();

    // Create the client connection
    let mut client_mqtt = match paho_mqtt::AsyncClient::new(create_opts) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Error creating the client: {:?}", err));
        }
    };

    // Define the set of options for the connection
    let conn_opts = {
        let mut opts_builder = paho_mqtt::ConnectOptionsBuilder::new();
        opts_builder.keep_alive_interval(Duration::from_secs(20));
        // opts_builder.mqtt_version(paho_mqtt::MQTT_VERSION_3_1_1);
        opts_builder.clean_session(true);
        opts_builder.connect_timeout(Duration::from_secs(7));
        opts_builder.user_name(&globs.configfile.broker_config.username);
        opts_builder.password(&globs.configfile.broker_config.password);
        opts_builder.automatic_reconnect(Duration::from_secs(5), Duration::from_secs(10));
        if let Some(CA_PATH) = &globs.configfile.broker_config.ca_cert {
            opts_builder.ssl_options(
                paho_mqtt::SslOptionsBuilder::new()
                    // .key_store(Path::new("./certs/client_certificate_diel_broker_v2.pem")).expect("Failed to load client certificate!")
                    // .private_key(Path::new("./certs/client_private_key_diel_broker_v2.pem")).expect("Failed to load private key!")
                    .trust_store(Path::new(CA_PATH))
                    .expect("Failed to load ca_certificate!")
                    .enable_server_cert_auth(true)
                    .finalize(),
            );
        }
        opts_builder.finalize()
    };

    // Get message stream before connecting.
    let strm = client_mqtt.get_stream(5000);

    // Make the connection to the broker
    // println!("Connecting to the MQTT server...");
    if let Err(err) = client_mqtt.connect(conn_opts).await {
        return Err(format!("Unable to connect to broker: {:?}", err));
    }

    return Ok((strm, client_mqtt));
}

async fn next_mqtt_message_paho(
    strm: &mut paho_mqtt::AsyncReceiver<Option<paho_mqtt::Message>>,
    globs: &Arc<GlobalVars>,
) -> Result<paho_mqtt::Message, String> {
    let Some(msg_opt) = strm.next().await else {
        let broker_host = &globs.configfile.broker_config.host;
        let broker_port = globs.configfile.broker_config.port;
        return Err(format!(
            "Broker disconnected [2]: {}:{}",
            broker_host, broker_port
        ));
    };
    if let Some(msg) = msg_opt {
        // println!("{}", msg);
        // println!("{} {}", msg.topic(), msg.payload_str());

        // let topic = msg.topic();
        // let payload_str = std::str::from_utf8(msg.payload())
        // 	.map_err(|err| format!("{} {}", topic, err))?;

        return Ok(msg);
    } else {
        // A "None" means we were disconnected. Try to reconnect...
        // println!("Lost connection. Attempting reconnect.");
        // while let Err(err) = client.reconnect().await {
        //     println!("Error reconnecting: {}", err);
        //     // For tokio use: tokio::time::delay_for()
        //     tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        // }
        let broker_host = &globs.configfile.broker_config.host;
        let broker_port = globs.configfile.broker_config.port;
        return Err(format!(
            "Broker disconnected [1]: {}:{}",
            broker_host, broker_port
        ));
    }
}
