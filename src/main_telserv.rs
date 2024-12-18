mod helpers {
    pub mod lib_dynamodb {
        pub mod client;
        pub mod query;
    }
    pub mod lib_log;
    pub mod lib_bigquery {
        pub mod client;
        pub mod saver;
    }
    pub mod lib_http {
        pub mod buffer;
        pub mod protocol;
        pub mod request;
        pub mod response;
        pub mod service;
        pub mod types;
    }
    pub mod lib_essential_thread;
    // pub mod lib_pahomqtt;
    pub mod lib_rumqtt;

    pub mod l1_virtual {
        pub mod dac_l1;
        pub mod dut_l1;
    }

    pub mod telemetry_payloads {
        pub mod dac_payload_json;
        pub mod dac_telemetry;
        pub mod dac_tsh_tsc;
        pub mod dal_payload_json;
        pub mod dal_telemetry;
        pub mod dam_payload_json;
        pub mod dam_telemetry;
        pub mod dma_payload_json;
        pub mod dma_telemetry;
        pub mod dmt_payload_json;
        pub mod dmt_telemetry;
        pub mod dri_telemetry;
        pub mod dut_payload_json;
        pub mod dut_telemetry;
        pub mod parse_json_props;
        pub mod telemetry_formats;
        pub mod temprt_value_checker;
        pub mod energy {
            pub mod dme;
            pub mod padronized;
        }
        pub mod dri {
            pub mod ccn;
            pub mod chiller_carrier_hx;
            pub mod chiller_carrier_xa;
            pub mod chiller_carrier_xa_hvar;
            pub mod vav_fancoil;
        }
        pub mod circ_buffer;
    }

    pub mod diel_hist_tables;
    pub mod envvars_loader;
    pub mod tls_socket_rustls;
}

mod app_relay {
    pub mod commands_sender;
    pub mod configs;
    pub mod dash_update;
    pub mod global_vars;
    pub mod http_router;
    pub mod on_mqtt_message;
    pub mod payload_conversions;
    pub mod redis_connection;
    pub mod state_persistence;
    pub mod statistics;
}
mod app_br2db {
    pub mod api;
    pub mod configs;
    pub mod global_vars;
    pub mod log;
    pub mod on_table_not_found;
    pub mod save_to_bigquery;
    pub mod save_to_dynamodb;
    pub mod socket_protocol;
    pub mod statistics;
}
mod app_telserv {
    pub mod configs;
    pub mod global_vars;
    pub mod merge_calculated_values;
    pub mod mqtt_task;
    pub mod on_mqtt_message;
    pub mod statistics;
}

use app_br2db::{log, save_to_bigquery, save_to_dynamodb};
use app_relay::{commands_sender, dash_update, redis_connection};
use app_telserv::global_vars::GlobalVars;
use app_telserv::*;
use configs::ConfigFile;
use helpers::*;
use lib_log::AppLog;
use std::sync::Arc;

static LOG: AppLog = AppLog {
    app_name: "telserv",
};

fn main() {
    // Criar pasta de logs e já inserir um registro indicando que iniciou o serviço
    lib_log::create_log_dir().expect("Não foi possível criar a pasta de logs");

    // Verifica se é só para testar o arquivo de config
    for arg in std::env::args().skip(1) {
        if arg == "--test-config" {
            envvars_loader::check_configfile();
            std::process::exit(0);
        }
    }

    crate::LOG.append_log_tag_msg("INIT", "Serviço iniciado");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Error creating tokio runtime");

    let result = rt.block_on(main2());

    println!("[EXIT] {:?}", result);
}

async fn main2() {
    let configfile = ConfigFile::from_env().expect("configfile inválido");
    let (globs, receiver_fila, receiver_bigquery) = GlobalVars::new(configfile).await;
    let globs = Arc::new(globs);
    let globs_clone1 = globs.clone();

    // Inicia e aguarda as threads principais
    tokio::select! {
        result = tokio::spawn(
            statistics::run_service(globs.clone())
        ) => { panic!("Some thread stopped: {:?}", result.unwrap()); },

        result = tokio::spawn(
            lib_http::service::run_service_result(globs.configfile.listen_http_api.to_owned(), globs.clone(), &app_relay::http_router::on_http_req)
        ) => { panic!("Some thread stopped: {:?}", result.unwrap()); },

        result = tokio::spawn(
            mqtt_task::task_mqtt_broker_reader(globs.clone())
        ) => { panic!("Some thread stopped: {:?}", result.unwrap()); },

        result = tokio::spawn(async move {
            // Quando inicia o serviço (e também de tempo em tempo) tem que solicitar as configs de hardware do API-Server
            dash_update::run_task(&globs_clone1, &globs_clone1.conv_vars).await;
        }) => { panic!("Some thread stopped: {:?}", result.unwrap()); },

        result = tokio::spawn(
            redis_connection::manter_conexao_redis(globs.clone())
        ) => { panic!("Some thread stopped: {:?}", result.unwrap()); },

        result = tokio::spawn(
            lib_bigquery::saver::task_save_to_bigquery(globs.clone(), receiver_bigquery, 2800),
        ) => { panic!("Some thread stopped: {:?}", result.unwrap()); },

        result = tokio::spawn(
            lib_bigquery::saver::task_force_save_to_bigquery(globs.to_bigquery.clone()),
        ) => { panic!("Some thread stopped: {:?}", result.unwrap()); },

        result = tokio::spawn(
            commands_sender::task_mqtt_broker_writer(receiver_fila, globs.clone()),
        ) => { panic!("Some thread stopped: {:?}", result.unwrap()); },
    }
}
