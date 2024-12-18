mod helpers {
    pub mod lib_dynamodb {
        pub mod client;
    }
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
    pub mod telemetry_payloads {
        pub mod parse_json_props;
    }
    pub mod diel_hist_tables;
    pub mod envvars_loader;
    pub mod lib_log;
    pub mod tls_socket_rustls;
}

mod app_br2db {
    pub mod api;
    pub mod configs;
    pub mod global_vars;
    pub mod log;
    pub mod mqtt_task;
    pub mod on_control_message;
    pub mod on_data_message;
    pub mod on_mqtt_message;
    pub mod on_table_not_found;
    pub mod save_to_bigquery;
    pub mod save_to_dynamodb;
    pub mod socket_protocol;
    pub mod statistics;
}

use app_br2db::*;
use configs::ConfigFile;
use global_vars::GlobalVars;
use helpers::*;
use std::sync::Arc;

static LOG: lib_log::AppLog = lib_log::AppLog { app_name: "br2db" };

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

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("Error creating tokio runtime");
    let result = rt.block_on(main2());
    println!("{:?}", result);
}

async fn main2() {
    let configfile = ConfigFile::from_env().expect("configfile inválido");
    let (globs, receiver_bigquery) = GlobalVars::new(configfile).await;
    let globs = Arc::new(globs);

    lib_essential_thread::run_thread_async(
        "statistics".to_owned(),
        statistics::run_service(globs.clone()),
    );

    lib_essential_thread::run_thread_async_loop_pars("http".to_owned(), globs.clone(), |globs| {
        let addr = globs.configfile.listen_http_api.to_owned();
        lib_http::service::run_service_result(addr, globs, &app_br2db::api::on_http_req)
    });

    lib_essential_thread::run_thread_async(
        "save_to_bigquery".to_owned(),
        lib_bigquery::saver::task_save_to_bigquery(globs.clone(), receiver_bigquery, 2800),
    );

    lib_essential_thread::run_thread_async_loop_pars(
        "force_save_to_bigquery".to_owned(),
        globs.clone(),
        |globs| lib_bigquery::saver::task_force_save_to_bigquery(globs.to_bigquery.clone()),
    );

    lib_essential_thread::run_thread_async_loop_pars(
        "task_mqtt_client_broker".to_owned(),
        globs.clone(),
        |globs| async move {
            loop {
                let result_msg = mqtt_task::task_mqtt_client_broker(globs.clone()).await;
                crate::LOG.append_log_tag_msg(
                    "error",
                    &format!(
                        "task_mqtt_client_broker interrupted, will restart: {}:{} {:?}",
                        globs.configfile.broker_config.host,
                        globs.configfile.broker_config.port,
                        result_msg
                    ),
                );
                tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            }
        },
    )
    .join();
}
