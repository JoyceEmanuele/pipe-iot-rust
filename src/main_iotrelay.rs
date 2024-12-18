mod helpers {
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
    pub mod lib_http {
        pub mod buffer;
        pub mod protocol;
        pub mod request;
        pub mod response;
        pub mod service;
        pub mod types;
    }
    pub mod envvars_loader;
    pub mod lib_essential_thread;
    pub mod lib_log;
    pub mod lib_rumqtt;
    pub mod tls_socket_rustls;
}
mod app_relay {
    pub mod commands_sender;
    pub mod configs;
    pub mod dash_update;
    pub mod global_vars;
    pub mod http_router;
    pub mod mqtt_task;
    pub mod on_mqtt_message;
    pub mod payload_conversions;
    pub mod redis_connection;
    pub mod state_persistence;
    pub mod statistics;
}

use app_relay::*;
use configs::ConfigFile;
use global_vars::GlobalVars;
use helpers::*;
use std::sync::Arc;

static LOG: lib_log::AppLog = lib_log::AppLog {
    app_name: "iotrelay",
};

fn main() {
    lib_log::create_log_dir().expect("Não foi possível criar a pasta de logs");

    // Verifica se é só para testar o arquivo de config
    for arg in std::env::args().skip(1) {
        if arg == "--test-config" {
            envvars_loader::check_configfile();
            std::process::exit(0);
        }
    }

    crate::LOG.append_log_tag_msg("INIT", "Serviço iniciado");

    let configfile = ConfigFile::from_env().expect("configfile inválido");
    let (globs, receiver_fila) = GlobalVars::new(configfile);
    let globs = Arc::new(globs);

    lib_essential_thread::run_thread_async(
        "statistics".to_owned(),
        statistics::task_stats(globs.clone()),
    );

    lib_essential_thread::run_thread_async_loop_pars("http".to_owned(), globs.clone(), |globs| {
        let addr = globs.configfile.listen_http_api.to_owned();
        lib_http::service::run_service_result(addr, globs, &http_router::on_http_req)
    });

    lib_essential_thread::run_thread_async(
        format!("mqtt_client_broker"),
        mqtt_task::task_mqtt_client_broker(globs.clone()),
    );

    // Quando inicia o serviço (e também de tempo em tempo) tem que solicitar as configs de hardware do API-Server
    lib_essential_thread::run_thread_async_loop_pars(
        "atualizar-cfgs-hw".to_owned(),
        globs.clone(),
        |globs| async move {
            dash_update::run_task(&globs, &globs.conv_vars).await;
        },
    );

    lib_essential_thread::run_thread_async_loop_pars(
        "manter-conexao-redis".to_owned(),
        globs.clone(),
        |globs| async move {
            redis_connection::manter_conexao_redis(globs).await;
        },
    );

    lib_essential_thread::run_thread_async(
        format!("broker_queue"),
        commands_sender::task_mqtt_broker_writer(receiver_fila, globs.clone()),
    )
    .join()
    .unwrap();
}

fn check_certificate_validity(path: &str) -> String {
    // use x509_parser::prelude::*;
    let file_contents = match std::fs::read(path) {
        Ok(v) => v,
        Err(err) => {
            return format!("Error reading the file: {}", err);
        }
    };
    let (_, pem) = match x509_parser::pem::parse_x509_pem(&file_contents) {
        Ok(v) => v,
        Err(err) => {
            return format!("Error parsing the pem certificate: {}", err);
        }
    };
    let cert = match pem.parse_x509() {
        Ok(v) => v,
        Err(err) => {
            return format!("Error parsing the x509 certificate: {}", err);
        }
    };
    // pub fn time_to_expiration(&self) -> Option<std::time::Duration>
    let duration = match cert.tbs_certificate.validity.time_to_expiration() {
        Some(v) => v,
        None => {
            return "Could not get certificate validity".to_owned();
        }
    };
    return format!(
        "Certificate valid for: {} days",
        duration.as_seconds_f32() / 60. / 60. / 24.
    );
}
