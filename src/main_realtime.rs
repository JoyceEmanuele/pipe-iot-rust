mod helpers {
    pub mod lib_log;
    pub mod lib_http {
        pub mod buffer;
        pub mod protocol;
        pub mod request;
        pub mod response;
        pub mod service;
        pub mod types;
    }
    pub mod lib_essential_thread;
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
    pub mod envvars_loader;
    pub mod tls_socket_rustls;
}

mod app_realtime {
    pub mod configs;
    pub mod devs_cache;
    pub mod global_vars;
    pub mod http_router;
    pub mod mqtt_task;
    pub mod on_mqtt_message;
    pub mod endpoints {
        pub mod get_devices_last_telemetries;
        pub mod get_devices_last_ts;
    }
}

use app_realtime::*;
use configs::ConfigFile;
use global_vars::GlobalVars;
use helpers::*;
use std::sync::Arc;

static LOG: lib_log::AppLog = lib_log::AppLog {
    app_name: "realtime",
};

/*
Ideia deste serviço:
 - Os outros serviços vão requisitar deste o status online/offline dos dispositivos.
 - Registra a última telemetria de cada dispositivo e o horário da última mensagem.
 - Este serviço pode também atualizar o banco de dados quando tem alteração de status online.
 - Precisa de uma estratégia para retirar da lista dispositivos removidos do Celsius.
*/

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
    let globs = GlobalVars::new(configfile).await;
    let globs = Arc::new(globs);

    // Inicia e aguarda as threads principais
    tokio::select! {
        result = tokio::spawn(
            lib_http::service::run_service_result(globs.configfile.listen_http_api.to_owned(), globs.clone(), &http_router::on_http_req)
        ) => { panic!("Some thread stopped: {:?}", result.unwrap()); },

        result = tokio::spawn(
            mqtt_task::task_mqtt_broker_reader(globs.clone())
        ) => { panic!("Some thread stopped: {:?}", result.unwrap()); },

        result = tokio::spawn(
            devs_cache::run_service(globs.clone())
        ) => { panic!("Some thread stopped: {:?}", result.unwrap()); },
    }
}
