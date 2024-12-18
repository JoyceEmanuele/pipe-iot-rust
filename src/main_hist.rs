mod helpers {
    pub mod lib_http {
        pub mod buffer;
        pub mod protocol;
        pub mod request;
        pub mod response;
        pub mod service;
        pub mod types;
    }
    pub mod lib_dynamodb {
        pub mod client;
        pub mod query;
    }
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
    pub mod compression {
        pub mod common_func;
        pub mod compiler_DAC;
        pub mod compiler_DAL;
        pub mod compiler_DAM;
        pub mod compiler_DMA;
        pub mod compiler_DMT;
        pub mod compiler_DRI;
        pub mod compiler_DUT;
        pub mod compiler_common;
    }
    pub mod diel_hist_tables;
    pub mod envvars_loader;
    pub mod lib_essential_thread;
    pub mod lib_log;
}
mod app_history {
    pub mod cache_files;
    pub mod compiler_queues;
    pub mod configs;
    pub mod dac_hist;
    pub mod dal_hist;
    pub mod dam_hist;
    pub mod dev_export;
    pub mod dma_hist;
    pub mod dmt_hist;
    pub mod dri_hist;
    pub mod dut_hist;
    pub mod energy_hist;
    pub mod energy_stats;
    pub mod global_vars;
    pub mod http_router;
}

use app_history::*;
use configs::ConfigFile;
use global_vars::GlobalVars;
use helpers::*;
use std::sync::Arc;

static LOG: lib_log::AppLog = lib_log::AppLog {
    app_name: "rusthist",
};

fn main() {
    lib_log::create_log_dir().expect("Não foi possível criar a pasta de logs");
    cache_files::create_parts_dir().expect("Não foi possível criar a pasta de parts");

    // Verifica se é só para testar o arquivo de config
    for arg in std::env::args().skip(1) {
        if arg == "--test-config" {
            envvars_loader::check_configfile();
            std::process::exit(0);
        }
    }

    crate::LOG.append_log_tag_msg("INIT", "Serviço iniciado");

    let configfile = ConfigFile::from_env().expect("configfile inválido");
    let (globs, receiver_compiler) = GlobalVars::new(configfile);
    let globs = Arc::new(globs);

    lib_essential_thread::run_thread_async(
        "queue_manager".to_owned(),
        compiler_queues::task_queue_manager(receiver_compiler, globs.clone()),
    );

    lib_essential_thread::run_thread_async_loop_pars("http".to_owned(), globs.clone(), |globs| {
        let addr = globs.configfile.LISTEN_SOCKET_HIST.to_owned();
        lib_http::service::run_service_result(addr, globs, &http_router::on_http_req)
    })
    .join();
}
