mod helpers {
    pub mod envvars_loader;
    pub mod lib_essential_thread;
    pub mod lib_log;
    pub mod lib_dynamodb {
        pub mod client;
        pub mod query;
    }
    pub mod lib_http {
        pub mod buffer;
        pub mod protocol;
        pub mod request;
        pub mod response;
        pub mod service;
        pub mod types;
    }
}

mod app_getmac {
    pub mod configs;
    pub mod fetch_mac;
    pub mod global_vars;
    pub mod http_router;
    pub mod lib_fs;
}

use app_getmac::*;
use configs::ConfigFile;
use global_vars::GlobalVars;
use helpers::*;
use std::sync::Arc;

/*
A ideia desta ferramenta é extrair do DynamoDB o endereço MAC dos dispositivos
*/

static LOG: lib_log::AppLog = lib_log::AppLog { app_name: "getmac" };

fn main() {
    let configfile = ConfigFile::from_env().expect("configfile inválido");
    let globs = GlobalVars::new(configfile);
    let globs = Arc::new(globs);

    lib_essential_thread::run_thread_async_loop_pars("http".to_owned(), globs.clone(), |globs| {
        let addr = globs.configfile.LISTEN_SOCKET_GETMAC.to_owned();
        lib_http::service::run_service_result(addr, globs, &http_router::on_http_req)
    })
    .join();
}
