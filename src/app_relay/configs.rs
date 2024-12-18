use crate::envvars_loader;
use crate::lib_rumqtt::BrokerConfig;
use serde::Deserialize;

pub struct ConfigFile {
    pub listen_http_api: String,
    pub apiserver_internal_api: String,
    pub broker_config: BrokerConfig,
    pub url_redis: String,
    pub redis_prefix: String,
}

impl ConfigFile {
    pub fn from_env() -> Result<ConfigFile, String> {
        envvars_loader::load_env_vars();

        let LISTEN_SOCKET_IOTRELAY_HTTP =
            envvars_loader::get_var_string_required("LISTEN_SOCKET_IOTRELAY_HTTP")?;
        let STATS_SERVER_HTTP = envvars_loader::get_var_string_required("STATS_SERVER_HTTP")?;
        let URL_REDIS = envvars_loader::get_var_string_required("URL_REDIS")?;
        let BROKER_TLS_CA_PUBLIC_CERT =
            envvars_loader::get_var_string_optional("BROKER_TLS_CA_PUBLIC_CERT");
        let BROKER: BrokerInfo = envvars_loader::get_var_structure_required("BROKER")?;
        let redis_prefix = envvars_loader::get_var_string_optional("REDIS_PREFIX")
            .unwrap_or_else(|| "relay/".to_owned());

        let broker_config = BrokerConfig {
            host: BROKER.host,
            port: BROKER.port,
            username: BROKER.username,
            password: BROKER.password,
            use_tls: BROKER.use_tls,
            ca_cert: BROKER_TLS_CA_PUBLIC_CERT,
        };

        let apiserver_internal_api = if STATS_SERVER_HTTP.contains("://") {
            STATS_SERVER_HTTP
        } else {
            format!("http://{STATS_SERVER_HTTP}")
        };

        Ok(ConfigFile {
            listen_http_api: LISTEN_SOCKET_IOTRELAY_HTTP,
            apiserver_internal_api,
            broker_config,
            url_redis: URL_REDIS,
            redis_prefix,
        })
    }
}

#[derive(Deserialize)]
pub struct BrokerInfo {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub use_tls: bool,
}
