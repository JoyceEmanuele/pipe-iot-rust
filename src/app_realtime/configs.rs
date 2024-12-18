use crate::envvars_loader;
use crate::lib_rumqtt::BrokerConfig;

pub struct ConfigFile {
    pub listen_http_api: String,
    pub broker_config: BrokerConfig,
}

impl ConfigFile {
    pub fn from_env() -> Result<ConfigFile, String> {
        envvars_loader::load_env_vars();

        let host = envvars_loader::get_var_string_required("brokerConfig_host")?;
        let port = envvars_loader::get_var_u16_required("brokerConfig_port")?;
        let username = envvars_loader::get_var_string_required("brokerConfig_username")?;
        let password = envvars_loader::get_var_string_required("brokerConfig_password")?;
        let listen_http_api = envvars_loader::get_var_string_required("listen_http_api_realtime")?;

        let broker_config = BrokerConfig {
            host,
            port,
            username,
            password,
            use_tls: false,
            ca_cert: None,
        };

        Ok(ConfigFile {
            listen_http_api,
            broker_config,
        })
    }
}
