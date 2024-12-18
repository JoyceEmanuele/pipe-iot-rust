use crate::{envvars_loader, lib_dynamodb::client::AWSConfig};

pub struct ConfigFile {
    /* Credenciais para o rusthist buscar no DynamoDB as telemetrias */
    pub aws_config: AWSConfig,
    pub LISTEN_SOCKET_GETMAC: String,
}

impl ConfigFile {
    pub fn from_env() -> Result<ConfigFile, String> {
        envvars_loader::load_env_vars();

        let AWS_ACCESS_KEY_ID = envvars_loader::get_var_string_required("AWS_ACCESS_KEY_ID")?;
        let AWS_SECRET_ACCESS_KEY =
            envvars_loader::get_var_string_required("AWS_SECRET_ACCESS_KEY")?;
        let AWS_SESSION_TOKEN = envvars_loader::get_var_string_optional("AWS_SESSION_TOKEN");
        let LISTEN_SOCKET_GETMAC = envvars_loader::get_var_string_required("LISTEN_SOCKET_GETMAC")?;

        let aws_config = AWSConfig {
            access_key_id: AWS_ACCESS_KEY_ID,
            secret_access_key: AWS_SECRET_ACCESS_KEY,
            session_token: AWS_SESSION_TOKEN,
        };

        Ok(ConfigFile {
            aws_config,
            LISTEN_SOCKET_GETMAC,
        })
    }
}
