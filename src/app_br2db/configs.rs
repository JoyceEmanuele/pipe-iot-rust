use crate::diel_hist_tables::{BigQueryHistoryTable, CustomTableRule};
use crate::envvars_loader;
use crate::lib_bigquery::client::GCPConfig;
use crate::lib_dynamodb::client::AWSConfig;
use crate::lib_rumqtt::BrokerConfig;

pub struct ConfigFile {
    pub listen_http_api: String,
    pub apiserver_internal_api: String,

    pub broker_config: BrokerConfig,
    pub topics: Vec<String>,

    pub gcp_config: Option<GCPConfig>,
    pub gcp_dest_table: BigQueryHistoryTable,

    pub aws_config: Option<AWSConfig>,
    pub default_aws_table_name: Option<String>,
    pub custom_aws_table_rules: Option<Vec<CustomTableRule>>,
}

impl ConfigFile {
    pub fn from_env() -> Result<ConfigFile, String> {
        envvars_loader::load_env_vars();

        let brokerConfig_host = envvars_loader::get_var_string_required("brokerConfig_host")?;
        let brokerConfig_port = envvars_loader::get_var_u16_required("brokerConfig_port")?;
        let brokerConfig_username =
            envvars_loader::get_var_string_required("brokerConfig_username")?;
        let brokerConfig_password =
            envvars_loader::get_var_string_required("brokerConfig_password")?;
        let CA_PATH = envvars_loader::get_var_string_optional("CA_PATH");
        let gcp_sa_key = envvars_loader::get_var_string_optional("gcp_sa_key");
        let gcp_project_id = envvars_loader::get_var_string_optional("gcp_project_id");
        let gcp_dataset_id = envvars_loader::get_var_string_optional("gcp_dataset_id");
        let gcp_default_table_id = envvars_loader::get_var_string_optional("gcp_default_table_id");
        let awsConfig_accessKeyId =
            envvars_loader::get_var_string_optional("awsConfig_accessKeyId");
        let awsConfig_secretAccessKey =
            envvars_loader::get_var_string_optional("awsConfig_secretAccessKey");
        let awsConfig_sessionToken =
            envvars_loader::get_var_string_optional("awsConfig_sessionToken");
        let HTTP_API_PORT = envvars_loader::get_var_string_required("HTTP_API_PORT")?;
        let STATS_SERVER_HTTP = envvars_loader::get_var_string_required("STATS_SERVER_HTTP")?;
        let brokerConfig_topics: Vec<String> =
            envvars_loader::get_var_structure_required("brokerConfig_topics")?;
        let awsConfig_default_table_name =
            envvars_loader::get_var_string_optional("awsConfig_default_table_name");
        let awsConfig_custom_table_rules: Option<Vec<CustomTableRule>> =
            envvars_loader::get_var_structure_optional("awsConfig_custom_table_rules")?;

        let use_tls = CA_PATH.is_some();

        let broker_config = BrokerConfig {
            host: brokerConfig_host,
            port: brokerConfig_port,
            username: brokerConfig_username,
            password: brokerConfig_password,
            use_tls,
            ca_cert: CA_PATH,
        };

        let gcp_config = if gcp_dataset_id.is_some() {
            Some(GCPConfig {
                credentials_file: gcp_sa_key.ok_or_else(|| "Invalid BigQuery config".to_owned())?,
                project_id: gcp_project_id.ok_or_else(|| "Invalid BigQuery config".to_owned())?,
                dataset_id: gcp_dataset_id.ok_or_else(|| "Invalid BigQuery config".to_owned())?,
            })
        } else {
            None
        };

        let gcp_dest_table = match gcp_default_table_id.as_deref() {
            None => BigQueryHistoryTable::DevType, // default behaviour
            Some("@none") => BigQueryHistoryTable::None,
            Some("@dev_type") => BigQueryHistoryTable::DevType,
            Some("@dev_gen") => BigQueryHistoryTable::DevGeneration,
            Some("@dev_id") => BigQueryHistoryTable::DevId,
            Some(table_id) => {
                if table_id.starts_with("@") {
                    return Err(format!("Invalid gcp_default_table_id: {table_id}"));
                }
                BigQueryHistoryTable::SingleTable(table_id.to_owned())
            }
        };

        let aws_config = if awsConfig_accessKeyId.is_some() {
            Some(AWSConfig {
                access_key_id: awsConfig_accessKeyId
                    .ok_or_else(|| "Invalid AWS config".to_owned())?,
                secret_access_key: awsConfig_secretAccessKey
                    .ok_or_else(|| "Invalid AWS config".to_owned())?,
                session_token: awsConfig_sessionToken,
            })
        } else {
            None
        };

        let apiserver_internal_api = if STATS_SERVER_HTTP.contains("://") {
            STATS_SERVER_HTTP
        } else {
            format!("http://{STATS_SERVER_HTTP}")
        };

        Ok(ConfigFile {
            listen_http_api: HTTP_API_PORT,
            apiserver_internal_api,
            broker_config,
            topics: brokerConfig_topics,
            gcp_config,
            aws_config,
            default_aws_table_name: awsConfig_default_table_name,
            custom_aws_table_rules: awsConfig_custom_table_rules,
            gcp_dest_table,
        })
    }
}
