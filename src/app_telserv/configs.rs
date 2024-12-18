use crate::app_relay::configs::BrokerInfo;
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

    pub aws_config: Option<AWSConfig>,
    pub default_aws_table_name: Option<String>,
    pub custom_aws_table_rules: Option<Vec<CustomTableRule>>,

    pub url_redis: String,

    pub enable_forward_to_broker: bool,
    pub enable_save_to_dynamodb: bool,
    pub enable_save_to_bigquery: bool,
    pub gcp_dest_table: BigQueryHistoryTable,
    pub redis_prefix: String,
}

impl ConfigFile {
    pub fn from_env() -> Result<ConfigFile, String> {
        envvars_loader::load_env_vars();

        let broker_info: BrokerInfo = envvars_loader::get_var_structure_required("BROKER")?;
        let gcp_dataset_id = envvars_loader::get_var_string_optional("gcp_dataset_id");
        let gcp_sa_key = envvars_loader::get_var_string_optional("gcp_sa_key");
        let gcp_project_id = envvars_loader::get_var_string_optional("gcp_project_id");
        let gcp_default_table_id = envvars_loader::get_var_string_optional("gcp_default_table_id");
        let awsConfig_accessKeyId =
            envvars_loader::get_var_string_optional("awsConfig_accessKeyId");
        let awsConfig_secretAccessKey =
            envvars_loader::get_var_string_optional("awsConfig_secretAccessKey");
        let awsConfig_sessionToken =
            envvars_loader::get_var_string_optional("awsConfig_sessionToken");
        let STATS_SERVER_HTTP = envvars_loader::get_var_string_required("STATS_SERVER_HTTP")?;
        let disable_forward_to_broker =
            envvars_loader::get_var_bool_optional("disable_forward_to_broker")?;
        let disable_save_to_dynamodb =
            envvars_loader::get_var_bool_optional("disable_save_to_dynamodb")?;
        let disable_save_to_bigquery =
            envvars_loader::get_var_bool_optional("disable_save_to_bigquery")?;
        let listen_http_api = envvars_loader::get_var_string_optional("LISTEN_HTTP_API_TELSERV");
        let URL_REDIS = envvars_loader::get_var_string_required("URL_REDIS")?;
        let brokerConfig_topics: Vec<String> =
            envvars_loader::get_var_structure_required("brokerConfig_topics")?;
        let awsConfig_default_table_name =
            envvars_loader::get_var_string_optional("awsConfig_default_table_name");
        let awsConfig_custom_table_rules: Option<Vec<CustomTableRule>> =
            envvars_loader::get_var_structure_optional("awsConfig_custom_table_rules")?;
        let redis_prefix = envvars_loader::get_var_string_optional("REDIS_PREFIX")
            .unwrap_or_else(|| "tel/".to_owned());

        let broker_config = BrokerConfig {
            host: broker_info.host,
            port: broker_info.port,
            username: broker_info.username,
            password: broker_info.password,
            use_tls: false,
            ca_cert: None,
        };

        let gcp_config = if gcp_dataset_id.is_some() {
            Some(GCPConfig {
                credentials_file: gcp_sa_key.ok_or_else(|| "Invalid BigQuery config".to_owned())?,
                project_id: gcp_project_id.ok_or_else(|| "Invalid BigQuery config".to_owned())?,
                dataset_id: gcp_dataset_id.ok_or_else(|| "Invalid BigQuery config".to_owned())?,
                // default_table_id: gcp_default_table_id,
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
            format!("http://{}", STATS_SERVER_HTTP)
        };

        let enable_forward_to_broker = disable_forward_to_broker != Some(true);
        let enable_save_to_dynamodb = disable_save_to_dynamodb != Some(true);
        let enable_save_to_bigquery = disable_save_to_bigquery != Some(true);

        Ok(ConfigFile {
            listen_http_api: listen_http_api.unwrap_or_else(|| "0.0.0.0:29582".to_owned()),
            apiserver_internal_api,
            url_redis: if enable_forward_to_broker {
                URL_REDIS
            } else {
                format!("{}1", URL_REDIS)
            },
            topics: brokerConfig_topics,
            default_aws_table_name: awsConfig_default_table_name,
            custom_aws_table_rules: awsConfig_custom_table_rules,

            broker_config,
            gcp_config: if enable_save_to_bigquery {
                gcp_config
            } else {
                None
            },
            aws_config: if enable_save_to_dynamodb {
                aws_config
            } else {
                None
            },

            enable_forward_to_broker,
            enable_save_to_dynamodb,
            enable_save_to_bigquery,
            gcp_dest_table,
            redis_prefix,
        })
    }
}
