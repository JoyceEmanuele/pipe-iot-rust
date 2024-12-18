use super::on_table_not_found::{on_table_not_found_bigquery, on_table_not_found_dynamodb};
use super::statistics::StatisticsCounters;
use crate::configs::{self, ConfigFile};
use crate::diel_hist_tables::{self, TablesConfig};
use crate::lib_bigquery::client::BigQueryClient;
use crate::lib_bigquery::saver::SaveToBqEvent;
use crate::lib_dynamodb::client::DynamoDBClientDiel;
use crate::log::LogInfo;
use regex::Regex;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tokio::sync::Mutex;

pub struct GlobalVars {
    pub configfile: configs::ConfigFile,
    pub stats: StatisticsCounters,
    pub last_table_create_command_aws: Mutex<Option<std::time::Instant>>,
    pub last_table_create_command_bq: Mutex<Option<std::time::Instant>>,
    pub client_dynamo: Option<DynamoDBClientDiel>,
    pub client_bigquery: Option<BigQueryClient>,
    pub to_bigquery: mpsc::Sender<SaveToBqEvent>,
    pub log_info: Mutex<LogInfo>,
    pub tables: TablesConfig,
    pub valid_dev_id_checker: Regex,
    pub valid_dev_type_checker: Regex,
}

impl GlobalVars {
    pub async fn new(configfile: ConfigFile) -> (GlobalVars, mpsc::Receiver<SaveToBqEvent>) {
        create_globs(configfile).await
    }
}

pub async fn create_globs(configfile: ConfigFile) -> (GlobalVars, mpsc::Receiver<SaveToBqEvent>) {
    let stats = StatisticsCounters::new();
    let (sender_bigquery, receiver_bigquery) = crate::lib_bigquery::saver::create_channel();

    // Cria o objeto de conexão com o DynamoDB
    let client_dynamo = if let Some(aws_config) = &configfile.aws_config {
        let client_dynamo = DynamoDBClientDiel::new(aws_config, &on_table_not_found_dynamodb);
        Some(client_dynamo)
    } else {
        None
    };
    let client_bigquery = if let Some(gcp_config) = &configfile.gcp_config {
        let client_bigquery = BigQueryClient::new(gcp_config, Some(&on_table_not_found_bigquery))
            .await
            .unwrap();
        Some(client_bigquery)
    } else {
        None
    };

    // Carrega o arquivo que informa os nomes das tabelas para salvar as telemetrias
    let tables = if let Some(custom_aws_table_rules) = &configfile.custom_aws_table_rules {
        match diel_hist_tables::load_tables(custom_aws_table_rules) {
            Ok(v) => v,
            Err(err) => panic!("{:?}", err),
        }
    } else {
        vec![]
    };

    // let globs2 = GlobalVars2 {
    // 	tables,
    // 	tables_valid_until: (chrono::Utc::now() - chrono::Duration::seconds(100)),
    // };

    let log_info = LogInfo {
        telemetrySaved_c: HashMap::new(),
        devError_c: HashMap::new(),
        topicError_c: HashMap::new(),
    };

    let globs = GlobalVars {
        configfile,
        stats,
        last_table_create_command_aws: Mutex::new(None),
        last_table_create_command_bq: Mutex::new(None),
        client_dynamo,
        client_bigquery,
        to_bigquery: sender_bigquery,
        log_info: Mutex::new(log_info),
        // globs2: Mutex::new(globs2),
        tables,
        valid_dev_id_checker: Regex::new(r"^D[A-Z0-9]{2}\d{9}$").expect("ERRO 24"),
        valid_dev_type_checker: Regex::new(r"^D[A-Z0-9]{2}\d").expect("ERRO 24"),
    };

    (globs, receiver_bigquery)
}
