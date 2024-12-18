use super::configs::ConfigFile;
use crate::app_br2db::on_table_not_found::{
    on_table_not_found_bigquery, on_table_not_found_dynamodb,
};
use crate::app_relay::commands_sender::MsgToBroker;
pub use crate::app_relay::global_vars::ConversionVars;
use crate::diel_hist_tables::{self, TablesConfig};
use crate::lib_bigquery::client::BigQueryClient;
use crate::lib_bigquery::saver::SaveToBqEvent;
use crate::lib_dynamodb::client::DynamoDBClientDiel;
use crate::log::LogInfo;
use crate::telemetry_payloads::dac_telemetry::HwInfoDAC;
use crate::telemetry_payloads::dri_telemetry::HwInfoDRI;
use crate::telemetry_payloads::dut_telemetry::HwInfoDUT;
use crate::{configs, statistics};
use regex::Regex;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};

pub struct GlobalVars {
    pub configfile: configs::ConfigFile,
    pub conv_vars: Mutex<ConversionVars>,
    pub configs_ready: Mutex<bool>,
    pub default_dac_hw: HwInfoDAC,
    pub default_dut_hw: HwInfoDUT,
    pub default_dri_hw: HwInfoDRI,
    pub broker_client: RwLock<Option<Arc<rumqttc::AsyncClient>>>,
    pub redis_client: Mutex<Option<redis::aio::ConnectionManager>>,
    pub to_broker: mpsc::Sender<MsgToBroker>,
    pub to_bigquery: mpsc::Sender<SaveToBqEvent>,
    pub need_update_configs: AtomicBool,
    pub stats: statistics::StatisticsCounters,

    pub last_table_create_command_aws: Mutex<Option<std::time::Instant>>,
    pub last_table_create_command_bq: Mutex<Option<std::time::Instant>>,
    pub client_dynamo: Option<DynamoDBClientDiel>,
    pub client_bigquery: Option<BigQueryClient>,
    pub log_info: Mutex<LogInfo>,
    pub tables: TablesConfig,
    pub valid_dev_id_checker: Regex,
    pub valid_dev_type_checker: Regex,
    pub insercoes_bq_em_curso: AtomicUsize,
}

impl GlobalVars {
    pub async fn new(
        configfile: ConfigFile,
    ) -> (
        GlobalVars,
        mpsc::Receiver<MsgToBroker>,
        mpsc::Receiver<SaveToBqEvent>,
    ) {
        create_globs(configfile).await
    }
}

pub async fn create_globs(
    configfile: ConfigFile,
) -> (
    GlobalVars,
    mpsc::Receiver<MsgToBroker>,
    mpsc::Receiver<SaveToBqEvent>,
) {
    // let (sender_stats, receiver_stats) = mpsc::channel::<StatsEvent>(2000);
    let (sender_fila, receiver_fila) = mpsc::channel::<MsgToBroker>(20000);
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

    let log_info = LogInfo {
        telemetrySaved_c: HashMap::new(),
        devError_c: HashMap::new(),
        topicError_c: HashMap::new(),
    };

    let globs = GlobalVars {
        configfile,
        // stats,
        stats: statistics::StatisticsCounters::new(),
        last_table_create_command_aws: Mutex::new(None),
        last_table_create_command_bq: Mutex::new(None),
        client_dynamo,
        client_bigquery,
        tables,
        valid_dev_id_checker: Regex::new(r"^D[A-Z0-9]{2}\d{9}$").expect("ERRO 24"),
        valid_dev_type_checker: Regex::new(r"^D[A-Z0-9]{2}\d").expect("ERRO 24"),

        configs_ready: Mutex::new(false),
        conv_vars: Mutex::new(ConversionVars {
            devs: HashMap::new(),
        }),
        default_dac_hw: HwInfoDAC {
            isVrf: false,
            calculate_L1_fancoil: Some(false),
            debug_L1_fancoil: Some(false),
            hasAutomation: false,
            P0Psuc: false,
            P1Psuc: false,
            P0Pliq: false,
            P1Pliq: false,
            P0multQuad: 0.0,
            P0multLin: 1.0,
            P0ofst: 0.0,
            P1multQuad: 0.0,
            P1multLin: 1.0,
            P1ofst: 0.0,
            fluid: None,
            t_cfg: None,
            simulate_l1: false,
            l1_psuc_offset: 0.0,
            DAC_APPL: None,
            DAC_TYPE: None,
        },
        default_dut_hw: HwInfoDUT {
            temperature_offset: 0.0,
        },
        default_dri_hw: HwInfoDRI { formulas: None },
        broker_client: RwLock::new(None),
        redis_client: Mutex::new(None),
        // certs_vld: HashMap::new(),
        to_broker: sender_fila,
        to_bigquery: sender_bigquery,
        need_update_configs: AtomicBool::new(true),
        log_info: Mutex::new(log_info),
        insercoes_bq_em_curso: AtomicUsize::new(0),
    };

    (globs, receiver_fila, receiver_bigquery)
}
