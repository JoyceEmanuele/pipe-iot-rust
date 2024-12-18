use super::commands_sender::MsgToBroker;
use super::configs::ConfigFile;
use super::dash_update::DevHwConfig;
use super::statistics;
use crate::telemetry_payloads::dac_telemetry::HwInfoDAC;
use crate::telemetry_payloads::dri_telemetry::HwInfoDRI;
use crate::telemetry_payloads::dut_telemetry::HwInfoDUT;
use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tokio::sync::{mpsc, Mutex, RwLock};

pub struct GlobalVars {
    pub configfile: ConfigFile,
    pub conv_vars: Mutex<ConversionVars>,
    pub configs_ready: Mutex<bool>,
    pub default_dac_hw: HwInfoDAC,
    pub default_dut_hw: HwInfoDUT,
    pub default_dri_hw: HwInfoDRI,
    pub broker_client: RwLock<Option<Arc<rumqttc::AsyncClient>>>,
    pub redis_client: Mutex<Option<redis::aio::ConnectionManager>>,
    pub certs_vld: HashMap<String, String>,
    pub to_broker: mpsc::Sender<MsgToBroker>,
    pub need_update_configs: AtomicBool,
    pub stats: statistics::StatisticsCounters,
}

pub struct ConversionVars {
    pub devs: HashMap<String, DevHwConfig>,
}

impl GlobalVars {
    pub fn new(configfile: ConfigFile) -> (GlobalVars, mpsc::Receiver<MsgToBroker>) {
        create_globs(configfile)
    }
}

pub fn create_globs(configfile: ConfigFile) -> (GlobalVars, mpsc::Receiver<MsgToBroker>) {
    let (sender_fila, receiver_fila) = mpsc::channel::<MsgToBroker>(20000);

    let globs = GlobalVars {
        configfile,
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
        certs_vld: HashMap::new(),
        to_broker: sender_fila,
        need_update_configs: AtomicBool::new(true),
        stats: statistics::StatisticsCounters::new(),
    };

    (globs, receiver_fila)
}
