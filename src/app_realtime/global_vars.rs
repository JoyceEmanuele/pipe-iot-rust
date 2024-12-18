use crate::ConfigFile;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
};
use tokio::sync::RwLock;

pub struct GlobalVars {
    pub configfile: ConfigFile,
    // pub broker_client: RwLock<Option<Arc<rumqttc::AsyncClient>>>,
    pub last_telemetry: RwLock<HashMap<String, RwLock<DevLastMessage>>>,
    pub last_timestamp: RwLock<HashMap<String, AtomicU64>>, // Timestamp do servidor da última vez que chegou mensagem do dispostivo
}

#[derive(Deserialize, Serialize)]
pub struct DevLastMessage {
    pub telemetry: serde_json::Value, // último JSON que chegou em tópico 'data/...'
    pub ts: u64, // Timestamp do servidor da última vez que chegou mensagem do dispostivo
                 // pub topic?: TopicType // Tópico 'data/...' que foi usado, e não o tipo do dispositivo. O DMA por exemplo usa tópico de DUT.
                 // pub tsBefore: number // Timestamp do servidor da telemetria anterior à atual
}

impl GlobalVars {
    pub async fn new(configfile: ConfigFile) -> GlobalVars {
        create_globs(configfile).await
    }
}

pub async fn create_globs(configfile: ConfigFile) -> GlobalVars {
    let globs = GlobalVars {
        configfile,
        // broker_client: RwLock::new(None),
        last_telemetry: RwLock::new(HashMap::new()),
        last_timestamp: RwLock::new(HashMap::new()),
    };

    globs
}
