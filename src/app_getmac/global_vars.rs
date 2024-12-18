use crate::lib_dynamodb::client::DynamoDBClientDiel;
use crate::ConfigFile;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct GlobalVars {
    pub configfile: ConfigFile,
    pub client_dynamo: DynamoDBClientDiel,
    pub devs_macs: Arc<Mutex<HashMap<String, String>>>,
}

impl GlobalVars {
    pub fn new(configfile: ConfigFile) -> GlobalVars {
        create_globs(configfile)
    }
}

pub fn create_globs(configfile: ConfigFile) -> GlobalVars {
    let devs_macs = crate::lib_fs::load_devs_macs().expect("concluídos inválido");
    let devs_macs = Arc::new(Mutex::new(devs_macs));

    let client_dynamo = DynamoDBClientDiel::new(&configfile.aws_config, &|_, _| {});

    let globs = GlobalVars {
        configfile,
        client_dynamo,
        devs_macs,
    };

    globs
}
