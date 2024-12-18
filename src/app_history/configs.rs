use crate::diel_hist_tables::PrefixAndTable;
use crate::envvars_loader;
use crate::lib_dynamodb::client::AWSConfig;

pub struct ConfigFile {
    pub aws_config: AWSConfig,
    pub LISTEN_SOCKET_HIST: String,
    pub EXTERNAL_REQUESTS_TOKEN: Option<String>,
    pub CUSTOM_TABLE_NAMES_DAC: Vec<PrefixAndTable>,
    pub CUSTOM_TABLE_NAMES_DUT: Vec<PrefixAndTable>,
    pub CUSTOM_TABLE_NAMES_DAM: Vec<PrefixAndTable>,
    pub CUSTOM_TABLE_NAMES_DRI: Vec<PrefixAndTable>,
    pub CUSTOM_TABLE_NAMES_DMA: Vec<PrefixAndTable>,
    pub CUSTOM_TABLE_NAMES_DMT: Vec<PrefixAndTable>,
    pub CUSTOM_TABLE_NAMES_DAL: Vec<PrefixAndTable>,
}

impl ConfigFile {
    pub fn from_env() -> Result<ConfigFile, String> {
        envvars_loader::load_env_vars();

        let AWS_ACCESS_KEY_ID = envvars_loader::get_var_string_required("AWS_ACCESS_KEY_ID")?;
        let AWS_SECRET_ACCESS_KEY =
            envvars_loader::get_var_string_required("AWS_SECRET_ACCESS_KEY")?;
        let AWS_SESSION_TOKEN = envvars_loader::get_var_string_optional("AWS_SESSION_TOKEN");
        let LISTEN_SOCKET_HIST = envvars_loader::get_var_string_required("LISTEN_SOCKET_HIST")?;
        let EXTERNAL_REQUESTS_TOKEN =
            envvars_loader::get_var_string_optional("EXTERNAL_REQUESTS_TOKEN");
        let CUSTOM_TABLE_NAMES_DAC =
            envvars_loader::get_var_structure_required("CUSTOM_TABLE_NAMES_DAC")?;
        let CUSTOM_TABLE_NAMES_DUT =
            envvars_loader::get_var_structure_required("CUSTOM_TABLE_NAMES_DUT")?;
        let CUSTOM_TABLE_NAMES_DAM =
            envvars_loader::get_var_structure_required("CUSTOM_TABLE_NAMES_DAM")?;
        let CUSTOM_TABLE_NAMES_DRI =
            envvars_loader::get_var_structure_required("CUSTOM_TABLE_NAMES_DRI")?;
        let CUSTOM_TABLE_NAMES_DMA =
            envvars_loader::get_var_structure_required("CUSTOM_TABLE_NAMES_DMA")?;
        let CUSTOM_TABLE_NAMES_DMT =
            envvars_loader::get_var_structure_required("CUSTOM_TABLE_NAMES_DMT")?;
        let CUSTOM_TABLE_NAMES_DAL =
            envvars_loader::get_var_structure_required("CUSTOM_TABLE_NAMES_DAL")?;

        let aws_config = AWSConfig {
            access_key_id: AWS_ACCESS_KEY_ID,
            secret_access_key: AWS_SECRET_ACCESS_KEY,
            session_token: AWS_SESSION_TOKEN,
        };

        Ok(ConfigFile {
            aws_config,
            LISTEN_SOCKET_HIST: LISTEN_SOCKET_HIST,
            EXTERNAL_REQUESTS_TOKEN: EXTERNAL_REQUESTS_TOKEN,
            CUSTOM_TABLE_NAMES_DAC: CUSTOM_TABLE_NAMES_DAC,
            CUSTOM_TABLE_NAMES_DUT: CUSTOM_TABLE_NAMES_DUT,
            CUSTOM_TABLE_NAMES_DAM: CUSTOM_TABLE_NAMES_DAM,
            CUSTOM_TABLE_NAMES_DRI: CUSTOM_TABLE_NAMES_DRI,
            CUSTOM_TABLE_NAMES_DMA: CUSTOM_TABLE_NAMES_DMA,
            CUSTOM_TABLE_NAMES_DMT: CUSTOM_TABLE_NAMES_DMT,
            CUSTOM_TABLE_NAMES_DAL: CUSTOM_TABLE_NAMES_DAL,
        })
    }
}
