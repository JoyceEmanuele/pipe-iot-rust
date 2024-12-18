use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::str::FromStr;

pub fn load_env_vars() {
    // Por enquanto vai continuar aceitando o arquivo "configfile.json5"
    let legacy_json_configfile = "./configfile.json5";
    let legacy_config_present = std::path::Path::new(legacy_json_configfile).exists();

    // Carrega o arquivo ".env" ou "--config=/path/to/custom.env"
    let default_env_file = ".env";
    let default_dotenv_present = std::path::Path::new(&default_env_file).exists();

    let mut custom_dotenv = false;
    for arg in std::env::args().skip(1) {
        if arg.starts_with("--config=") {
            custom_dotenv = true;
            let env_file = &arg["--config=".len()..];
            if !env_file.is_empty() {
                load_detenv(env_file);
            }
        }
        if arg == "--config-example" {
            custom_dotenv = true;
            load_example_detenv();
        }
    }

    if !custom_dotenv {
        if default_dotenv_present {
            load_detenv(default_env_file);
        } else if legacy_config_present {
            println!("Carregando '{legacy_json_configfile}' antigo");
            load_legacy_json(legacy_json_configfile);
        } else {
            println!("Nenhum arquivo de configuração encontrado, usando configuração de exemplo");
            load_example_detenv();
        }
    }
}

fn load_detenv(env_file: &str) {
    let result = dotenvy::from_filename(&env_file);
    if let Err(err) = result {
        crate::LOG.append_log_tag_msg("ERROR", &format!("Erro ao carregar '{env_file}': {err}"));
    }
}

fn load_example_detenv() {
    let example_config = include_str!("../../.env.example");
    let result = dotenvy::from_read(example_config.as_bytes());
    if let Err(err) = result {
        crate::LOG.append_log_tag_msg(
            "ERROR",
            &format!("Erro ao carregar config de exemplo: {err}"),
        );
    }
}

pub fn check_configfile() {
    if let Err(err) = crate::ConfigFile::from_env() {
        println!("ERRO nas configs [{}]: {err}", crate::LOG.app_name);
    } else {
        println!("OK configs [{}]", crate::LOG.app_name);
    }
}

fn load_legacy_json(path: &str) {
    // let default_path = "./configfile.json5";
    match std::fs::read_to_string(&path) {
        Err(err) => {
            crate::LOG.append_log_tag_msg("ERROR", &format!("Error reading {path}: {err:?}"));
        }
        Ok(file_contents) => {
            load_configfile_json_vars(&file_contents, path);
        }
    }
}

fn load_configfile_json_vars(file_contents: &str, file_location: &str) {
    let config_json = match json5::from_str(file_contents) {
        Ok(serde_json::Value::Object(x)) => x,
        Err(err) => {
            crate::LOG
                .append_log_tag_msg("ERROR", &format!("Error reading {file_location}: {err:?}"));
            return;
        }
        _ => {
            crate::LOG.append_log_tag_msg(
                "ERROR",
                &format!("Error reading {file_location}: invalid JSON"),
            );
            return;
        }
    };

    for (name, val) in config_json.iter() {
        let val = match val {
            serde_json::Value::Null => Ok("".to_owned()),
            serde_json::Value::String(x) => Ok(x.to_owned()),
            serde_json::Value::Array(x) => serde_json::to_string(x),
            serde_json::Value::Object(x) => serde_json::to_string(x),
            serde_json::Value::Bool(x) => serde_json::to_string(x),
            serde_json::Value::Number(x) => serde_json::to_string(x),
        };
        if let Ok(val) = val {
            std::env::set_var(name, val);
        }
    }
}

pub fn get_var_string_optional(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|x| !x.is_empty())
}
pub fn get_var_string_required(name: &str) -> Result<String, String> {
    let val = get_var_string_optional(name);
    match val {
        Some(val) => {
            return Ok(val);
        }
        None => {
            return Err(format!("Faltou informar a configuração '{name}'"));
        }
    };
}
pub fn get_var_u16_optional(name: &str) -> Result<Option<u16>, String> {
    let val = get_var_string_optional(name);
    let val = match val {
        Some(val) => val,
        None => {
            return Ok(None);
        }
    };
    let val = u16::from_str(&val)
        .map_err(|err| format!("A configuração '{name}' informada é inválida: {err}"))?;
    Ok(Some(val))
}
pub fn get_var_u16_required(name: &str) -> Result<u16, String> {
    let val = get_var_string_optional(name);
    let val = match val {
        Some(val) => val,
        None => {
            return Err(format!("Faltou informar a configuração '{name}'"));
        }
    };
    u16::from_str(&val)
        .map_err(|err| format!("A configuração '{name}' informada é inválida: {err}"))
}
pub fn get_var_bool_optional(name: &str) -> Result<Option<bool>, String> {
    let val = get_var_string_optional(name);
    match val.as_deref() {
        None => Ok(None),
        Some("1") => Ok(Some(true)),
        Some("0") => Ok(Some(false)),
        Some("true") => Ok(Some(true)),
        Some("false") => Ok(Some(false)),
        Some("TRUE") => Ok(Some(true)),
        Some("FALSE") => Ok(Some(false)),
        x => Err(format!(
            "A configuração '{name}' informada é inválida: {x:?}"
        )),
    }
}
pub fn get_var_structure_optional<T: DeserializeOwned>(name: &str) -> Result<Option<T>, String> {
    let Some(var_str) = get_var_string_optional(name) else {
        return Ok(None);
    };
    match serde_json::from_str::<T>(&var_str) {
        Ok(obj) => {
            return Ok(Some(obj));
        }
        Err(err) => {
            return Err(format!(
                "A configuração '{name}' informada é inválida: '{var_str}' {err}"
            ));
        }
    };
}
pub fn get_var_structure_required<T: DeserializeOwned>(name: &str) -> Result<T, String> {
    let val = get_var_structure_optional(name)?;
    match val {
        Some(val) => {
            return Ok(val);
        }
        None => {
            return Err(format!("Faltou informar a configuração '{name}'"));
        }
    };
}
