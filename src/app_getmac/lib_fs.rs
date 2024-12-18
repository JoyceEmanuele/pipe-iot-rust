use std::collections::{HashMap, HashSet};
use std::io::Write;

pub fn append_dev_processado(dev_id: &str, mac: &str, erro: &str) -> Result<(), String> {
    let linha = format!("{}\t{}\t{}\n", dev_id, mac, erro);
    std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open("./log_getmac_processado.txt")
        .and_then(|mut file| file.write_all(linha.as_bytes()))
        .map_err(|err| err.to_string())?;
    Ok(())
}

pub fn load_devs_macs() -> Result<HashMap<String, String>, String> {
    let file_contents = match std::fs::read_to_string("./log_getmac_processado.txt") {
        Ok(x) => x,
        Err(err) => {
            if err.kind() == std::io::ErrorKind::NotFound {
                "".to_owned()
            } else {
                return Err(err.to_string());
            }
        }
    };
    let linhas = file_contents.split('\n');

    let mut devs_macs: HashMap<String, String> = HashMap::new();

    for linha in linhas {
        if linha.is_empty() {
            continue;
        }
        let partes: Vec<&str> = linha.split('\t').collect();
        if partes.len() != 3 {
            return Err(format!("Arquivo de concluídos inválido: {}", linha));
        }
        let dev_id = partes[0];
        let mac = partes[1];
        let erro = partes[2];
        if erro.is_empty() && !mac.is_empty() {
            devs_macs.insert(dev_id.to_owned(), mac.to_owned());
        }
    }

    Ok(devs_macs)
}
