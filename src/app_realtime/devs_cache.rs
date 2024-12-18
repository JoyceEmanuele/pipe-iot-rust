use super::global_vars::DevLastMessage;
use crate::GlobalVars;
use std::{
    collections::HashMap,
    io::ErrorKind,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{io::AsyncWriteExt, sync::RwLock};

pub async fn run_service(globs: Arc<GlobalVars>) -> Result<(), String> {
    tokio::fs::create_dir_all("./cache")
        .await
        .map_err(|err| format!("[5] {err}"))?;
    let result = load_from_cache(&globs).await;
    if let Err(err) = result {
        crate::LOG.append_log_tag_msg(
            "ERROR",
            &format!("Error on lastMessages SavingService loading cache: {err}"),
        );
    }

    // TODO: Remover do cache dispositivos não cadastrados no Celsius

    loop {
        tokio::time::sleep(Duration::from_millis(3 * 60 * 1000)).await;
        let result = dump_to_file(&globs).await;
        if let Err(err) = result {
            crate::LOG.append_log_tag_msg(
                "ERROR",
                &format!("Error on lastMessages SavingService: {err}"),
            );
        }
    }
}

async fn dump_to_file(globs: &Arc<GlobalVars>) -> Result<(), String> {
    let mut out_file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open("./cache/lastMessages-tmp.json")
        .await
        .map_err(|err| format!("[37] {err}"))?;
    out_file
        .write_all(b"{")
        .await
        .map_err(|err| format!("[38] {err}"))?;
    // let mut resp_devs = json!({});

    let mut need_comma = false;
    let all_devs = globs.last_telemetry.read().await;
    for (dev_id, dev_info) in all_devs.iter() {
        if need_comma {
            out_file
                .write_all(&format!(r#","{dev_id}":"#).as_bytes())
                .await
                .map_err(|err| format!("[38] {err}"))?;
        } else {
            out_file
                .write_all(&format!(r#""{dev_id}":"#).as_bytes())
                .await
                .map_err(|err| format!("[38] {err}"))?;
            need_comma = true;
        }
        let dev_info = dev_info.read().await;
        let bytes = serde_json::to_vec(&*dev_info).map_err(|err| format!("[25] {err}"))?;
        out_file
            .write_all(&bytes)
            .await
            .map_err(|err| format!("[38] {err}"))?;
    }
    drop(all_devs);

    out_file
        .write_all(b"}")
        .await
        .map_err(|err| format!("[38] {err}"))?;
    out_file
        .flush()
        .await
        .map_err(|err| format!("[59] {err}"))?;
    drop(out_file);

    tokio::fs::rename("./cache/lastMessages-tmp.json", "./cache/lastMessages.json")
        .await
        .map_err(|err| format!("[63] {err}"))?;

    Ok(())
}

async fn load_from_cache(globs: &Arc<GlobalVars>) -> Result<(), String> {
    // TODO: se der erro no parse do JSON (por exemplo arquivo corrompido) o sistema não vai conseguir se recuperar

    let file_contents = match tokio::fs::read_to_string("./cache/lastMessages.json").await {
        Ok(x) => x,
        Err(err) => {
            if err.kind() != ErrorKind::NotFound {
                crate::LOG.append_log_tag_msg("WARN", &format!("Could not load cache: {err}"));
            }
            "{}".to_owned()
        }
    };

    let last_messages: HashMap<String, DevLastMessage> =
        serde_json::from_str(&file_contents).map_err(|err| format!("[71] {err}"))?;

    let mut all_devs = globs.last_telemetry.write().await;
    let mut last_timestamp = globs.last_timestamp.write().await;
    for (dev_id, dev_info) in last_messages.into_iter() {
        let mut ts_secs = dev_info.ts;
        let old = all_devs.insert(dev_id.to_owned(), RwLock::new(dev_info));
        if let Some(old) = old {
            // Se já tinha um valor, ele deve ser mais novo do que o cache, então mantém ele
            ts_secs = old.read().await.ts;
            all_devs.insert(dev_id.to_owned(), old);
            definir_last_ts(&mut last_timestamp, &dev_id, ts_secs);
        } else {
            definir_last_ts(&mut last_timestamp, &dev_id, ts_secs);
        }
    }
    drop(last_timestamp);
    drop(all_devs);

    Ok(())
}

fn definir_last_ts(last_timestamp: &mut HashMap<String, AtomicU64>, dev_id: &str, ts_secs: u64) {
    // Atualiza o last_timestamp
    let mut need_insert = false;
    match last_timestamp.get(dev_id) {
        Some(dev_info) => {
            dev_info.store(ts_secs, Ordering::Relaxed);
        }
        None => {
            need_insert = true;
        }
    };
    if need_insert {
        last_timestamp.insert(dev_id.to_owned(), AtomicU64::new(ts_secs));
    }
}
