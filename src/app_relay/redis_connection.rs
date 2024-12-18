use super::state_persistence::serialize_state_obj;
use crate::GlobalVars;
use std::{sync::Arc, time::Duration};

pub async fn manter_conexao_redis(globs: Arc<GlobalVars>) {
    loop {
        // if let Some(url_redis) = &globs.configfile.URL_REDIS {
        //     ...
        // }
        let url_redis = &globs.configfile.url_redis;
        let client_is_none = { globs.redis_client.lock().await.is_none() };
        if client_is_none {
            // https://docs.rs/redis/latest/redis/
            // let urlredis = globs.configfile.URL_REDIS.to_owned();
            let client = match redis::Client::open(url_redis.to_owned()) {
                Ok(x) => x,
                Err(err) => {
                    println!("{}", err);
                    return;
                }
            };
            let con = match client.get_connection_manager().await {
                Ok(x) => x,
                Err(err) => {
                    println!("{}", err);
                    return;
                }
            };

            let mut redis_client = globs.redis_client.lock().await;
            *redis_client = Some(con);
            crate::LOG.append_log_tag_msg("INFO", "redis connected");
        }
        tokio::time::sleep(Duration::from_micros(3600)).await;
    }
}

pub async fn get_dev_state_redis(
    dev_id: &str,
    globs: &Arc<GlobalVars>,
) -> Result<Option<Vec<u8>>, String> {
    let mut db_client = match globs.redis_client.lock().await.clone() {
        None => return Err(format!("redis_client not avaliable")),
        Some(x) => x,
    };

    let mut cmd = redis::Cmd::new();
    let key = format!("{}{}", globs.configfile.redis_prefix, dev_id);
    cmd.arg("GET").arg(&key);
    let resp = db_client
        .send_packed_command(&cmd)
        .await
        .map_err(|err| err.to_string())?;

    match resp {
        redis::Value::BulkString(dev_state_bytes) => {
            return Ok(Some(dev_state_bytes));
        }
        redis::Value::Nil => {
            return Ok(None);
        }
        x => {
            return Err(format!("Invalid response from redis: {:?}", x));
        }
    };
}

pub async fn save_dev_state_redis(
    dev_id: &str,
    globs: &Arc<GlobalVars>,
    dev_state_bytes: Vec<u8>,
) -> Result<(), String> {
    let mut db_client = match globs.redis_client.lock().await.clone() {
        None => return Err(format!("redis_client not avaliable")),
        Some(x) => x,
    };

    let mut cmd = redis::Cmd::new();
    let key = format!("{}{}", globs.configfile.redis_prefix, dev_id);
    cmd.arg("SET").arg(&key).arg(dev_state_bytes);
    let _resp = db_client
        .send_packed_command(&cmd)
        .await
        .map_err(|err| err.to_string())?;

    Ok(())
}

pub async fn save_dev_state_obj_redis<T>(
    dev_id: &str,
    globs: &Arc<GlobalVars>,
    dev_state: T,
) -> Result<(), String>
where
    T: serde::Serialize,
{
    let dev_state_bytes = serialize_state_obj(dev_state)?;
    return save_dev_state_redis(dev_id, globs, dev_state_bytes).await;
}
