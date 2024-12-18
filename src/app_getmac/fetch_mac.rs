use super::lib_fs::append_dev_processado;
use crate::GlobalVars;
use rusoto_core::RusotoError;
use rusoto_dynamodb::{AttributeValue, DynamoDb, QueryError, QueryInput};
use serde_dynamo::from_items;
use std::collections::HashMap;
use std::sync::Arc;

pub async fn processar(globs: &Arc<GlobalVars>, body: &str) -> Result<String, String> {
    let body = body.replace(&[';', ',', ' ', '\t', '"'], "\n");
    let devs = body.split("\n");

    let mut resultado_csv = String::with_capacity(body.len() * 3);

    for dev_id in devs {
        if dev_id.is_empty() {
            continue;
        }
        if dev_id.len() < 8 {
            continue;
        }
        {
            let devs_macs: &mut HashMap<String, String> = &mut *globs.devs_macs.lock().await;
            if let Some(mac) = devs_macs.get(dev_id) {
                println!("Já processou {}: {}", dev_id, mac);
                resultado_csv.push_str(dev_id);
                resultado_csv.push('\t');
                resultado_csv.push_str(&mac);
                resultado_csv.push('\n');
                continue;
            };
        }
        let resultado = verificar_dev(&dev_id, &globs).await;
        let devs_macs: &mut HashMap<String, String> = &mut *globs.devs_macs.lock().await;
        println!("{}: {:?}", dev_id, resultado);
        match resultado {
            Ok(mac) => {
                devs_macs.insert(dev_id.to_owned(), mac.to_owned());
                append_dev_processado(&dev_id, &mac[..], "")?;
                resultado_csv.push_str(dev_id);
                resultado_csv.push('\t');
                resultado_csv.push_str(&mac);
                resultado_csv.push('\n');
            }
            Err(err) => {
                append_dev_processado(&dev_id, "", &err[..])?;
                resultado_csv.push_str("ERRO");
                resultado_csv.push('\t');
                resultado_csv.push_str(dev_id);
                resultado_csv.push_str(": ");
                resultado_csv.push_str(&err);
                resultado_csv.push('\n');
            }
        };
    }

    Ok(resultado_csv)
}

pub async fn verificar_dev(dev_id: &str, globs: &Arc<GlobalVars>) -> Result<String, String> {
    if dev_id.len() < 8 {
        return Err(format!("dev_id inválido: {}", dev_id));
    }
    let table_name = format!("{}XXXX_RAW", &dev_id[..8]);
    let client = &globs.client_dynamo.client;

    let query_input = QueryInput {
        table_name: table_name.to_owned(),
        consistent_read: Some(false),
        projection_expression: None, // Some(String::from("#ts,L1,#State,#Mode"))
        key_condition_expression: Some(format!("dev_id = :dev_id and #ts < :ts_end")),
        // key_condition_expression: Some(format!("{key} = :{key} and begins_with(#ts, :day)", key = key_var_name)),
        expression_attribute_names: {
            let mut map = HashMap::new();
            map.insert("#ts".to_owned(), "timestamp".to_owned());
            Some(map)
        },
        expression_attribute_values: {
            let mut map = HashMap::new();
            map.insert(
                format!(":dev_id"),
                AttributeValue {
                    s: Some(dev_id.to_owned()),
                    ..AttributeValue::default()
                },
            );
            map.insert(
                format!(":ts_end"),
                AttributeValue {
                    s: Some("9".to_owned()),
                    ..AttributeValue::default()
                },
            );
            Some(map)
        },
        scan_index_forward: Some(false),
        limit: Some(3),
        ..QueryInput::default()
    };

    let result_page = match client.query(query_input.clone()).await {
        Ok(result_page) => result_page,
        Err(err) => {
            match &err {
                RusotoError::Service(QueryError::ProvisionedThroughputExceeded(err_msg)) => {
                    tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
                    return Err(format!("ProvisionedThroughputExceeded: {}", &err_msg));
                }
                RusotoError::Service(QueryError::ResourceNotFound(err_msg)) => {
                    // Table not found
                    return Err(format!("ResourceNotFound: {}", &err_msg));
                }
                _ => return Err(format!("[113] {err}")),
            };
        }
    };

    let items = result_page
        .items
        .ok_or_else(|| "Query returned no items".to_owned())?;
    let items: Vec<serde_json::Value> = from_items(items).map_err(|err| format!("[121] {err}"))?;
    if items.is_empty() {
        return Err(format!("Nenhuma telemetria encontrada"));
    }
    let last_tel = &items[0];

    match last_tel["MAC"].as_str() {
        Some(mac) => Ok(mac.to_owned()),
        None => Err(format!("Telemetria sem MAC: {}", last_tel.to_string())),
    }
}
