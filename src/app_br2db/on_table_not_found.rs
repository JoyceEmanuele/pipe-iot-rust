use crate::GlobalVars;
use std::sync::Arc;

pub fn on_table_not_found_bigquery(
    globs: &Arc<GlobalVars>,
    proposed_table_name: &str,
    // error: &NestedResponseError,
) {
    let proposed_table_name = proposed_table_name.to_owned();
    let globs = globs.to_owned();
    tokio::spawn(async move {
        {
            let last_create: &mut Option<std::time::Instant> =
                &mut *globs.last_table_create_command_bq.lock().await;
            if let Some(last_create) = &last_create {
                if last_create.elapsed() < std::time::Duration::from_secs(1) {
                    // Se fizer menos de 1 segundo que enviamos um comando de criar tabela, não continua.
                    return;
                }
            }
            *last_create = Some(std::time::Instant::now());
        }

        let resp = globs
            .client_bigquery
            .as_ref()
            .expect("BigQuery client is null")
            .create_new_table(&proposed_table_name)
            .await;

        println!("{:?}", resp);
    });
}

pub fn on_table_not_found_dynamodb(globs: &Arc<GlobalVars>, proposed_table_name: &str) {
    let proposed_table_name = proposed_table_name.to_owned();
    let globs = globs.to_owned();
    tokio::spawn(async move {
        {
            let last_create = &mut *globs.last_table_create_command_aws.lock().await;
            if let Some(last_create) = &last_create {
                if last_create.elapsed() < std::time::Duration::from_secs(5 * 60) {
                    // Se fizer menos de 5 minutos que enviamos um comando de criar tabela, não continua.
                    return;
                }
            }
            *last_create = Some(std::time::Instant::now());
        }

        let resp = globs
            .client_dynamo
            .as_ref()
            .expect("DynamoDB client is null")
            .create_new_table(proposed_table_name)
            .await;
        println!("{:?}", resp);
    });
}
