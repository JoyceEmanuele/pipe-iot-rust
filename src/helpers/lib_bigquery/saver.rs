use super::client::RowBQ;
use crate::GlobalVars;
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};
use tokio::{sync::mpsc, time::Instant};

pub enum SaveToBqEvent {
    PayloadToSave(String, RowBQ),
    PayloadListToSave(String, Vec<RowBQ>),
    TimeTick,
}

pub async fn task_save_to_bigquery(
    globs: Arc<GlobalVars>,
    mut receiver: mpsc::Receiver<SaveToBqEvent>,
    max_time_interval: u128, // 2800 ms
) {
    let mut table_queues: HashMap<String, (Instant, Vec<RowBQ>)> = HashMap::new();

    loop {
        let event = receiver.recv().await.expect("Erro ao receber do mpsc");
        match event {
            SaveToBqEvent::PayloadToSave(table_name, row) => {
                let mut queue = table_queues.get_mut(&table_name);
                if queue.is_none() {
                    // on_new_table_queue(&table_name, &globs);
                    table_queues.insert(
                        table_name.clone(),
                        (Instant::now(), Vec::with_capacity(1100)),
                    );
                    queue = table_queues.get_mut(&table_name);
                }
                if let Some(queue) = queue {
                    if queue.1.is_empty() {
                        queue.0 = Instant::now();
                    }
                    queue.1.push(row);
                }
            }
            SaveToBqEvent::PayloadListToSave(table_name, mut rows) => {
                let mut queue = table_queues.get_mut(&table_name);
                if queue.is_none() {
                    // on_new_table_queue(&table_name, &globs);
                    table_queues.insert(
                        table_name.clone(),
                        (Instant::now(), Vec::with_capacity(1100)),
                    );
                    queue = table_queues.get_mut(&table_name);
                }
                if let Some(queue) = queue {
                    if queue.1.is_empty() {
                        queue.0 = Instant::now();
                    }

                    queue.1.append(&mut rows);
                }
            }
            SaveToBqEvent::TimeTick => {
                // Nothing to do here
            }
        }

        for (table_name, table_queue) in table_queues.iter_mut() {
            if table_queue.1.is_empty() {
                continue;
            }
            let need_send = (table_queue.1.len() >= 3000)
                || (table_queue.0.elapsed().as_millis() > max_time_interval);
            if !need_send {
                continue;
            }
            let rows: Vec<RowBQ> = table_queue.1.drain(..).collect();
            let globs = globs.clone();
            let table_name = table_name.clone();
            tokio::spawn(async move {
                let mut client_bigquery = globs
                    .client_bigquery
                    .as_ref()
                    .expect("BigQuery client is null")
                    .clone();
                let resp = client_bigquery
                    .insert_telemetry_list_by_storage(&rows, &table_name, &globs)
                    .await;
                if let Err(err) = resp {
                    crate::LOG.append_log_tag_msg("ERRO", &format!("[56] {:?}", err));
                }
            });
        }
    }
}

pub async fn task_force_save_to_bigquery(sender: mpsc::Sender<SaveToBqEvent>) {
    // Task to make sure stats are sent even if there are no important events
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(3000)).await;
        sender.send(SaveToBqEvent::TimeTick).await.unwrap();
    }
}

pub fn create_channel() -> (mpsc::Sender<SaveToBqEvent>, mpsc::Receiver<SaveToBqEvent>) {
    mpsc::channel::<SaveToBqEvent>(20000)
}

pub async fn push_row_to_storage(
    sender: &mpsc::Sender<SaveToBqEvent>,
    table_name: String,
    row: RowBQ,
) -> Result<(), String> {
    sender
        .send(SaveToBqEvent::PayloadToSave(table_name, row))
        .await
        .map_err(|err| format!("{:?}", err))
}

pub async fn push_rows_to_storage(
    sender: &mpsc::Sender<SaveToBqEvent>,
    table_name: String,
    rows: Vec<RowBQ>,
) -> Result<(), String> {
    sender
        .send(SaveToBqEvent::PayloadListToSave(table_name, rows))
        .await
        .map_err(|err| format!("{:?}", err))
}
