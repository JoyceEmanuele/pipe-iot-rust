use crate::GlobalVars;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

pub struct StatisticsCounters {
    pub unknown_topic: AtomicUsize,
    pub dev_id_missing: AtomicUsize,
    pub dynamodb_error: AtomicUsize,
    pub saved_telemetry: AtomicUsize,
    pub saved_control: AtomicUsize,
    pub saved_command: AtomicUsize,
    pub bigquery_insertions: AtomicUsize,
    pub time_nosave_s: AtomicUsize,
    pub payloads_received: AtomicUsize,
    pub payloads_discarded: AtomicUsize,
    pub payloads_with_insert_error: AtomicUsize,
    pub bq_rows_inserted: AtomicUsize,
}

impl StatisticsCounters {
    pub fn new() -> StatisticsCounters {
        StatisticsCounters {
            unknown_topic: AtomicUsize::new(0),
            dev_id_missing: AtomicUsize::new(0),
            dynamodb_error: AtomicUsize::new(0),
            saved_telemetry: AtomicUsize::new(0),
            saved_control: AtomicUsize::new(0),
            saved_command: AtomicUsize::new(0),
            bigquery_insertions: AtomicUsize::new(0),
            time_nosave_s: AtomicUsize::new(0),
            payloads_received: AtomicUsize::new(0),
            payloads_discarded: AtomicUsize::new(0),
            payloads_with_insert_error: AtomicUsize::new(0),
            bq_rows_inserted: AtomicUsize::new(0),
        }
    }
}

pub async fn run_service(globs: Arc<GlobalVars>) {
    // "127.0.0.1:8080"
    const INTERVAL: u64 = 120; // em segundos
    let mut ts_start = std::time::Instant::now();

    loop {
        let elapsed = ts_start.elapsed().as_secs();
        montar_e_enviar_estatisticas(elapsed, &globs);
        ts_start = std::time::Instant::now();
        tokio::time::sleep(std::time::Duration::from_secs(INTERVAL)).await;
    }
}

fn montar_e_enviar_estatisticas(elapsed: u64, globs: &Arc<GlobalVars>) {
    let stats = &globs.stats;
    let saved_telemetry = get_reset_atomic_usize(&stats.saved_telemetry);
    let bigquery_insertions = get_reset_atomic_usize(&stats.bigquery_insertions);

    {
        let mut time_nosave_s = stats.time_nosave_s.load(Ordering::Relaxed);
        if saved_telemetry == 0 && bigquery_insertions == 0 {
            if time_nosave_s < 100_000 {
                time_nosave_s += elapsed as usize;
            }
            stats.time_nosave_s.store(time_nosave_s, Ordering::Relaxed);
            println!(
                "DBG59_SAV0 - nenhuma telemetrias salva nos últimos {}s",
                time_nosave_s
            );
        } else if time_nosave_s != 0 {
            stats.time_nosave_s.store(0, Ordering::Relaxed);
        }
    }

    let message = serde_json::json!({
        "origin": "broker2dynamo-v2",
        "interval": elapsed,

        "unknown_topic": get_reset_atomic_usize(&stats.unknown_topic),
        "dev_id_missing": get_reset_atomic_usize(&stats.dev_id_missing),
        "dynamodb_error": get_reset_atomic_usize(&stats.dynamodb_error),
        "aws_saved_telemetry": saved_telemetry,
        "aws_saved_control": get_reset_atomic_usize(&stats.saved_control),
        "aws_saved_command": get_reset_atomic_usize(&stats.saved_command),
        "bigquery_insertions": bigquery_insertions,
        "payloads_received": get_reset_atomic_usize(&stats.payloads_received),
        "payloads_discarded": get_reset_atomic_usize(&stats.payloads_discarded),
        "payloads_with_insert_error": get_reset_atomic_usize(&stats.payloads_with_insert_error),
        "bq_rows_inserted": get_reset_atomic_usize(&stats.bq_rows_inserted),
    });

    let payload = message.to_string();
    crate::LOG.append_log_tag_msg("INFO", &format!("Thread stats: {}", payload));
    crate::LOG.append_statistics(&payload);
}

fn get_reset_atomic_usize(atomic_value: &AtomicUsize) -> usize {
    let value = atomic_value.load(Ordering::Relaxed);
    atomic_value.fetch_sub(value, Ordering::Relaxed);
    value
}
