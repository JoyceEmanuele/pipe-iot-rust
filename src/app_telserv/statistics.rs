use crate::app_relay::statistics::MessageSizeCounters;
use crate::GlobalVars;
use serde_json::json;
use std::io::Write;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
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
    pub mqtt_recv: AtomicUsize,
    pub http_reqs: AtomicUsize,

    pub fwbroker_sent: AtomicUsize,
    pub fwbroker_error: AtomicUsize,
    pub topic_data: AtomicUsize,
    pub topic_ctrl: AtomicUsize,

    pub msgsz_dac: MessageSizeCounters,
    pub msgsz_dut: MessageSizeCounters,
    pub msgsz_dam: MessageSizeCounters,
    pub msgsz_dma: MessageSizeCounters,
    pub msgsz_dmt: MessageSizeCounters,
    pub msgsz_dal: MessageSizeCounters,
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
            mqtt_recv: AtomicUsize::new(0),
            http_reqs: AtomicUsize::new(0),
            payloads_received: AtomicUsize::new(0),
            payloads_discarded: AtomicUsize::new(0),
            payloads_with_insert_error: AtomicUsize::new(0),
            bq_rows_inserted: AtomicUsize::new(0),

            fwbroker_sent: AtomicUsize::new(0),
            fwbroker_error: AtomicUsize::new(0),
            topic_data: AtomicUsize::new(0),
            topic_ctrl: AtomicUsize::new(0),

            msgsz_dac: MessageSizeCounters::new(),
            msgsz_dut: MessageSizeCounters::new(),
            msgsz_dam: MessageSizeCounters::new(),
            msgsz_dma: MessageSizeCounters::new(),
            msgsz_dmt: MessageSizeCounters::new(),
            msgsz_dal: MessageSizeCounters::new(),
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

// pub async fn task_force_event(globs: Arc<GlobalVars>) {
//     // Task to make sure stats are sent even if there are no important events
//     loop {
//         tokio::time::sleep(std::time::Duration::from_secs(5)).await;
//         globs.stats.sender.send(StatsEvent::time_tick).await.expect("Erro ao gerar estatísticas MQTT");
//     }
// }

// fn processar_evento_de_estatistica(event: StatsEvent, unknown_table: &mut HashMap<String, String>) {
// 	match event {
// 		StatsEvent::unknown_table(dev_id, topic) => {
// 			if !dev_id.is_empty() {
// 				unknown_table.insert(dev_id, topic);
// 			}
// 		},
// 		StatsEvent::time_tick => {
// 			// Nothing to do
// 		},
// 	}
// }

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
        "origin": "telserv-v2",
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

        "http_reqs": get_reset_atomic_usize(&globs.stats.http_reqs),
        "mqtt_recv": get_reset_atomic_usize(&globs.stats.mqtt_recv),
        "topic_data": get_reset_atomic_usize(&globs.stats.topic_data),
        "topic_ctrl": get_reset_atomic_usize(&globs.stats.topic_ctrl),
        "fwbroker_sent": get_reset_atomic_usize(&globs.stats.fwbroker_sent),
        "fwbroker_error": get_reset_atomic_usize(&globs.stats.fwbroker_error),

        "msgsz_dac": gerar_dev_msgsz_vec(&globs.stats.msgsz_dac),
        "msgsz_dut": gerar_dev_msgsz_vec(&globs.stats.msgsz_dut),
        "msgsz_dam": gerar_dev_msgsz_vec(&globs.stats.msgsz_dam),
        "msgsz_dma": gerar_dev_msgsz_vec(&globs.stats.msgsz_dma),
        "msgsz_dmt": gerar_dev_msgsz_vec(&globs.stats.msgsz_dmt),
        "msgsz_dal": gerar_dev_msgsz_vec(&globs.stats.msgsz_dal),
    });

    let payload = message.to_string();
    crate::LOG.append_log_tag_msg("INFO", &format!("Thread stats: {}", payload));
    crate::LOG.append_statistics(&payload);

    // let globs = globs.clone();
    // tokio::spawn(async move {
    //     if let Err(err) = announce_server_port(globs, message).await {
    //         println!("Error announcing server: {}", err);
    //     }
    // });
}

fn gerar_dev_msgsz_vec(msgsz_dev: &MessageSizeCounters) -> serde_json::Value {
    let data_bytes = msgsz_dev.data_bytes.load(Ordering::Relaxed);
    let data_count = msgsz_dev.data_count.load(Ordering::Relaxed);
    let data_max = msgsz_dev.data_max.load(Ordering::Relaxed);
    let ctrl_bytes = msgsz_dev.ctrl_bytes.load(Ordering::Relaxed);
    let ctrl_count = msgsz_dev.ctrl_count.load(Ordering::Relaxed);
    let ctrl_max = msgsz_dev.ctrl_max.load(Ordering::Relaxed);

    msgsz_dev
        .data_bytes
        .fetch_sub(data_bytes, Ordering::Relaxed);
    msgsz_dev
        .data_count
        .fetch_sub(data_count, Ordering::Relaxed);
    msgsz_dev.data_max.store(0, Ordering::Relaxed);
    msgsz_dev
        .ctrl_bytes
        .fetch_sub(ctrl_bytes, Ordering::Relaxed);
    msgsz_dev
        .ctrl_count
        .fetch_sub(ctrl_count, Ordering::Relaxed);
    msgsz_dev.ctrl_max.store(0, Ordering::Relaxed);

    return json!({
        "data_bytes": data_bytes,
        "data_count": data_count,
        "data_max": data_max,
        "ctrl_bytes": ctrl_bytes,
        "ctrl_count": ctrl_count,
        "ctrl_max": ctrl_max,
    });
}

fn get_reset_atomic_usize(atomic_value: &AtomicUsize) -> usize {
    let value = atomic_value.load(Ordering::Relaxed);
    atomic_value.fetch_sub(value, Ordering::Relaxed);
    value
}
