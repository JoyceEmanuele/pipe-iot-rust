use crate::GlobalVars;
use serde_json::json;
use std::sync::atomic::Ordering;
use std::sync::{atomic::AtomicUsize, Arc};

pub struct StatisticsCounters {
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
    pub fn new() -> Self {
        let stats = StatisticsCounters {
            mqtt_recv: AtomicUsize::new(0),
            http_reqs: AtomicUsize::new(0),
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
        };
        stats
    }
}

pub struct MessageSizeCounters {
    pub data_bytes: AtomicUsize,
    pub data_count: AtomicUsize,
    pub data_max: AtomicUsize,
    pub ctrl_bytes: AtomicUsize,
    pub ctrl_count: AtomicUsize,
    pub ctrl_max: AtomicUsize,
}
impl MessageSizeCounters {
    pub fn new() -> Self {
        MessageSizeCounters {
            data_bytes: AtomicUsize::new(0),
            data_count: AtomicUsize::new(0),
            data_max: AtomicUsize::new(0),
            ctrl_bytes: AtomicUsize::new(0),
            ctrl_count: AtomicUsize::new(0),
            ctrl_max: AtomicUsize::new(0),
        }
    }

    pub fn on_data(&self, msg_size: usize) {
        self.data_count.fetch_add(1, Ordering::Relaxed);
        self.data_bytes.fetch_add(msg_size, Ordering::Relaxed);
        self.data_max.fetch_max(msg_size, Ordering::Relaxed);
    }

    pub fn on_control(&self, msg_size: usize) {
        self.ctrl_count.fetch_add(1, Ordering::Relaxed);
        self.ctrl_bytes.fetch_add(msg_size, Ordering::Relaxed);
        self.ctrl_max.fetch_max(msg_size, Ordering::Relaxed);
    }
}

pub async fn task_stats(globs: Arc<GlobalVars>) {
    const INTERVAL: u64 = 60; // em segundos
    let mut ts_start = std::time::Instant::now();

    loop {
        let elapsed = ts_start.elapsed().as_secs();

        let message = serde_json::json!({
            "origin": "iotrelay-v1",
            "interval": elapsed,
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

        // ts_start.add_assign(Duration::from_secs(INTERVAL));
        // if ts_start.elapsed().as_secs() >= INTERVAL {
        // 	ts_start = std::time::Instant::now();
        // }
        ts_start = std::time::Instant::now();

        let payload = message.to_string();
        crate::LOG.append_log_tag_msg("INFO", &format!("Thread stats: {}", payload));
        crate::LOG.append_statistics(&payload);

        tokio::time::sleep(std::time::Duration::from_secs(INTERVAL)).await;
    }
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
