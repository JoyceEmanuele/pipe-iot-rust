use super::dac_l1_calculator::DacL1Calculator;
use crate::telemetry_payloads::circ_buffer::CircularBufferF64;
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime};

#[derive(Debug)]
struct TelemetryBasicData {
    tsuc: f64,
    tliq: f64,
}

const RESAMPLING_TIME: i32 = 15;
const RESAMPLING_TIME_USIZE: usize = RESAMPLING_TIME as usize;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct TemperatureOnlySelf {
    tsuc_memory_filtered: CircularBufferF64<{ 45 * 60 / RESAMPLING_TIME_USIZE + 1 }>,
    tliq_memory_filtered: CircularBufferF64<{ 150 * 60 / RESAMPLING_TIME_USIZE + 1 }>,
    tsuc_memory: CircularBufferF64<13>,
    tliq_memory: CircularBufferF64<13>,
    last_ts: Option<NaiveDateTime>,
    start_ts: Option<NaiveDateTime>,
    last_ts_memory: Option<NaiveDateTime>,
}

impl TemperatureOnlySelf {
    pub fn new() -> Self {
        Self::default()
    }

    fn populate_memory(
        &mut self,
        telemetry_data: TelemetryBasicData,
        current_ts: NaiveDateTime,
    ) -> TelemetryBasicData {
        let tsuc = telemetry_data.tsuc;
        let tliq = telemetry_data.tliq;
        match self.last_ts_memory {
            Some(last_t) if current_ts >= last_t + Duration::seconds(RESAMPLING_TIME.into()) => {
                let samples_amt = (current_ts - last_t).num_seconds() / i64::from(RESAMPLING_TIME);
                if samples_amt > 1 {
                    self.fill_gaps(current_ts, &telemetry_data);
                }
                let _ = self.tsuc_memory.insert_point(Some(tsuc));
                let _ = self.tliq_memory.insert_point(Some(tliq));

                let tsuc = self.tsuc_memory.moving_avg(12, 0);
                let tliq = self.tliq_memory.moving_avg(12, 0);

                let _ = self.tsuc_memory_filtered.insert_point(tsuc);
                let _ = self.tliq_memory_filtered.insert_point(tliq);
                self.last_ts_memory = Some(current_ts);
            }
            Some(_) => {}
            None => {
                let _ = self.tsuc_memory.insert_point(Some(tsuc));
                let _ = self.tliq_memory.insert_point(Some(tliq));

                let tsuc = self.tsuc_memory.moving_avg(12, 0);
                let tliq = self.tliq_memory.moving_avg(12, 0);

                let _ = self.tsuc_memory_filtered.insert_point(tsuc);
                let _ = self.tliq_memory_filtered.insert_point(tliq);
                self.last_ts_memory = Some(current_ts);
            }
        }
        let tsuc = self.tsuc_memory.moving_avg(12, 0).unwrap();
        let tliq = self.tliq_memory.moving_avg(12, 0).unwrap();
        TelemetryBasicData { tsuc, tliq }
    }

    fn fill_gaps(&mut self, final_ts: NaiveDateTime, telemetry_data: &TelemetryBasicData) {
        let amt_secs = match self.last_ts_memory {
            Some(t) => final_ts - t - Duration::seconds(1),
            None => Duration::zero(),
        };

        let base_raw_tsuc = self.tsuc_memory.get(0);
        let base_raw_tliq = self.tliq_memory.get(0);
        let base_filtered_tsuc = self.tsuc_memory_filtered.get(0);
        let base_filtered_tliq = self.tliq_memory_filtered.get(0);

        // unwrap falhar seria o equivalente ao tempo passado ser o mesmo do reset do unix time.
        let amt_seconds: i32 = amt_secs.num_seconds().try_into().unwrap();
        let secs_f64: f64 = f64::from(amt_seconds);
        let samples_amt = amt_seconds / RESAMPLING_TIME;
        let downsampled_secs = secs_f64 / f64::from(RESAMPLING_TIME);

        // Funções de regressão linear. Adicionam 1 a elapsed por causa do comportamento de fill_with.
        let raw_tsuc_regression = |elapsed: usize| {
            u32::try_from(elapsed)
                .ok()
                .map(f64::from)
                .zip(base_raw_tsuc)
                .map(|(x, base_raw_tsuc)| {
                    base_raw_tsuc
                        + (x + 1.) * (telemetry_data.tsuc - base_raw_tsuc) / downsampled_secs
                })
        };
        let raw_tliq_regression = |elapsed: usize| {
            u32::try_from(elapsed)
                .ok()
                .map(f64::from)
                .zip(base_raw_tliq)
                .map(|(x, base_raw_tliq)| {
                    base_raw_tliq
                        + (x + 1.) * (telemetry_data.tliq - base_raw_tliq) / downsampled_secs
                })
        };
        let tsuc_regression = |elapsed: usize| {
            u32::try_from(elapsed)
                .ok()
                .map(f64::from)
                .zip(base_filtered_tsuc)
                .map(|(x, base_raw_tsuc)| {
                    base_raw_tsuc
                        + (x + 1.) * (telemetry_data.tsuc - base_raw_tsuc) / downsampled_secs
                })
        };
        let tliq_regression = |elapsed: usize| {
            u32::try_from(elapsed)
                .ok()
                .map(f64::from)
                .zip(base_filtered_tliq)
                .map(|(x, base_raw_tliq)| {
                    base_raw_tliq
                        + (x + 1.) * (telemetry_data.tliq - base_raw_tliq) / downsampled_secs
                })
        };

        // .unwrap() só falha em arquiteturas 16-bit, que não é o caso
        self.tsuc_memory
            .fill_with(raw_tsuc_regression, samples_amt.try_into().unwrap());
        self.tliq_memory
            .fill_with(raw_tliq_regression, samples_amt.try_into().unwrap());
        self.tsuc_memory_filtered
            .fill_with(tsuc_regression, samples_amt.try_into().unwrap());
        self.tliq_memory_filtered
            .fill_with(tliq_regression, samples_amt.try_into().unwrap());
    }

    fn reset_memory(&mut self) {
        self.tliq_memory.clear();
        self.tliq_memory_filtered.clear();
        self.tsuc_memory.clear();
        self.tsuc_memory_filtered.clear();
        self.last_ts = None;
        self.start_ts = None;
    }
}

impl DacL1Calculator for TemperatureOnlySelf {
    fn calc_l1(
        &mut self,
        building_tel: &crate::telemetry_payloads::telemetry_formats::TelemetryDAC_v3,
        full_tel: &crate::telemetry_payloads::telemetry_formats::TelemetryDACv2,
        _cfg: &crate::telemetry_payloads::dac_telemetry::HwInfoDAC,
    ) -> Result<Option<bool>, String> {
        let Some(tamb) = building_tel.Tamb.map(|x| ((10.0 * x).round()) / 10.0) else {
            return Err("No Tamb".into());
        };
        let Some(tsuc) = building_tel.Tsuc.map(|x| ((10.0 * x).round()) / 10.0) else {
            return Err("No Tsuc".into());
        };
        let Some(tliq) = building_tel.Tliq.map(|x| ((10.0 * x).round()) / 10.0) else {
            return Err("No Tliq".into());
        };

        let ts = full_tel.timestamp;
        if let Some(last_ts) = self.last_ts {
            if last_ts >= ts {
                return Err("last_ts >= ts".into());
            }

            // resetar análise
            if ts - last_ts > Duration::minutes(5) {
                self.reset_memory();
            }
        }

        let temps = TelemetryBasicData { tsuc, tliq };

        let TelemetryBasicData { tsuc, tliq } = self.populate_memory(temps, ts);

        let mut conditions = [None; 18];

        conditions[0] = self
            .tsuc_memory_filtered
            .delta(60 / RESAMPLING_TIME_USIZE)
            .map(|delta| delta > -0.7 && tamb - tsuc < 2.5 && tliq - tamb < 3.0);

        conditions[1] = self
            .tsuc_memory_filtered
            .delta(60 / RESAMPLING_TIME_USIZE)
            .map(|delta| delta > 0.8)
            .or(Some(false))
            .map(|delta_res| delta_res && tliq - tamb < 2.5);

        let comparison_deltas = [
            4 * 60 / RESAMPLING_TIME_USIZE,
            6 * 60 / RESAMPLING_TIME_USIZE,
            8 * 60 / RESAMPLING_TIME_USIZE,
            10 * 60 / RESAMPLING_TIME_USIZE,
            12 * 60 / RESAMPLING_TIME_USIZE,
            15 * 60 / RESAMPLING_TIME_USIZE,
        ];

        conditions[2] = {
            let tliq_deltas = comparison_deltas
                .iter()
                .map(|x| self.tliq_memory_filtered.delta(*x))
                .any(|p| p.map(|p| p < -2.0).unwrap_or(false));

            let tsuc_deltas_gt_2 = comparison_deltas
                .iter()
                .map(|x| self.tsuc_memory_filtered.delta(*x))
                .any(|p| p.map(|p| p > 2.0).unwrap_or(false));

            let delta_tsuc = self
                .tsuc_memory_filtered
                .delta(120 / RESAMPLING_TIME_USIZE)
                .map(|delta| delta >= 0.0)
                .unwrap_or(false);

            let tsuc_deltas_gt_7 = comparison_deltas
                .iter()
                .map(|x| self.tsuc_memory_filtered.delta(*x))
                .any(|p| p.map(|p| p > 7.0).unwrap_or(false));

            let diff = tliq - tamb >= 2.5;

            Some(tsuc_deltas_gt_2 && delta_tsuc && (tliq_deltas || tsuc_deltas_gt_7) && diff)
        };

        conditions[3] = { Some(tliq - tamb < 3.0 && tsuc > 28.0) };

        conditions[4] = {
            let tsuc_deltas = comparison_deltas
                .iter()
                .map(|x| self.tsuc_memory_filtered.delta(*x))
                .any(|p| p.map(|p| p > 5.0).unwrap_or(false));

            let tsuc_delta = self
                .tsuc_memory_filtered
                .delta(120 / RESAMPLING_TIME_USIZE)
                .map(|x| tsuc_deltas && x >= 0.0)
                .unwrap_or(false);

            let diff = tliq - tamb < 2.5;

            Some(tsuc_delta && tsuc_deltas && diff)
        };

        conditions[5] = {
            match (self
                .tliq_memory_filtered
                .delta(20 * 60 / RESAMPLING_TIME_USIZE))
            {
                Some(delta) => Some(tliq - tamb < 3.0 && delta < 5.5 && tamb - tsuc < 2.5),
                _ => None,
            }
        };

        let comparison_deltas = [
            30 / RESAMPLING_TIME_USIZE,
            45 / RESAMPLING_TIME_USIZE,
            60 / RESAMPLING_TIME_USIZE,
            75 / RESAMPLING_TIME_USIZE,
            120 / RESAMPLING_TIME_USIZE,
            5 * 60 / RESAMPLING_TIME_USIZE,
            10 * 60 / RESAMPLING_TIME_USIZE,
        ];

        conditions[6] = {
            let tsuc_deltas = comparison_deltas
                .iter()
                .map(|x| self.tsuc_memory_filtered.delta(*x))
                .any(|p| p.map(|p| p > 5.0).unwrap_or(false));

            let tsuc_delta_1m = self.tsuc_memory_filtered.delta(60 / RESAMPLING_TIME_USIZE);
            let tsuc_delta_5m = self.tsuc_memory_filtered.delta(300 / RESAMPLING_TIME_USIZE);
            match (tsuc_delta_1m, tsuc_delta_5m) {
                (Some(t1m), Some(t5m)) => Some(tsuc_deltas && t1m > -5.0 && t5m > -5.0),
                _ => None,
            }
        };

        conditions[7] = {
            let dtsuc_gt_035 = self
                .tsuc_memory_filtered
                .delta(60 / RESAMPLING_TIME_USIZE)
                .map(|x| x > -0.35);
            match (dtsuc_gt_035) {
                Some(dts) => Some(dts && tamb - tsuc < 4.0 && tliq - tamb < 2.3),
                _ => None,
            }
        };

        conditions[8] = {
            let comparison_deltas = [
                4 * 60 / RESAMPLING_TIME_USIZE,
                270 / RESAMPLING_TIME_USIZE,
                5 * 60 / RESAMPLING_TIME_USIZE,
                8 * 60 / RESAMPLING_TIME_USIZE,
                10 * 60 / RESAMPLING_TIME_USIZE,
                12 * 60 / RESAMPLING_TIME_USIZE,
                15 * 60 / RESAMPLING_TIME_USIZE,
                20 * 60 / RESAMPLING_TIME_USIZE,
                25 * 60 / RESAMPLING_TIME_USIZE,
                30 * 60 / RESAMPLING_TIME_USIZE,
                35 * 60 / RESAMPLING_TIME_USIZE,
                45 * 60 / RESAMPLING_TIME_USIZE,
            ];
            let tsuc_deltas_gt6 = comparison_deltas
                .iter()
                .map(|x| self.tsuc_memory_filtered.delta(*x))
                .any(|p| p.map(|p| p > 6.0).unwrap_or(false));

            let comparison_deltas = [
                60 / RESAMPLING_TIME_USIZE,
                5 * 60 / RESAMPLING_TIME_USIZE,
                10 * 60 / RESAMPLING_TIME_USIZE,
            ];
            let tsuc_deltas_gt_m5 = comparison_deltas
                .iter()
                .map(|x| self.tsuc_memory_filtered.delta(*x))
                .any(|p| p.map(|p| p > 6.0).unwrap_or(false));

            Some(tsuc_deltas_gt6 && tsuc_deltas_gt_m5)
        };

        conditions[9] = {
            let dtsuc_gt_035 = self
                .tsuc_memory_filtered
                .delta(60 / RESAMPLING_TIME_USIZE)
                .map(|x| x > -0.35);
            match dtsuc_gt_035 {
                Some(dts) => Some(dts && tamb - tsuc < 4.0 && tliq - tamb < 2.3),
                _ => None,
            }
        };

        conditions[10] = { Some(tliq - tamb < 2.5 && tamb - tsuc < 2.5 && tliq - tsuc < 3.0) };
        conditions[11] = {
            let dts_60s = self
                .tsuc_memory_filtered
                .delta(60 / RESAMPLING_TIME_USIZE)
                .map(|x| x > 3.0);
            let dts_90s = self
                .tsuc_memory_filtered
                .delta(90 / RESAMPLING_TIME_USIZE)
                .map(|x| x > 3.0);
            match (dts_60s, dts_90s) {
                (Some(d60s), Some(d90s)) => Some((d60s || d90s) && tliq - tamb < 3.0),
                _ => None,
            }
        };
        conditions[12] = {
            let comparison_deltas = [
                60 / RESAMPLING_TIME_USIZE,
                120 / RESAMPLING_TIME_USIZE,
                4 * 60 / RESAMPLING_TIME_USIZE,
                270 / RESAMPLING_TIME_USIZE,
                5 * 60 / RESAMPLING_TIME_USIZE,
                6 * 60 / RESAMPLING_TIME_USIZE,
                8 * 60 / RESAMPLING_TIME_USIZE,
                10 * 60 / RESAMPLING_TIME_USIZE,
                12 * 60 / RESAMPLING_TIME_USIZE,
                15 * 60 / RESAMPLING_TIME_USIZE,
                20 * 60 / RESAMPLING_TIME_USIZE,
                25 * 60 / RESAMPLING_TIME_USIZE,
                30 * 60 / RESAMPLING_TIME_USIZE,
                35 * 60 / RESAMPLING_TIME_USIZE,
                45 * 60 / RESAMPLING_TIME_USIZE,
            ];

            let tsuc_gt_45 = comparison_deltas
                .iter()
                .map(|x| self.tsuc_memory_filtered.delta(*x))
                .any(|p| p.map(|p| p > 4.5).unwrap_or(false));

            let comparison_deltas = [
                60 / RESAMPLING_TIME_USIZE,
                5 * 60 / RESAMPLING_TIME_USIZE,
                10 * 60 / RESAMPLING_TIME_USIZE,
            ];

            let tsuc_gt_m5 = comparison_deltas
                .iter()
                .map(|x| self.tsuc_memory_filtered.delta(*x))
                .any(|p| p.map(|p| p > -5.0).unwrap_or(false));

            Some(tliq - tamb < 4.0 && tsuc_gt_45 && tsuc_gt_m5)
        };
        conditions[13] = { Some(tliq - tsuc < 3.0 && tsuc > 15.0) };
        conditions[14] = { Some(tliq - tamb < 11.0 && tamb - tsuc < 1.0 && tsuc > 35.0) };
        conditions[15] = {
            let comparison_deltas = [
                60 / RESAMPLING_TIME_USIZE,
                90 / RESAMPLING_TIME_USIZE,
                120 / RESAMPLING_TIME_USIZE,
                150 / RESAMPLING_TIME_USIZE,
                180 / RESAMPLING_TIME_USIZE,
            ];
            Some(
                comparison_deltas
                    .iter()
                    .map(|x| self.tsuc_memory_filtered.delta(*x))
                    .any(|p| p.map(|p| p > 2.0).unwrap_or(false)),
            )
        };
        conditions[16] = {
            let comparison_deltas = [
                4 * 60 / RESAMPLING_TIME_USIZE,
                270 / RESAMPLING_TIME_USIZE,
                5 * 60 / RESAMPLING_TIME_USIZE,
                8 * 60 / RESAMPLING_TIME_USIZE,
                10 * 60 / RESAMPLING_TIME_USIZE,
                12 * 60 / RESAMPLING_TIME_USIZE,
                15 * 60 / RESAMPLING_TIME_USIZE,
                20 * 60 / RESAMPLING_TIME_USIZE,
                25 * 60 / RESAMPLING_TIME_USIZE,
                30 * 60 / RESAMPLING_TIME_USIZE,
                35 * 60 / RESAMPLING_TIME_USIZE,
                45 * 60 / RESAMPLING_TIME_USIZE,
                60 * 60 / RESAMPLING_TIME_USIZE,
                75 * 60 / RESAMPLING_TIME_USIZE,
                90 * 60 / RESAMPLING_TIME_USIZE,
                120 * 60 / RESAMPLING_TIME_USIZE,
                150 * 60 / RESAMPLING_TIME_USIZE,
            ];
            Some(
                comparison_deltas
                    .iter()
                    .map(|x| self.tliq_memory_filtered.delta(*x))
                    .any(|p| p.map(|p| p < -5.0).unwrap_or(false)),
            )
        };
        conditions[17] = { Some(tliq - tsuc < 5.7 && tsuc > tamb) };

        let should_be_off = conditions.into_iter().any(|cond| cond.unwrap_or(false));

        let l1 = !should_be_off;

        match self.start_ts {
            Some(t) if ts - t < Duration::minutes(5) => Ok(None),
            Some(_) => Ok(Some(l1)),
            None => {
                self.start_ts = Some(ts);
                Ok(None)
            }
        }
    }
}
