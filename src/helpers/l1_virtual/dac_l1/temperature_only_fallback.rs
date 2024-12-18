use super::dac_l1_calculator::DacL1Calculator;
use crate::telemetry_payloads::circ_buffer::CircularBuffer;
use chrono::{Duration, NaiveDateTime};
use serde::{Deserialize, Serialize};

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TsucDependentL1 {
    tsuc_memory: CircularBuffer<901, f64>,
    last_ts: Option<NaiveDateTime>,
}

impl TsucDependentL1 {
    pub fn new() -> Self {
        Self::default()
    }

    fn push_nones(&mut self, final_ts: NaiveDateTime) {
        let amt_secs = match self.last_ts {
            Some(t) => final_ts - t - Duration::seconds(1),
            None => Duration::zero(),
        };

        let tsuc_to_fill = if amt_secs <= Duration::seconds(5) {
            self.tsuc_memory.get(0)
        } else {
            None
        };

        for _ in 0..amt_secs.num_seconds() {
            self.tsuc_memory.insert_point(tsuc_to_fill);
        }
        self.last_ts = Some(final_ts);
    }
}

impl DacL1Calculator for TsucDependentL1 {
    fn calc_l1(
        &mut self,
        building_tel: &crate::telemetry_payloads::telemetry_formats::TelemetryDAC_v3,
        full_tel: &crate::telemetry_payloads::telemetry_formats::TelemetryDACv2,
        _cfg: &crate::telemetry_payloads::dac_telemetry::HwInfoDAC,
    ) -> Result<Option<bool>, String> {
        let tamb = building_tel.Tamb;
        let tsuc = building_tel.Tsuc;
        let tliq = building_tel.Tliq;

        let ts = full_tel.timestamp;
        if let Some(last_ts) = self.last_ts {
            if last_ts >= ts {
                return Err("last_ts < ts".into());
            }
        }

        self.push_nones(ts);

        let _ = self.tsuc_memory.insert_point(tsuc);

        let mut conditions = [None, None, None, None];

        conditions[0] = self
            .tsuc_memory
            .delta(60)
            .zip(tamb)
            .zip(tsuc)
            .map(|((delta, tamb), tsuc)| delta > -0.35 && tamb - tsuc < 4.0);

        conditions[1] = self.tsuc_memory.delta(60).map(|delta| delta > 0.8);

        let comparison_deltas = [4 * 60, 6 * 60, 8 * 60, 10 * 60, 12 * 60, 15 * 60];

        conditions[2] = {
            let tsuc_deltas = comparison_deltas
                .iter()
                .map(|x| self.tsuc_memory.delta(*x))
                .any(|p| p.map(|p| p < -2.0).unwrap_or(false));

            self.tsuc_memory.delta(120).map(|x| tsuc_deltas && x >= 0.0)
        };

        conditions[3] = {
            let tsuc_delta = self.tsuc_memory.delta(60).map(|x| x > -0.35);

            tliq.zip(tsuc)
                .zip(tsuc_delta)
                .map(|((tliq, tsuc), delta)| delta && tliq - tsuc < 2.0 && tsuc > 20.0)
        };

        let should_be_off = conditions
            .into_iter()
            .fold(None, |prev, cond| match (prev, cond) {
                (Some(true), _) => Some(true),
                (_, Some(true)) => Some(true),
                (Some(false), _) => Some(false),
                (_, Some(false)) => Some(false),
                _ => None,
            });

        Ok(should_be_off.map(|x| !x))
    }
}
