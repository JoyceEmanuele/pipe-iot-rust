use std::f64;

use chrono::{Duration, NaiveDateTime};

use super::l1_calc::DutL1Calculator;
use crate::telemetry_payloads::circ_buffer::CircularBufferF64;
use crate::telemetry_payloads::dut_telemetry::HwInfoDUT;
use crate::telemetry_payloads::telemetry_formats::TelemetryDUTv2;

const RESAMPLING_TIME: i32 = 12;

const RESAMPLING_TIME_USIZE: usize = RESAMPLING_TIME as usize;

#[derive(Debug)]
struct TelemetryBasicData {
    tins: f64,
    tret: f64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct TempDiffL1 {
    min_tins_off: f64,
    min_tdiff_off: f64,
    lim_dtins_off: f64,
    raw_tins_memory: CircularBufferF64<7>,
    raw_tret_memory: CircularBufferF64<7>,
    mean_tins_memory: CircularBufferF64<{ (600 * 60 / 5) / RESAMPLING_TIME_USIZE + 1 }>,
    last_valid_timestamp: Option<NaiveDateTime>,
    last_ts_memory: Option<NaiveDateTime>,
}

/// Calcula uma condição para vários itens de uma lista (em geral, deltas de amostras) e retorna o OU de todas elas.
#[inline(always)]
fn reduce_with_or<const SIZE: usize, TYPE>(
    origins: &[TYPE; SIZE],
    condition_fun: impl Fn(&TYPE) -> Option<bool>,
) -> Option<bool> {
    origins
        .iter()
        .map(condition_fun)
        .fold(None, |part, next| match (part, next) {
            (Some(true), _) => Some(true),
            (_, Some(true)) => Some(true),
            (Some(false), _) => Some(false),
            (_, Some(false)) => Some(false),
            _ => None,
        })
}

impl TempDiffL1 {
    pub fn new() -> Self {
        Self {
            min_tins_off: 28.0,
            min_tdiff_off: 3.0,
            lim_dtins_off: 2.0,
            raw_tins_memory: CircularBufferF64::new(),
            raw_tret_memory: CircularBufferF64::new(),
            mean_tins_memory: CircularBufferF64::new(),
            last_valid_timestamp: None,
            last_ts_memory: None,
        }
    }

    fn insert_raw_point(&mut self, tins: f64, tret: f64) {
        self.raw_tins_memory.insert_point(Some(tins));
        self.raw_tret_memory.insert_point(Some(tret));
    }

    fn insert_point(&mut self, tins: f64, tret: f64, current_ts: NaiveDateTime) {
        self.raw_tins_memory.insert_point(Some(tins));
        self.raw_tret_memory.insert_point(Some(tret));

        let tins_mean = {
            let sum_count = self.raw_tins_memory.iter().fold(None, |sum_count, next| {
                if let Some(v) = next {
                    if let Some((sum, count)) = sum_count {
                        Some((sum + v, count + 1))
                    } else {
                        Some((v, 1))
                    }
                } else {
                    sum_count
                }
            });
            sum_count.map(|(sum, count)| sum / f64::from(count))
        };

        self.mean_tins_memory.insert_point(tins_mean);
        self.last_ts_memory = Some(current_ts);
    }

    fn populate_memory(
        &mut self,
        telemetry_data: TelemetryBasicData,
        current_ts: NaiveDateTime,
    ) -> TelemetryBasicData {
        let tins = telemetry_data.tins;
        let tret = telemetry_data.tret;
        match self.last_ts_memory {
            // I should write *at least* one new point into the undersampled memory
            Some(last_t)
                if current_ts >= last_t + Duration::seconds((5 * RESAMPLING_TIME).into()) =>
            {
                // count of samples taking into consideration the regular DUT 5s/sample
                let regular_samples = (current_ts - last_t).num_seconds() / 5;
                // undersampled
                let samples_amt = regular_samples / i64::from(RESAMPLING_TIME);
                //if we need to interpolate data
                if samples_amt > 1 {
                    // number of samples never should exceed i32 limits
                    self.fill_gaps((regular_samples - 1).try_into().unwrap(), &telemetry_data);
                }
                self.insert_point(tins, tret, current_ts);
            }
            // just fill the raw memories to get the average
            Some(last_t) => {
                let regular_samples = (current_ts - last_t).num_seconds() / 5;
                for _ in 0..regular_samples {
                    self.insert_raw_point(tins, tret);
                }
            }
            // first point
            None => {
                self.insert_point(tins, tret, current_ts);
            }
        }
        // Unwrap doesn't fail because parameters add to less than the structs' max length.
        let tins = self.mean_tins_memory[0].unwrap_or(telemetry_data.tins);
        let tret = self.raw_tret_memory.moving_avg(6, 0).unwrap();
        TelemetryBasicData { tins, tret }
    }

    /// Interpolates missing data, taking into account the undersampling strategy to reduce RAM footprint.
    fn fill_gaps(&mut self, samples_to_insert: i32, telemetry_data: &TelemetryBasicData) {
        let undersampled_amt = samples_to_insert / RESAMPLING_TIME;
        let samples_amt_f64 = f64::from(samples_to_insert);
        let extra_samples = samples_to_insert % RESAMPLING_TIME;

        // Inserts RESAMPLING_TIME points in the raw memories, then adds a single interpolated "undersampled" point taking that into account.
        // Repeats this until we insert the full amount of undersampled points planned.
        // Extraneous samples after that will be treated after the loop.
        // TODO: (Optimization) If RESAMPLING_TIME > SIZE of the raw CircularBufferF64 used, just insert the last SIZE points instead of the full RESAMPLING_TIME points.
        for undersampled_elapsed in 0..undersampled_amt {
            let (tins, tret) = if samples_to_insert <= 60 {
                (self.raw_tins_memory.get(0), self.raw_tret_memory.get(0))
            } else {
                (None, None)
            };
            let full_sample_elapsed = f64::from(undersampled_elapsed * RESAMPLING_TIME);
            // Regressions use the loop index `undersampled_elapsed` as `full_sample_elapsed` to slide the curve on the X axis
            // allowing us to index it from 0 to `RESAMPLING_TIME - 1` (inclusive) every time
            // but get the same result as indexing a single interpolation curve from 0 to `samples_to_insert - 1`
            self.populate_single_undersampled_point(
                tins,
                tret,
                |x, base_raw_tins| {
                    base_raw_tins
                        + (full_sample_elapsed + x + 1.) * (telemetry_data.tins - base_raw_tins)
                            / samples_amt_f64
                },
                |x, base_raw_tret| {
                    base_raw_tret
                        + (full_sample_elapsed + x + 1.) * (telemetry_data.tret - base_raw_tret)
                            / samples_amt_f64
                },
            );
            let tins_avg = self
                .raw_tins_memory
                .moving_avg(6, 0)
                .filter(|avg| !avg.is_nan())
                .or(tins); // if somehow our average is NaN, just use the regular Tins.
            self.mean_tins_memory.insert_point(tins_avg);
        }

        // Regular points after all our undersampled points;
        // same strategy applies.
        let (tins, tret) = if samples_to_insert <= 60 {
            (self.raw_tins_memory.get(0), self.raw_tret_memory.get(0))
        } else {
            (None, None)
        };

        let full_sample_elapsed = f64::from(undersampled_amt * RESAMPLING_TIME);
        self.populate_single_undersampled_point(
            tins,
            tret,
            |x, base_raw_tins| {
                base_raw_tins
                    + (full_sample_elapsed + x + 1.) * (telemetry_data.tins - base_raw_tins)
                        / samples_amt_f64
            },
            |x, base_raw_tret| {
                base_raw_tret
                    + (full_sample_elapsed + x + 1.) * (telemetry_data.tret - base_raw_tret)
                        / samples_amt_f64
            },
        );
    }

    fn populate_single_undersampled_point(
        &mut self,
        final_tins: Option<f64>,
        tret: Option<f64>,
        interpolation_tins_fn: impl Fn(f64, f64) -> f64,
        interpolation_tret_fn: impl Fn(f64, f64) -> f64,
    ) {
        // Regressions use the loop index `undersampled_elapsed` as `full_sample_elapsed` to slide the curve on the X axis
        // allowing us to index it from 0 to `RESAMPLING_TIME - 1` (inclusive) every time
        // but get the same result as indexing a single interpolation curve from 0 to `samples_to_insert - 1`
        let raw_tins_regression = |elapsed: usize| {
            u32::try_from(elapsed)
                .ok()
                .map(f64::from)
                .zip(final_tins)
                .map(|(x, y)| interpolation_tins_fn(x, y))
        };

        let raw_tret_regression = |elapsed: usize| {
            u32::try_from(elapsed)
                .ok()
                .map(f64::from)
                .zip(tret)
                .map(|(x, y)| interpolation_tret_fn(x, y))
        };
        self.raw_tins_memory
            .fill_with(raw_tins_regression, RESAMPLING_TIME_USIZE);
        self.raw_tret_memory
            .fill_with(raw_tret_regression, RESAMPLING_TIME_USIZE);
    }
}

impl DutL1Calculator for TempDiffL1 {
    fn calc_l1(
        &mut self,
        payload: &TelemetryDUTv2,
        _cfg: &HwInfoDUT,
    ) -> Result<Option<bool>, String> {
        let ts = payload.timestamp;

        if self
            .last_valid_timestamp
            .is_some_and(|last_ts| ts <= last_ts)
        {
            return Err("tempo andou para trás".into());
        }

        let tins = payload.temp_1.ok_or_else(|| "No tins".to_string())?;
        let tret = payload.temp.ok_or_else(|| "No tret".to_string())?;

        let temps = TelemetryBasicData { tins, tret };
        let TelemetryBasicData { tins, tret } = self.populate_memory(temps, ts);

        let mut conditions = [None; 7];

        conditions[0] = Some((tret - tins) < self.min_tdiff_off);

        conditions[1] = {
            let comparison_deltas = [
                4 * 60 / (5 * RESAMPLING_TIME_USIZE),
                6 * 60 / (5 * RESAMPLING_TIME_USIZE),
                8 * 60 / (5 * RESAMPLING_TIME_USIZE),
                10 * 60 / (5 * RESAMPLING_TIME_USIZE),
                12 * 60 / (5 * RESAMPLING_TIME_USIZE),
                15 * 60 / (5 * RESAMPLING_TIME_USIZE),
                18 * 60 / (5 * RESAMPLING_TIME_USIZE),
            ];
            let deltas = reduce_with_or(&comparison_deltas, |delta| {
                self.mean_tins_memory
                    .delta(*delta)
                    .map(|x| x > self.lim_dtins_off)
            });

            let d_tins = self
                .mean_tins_memory
                .delta(8 * 60 / (5 * RESAMPLING_TIME_USIZE))
                .map(|x| x > 0.0);
            let tins_cmp = tins > 17.0;

            deltas
                .zip(d_tins)
                .map(|(deltas, d_tins)| deltas && d_tins && tins_cmp)
        };

        conditions[2] = Some(tins > self.min_tins_off);

        conditions[3] = {
            let comparison_deltas = [
                20 * 60 / (5 * RESAMPLING_TIME_USIZE),
                25 * 60 / (5 * RESAMPLING_TIME_USIZE),
                30 * 60 / (5 * RESAMPLING_TIME_USIZE),
                35 * 60 / (5 * RESAMPLING_TIME_USIZE),
                38 * 60 / (5 * RESAMPLING_TIME_USIZE),
                40 * 60 / (5 * RESAMPLING_TIME_USIZE),
                45 * 60 / (5 * RESAMPLING_TIME_USIZE),
            ];
            let deltas = reduce_with_or(&comparison_deltas, |delta| {
                self.mean_tins_memory
                    .delta(*delta)
                    .map(|x| x > self.lim_dtins_off)
            });

            let tins_cmp = tins > 20.0;

            deltas.map(|deltas| deltas && tins_cmp)
        };

        conditions[4] = {
            let cond1 = tret - tins < 5.0 && tins > 20.0;
            let cond2 = self
                .mean_tins_memory
                .delta(2 * 60 / (5 * RESAMPLING_TIME_USIZE))
                .map(|x| x > 4.0);
            cond2.map(|x| x && cond1)
        };

        conditions[5] = {
            let comparison_deltas = [
                1 * 60 / (5 * RESAMPLING_TIME_USIZE),
                2 * 60 / (5 * RESAMPLING_TIME_USIZE),
                4 * 60 / (5 * RESAMPLING_TIME_USIZE),
                5 * 60 / (5 * RESAMPLING_TIME_USIZE),
                6 * 60 / (5 * RESAMPLING_TIME_USIZE),
                8 * 60 / (5 * RESAMPLING_TIME_USIZE),
                10 * 60 / (5 * RESAMPLING_TIME_USIZE),
                12 * 60 / (5 * RESAMPLING_TIME_USIZE),
                15 * 60 / (5 * RESAMPLING_TIME_USIZE),
                18 * 60 / (5 * RESAMPLING_TIME_USIZE),
                20 * 60 / (5 * RESAMPLING_TIME_USIZE),
                25 * 60 / (5 * RESAMPLING_TIME_USIZE),
                30 * 60 / (5 * RESAMPLING_TIME_USIZE),
                35 * 60 / (5 * RESAMPLING_TIME_USIZE),
                40 * 60 / (5 * RESAMPLING_TIME_USIZE),
                45 * 60 / (5 * RESAMPLING_TIME_USIZE),
                60 * 60 / (5 * RESAMPLING_TIME_USIZE),
                120 * 60 / (5 * RESAMPLING_TIME_USIZE),
                150 * 60 / (5 * RESAMPLING_TIME_USIZE),
                180 * 60 / (5 * RESAMPLING_TIME_USIZE),
                210 * 60 / (5 * RESAMPLING_TIME_USIZE),
                240 * 60 / (5 * RESAMPLING_TIME_USIZE),
                270 * 60 / (5 * RESAMPLING_TIME_USIZE),
                300 * 60 / (5 * RESAMPLING_TIME_USIZE),
                330 * 60 / (5 * RESAMPLING_TIME_USIZE),
                360 * 60 / (5 * RESAMPLING_TIME_USIZE),
                390 * 60 / (5 * RESAMPLING_TIME_USIZE),
                420 * 60 / (5 * RESAMPLING_TIME_USIZE),
                450 * 60 / (5 * RESAMPLING_TIME_USIZE),
                480 * 60 / (5 * RESAMPLING_TIME_USIZE),
                510 * 60 / (5 * RESAMPLING_TIME_USIZE),
                540 * 60 / (5 * RESAMPLING_TIME_USIZE),
                570 * 60 / (5 * RESAMPLING_TIME_USIZE),
                600 * 60 / (5 * RESAMPLING_TIME_USIZE),
            ];

            let deltas_gt_4 = reduce_with_or(&comparison_deltas, |delta| {
                self.mean_tins_memory.delta(*delta).map(|x| x > 4.0)
            });

            let deltas_gt_m_half = reduce_with_or(
                &[
                    4 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    8 * 60 / (5 * RESAMPLING_TIME_USIZE),
                ],
                |delta| self.mean_tins_memory.delta(*delta).map(|x| x > -0.5),
            );

            deltas_gt_4
                .zip(deltas_gt_m_half)
                .map(|(d1, d2)| d1 && d2 && tins > 5.0)
        };

        conditions[6] = {
            reduce_with_or(
                &[
                    1 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    2 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    4 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    5 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    6 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    8 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    10 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    12 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    15 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    18 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    20 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    25 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    30 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    35 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    40 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    45 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    60 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    120 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    180 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    240 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    300 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    360 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    420 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    480 * 60 / (5 * RESAMPLING_TIME_USIZE),
                    600 * 60 / (5 * RESAMPLING_TIME_USIZE),
                ],
                |delta| self.mean_tins_memory.delta(*delta).map(|x| x > 10.0),
            )
        };
        let should_be_off = reduce_with_or(&conditions, |x| *x);
        let l1 = should_be_off.map(|x| !x);

        Ok(l1)
    }
}
