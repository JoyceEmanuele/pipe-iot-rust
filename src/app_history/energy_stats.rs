use super::energy_hist::EnergyHistParams;
use crate::{telemetry_payloads::energy::padronized::PadronizedEnergyTelemetry, GlobalVars};
use chrono::{Duration, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::{convert::TryFrom, sync::Arc};

#[derive(Debug, Serialize, Deserialize)]
pub struct EnergyStatParams {
    pub energy_device_id: String,
    serial: String,
    manufacturer: String,
    model: String,
    start_time: NaiveDateTime,
    end_time: NaiveDateTime,
    stats_interval_hours: i32,
}

impl EnergyStatParams {
    pub async fn process(mut self, globs: &Arc<GlobalVars>) -> Result<EnergyStats, String> {
        let hist_params = EnergyHistParams {
            energy_device_id: self.energy_device_id,
            serial: self.serial,
            manufacturer: self.manufacturer,
            model: self.model,
            start_time: self.start_time,
            end_time: self.end_time,
            formulas: None,
            params: None,
            calculate_demand_hour_graphic: None,
        };

        let hist = hist_params.process_query(globs).await?;

        Ok(hist.calculate_stats(self.start_time, self.end_time, self.stats_interval_hours))
    }
}

#[derive(Debug, Serialize, Default, Deserialize)]
pub struct EnergyStat {
    max: f64,
    avg: f64,
    min: f64,
}

#[derive(Debug, Serialize, Default, Deserialize)]
pub struct EnergyStatVars {
    pub v_a: Option<EnergyStat>,
    pub v_b: Option<EnergyStat>,
    pub v_c: Option<EnergyStat>,
    pub i_a: Option<EnergyStat>,
    pub i_b: Option<EnergyStat>,
    pub i_c: Option<EnergyStat>,
    pub demanda: Option<EnergyStat>,
    pub en_at_tri: Option<EnergyStat>,
    pub en_re_tri: Option<EnergyStat>,
    pub fp: Option<EnergyStat>,
    pub start_timestamp: Option<NaiveDateTime>,
    pub end_timestamp: Option<NaiveDateTime>,
    pub demanda_med_at: Option<EnergyStat>,
}

impl EnergyStatVars {
    pub fn from_tel_slice(tels: &[PadronizedEnergyTelemetry]) -> Result<Self, String> {
        Ok(Self {
            v_a: {
                let v_iter = tels.iter().filter_map(|t| t.v_a).filter(|f| f.is_finite());
                let size: f64 = match i32::try_from(v_iter.clone().count()) {
                    Ok(t) => t,
                    Err(e) => {
                        crate::LOG.append_log_tag_msg("ERROR", &format!("Slice too large!: {}", e));
                        return Err("Slice too large!".into());
                    }
                }
                .into();

                if size > 0.0 {
                    Some(EnergyStat {
                        max: v_iter
                            .clone()
                            .max_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                        avg: v_iter.clone().sum::<f64>() / size,
                        min: v_iter
                            .clone()
                            .min_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                    })
                } else {
                    None
                }
            },
            v_b: {
                let v_iter = tels.iter().filter_map(|t| t.v_b).filter(|f| f.is_finite());
                let size: f64 = match i32::try_from(v_iter.clone().count()) {
                    Ok(t) => t,
                    Err(e) => {
                        crate::LOG.append_log_tag_msg("ERROR", &format!("Slice too large!: {}", e));
                        return Err("Slice too large!".into());
                    }
                }
                .into();

                if size > 0.0 {
                    Some(EnergyStat {
                        max: v_iter
                            .clone()
                            .max_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                        avg: v_iter.clone().sum::<f64>() / size,
                        min: v_iter
                            .clone()
                            .min_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                    })
                } else {
                    None
                }
            },
            v_c: {
                let v_iter = tels.iter().filter_map(|t| t.v_c).filter(|f| f.is_finite());
                let size: f64 = match i32::try_from(v_iter.clone().count()) {
                    Ok(t) => t,
                    Err(e) => {
                        crate::LOG.append_log_tag_msg("ERROR", &format!("Slice too large!: {}", e));
                        return Err("Slice too large!".into());
                    }
                }
                .into();

                if size > 0.0 {
                    Some(EnergyStat {
                        max: v_iter
                            .clone()
                            .max_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                        avg: v_iter.clone().sum::<f64>() / size,
                        min: v_iter
                            .clone()
                            .min_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                    })
                } else {
                    None
                }
            },
            i_a: {
                let i_iter = tels.iter().filter_map(|t| t.i_a).filter(|f| f.is_finite());
                let size: f64 = match i32::try_from(i_iter.clone().count()) {
                    Ok(t) => t,
                    Err(e) => {
                        crate::LOG.append_log_tag_msg("ERROR", &format!("Slice too large!: {}", e));
                        return Err("Slice too large!".into());
                    }
                }
                .into();

                if size > 0.0 {
                    Some(EnergyStat {
                        max: i_iter
                            .clone()
                            .max_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                        avg: i_iter.clone().sum::<f64>() / size,
                        min: i_iter
                            .clone()
                            .min_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                    })
                } else {
                    None
                }
            },
            i_b: {
                let i_iter = tels.iter().filter_map(|t| t.i_b).filter(|f| f.is_finite());
                let size: f64 = match i32::try_from(i_iter.clone().count()) {
                    Ok(t) => t,
                    Err(e) => {
                        crate::LOG.append_log_tag_msg("ERROR", &format!("Slice too large!: {}", e));
                        return Err("Slice too large!".into());
                    }
                }
                .into();

                if size > 0.0 {
                    Some(EnergyStat {
                        max: i_iter
                            .clone()
                            .max_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                        avg: i_iter.clone().sum::<f64>() / size,
                        min: i_iter
                            .clone()
                            .min_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                    })
                } else {
                    None
                }
            },
            i_c: {
                let i_iter = tels.iter().filter_map(|t| t.i_c).filter(|f| f.is_finite());
                let size: f64 = match i32::try_from(i_iter.clone().count()) {
                    Ok(t) => t,
                    Err(e) => {
                        crate::LOG.append_log_tag_msg("ERROR", &format!("Slice too large!: {}", e));
                        return Err("Slice too large!".into());
                    }
                }
                .into();

                if size > 0.0 {
                    Some(EnergyStat {
                        max: i_iter
                            .clone()
                            .max_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                        avg: i_iter.clone().sum::<f64>() / size,
                        min: i_iter
                            .clone()
                            .min_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                    })
                } else {
                    None
                }
            },
            demanda: {
                let demanda_iter = tels
                    .iter()
                    .filter_map(|t| t.demanda_at)
                    .filter(|f| f.is_finite());
                let size: f64 = match i32::try_from(demanda_iter.clone().count()) {
                    Ok(t) => t,
                    Err(e) => {
                        crate::LOG.append_log_tag_msg("ERROR", &format!("Slice too large!: {}", e));
                        return Err("Slice too large!".into());
                    }
                }
                .into();

                if size > 0.0 {
                    Some(EnergyStat {
                        max: demanda_iter
                            .clone()
                            .max_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                        avg: demanda_iter.clone().sum::<f64>() / size,
                        min: demanda_iter
                            .clone()
                            .min_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                    })
                } else {
                    None
                }
            },
            demanda_med_at: {
                let demanda_med_iter = tels
                    .iter()
                    .filter_map(|t| t.demanda_med_at)
                    .filter(|f| f.is_finite());
                let size: f64 = match i32::try_from(demanda_med_iter.clone().count()) {
                    Ok(t) => t,
                    Err(e) => {
                        crate::LOG.append_log_tag_msg("ERROR", &format!("Slice too large!: {}", e));
                        return Err("Slice too large!".into());
                    }
                }
                .into();

                if size > 0.0 {
                    Some(EnergyStat {
                        max: demanda_med_iter
                            .clone()
                            .max_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                        avg: demanda_med_iter.clone().sum::<f64>() / size,
                        min: demanda_med_iter
                            .clone()
                            .min_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                    })
                } else {
                    None
                }
            },
            en_at_tri: {
                let en_iter = tels
                    .iter()
                    .filter_map(|t| t.en_at_tri)
                    .filter(|f| f.is_finite());
                let size: f64 = match i32::try_from(en_iter.clone().count()) {
                    Ok(t) => t,
                    Err(e) => {
                        crate::LOG.append_log_tag_msg("ERROR", &format!("Slice too large!: {}", e));
                        return Err("Slice too large!".into());
                    }
                }
                .into();

                if size > 0.0 {
                    Some(EnergyStat {
                        max: en_iter
                            .clone()
                            .max_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                        avg: en_iter.clone().sum::<f64>() / size,
                        min: en_iter
                            .clone()
                            .min_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                    })
                } else {
                    None
                }
            },
            en_re_tri: {
                let en_iter = tels
                    .iter()
                    .filter_map(|t| t.en_re_tri)
                    .filter(|f| f.is_finite());
                let size: f64 = match i32::try_from(en_iter.clone().count()) {
                    Ok(t) => t,
                    Err(e) => {
                        crate::LOG.append_log_tag_msg("ERROR", &format!("Slice too large!: {}", e));
                        return Err("Slice too large!".into());
                    }
                }
                .into();

                if size > 0.0 {
                    Some(EnergyStat {
                        max: en_iter
                            .clone()
                            .max_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                        avg: en_iter.clone().sum::<f64>() / size,
                        min: en_iter
                            .clone()
                            .min_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                    })
                } else {
                    None
                }
            },
            fp: {
                let fp_iter = tels.iter().filter_map(|t| t.fp).filter(|f| f.is_finite());
                let size: f64 = match i32::try_from(fp_iter.clone().count()) {
                    Ok(t) => t,
                    Err(e) => {
                        crate::LOG.append_log_tag_msg("ERROR", &format!("Slice too large!: {}", e));
                        return Err("Slice too large!".into());
                    }
                }
                .into();

                if size > 0.0 {
                    Some(EnergyStat {
                        max: fp_iter
                            .clone()
                            .max_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                        avg: fp_iter.clone().sum::<f64>() / size,
                        min: fp_iter
                            .clone()
                            .min_by(|x, y| x.partial_cmp(y).unwrap())
                            .unwrap(),
                    })
                } else {
                    None
                }
            },
            start_timestamp: tels.first().map(|t| t.timestamp),
            end_timestamp: tels.last().map(|t| t.timestamp),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EnergyStats {
    pub serial: String,
    pub manufacturer: String,
    pub data: Vec<EnergyStatVars>,
}

impl EnergyStats {
    pub fn new(serial: String, manufacturer: String, data: Vec<EnergyStatVars>) -> Self {
        Self {
            serial,
            manufacturer,
            data,
        }
    }
}
