use crate::telemetry_payloads::energy::dme::EnergyDemandTelemetry;
use crate::GlobalVars;
use std::sync::Arc;
use std::{collections::HashMap, convert::TryInto};

use crate::telemetry_payloads::energy::padronized::formatPadronizedEnergyTelemetry;
use crate::telemetry_payloads::energy::{dme::TelemetryDME, padronized::PadronizedEnergyTelemetry};
use chrono::{Duration, NaiveDateTime};
use serde::{Deserialize, Serialize};

use super::energy_stats::{EnergyStatVars, EnergyStats};

#[derive(Debug, Serialize, Deserialize)]
pub struct EnergyHistParams {
    pub energy_device_id: String,
    pub serial: String,
    pub manufacturer: String,
    pub model: String,
    pub start_time: NaiveDateTime,
    pub end_time: NaiveDateTime,
    pub formulas: Option<HashMap<String, String>>,
    pub params: Option<Vec<String>>,
    pub calculate_demand_hour_graphic: Option<bool>,
}

impl EnergyHistParams {
    pub async fn process_query(mut self, globs: &Arc<GlobalVars>) -> Result<EnergyHist, String> {
        if let Some(calculate_demand_hour_graphic) = self.calculate_demand_hour_graphic {
            if self.manufacturer == "Diel Energia" {
                let demands = self
                    .process_demand_dme(calculate_demand_hour_graphic, globs)
                    .await?;
                return Ok(EnergyHist::new(
                    self.energy_device_id,
                    self.serial,
                    self.manufacturer,
                    self.model,
                    Vec::new(),
                    Some(demands),
                ));
            }
        }

        let tels = match &self.manufacturer[..] {
            "Diel Energia" => self.process_dme_query(globs).await,
            _ => Err("Unknown manufacturer!".to_string()),
        }?;

        Ok(EnergyHist::new(
            self.energy_device_id,
            self.serial,
            self.manufacturer,
            self.model,
            tels,
            None,
        ))
    }

    async fn process_dme_query(
        &self,
        globs: &Arc<GlobalVars>,
    ) -> Result<Vec<PadronizedEnergyTelemetry>, String> {
        let final_tels = self.process_common(globs).await?;

        let formattedFinalTels = final_tels
            .into_iter()
            .map(|tel| formatPadronizedEnergyTelemetry(tel, self.params.as_ref()))
            .collect::<Vec<PadronizedEnergyTelemetry>>();

        Ok(formattedFinalTels)
    }

    async fn process_common(
        &self,
        globs: &Arc<GlobalVars>,
    ) -> Result<Vec<PadronizedEnergyTelemetry>, String> {
        let dev_id_upper = self.energy_device_id.to_uppercase();
        let mut table_name = {
            if (self.energy_device_id.len() == 12) && dev_id_upper.starts_with("DRI") {
                format!("{}XXXX_RAW", &dev_id_upper[0..8])
            } else {
                String::new()
            }
        };

        for custom in &globs.configfile.CUSTOM_TABLE_NAMES_DRI {
            if dev_id_upper.starts_with(&custom.dev_prefix) {
                table_name = custom.table_name.to_owned();
                break;
            }
        }

        if table_name.is_empty() {
            return Err(format!("Unknown DRI generation: {}", self.energy_device_id));
        }

        let ts_ini = self.start_time.format("%Y-%m-%dT%H:%M:%S").to_string();
        let ts_end = self.end_time.format("%Y-%m-%dT%H:%M:%S").to_string();
        let querier = crate::lib_dynamodb::query::QuerierDevIdTimestamp::new_custom(
            table_name.to_owned(),
            "dev_id".to_owned(),
            "timestamp".to_owned(),
            self.energy_device_id.to_owned(),
            &globs.configfile.aws_config,
        );
        let mut final_tels = Vec::new();

        querier
            .run(&ts_ini, &ts_end, &mut |tels: Vec<TelemetryDME>| {
                let mut x = tels
                    .into_iter()
                    .filter_map(|mut tel| {
                        tel.formulas = self.formulas.clone();
                        tel.try_into().ok()
                    })
                    .collect::<Vec<PadronizedEnergyTelemetry>>();
                final_tels.append(&mut x);
                Ok(())
            })
            .await?;

        Ok(final_tels)
    }

    async fn process_demand_dme(
        &self,
        hour_interval: bool,
        globs: &Arc<GlobalVars>,
    ) -> Result<Vec<EnergyDemandTelemetry>, String> {
        let final_tels = self.process_common(globs).await?;

        let grouped_telemetries_demand =
            EnergyDemandTelemetry::group_telemetries_by_interval(final_tels, hour_interval);
        let mut grouped_averages =
            EnergyDemandTelemetry::calculate_group_telemetries(&grouped_telemetries_demand);
        grouped_averages.sort_by(|a, b| a.record_date.cmp(&b.record_date));

        Ok(grouped_averages)
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EnergyHist {
    energy_device_id: String,
    serial: String,
    manufacturer: String,
    model: String,
    data: Vec<PadronizedEnergyTelemetry>,
    grouped_demand: Option<Vec<EnergyDemandTelemetry>>,
}

impl EnergyHist {
    fn new(
        energy_device_id: String,
        serial: String,
        manufacturer: String,
        model: String,
        data: Vec<PadronizedEnergyTelemetry>,
        grouped_demand: Option<Vec<EnergyDemandTelemetry>>,
    ) -> Self {
        Self {
            energy_device_id,
            serial,
            manufacturer,
            model,
            data,
            grouped_demand,
        }
    }

    pub fn calculate_stats(
        self,
        start_time: NaiveDateTime,
        end_time: NaiveDateTime,
        stats_interval_hours: i32,
    ) -> EnergyStats {
        let interval = Duration::hours(stats_interval_hours.into());
        let mut start_time = start_time;
        let stats = Vec::new();
        let mut start_limit = 0;
        let mut slice_limit = self.data[start_limit..]
            .iter()
            .position(|x| x.timestamp >= start_time + interval)
            .unwrap_or_else(|| self.data[start_limit..].len());

        let mut stats = EnergyStats {
            serial: self.serial,
            manufacturer: self.manufacturer,
            data: stats,
        };

        let last_time = match self.data.last().map(|x| x.timestamp) {
            Some(t) => t,
            None => return stats, // data is empty
        };

        while start_time + interval <= last_time && start_limit <= slice_limit {
            let slice = &self.data[start_limit..slice_limit]; // slice vai até o anterior ao que tem timestamp >= time_limit
            let stat = if slice.is_empty() {
                Ok(EnergyStatVars {
                    start_timestamp: Some(start_time),
                    end_timestamp: Some(start_time + interval),
                    ..EnergyStatVars::default()
                })
            } else {
                EnergyStatVars::from_tel_slice(slice).map(|mut e| {
                    e.start_timestamp = Some(start_time);
                    e.end_timestamp = Some(start_time + interval);
                    e
                })
            };

            match stat {
                Ok(s) => stats.data.push(s),
                Err(e) => {}
            }

            start_limit = slice_limit;

            start_time += interval;
            slice_limit += self.data[start_limit..]
                .iter()
                .position(|x| x.timestamp >= start_time + interval)
                .unwrap_or_else(|| self.data[start_limit..].len());
        }

        let stat = if self.data[start_limit..].is_empty() {
            Ok(EnergyStatVars {
                start_timestamp: Some(start_time),
                end_timestamp: Some(end_time),
                ..EnergyStatVars::default()
            })
        } else {
            EnergyStatVars::from_tel_slice(&self.data[start_limit..]).map(|mut e| {
                e.start_timestamp = Some(start_time);
                e.end_timestamp = Some(end_time);
                e
            })
        };

        match stat {
            Ok(s) => stats.data.push(s),
            Err(e) => {}
        }

        stats
    }
}
