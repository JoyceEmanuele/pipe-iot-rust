use crate::compression::compiler_DRI::{
    DRICCNCompiledPeriod, DRICCNTelemetryCompiler, DRIVAVandFancoilCompiledPeriod,
    DRIVAVandFancoilTelemetryCompiler,
};
use crate::telemetry_payloads::dri::ccn::{split_pack_ccn, DriCCNTelemetry};
use crate::telemetry_payloads::dri::chiller_carrier_hx::{
    DriChillerCarrierHXTelemetry, TelemetryDriChillerCarrierHX,
};
use crate::telemetry_payloads::dri::chiller_carrier_xa::{
    DriChillerCarrierXATelemetry, TelemetryDriChillerCarrierXA,
};
use crate::telemetry_payloads::dri::chiller_carrier_xa_hvar::{
    DriChillerCarrierXAHvarTelemetry, TelemetryDriChillerCarrierXAHvar,
};
use crate::telemetry_payloads::dri::vav_fancoil::{
    split_pack_vav_and_fancoil, DriVAVandFancoilTelemetry,
};
use crate::telemetry_payloads::dri_telemetry::{ChillerParametersChangesHist, TelemetryDri};
use crate::GlobalVars;
use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::convert::TryInto;
use std::sync::Arc;

#[derive(Debug, Serialize, Deserialize)]
pub struct DriHistParams {
    pub dev_id: String,
    pub dri_type: String,
    pub dri_interval: Option<isize>,
    pub day: NaiveDate,
    pub formulas: Option<HashMap<String, String>>,
    pub timezone_offset: Option<i64>,
    pub chiller_carrier_hour_graphic: Option<bool>,
}

impl DriHistParams {
    pub async fn process_query(self, globs: &Arc<GlobalVars>) -> Result<DriHist, String> {
        let day = self.day;
        let tels = match &self.dri_type[..] {
            "CCN" => self
                .process_ccn_query(globs)
                .await?
                .map(DriCompiledPeriod::DRICCNCompiledPeriod),
            "VAV" => self
                .process_vav_and_fancoil_query(globs)
                .await?
                .map(DriCompiledPeriod::DRIVAVandFancoilCompiledPeriod),
            "FANCOIL" => self
                .process_vav_and_fancoil_query(globs)
                .await?
                .map(DriCompiledPeriod::DRIVAVandFancoilCompiledPeriod),
            "CHILLER_CARRIER_HX" => self
                .process_chiller_carrier_hx_query(globs)
                .await?
                .map(DriCompiledPeriod::DRIChillerCarrierHXCompiledPeriod),
            "CHILLER_CARRIER_XA" => self
                .process_chiller_carrier_xa_query(globs)
                .await?
                .map(DriCompiledPeriod::DRIChillerCarrierXACompiledPeriod),
            "CHILLER_CARRIER_XA_HVAR" => self
                .process_chiller_carrier_xa_hvar_query(globs)
                .await?
                .map(DriCompiledPeriod::DRIChillerCarrierXAHvarCompiledPeriod),
            _ => return Err("Unknown DRI type!".to_string()),
        };
        Ok(DriHist::new(self.dev_id, self.dri_type, day, tels))
    }

    async fn process_ccn_query(
        &self,
        globs: &Arc<GlobalVars>,
    ) -> Result<Option<DRICCNCompiledPeriod>, String> {
        let dev_id_upper = self.dev_id.to_uppercase();
        let mut table_name = {
            if (self.dev_id.len() == 12) && dev_id_upper.starts_with("DRI") {
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
            return Err(format!("Unknown DRI generation: {}", self.dev_id));
        }

        let interval_length_s = 24 * 60 * 60;
        let (ts_ini, ts_end) = {
            let i_ts_ini = self.day.and_hms(0, 0, 0).timestamp();

            let i_ts_end = i_ts_ini + interval_length_s;
            let ts_ini = NaiveDateTime::from_timestamp(i_ts_ini, 0)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            let ts_end = NaiveDateTime::from_timestamp(i_ts_end, 0)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            (ts_ini, ts_end)
        };

        let mut tcomp = DRICCNTelemetryCompiler::new(self.dri_interval);

        let querier = crate::lib_dynamodb::query::QuerierDevIdTimestamp::new_diel_dev(
            table_name,
            self.dev_id.to_owned(),
            &globs.configfile.aws_config,
        );
        let mut final_tels = Vec::new();
        querier
            .run(&ts_ini, &ts_end, &mut |items: Vec<TelemetryDri>| {
                let mut x = items
                    .into_iter()
                    .filter_map(|tel| tel.try_into().ok())
                    .collect::<Vec<DriCCNTelemetry>>();
                final_tels.append(&mut x);
                Ok(())
            })
            .await?;

        let day = self.day.to_string();
        let ts_ini = day.as_str();

        let i_ts_ini = match NaiveDateTime::parse_from_str(
            &format!("{}T00:00:00", ts_ini),
            "%Y-%m-%dT%H:%M:%S",
        ) {
            Err(err) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("{} {}", &format!("{}T00:00:00", ts_ini), err),
                );
                return Err(err.to_string());
            }
            Ok(date) => date.timestamp(),
        };

        let i_ts_end = i_ts_ini + interval_length_s;
        let ts_end = NaiveDateTime::from_timestamp(i_ts_end + 10, 0)
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();

        for item in final_tels.iter() {
            let result = split_pack_ccn(
                item,
                i_ts_ini,
                i_ts_end,
                &mut |item: &DriCCNTelemetry, index: isize| {
                    tcomp.AdcPontos(item, index as isize);
                },
            );
            match result {
                Ok(()) => {}
                Err(err) => return Err(err),
            };
        }

        let period_data = tcomp.CheckClosePeriod(isize::try_from(interval_length_s).unwrap());
        let result = match period_data {
            Ok(v) => Some(v),
            Err(_) => None,
        };

        Ok(result.unwrap())
    }

    async fn process_vav_and_fancoil_query(
        &self,
        globs: &Arc<GlobalVars>,
    ) -> Result<Option<DRIVAVandFancoilCompiledPeriod>, String> {
        let dev_id_upper = self.dev_id.to_uppercase();
        let mut table_name = {
            if (self.dev_id.len() == 12) && dev_id_upper.starts_with("DRI") {
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
            return Err(format!("Unknown DRI generation: {}", self.dev_id));
        }

        let interval_length_s = 24 * 60 * 60;
        let (ts_ini, ts_end) = {
            let i_ts_ini = self.day.and_hms(0, 0, 0).timestamp();
            let i_ts_end = i_ts_ini + interval_length_s;
            let ts_ini = NaiveDateTime::from_timestamp(i_ts_ini, 0)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            let ts_end = NaiveDateTime::from_timestamp(i_ts_end, 0)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            (ts_ini, ts_end)
        };

        let mut tcomp = DRIVAVandFancoilTelemetryCompiler::new(self.dri_interval);

        let querier = crate::lib_dynamodb::query::QuerierDevIdTimestamp::new_diel_dev(
            table_name,
            self.dev_id.to_owned(),
            &globs.configfile.aws_config,
        );
        let mut final_tels = Vec::new();
        querier
            .run(&ts_ini, &ts_end, &mut |items: Vec<TelemetryDri>| {
                let mut x = items
                    .into_iter()
                    .filter_map(|mut tel| {
                        tel.formulas = self.formulas.clone();
                        tel.try_into().ok()
                    })
                    .collect::<Vec<DriVAVandFancoilTelemetry>>();
                final_tels.append(&mut x);
                Ok(())
            })
            .await?;

        let day = self.day.to_string();
        let ts_ini = day.as_str();
        let i_ts_ini = match NaiveDateTime::parse_from_str(
            &format!("{}T00:00:00", ts_ini),
            "%Y-%m-%dT%H:%M:%S",
        ) {
            Err(err) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("{} {}", &format!("{}T00:00:00", ts_ini), err),
                );
                return Err(err.to_string());
            }
            Ok(date) => date.timestamp(),
        };
        let i_ts_end = i_ts_ini + interval_length_s;
        let ts_end = NaiveDateTime::from_timestamp(i_ts_end + 10, 0)
            .format("%Y-%m-%dT%H:%M:%S")
            .to_string();

        for item in final_tels.iter() {
            let result = split_pack_vav_and_fancoil(
                item,
                i_ts_ini,
                i_ts_end,
                &mut |item: &DriVAVandFancoilTelemetry, index: isize| {
                    tcomp.AdcPontos(item, index as isize);
                },
            );
            match result {
                Ok(()) => {}
                Err(err) => return Err(err),
            };
        }

        let period_data = tcomp.CheckClosePeriod(isize::try_from(interval_length_s).unwrap());
        let result = match period_data {
            Ok(v) => Some(v),
            Err(_) => None,
        };

        Ok(result.unwrap())
    }

    async fn process_chiller_carrier_hx_query(
        &self,
        globs: &Arc<GlobalVars>,
    ) -> Result<Option<DriChillerCarrierHXCompiledPeriod>, String> {
        let dev_id_upper = self.dev_id.to_uppercase();
        let mut table_name = {
            if (self.dev_id.len() == 12) && dev_id_upper.starts_with("DRI") {
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
            return Err(format!("Unknown DRI generation: {}", self.dev_id));
        }

        let interval_length_s = 24 * 60 * 60;
        let (ts_ini, ts_end) = {
            let i_ts_ini = self.day.and_hms(0, 0, 0).timestamp();
            let i_ts_end = i_ts_ini + interval_length_s;
            let ts_ini = NaiveDateTime::from_timestamp(i_ts_ini, 0)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            let ts_end = NaiveDateTime::from_timestamp(i_ts_end, 0)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            (ts_ini, ts_end)
        };

        let querier = crate::lib_dynamodb::query::QuerierDevIdTimestamp::new_diel_dev(
            table_name,
            self.dev_id.to_owned(),
            &globs.configfile.aws_config,
        );
        let mut final_tels = Vec::new();
        querier
            .run(&ts_ini, &ts_end, &mut |items: Vec<
                TelemetryDriChillerCarrierHX,
            >| {
                let mut x = items
                    .into_iter()
                    .filter_map(|mut tel| {
                        tel.formulas = self.formulas.clone();
                        tel.try_into().ok()
                    })
                    .collect::<Vec<DriChillerCarrierHXTelemetry>>();
                final_tels.append(&mut x);
                Ok(())
            })
            .await?;

        let (grouped_telemetries, params_changes_hist) =
            DriChillerCarrierHXTelemetry::group_telemetries(
                &self.dev_id,
                final_tels.clone(),
                self.chiller_carrier_hour_graphic.unwrap_or(false),
            );

        let mut grouped_averages =
            DriChillerCarrierHXTelemetry::calculate_group_averages(&grouped_telemetries);

        grouped_averages.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        let dri_chiller_carrier_hist = DriChillerCarrierHXCompiledPeriod {
            params_grouped: grouped_averages,
            params_changed: params_changes_hist,
        };

        Ok(Some(dri_chiller_carrier_hist))
    }

    async fn process_chiller_carrier_xa_query(
        &self,
        globs: &Arc<GlobalVars>,
    ) -> Result<Option<DriChillerCarrierXACompiledPeriod>, String> {
        let dev_id_upper = self.dev_id.to_uppercase();
        let mut table_name = {
            if (self.dev_id.len() == 12) && dev_id_upper.starts_with("DRI") {
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
            return Err(format!("Unknown DRI generation: {}", self.dev_id));
        }

        let interval_length_s = 24 * 60 * 60;
        let (ts_ini, ts_end) = {
            let i_ts_ini = self.day.and_hms(0, 0, 0).timestamp();
            let i_ts_end = i_ts_ini + interval_length_s;
            let ts_ini = NaiveDateTime::from_timestamp(i_ts_ini, 0)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            let ts_end = NaiveDateTime::from_timestamp(i_ts_end, 0)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            (ts_ini, ts_end)
        };

        let querier = crate::lib_dynamodb::query::QuerierDevIdTimestamp::new_diel_dev(
            table_name,
            self.dev_id.to_owned(),
            &globs.configfile.aws_config,
        );
        let mut final_tels = Vec::new();
        querier
            .run(&ts_ini, &ts_end, &mut |items: Vec<
                TelemetryDriChillerCarrierXA,
            >| {
                let mut x = items
                    .into_iter()
                    .filter_map(|mut tel| {
                        tel.formulas = self.formulas.clone();
                        tel.try_into().ok()
                    })
                    .collect::<Vec<DriChillerCarrierXATelemetry>>();
                final_tels.append(&mut x);
                Ok(())
            })
            .await?;

        let (grouped_telemetries, params_changes_hist) =
            DriChillerCarrierXATelemetry::group_telemetries(
                &self.dev_id,
                final_tels.clone(),
                self.chiller_carrier_hour_graphic.unwrap_or(false),
            );

        let mut grouped_averages =
            DriChillerCarrierXATelemetry::calculate_group_averages(&grouped_telemetries);

        grouped_averages.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        let dri_chiller_carrier_hist = DriChillerCarrierXACompiledPeriod {
            params_grouped: grouped_averages,
            params_changed: params_changes_hist,
        };

        Ok(Some(dri_chiller_carrier_hist))
    }

    async fn process_chiller_carrier_xa_hvar_query(
        &self,
        globs: &Arc<GlobalVars>,
    ) -> Result<Option<DriChillerCarrierXAHvarCompiledPeriod>, String> {
        let dev_id_upper = self.dev_id.to_uppercase();
        let mut table_name = {
            if (self.dev_id.len() == 12) && dev_id_upper.starts_with("DRI") {
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
            return Err(format!("Unknown DRI generation: {}", self.dev_id));
        }

        let interval_length_s = 24 * 60 * 60;
        let (ts_ini, ts_end) = {
            let i_ts_ini = self.day.and_hms(0, 0, 0).timestamp();
            let i_ts_end = i_ts_ini + interval_length_s;
            let ts_ini = NaiveDateTime::from_timestamp(i_ts_ini, 0)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            let ts_end = NaiveDateTime::from_timestamp(i_ts_end, 0)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
            (ts_ini, ts_end)
        };

        let querier = crate::lib_dynamodb::query::QuerierDevIdTimestamp::new_diel_dev(
            table_name,
            self.dev_id.to_owned(),
            &globs.configfile.aws_config,
        );
        let mut final_tels = Vec::new();
        querier
            .run(&ts_ini, &ts_end, &mut |items: Vec<
                TelemetryDriChillerCarrierXAHvar,
            >| {
                let mut x = items
                    .into_iter()
                    .filter_map(|mut tel| {
                        tel.formulas = self.formulas.clone();
                        tel.try_into().ok()
                    })
                    .collect::<Vec<DriChillerCarrierXAHvarTelemetry>>();
                final_tels.append(&mut x);
                Ok(())
            })
            .await?;

        let (grouped_telemetries, params_changes_hist) =
            DriChillerCarrierXAHvarTelemetry::group_telemetries(
                &self.dev_id,
                final_tels.clone(),
                self.chiller_carrier_hour_graphic.unwrap_or(false),
            );

        let mut grouped_averages =
            DriChillerCarrierXAHvarTelemetry::calculate_group_averages(&grouped_telemetries);

        grouped_averages.sort_by(|a, b| a.timestamp.cmp(&b.timestamp));

        let dri_chiller_carrier_hist = DriChillerCarrierXAHvarCompiledPeriod {
            params_grouped: grouped_averages,
            params_changed: params_changes_hist,
        };

        Ok(Some(dri_chiller_carrier_hist))
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DriCompiledPeriod {
    DRICCNCompiledPeriod(DRICCNCompiledPeriod),
    DRIVAVandFancoilCompiledPeriod(DRIVAVandFancoilCompiledPeriod),
    DRIChillerCarrierHXCompiledPeriod(DriChillerCarrierHXCompiledPeriod),
    DRIChillerCarrierXACompiledPeriod(DriChillerCarrierXACompiledPeriod),
    DRIChillerCarrierXAHvarCompiledPeriod(DriChillerCarrierXAHvarCompiledPeriod),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DriHist {
    dev_id: String,
    dri_type: String,
    timestamp: NaiveDate,
    data: Option<DriCompiledPeriod>,
}

impl DriHist {
    fn new(
        dev_id: String,
        dri_type: String,
        timestamp: NaiveDate,
        data: Option<DriCompiledPeriod>,
    ) -> Self {
        Self {
            dev_id,
            dri_type,
            timestamp,
            data,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DriChillerCarrierHXCompiledPeriod {
    params_grouped: Vec<DriChillerCarrierHXTelemetry>,
    params_changed: Vec<ChillerParametersChangesHist>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DriChillerCarrierXACompiledPeriod {
    params_grouped: Vec<DriChillerCarrierXATelemetry>,
    params_changed: Vec<ChillerParametersChangesHist>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DriChillerCarrierXAHvarCompiledPeriod {
    params_grouped: Vec<DriChillerCarrierXAHvarTelemetry>,
    params_changed: Vec<ChillerParametersChangesHist>,
}
