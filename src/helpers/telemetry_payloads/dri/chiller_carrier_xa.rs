use super::super::dri_telemetry::{ChillerParametersChangesHist, HwInfoDRI};
use crate::telemetry_payloads::energy::padronized::calculateFormulas;
use chrono::{Local, NaiveDateTime, Timelike};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TelemetryDriChillerCarrier30XA<'a> {
    pub dev_id: Cow<'a, String>,
    pub timestamp: Cow<'a, String>,
    #[serde(rename = "type")]
    pub dev_type: Cow<'a, String>,
    pub CAP_T: Option<i16>,
    pub CHIL_OCC: Option<i16>,
    pub CHIL_S_S: Option<i16>,
    pub COND_EWT: Option<i16>,
    pub COND_LWT: Option<i16>,
    pub COOL_EWT: Option<i16>,
    pub COOL_LWT: Option<i16>,
    pub CTRL_PNT: Option<i16>,
    pub CTRL_TYP: Option<i16>,
    pub DEM_LIM: Option<i16>,
    pub DP_A: Option<i16>,
    pub DP_B: Option<i16>,
    pub EMSTOP: Option<i16>,
    pub HR_CP_A: Option<i16>,
    pub HR_CP_B: Option<i16>,
    pub HR_MACH: Option<i16>,
    pub HR_MACH_B: Option<i16>,
    pub OAT: Option<i16>,
    pub OP_A: Option<i16>,
    pub OP_B: Option<i16>,
    pub SCT_A: Option<i16>,
    pub SCT_B: Option<i16>,
    pub SLC_HM: Option<i16>,
    pub SLT_A: Option<i16>,
    pub SLT_B: Option<i16>,
    pub SP: Option<i16>,
    pub SP_A: Option<i16>,
    pub SP_B: Option<i16>,
    pub SP_OCC: Option<i16>,
    pub SST_A: Option<i16>,
    pub SST_B: Option<i16>,
    pub STATUS: Option<i16>,
    pub formulas: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct DriChillerCarrier30XATelemetry {
    pub timestamp: String,
    pub CAP_T: Option<f64>,
    pub CHIL_OCC: Option<f64>,
    pub CHIL_S_S: Option<f64>,
    pub COND_EWT: Option<f64>,
    pub COND_LWT: Option<f64>,
    pub COOL_EWT: Option<f64>,
    pub COOL_LWT: Option<f64>,
    pub CTRL_PNT: Option<f64>,
    pub CTRL_TYP: Option<f64>,
    pub DEM_LIM: Option<f64>,
    pub DP_A: Option<f64>,
    pub DP_B: Option<f64>,
    pub EMSTOP: Option<f64>,
    pub HR_CP_A: Option<f64>,
    pub HR_CP_B: Option<f64>,
    pub HR_MACH: Option<f64>,
    pub HR_MACH_B: Option<f64>,
    pub OAT: Option<f64>,
    pub OP_A: Option<f64>,
    pub OP_B: Option<f64>,
    pub SCT_A: Option<f64>,
    pub SCT_B: Option<f64>,
    pub SLC_HM: Option<f64>,
    pub SLT_A: Option<f64>,
    pub SLT_B: Option<f64>,
    pub SP: Option<f64>,
    pub SP_A: Option<f64>,
    pub SP_B: Option<f64>,
    pub SP_OCC: Option<f64>,
    pub SST_A: Option<f64>,
    pub SST_B: Option<f64>,
    pub STATUS: Option<f64>,
    pub record_date: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TelemetryDriChillerCarrierXA<'a> {
    pub dev_id: Cow<'a, String>,
    pub timestamp: Cow<'a, String>,
    #[serde(rename = "type")]
    pub dev_type: Cow<'a, String>,
    pub CAP_T: Option<i16>,
    pub CHIL_OCC: Option<i16>,
    pub CHIL_S_S: Option<i16>,
    pub COND_EWT: Option<i16>,
    pub COND_LWT: Option<i16>,
    pub COOL_EWT: Option<i16>,
    pub COOL_LWT: Option<i16>,
    pub CTRL_PNT: Option<i16>,
    pub CTRL_TYP: Option<i16>,
    pub DEM_LIM: Option<i16>,
    pub DP_A: Option<i16>,
    pub DP_B: Option<i16>,
    pub EMSTOP: Option<i16>,
    pub HR_CP_A: Option<i16>,
    pub HR_CP_B: Option<i16>,
    pub HR_MACH: Option<i16>,
    pub HR_MACH_B: Option<i16>,
    pub OAT: Option<i16>,
    pub OP_A: Option<i16>,
    pub OP_B: Option<i16>,
    pub SCT_A: Option<i16>,
    pub SCT_B: Option<i16>,
    pub SLC_HM: Option<i16>,
    pub SLT_A: Option<i16>,
    pub SLT_B: Option<i16>,
    pub SP: Option<i16>,
    pub SP_A: Option<i16>,
    pub SP_B: Option<i16>,
    pub SP_OCC: Option<i16>,
    pub SST_A: Option<i16>,
    pub SST_B: Option<i16>,
    pub STATUS: Option<i16>,
    pub formulas: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DriChillerCarrierXATelemetry {
    pub timestamp: String,
    pub CAP_T: Option<f64>,
    pub CHIL_OCC: Option<f64>,
    pub CHIL_S_S: Option<f64>,
    pub COND_EWT: Option<f64>,
    pub COND_LWT: Option<f64>,
    pub COOL_EWT: Option<f64>,
    pub COOL_LWT: Option<f64>,
    pub CTRL_PNT: Option<f64>,
    pub CTRL_TYP: Option<f64>,
    pub DEM_LIM: Option<f64>,
    pub DP_A: Option<f64>,
    pub DP_B: Option<f64>,
    pub EMSTOP: Option<f64>,
    pub HR_CP_A: Option<f64>,
    pub HR_CP_B: Option<f64>,
    pub HR_MACH: Option<f64>,
    pub HR_MACH_B: Option<f64>,
    pub OAT: Option<f64>,
    pub OP_A: Option<f64>,
    pub OP_B: Option<f64>,
    pub SCT_A: Option<f64>,
    pub SCT_B: Option<f64>,
    pub SLC_HM: Option<f64>,
    pub SLT_A: Option<f64>,
    pub SLT_B: Option<f64>,
    pub SP: Option<f64>,
    pub SP_A: Option<f64>,
    pub SP_B: Option<f64>,
    pub SP_OCC: Option<f64>,
    pub SST_A: Option<f64>,
    pub SST_B: Option<f64>,
    pub STATUS: Option<f64>,
    pub record_date: Option<String>,
}

impl<'a> TryFrom<TelemetryDriChillerCarrierXA<'a>> for DriChillerCarrierXATelemetry {
    type Error = String;
    fn try_from(
        value: TelemetryDriChillerCarrierXA,
    ) -> Result<DriChillerCarrierXATelemetry, String> {
        let tel = json!(value);

        let result = DriChillerCarrierXATelemetry {
            timestamp: value.timestamp.to_string(),
            CAP_T: match value.CAP_T {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CAP_T",
                    value.CAP_T.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CHIL_OCC: match value.CHIL_OCC {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CHIL_OCC",
                    value.CHIL_OCC.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CHIL_S_S: match value.CHIL_S_S {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CHIL_S_S",
                    value.CHIL_S_S.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            COND_EWT: match value.COND_EWT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "COND_EWT",
                    value.COND_EWT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            COND_LWT: match value.COND_LWT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "COND_LWT",
                    value.COND_LWT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            COOL_EWT: match value.COOL_EWT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "COOL_EWT",
                    value.COOL_EWT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            COOL_LWT: match value.COOL_LWT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "COOL_LWT",
                    value.COOL_LWT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CTRL_PNT: match value.CTRL_PNT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CTRL_PNT",
                    value.CTRL_PNT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CTRL_TYP: match value.CTRL_TYP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CTRL_TYP",
                    value.CTRL_TYP.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DEM_LIM: match value.DEM_LIM {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DEM_LIM",
                    value.DEM_LIM.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DP_A: match value.DP_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DP_A",
                    value.DP_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DP_B: match value.DP_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DP_B",
                    value.DP_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            EMSTOP: match value.EMSTOP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "EMSTOP",
                    value.EMSTOP.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            HR_CP_A: match value.HR_CP_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "HR_CP_A",
                    value.HR_CP_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            HR_CP_B: match value.HR_CP_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "HR_CP_B",
                    value.HR_CP_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            HR_MACH: match value.HR_MACH {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "HR_MACH",
                    value.HR_MACH.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            HR_MACH_B: match value.HR_MACH_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "HR_MACH_B",
                    value.HR_MACH_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            OAT: match value.OAT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "OAT",
                    value.OAT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            OP_A: match value.OP_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "OP_A",
                    value.OP_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            OP_B: match value.OP_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "OP_B",
                    value.OP_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SCT_A: match value.SCT_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SCT_A",
                    value.SCT_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SCT_B: match value.SCT_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SCT_B",
                    value.SCT_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SLC_HM: match value.SLC_HM {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SLC_HM",
                    value.SLC_HM.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SLT_A: match value.SLT_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SLT_A",
                    value.SLT_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SLT_B: match value.SLT_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SLT_B",
                    value.SLT_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SP: match value.SP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SP",
                    value.SP.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SP_A: match value.SP_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SP_A",
                    value.SP_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SP_B: match value.SP_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SP_B",
                    value.SP_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SP_OCC: match value.SP_OCC {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SP_OCC",
                    value.SP_OCC.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SST_A: match value.SST_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SST_A",
                    value.SST_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SST_B: match value.SST_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SST_B",
                    value.SST_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            STATUS: match value.STATUS {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "STATUS",
                    value.STATUS.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            record_date: None,
        };
        Ok(result)
    }
}

pub fn convert_chiller_carrier_xa_payload<'a>(
    mut payload: TelemetryDriChillerCarrierXA<'a>,
    dev: &'a HwInfoDRI,
) -> Result<DriChillerCarrierXATelemetry, String> {
    if dev.formulas.is_some() {
        payload.formulas = dev.formulas.clone();
    }
    return payload.try_into();
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DriChillerCarrierXAChangeParams {
    pub timestamp: NaiveDateTime,
    pub STATUS: Option<f64>,
    pub CHIL_S_S: Option<f64>,
    pub CHIL_OCC: Option<f64>,
    pub CTRL_TYP: Option<f64>,
    pub SLC_HM: Option<f64>,
    pub DEM_LIM: Option<f64>,
    pub SP_OCC: Option<f64>,
    pub EMSTOP: Option<f64>,
}

impl DriChillerCarrierXAChangeParams {
    pub fn new(timestamp: NaiveDateTime) -> Self {
        Self {
            timestamp,
            STATUS: None,
            CHIL_S_S: None,
            CHIL_OCC: None,
            CTRL_TYP: None,
            SLC_HM: None,
            DEM_LIM: None,
            SP_OCC: None,
            EMSTOP: None,
        }
    }
}

impl DriChillerCarrierXATelemetry {
    pub fn group_telemetries(
        device_code: &str,
        telemetries: Vec<DriChillerCarrierXATelemetry>,
        hour_graphic: bool,
    ) -> (
        HashMap<NaiveDateTime, Vec<DriChillerCarrierXATelemetry>>,
        Vec<ChillerParametersChangesHist>,
    ) {
        let mut grouped_telemetries: HashMap<NaiveDateTime, Vec<DriChillerCarrierXATelemetry>> =
            HashMap::new();
        let mut last_telemetry = DriChillerCarrierXAChangeParams::new(Local::now().naive_local());
        let mut params_changes_hist = Vec::<ChillerParametersChangesHist>::new();
        let interval = if hour_graphic { 60 } else { 10 };

        for (index, telemetry) in telemetries.iter().enumerate() {
            let timestamp =
                NaiveDateTime::parse_from_str(&telemetry.timestamp, "%Y-%m-%dT%H:%M:%S")
                    .unwrap_or_default();
            let rounded_minute = (timestamp.minute() / interval) * interval;
            let rounded_timestamp = timestamp
                .date()
                .and_hms(timestamp.hour(), rounded_minute, 0);
            grouped_telemetries
                .entry(rounded_timestamp)
                .or_insert_with(Vec::new)
                .push(telemetry.clone());

            let iterate_telemetry = DriChillerCarrierXAChangeParams {
                timestamp,
                STATUS: telemetry.STATUS,
                CHIL_S_S: telemetry.CHIL_S_S,
                CHIL_OCC: telemetry.CHIL_OCC,
                CTRL_TYP: telemetry.CTRL_TYP,
                SLC_HM: telemetry.SLC_HM,
                DEM_LIM: telemetry.DEM_LIM,
                SP_OCC: telemetry.SP_OCC,
                EMSTOP: telemetry.EMSTOP,
            };

            if index == 0 || index == telemetries.len() - 1 {
                // primeira telemetria ou última, salvar dados
                Self::salve_all_params_change(
                    device_code,
                    iterate_telemetry,
                    &mut params_changes_hist,
                );
            } else {
                Self::verify_and_save_params_changes(
                    device_code,
                    iterate_telemetry,
                    last_telemetry.clone(),
                    &mut params_changes_hist,
                );
            }

            last_telemetry.timestamp = timestamp;
            last_telemetry.STATUS = telemetry.STATUS;
            last_telemetry.CHIL_S_S = telemetry.CHIL_S_S;
            last_telemetry.CHIL_OCC = telemetry.CHIL_OCC;
            last_telemetry.CTRL_TYP = telemetry.CTRL_TYP;
            last_telemetry.SLC_HM = telemetry.SLC_HM;
            last_telemetry.DEM_LIM = telemetry.DEM_LIM;
            last_telemetry.SP_OCC = telemetry.SP_OCC;
            last_telemetry.EMSTOP = telemetry.EMSTOP;
        }

        (grouped_telemetries, params_changes_hist)
    }

    pub fn calculate_group_averages(
        grouped_telemetries: &HashMap<NaiveDateTime, Vec<DriChillerCarrierXATelemetry>>,
    ) -> Vec<DriChillerCarrierXATelemetry> {
        let mut group_averages: Vec<DriChillerCarrierXATelemetry> = Vec::new();

        for (time_interval, telemetry_array) in grouped_telemetries {
            let mut total_values: HashMap<&str, (f64, usize)> = HashMap::new();

            for telemetry in telemetry_array {
                for (field, value) in &[
                    ("CAP_T", telemetry.CAP_T),
                    ("COND_EWT", telemetry.COND_EWT),
                    ("COND_LWT", telemetry.COND_LWT),
                    ("COOL_EWT", telemetry.COOL_EWT),
                    ("COOL_LWT", telemetry.COOL_LWT),
                    ("CTRL_PNT", telemetry.CTRL_PNT),
                    ("DP_A", telemetry.DP_A),
                    ("DP_B", telemetry.DP_B),
                    ("HR_CP_A", telemetry.HR_CP_A),
                    ("HR_CP_B", telemetry.HR_CP_B),
                    ("HR_MACH", telemetry.HR_MACH),
                    ("HR_MACH_B", telemetry.HR_MACH_B),
                    ("OAT", telemetry.OAT),
                    ("OP_A", telemetry.OP_A),
                    ("OP_B", telemetry.OP_B),
                    ("SCT_A", telemetry.SCT_A),
                    ("SCT_B", telemetry.SCT_B),
                    ("SLT_A", telemetry.SLT_A),
                    ("SLT_B", telemetry.SLT_B),
                    ("SP", telemetry.SP),
                    ("SP_A", telemetry.SP_A),
                    ("SP_B", telemetry.SP_B),
                    ("SST_A", telemetry.SST_A),
                    ("SST_B", telemetry.SST_B),
                ] {
                    if let Some(v) = value {
                        let (sum, count_val) = total_values.entry(field).or_insert((0.0, 0));
                        *sum += v;
                        *count_val += 1;
                    }
                }
            }
            let mut averaged_telemetry =
                DriChillerCarrierXATelemetry::new(time_interval.to_string());
            let date_clone = &averaged_telemetry.timestamp.clone();
            averaged_telemetry.record_date =
                Some([&date_clone[..10], "T", &date_clone[11..]].concat());

            for (field, (total, field_count)) in total_values {
                let avg = total / field_count as f64;
                let avg_rounded = (avg * 100.0).round() / 100.0;
                averaged_telemetry.set_field_average(field, avg_rounded);
            }

            group_averages.push(averaged_telemetry);
        }

        group_averages
    }

    fn verify_and_save_params_changes(
        device_code: &str,
        iterate_telemetry: DriChillerCarrierXAChangeParams,
        last_telemetry: DriChillerCarrierXAChangeParams,
        history_vec: &mut Vec<ChillerParametersChangesHist>,
    ) {
        Self::save_if_changed(
            "STATUS",
            last_telemetry.STATUS,
            iterate_telemetry.STATUS,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "CHIL_S_S",
            last_telemetry.CHIL_S_S,
            iterate_telemetry.CHIL_S_S,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "CHIL_OCC",
            last_telemetry.CHIL_OCC,
            iterate_telemetry.CHIL_OCC,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "CTRL_TYP",
            last_telemetry.CTRL_TYP,
            iterate_telemetry.CTRL_TYP,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "SLC_HM",
            last_telemetry.SLC_HM,
            iterate_telemetry.SLC_HM,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "DEM_LIM",
            last_telemetry.DEM_LIM,
            iterate_telemetry.DEM_LIM,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "SP_OCC",
            last_telemetry.SP_OCC,
            iterate_telemetry.SP_OCC,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "EMSTOP",
            last_telemetry.EMSTOP,
            iterate_telemetry.EMSTOP,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
    }

    fn save_if_changed(
        param_name: &str,
        last_value: Option<f64>,
        new_value: Option<f64>,
        timestamp: NaiveDateTime,
        device_code: &str,
        history_vec: &mut Vec<ChillerParametersChangesHist>,
    ) {
        if let Some(new_val) = new_value {
            if last_value != new_value {
                Self::save_param_change_hist(
                    timestamp,
                    device_code,
                    param_name,
                    Some(new_val),
                    history_vec,
                );
            }
        }
    }

    fn save_param_change_hist(
        day: NaiveDateTime,
        device_code: &str,
        parameter_name: &str,
        parameter_value: Option<f64>,
        history_vec: &mut Vec<ChillerParametersChangesHist>,
    ) {
        if let Some(value) = parameter_value {
            let history = ChillerParametersChangesHist {
                device_code: String::from(device_code),
                parameter_name: parameter_name.to_string(),
                record_date: day,
                parameter_value: value.round() as i32,
            };

            history_vec.push(history);
        }
    }

    fn salve_all_params_change(
        device_code: &str,
        last_telemetry: DriChillerCarrierXAChangeParams,
        history_vec: &mut Vec<ChillerParametersChangesHist>,
    ) {
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "STATUS",
            last_telemetry.STATUS,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "CHIL_S_S",
            last_telemetry.CHIL_S_S,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "CHIL_OCC",
            last_telemetry.CHIL_OCC,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "CTRL_TYP",
            last_telemetry.CTRL_TYP,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "SLC_HM",
            last_telemetry.SLC_HM,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "DEM_LIM",
            last_telemetry.DEM_LIM,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "SP_OCC",
            last_telemetry.SP_OCC,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "EMSTOP",
            last_telemetry.EMSTOP,
            history_vec,
        );
    }

    fn set_field_average(&mut self, field: &str, value: f64) {
        match field {
            "CAP_T" => self.CAP_T = Some(value),
            "COND_EWT" => self.COND_EWT = Some(value),
            "COND_LWT" => self.COND_LWT = Some(value),
            "COOL_EWT" => self.COOL_EWT = Some(value),
            "COOL_LWT" => self.COOL_LWT = Some(value),
            "CTRL_PNT" => self.CTRL_PNT = Some(value),
            "DP_A" => self.DP_A = Some(value),
            "DP_B" => self.DP_B = Some(value),
            "HR_CP_A" => self.HR_CP_A = Some(value),
            "HR_CP_B" => self.HR_CP_B = Some(value),
            "HR_MACH" => self.HR_MACH = Some(value),
            "HR_MACH_B" => self.HR_MACH_B = Some(value),
            "OAT" => self.OAT = Some(value),
            "OP_A" => self.OP_A = Some(value),
            "OP_B" => self.OP_B = Some(value),
            "SCT_A" => self.SCT_A = Some(value),
            "SCT_B" => self.SCT_B = Some(value),
            "SLT_A" => self.SLT_A = Some(value),
            "SLT_B" => self.SLT_B = Some(value),
            "SP" => self.SP = Some(value),
            "SP_A" => self.SP_A = Some(value),
            "SP_B" => self.SP_B = Some(value),
            "SST_A" => self.SST_A = Some(value),
            "SST_B" => self.SST_B = Some(value),
            _ => (),
        }
    }

    fn new(timestamp: String) -> Self {
        Self {
            timestamp,
            CAP_T: None,
            CHIL_OCC: None,
            CHIL_S_S: None,
            COND_EWT: None,
            COND_LWT: None,
            COOL_EWT: None,
            COOL_LWT: None,
            CTRL_PNT: None,
            CTRL_TYP: None,
            DEM_LIM: None,
            DP_A: None,
            DP_B: None,
            EMSTOP: None,
            HR_CP_A: None,
            HR_CP_B: None,
            HR_MACH: None,
            HR_MACH_B: None,
            OAT: None,
            OP_A: None,
            OP_B: None,
            SCT_A: None,
            SCT_B: None,
            SLC_HM: None,
            SLT_A: None,
            SLT_B: None,
            SP: None,
            SP_A: None,
            SP_B: None,
            SP_OCC: None,
            SST_A: None,
            SST_B: None,
            STATUS: None,
            record_date: None,
        }
    }
}
