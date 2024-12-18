use super::super::dri_telemetry::{HwInfoDRI, TelemetryDri};
use crate::telemetry_payloads::energy::padronized::calculateFormulas;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::convert::TryFrom;

#[derive(Debug, Serialize, Deserialize)]
pub struct DriVAVandFancoilTelemetry {
    pub timestamp: String,
    pub ThermOn: Option<f64>,
    pub Fanspeed: Option<f64>,
    pub Mode: Option<f64>,
    pub Setpoint: Option<f64>,
    pub Lock: Option<f64>,
    pub TempAmb: Option<f64>,
    pub ValveOn: Option<f64>,
    pub FanStatus: Option<f64>,
    pub gmt: Option<i64>,
}

impl<'a> TryFrom<TelemetryDri<'a>> for DriVAVandFancoilTelemetry {
    type Error = String;
    fn try_from(value: TelemetryDri) -> Result<DriVAVandFancoilTelemetry, String> {
        // if !value.dev_type.to_string().starts_with("VAV") {
        //     return Err("The dev type and telemetry type does not match".to_string())
        // }
        let tel = json!(value);

        let result = DriVAVandFancoilTelemetry {
            timestamp: value.timestamp.to_string(),
            ThermOn: match value.therm_on {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "therm-on",
                    value.therm_on.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            Fanspeed: match value.fanspeed {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "fanspeed",
                    value.fanspeed.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            Mode: match value.mode {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "mode",
                    value.mode.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            Setpoint: match value.setpoint {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "setpoint",
                    value.setpoint.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            Lock: match value.lock {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "lock",
                    value.lock.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            TempAmb: match value.temp_amb {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "temp-amb",
                    value.temp_amb.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            ValveOn: match value.valve_on {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "valve-on",
                    value.valve_on.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            FanStatus: match value.fan_status {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "fan-status",
                    value.fan_status.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            gmt: match value.gmt {
                None => Some(-3),
                Some(v) => Some(v),
            },
        };
        Ok(result)
    }
}

pub fn split_pack_vav_and_fancoil(
    mut payload: &DriVAVandFancoilTelemetry,
    ts_ini: i64,
    ts_next: i64,
    itemCallback: &mut dyn FnMut(&DriVAVandFancoilTelemetry, isize),
) -> Result<(), String> {
    let pack_ts = match NaiveDateTime::parse_from_str(&payload.timestamp, "%Y-%m-%dT%H:%M:%S") {
        Err(_) => {
            crate::LOG.append_log_tag_msg("ERROR", &format!("Error parsing Date: {:?}", payload));
            return Err("Error parsing Date".to_owned());
        }
        Ok(date) => date.timestamp(),
    };

    if (pack_ts < ts_ini) || (pack_ts >= ts_next) {
    }
    // ignore
    else {
        itemCallback(&mut payload, isize::try_from(pack_ts - ts_ini).unwrap());
    }

    return Ok(());
}

pub fn convert_vav_and_fancoil_payload<'a>(
    mut payload: TelemetryDri<'a>,
    dev: &'a HwInfoDRI,
) -> Result<DriVAVandFancoilTelemetry, String> {
    if dev.formulas.is_some() {
        payload.formulas = dev.formulas.clone();
    }
    return payload.try_into();
}
