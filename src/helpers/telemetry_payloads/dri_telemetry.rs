use crate::telemetry_payloads::energy::padronized::calculateFormulas;
use chrono::{Local, NaiveDateTime, Timelike};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TelemetryDri<'a> {
    pub dev_id: Cow<'a, String>,
    pub timestamp: Cow<'a, String>,
    #[serde(rename = "type")]
    pub dev_type: Cow<'a, String>,
    pub values: Option<Vec<Option<i16>>>,
    #[serde(rename = "therm-on")]
    pub therm_on: Option<i16>,
    pub fanspeed: Option<i16>,
    pub mode: Option<i16>,
    pub setpoint: Option<i16>,
    pub lock: Option<i16>,
    #[serde(rename = "temp-amb")]
    pub temp_amb: Option<i16>,
    #[serde(rename = "valve-on")]
    pub valve_on: Option<i16>,
    #[serde(rename = "fan-status")]
    pub fan_status: Option<i16>,
    pub formulas: Option<HashMap<String, String>>,
    pub gmt: Option<i64>,
}

#[derive(Debug)]
pub struct HwInfoDRI {
    pub formulas: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DriChillerCarrierChangeParams {
    pub timestamp: NaiveDateTime,
    pub CHIL_S_S: Option<f64>,
    pub ALM: Option<f64>,
    pub EMSTOP: Option<f64>,
    pub STATUS: Option<f64>,
    pub CP_A1: Option<f64>,
    pub CP_A2: Option<f64>,
    pub CP_B1: Option<f64>,
    pub CP_B2: Option<f64>,
    pub CHIL_OCC: Option<f64>,
}

impl DriChillerCarrierChangeParams {
    pub fn new(timestamp: NaiveDateTime) -> Self {
        Self {
            timestamp,
            CHIL_S_S: None,
            ALM: None,
            EMSTOP: None,
            STATUS: None,
            CP_A1: None,
            CP_A2: None,
            CP_B1: None,
            CP_B2: None,
            CHIL_OCC: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ChillerParametersChangesHist {
    pub device_code: String,
    pub parameter_name: String,
    pub record_date: NaiveDateTime,
    pub parameter_value: i32,
}
