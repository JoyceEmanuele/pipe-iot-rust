use super::super::dri_telemetry::TelemetryDri;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

#[derive(Debug, Serialize, Deserialize)]
pub struct DriCCNTelemetry {
    pub timestamp: String,
    pub Setpoint: Option<i16>,
    pub Status: Option<i16>,
    pub Mode: Option<i16>,
}

impl<'a> TryFrom<TelemetryDri<'a>> for DriCCNTelemetry {
    type Error = String;
    fn try_from(value: TelemetryDri) -> Result<DriCCNTelemetry, String> {
        if value.dev_type.to_string() != String::from("CCN") {
            return Err("The dev type and telemetry type does not match".to_string());
        }
        if value.values.is_none() {
            return Err("Telemetry does not have \"values\" field".to_string());
        }
        let values = value.values.unwrap();
        let result = DriCCNTelemetry {
            timestamp: value.timestamp.to_string(),
            Setpoint: match values[0] {
                Some(-1) => None,
                _ => values[0],
            },
            Status: match values[1] {
                Some(-1) => None,
                _ => values[1],
            },
            Mode: match values[2] {
                Some(-1) => None,
                _ => values[2],
            },
        };
        Ok(result)
    }
}

pub fn split_pack_ccn(
    mut payload: &DriCCNTelemetry,
    ts_ini: i64,
    ts_next: i64,
    itemCallback: &mut dyn FnMut(&DriCCNTelemetry, isize),
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
