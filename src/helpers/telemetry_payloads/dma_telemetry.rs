use super::telemetry_formats::{TelemetryDMA, TelemetryPackDMA};
use chrono::NaiveDateTime;
use std::convert::TryFrom;

pub fn split_pack(
    payload: &TelemetryPackDMA,
    ts_ini: i64,
    ts_next: i64,
    itemCallback: &mut dyn FnMut(&mut TelemetryDMA, isize),
) -> Result<(), String> {
    let mut telemetry = TelemetryDMA {
        timestamp: payload.timestamp.to_owned(),
        dev_id: payload.dev_id.to_owned(),
        pulses: None,
        operation_mode: None,
        mode: None,
        samplingTime: None,
        gmt: payload.GMT.to_owned(),
    };

    let pack_ts = match NaiveDateTime::parse_from_str(&payload.timestamp, "%Y-%m-%dT%H:%M:%S") {
        Err(_) => {
            crate::LOG.append_log_tag_msg("ERROR", &format!("Error parsing Date: {:?}", payload));
            return Err("Error parsing Date".to_owned());
        }
        Ok(date) => date.timestamp(),
    };

    checkSetTelemetryValues(payload, &mut telemetry);

    if (pack_ts >= ts_ini) && (pack_ts < ts_next) {
        itemCallback(&mut telemetry, isize::try_from(pack_ts - ts_ini).unwrap());
    }
    return Ok(());
}

fn checkSetTelemetryValues(payload: &TelemetryPackDMA, telemetry: &mut TelemetryDMA) {
    telemetry.pulses = payload.pulses.clone();
    telemetry.operation_mode = payload.operation_mode.clone();
    telemetry.mode = payload.mode.clone();
    telemetry.dev_id = payload.dev_id.clone();
    telemetry.samplingTime = payload.samplingTime.clone();
    telemetry.gmt = payload.GMT.clone();
}
