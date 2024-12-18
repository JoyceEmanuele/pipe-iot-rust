use super::telemetry_formats::{TelemetryDAL, TelemetryPackDAL};
use chrono::NaiveDateTime;
use std::convert::TryFrom;

pub fn split_pack(
    payload: &TelemetryPackDAL,
    ts_ini: i64,
    ts_next: i64,
    itemCallback: &mut dyn FnMut(&mut TelemetryDAL, isize),
) -> Result<(), String> {
    let pack_ts = match NaiveDateTime::parse_from_str(&payload.timestamp, "%Y-%m-%dT%H:%M:%S") {
        Err(_) => {
            crate::LOG.append_log_tag_msg("ERROR", &format!("Error parsing Date: {:?}", payload));
            return Err("Error parsing Date".to_owned());
        }
        Ok(date) => date.timestamp(),
    };

    let mut telemetry = TelemetryDAL {
        timestamp: payload.timestamp.to_owned(),
        dev_id: payload.dev_id.to_owned(),
        State: payload.State.to_owned(),
        Mode: payload.Mode.to_owned(),
        Relays: payload.Relays.to_owned(),
        Feedback: payload.Feedback.to_owned(),
        GMT: payload.GMT.to_owned(),
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

pub fn convert_payload(payload: &TelemetryPackDAL) -> Result<TelemetryDAL, String> {
    if payload.Feedback.len() < 4 || payload.Relays.len() < 4 {
        return Err("Invalid Package Length".to_owned());
    }

    let telemetry_pack = TelemetryDAL {
        timestamp: payload.timestamp.to_string(),
        dev_id: payload.dev_id.to_string(),
        State: payload.State.to_string(),
        Mode: payload.Mode.to_owned(),
        Feedback: payload.Feedback.to_owned(),
        Relays: payload.Relays.to_owned(),
        GMT: payload.GMT.clone(),
    };

    return Ok(telemetry_pack);
}

fn checkSetTelemetryValues(payload: &TelemetryPackDAL, telemetry: &mut TelemetryDAL) {
    telemetry.Relays = if payload.Relays.len() >= 4 {
        payload.Relays.to_owned()
    } else {
        Vec::new()
    };
    telemetry.Feedback = if payload.Feedback.len() >= 4 {
        payload.Feedback.to_owned()
    } else {
        Vec::new()
    };
}
