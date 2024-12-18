use super::telemetry_formats::{TelemetryDMT, TelemetryPackDMT};
use chrono::NaiveDateTime;
use std::convert::TryFrom;

pub fn split_pack(
    payload: &TelemetryPackDMT,
    ts_ini: i64,
    ts_next: i64,
    itemCallback: &mut dyn FnMut(&mut TelemetryDMT, isize),
) -> Result<(), String> {
    let pack_ts = match NaiveDateTime::parse_from_str(&payload.timestamp, "%Y-%m-%dT%H:%M:%S") {
        Err(_) => {
            crate::LOG.append_log_tag_msg("ERROR", &format!("Error parsing Date: {:?}", payload));
            return Err("Error parsing Date".to_owned());
        }
        Ok(date) => date.timestamp(),
    };
    let sampling_time: i64 = payload.samplingTime; // de quantos em quantos segundos o firmware lê os sensores e insere nos vetores.

    let mut telemetry = TelemetryDMT {
        timestamp: payload.timestamp.to_owned(),
        dev_id: payload.dev_id.to_owned(),
        F1: None,
        F2: None,
        F3: None,
        F4: None,
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

pub fn convert_payload(payload: &TelemetryPackDMT) -> Result<TelemetryDMT, String> {
    if payload.Feedback.len() < 4 {
        return Err("Invalid Package Length".to_owned());
    }

    let pack_ts = match NaiveDateTime::parse_from_str(&payload.timestamp, "%Y-%m-%dT%H:%M:%S") {
        Err(_) => {
            crate::LOG.append_log_tag_msg("ERROR", &format!("Error parsing Date: {:?}", payload));
            return Err("Error parsing Date".to_owned());
        }
        Ok(date) => date,
    };

    let mut telemetry_pack = TelemetryDMT {
        timestamp: payload.timestamp.to_string(),
        dev_id: payload.dev_id.to_string(),
        F1: payload.Feedback[0],
        F2: payload.Feedback[1],
        F3: payload.Feedback[2],
        F4: payload.Feedback[3],
        GMT: payload.GMT.to_owned(),
    };

    return Ok(telemetry_pack);
}

fn checkSetTelemetryValues(payload: &TelemetryPackDMT, telemetry: &mut TelemetryDMT) {
    telemetry.F1 = if payload.Feedback.len() >= 4 {
        payload.Feedback[0]
    } else {
        None
    };
    telemetry.F2 = if payload.Feedback.len() >= 4 {
        payload.Feedback[1]
    } else {
        None
    };
    telemetry.F3 = if payload.Feedback.len() >= 4 {
        payload.Feedback[2]
    } else {
        None
    };
    telemetry.F4 = if payload.Feedback.len() >= 4 {
        payload.Feedback[3]
    } else {
        None
    };
}
