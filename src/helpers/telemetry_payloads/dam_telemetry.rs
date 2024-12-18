use crate::telemetry_payloads::telemetry_formats::TelemetryRawDAM_v1;
use chrono::NaiveDateTime;
use std::convert::TryFrom;

pub fn split_pack(
    mut payload: &TelemetryRawDAM_v1,
    ts_ini: i64,
    ts_next: i64,
    itemCallback: &mut dyn FnMut(&TelemetryRawDAM_v1, isize),
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
