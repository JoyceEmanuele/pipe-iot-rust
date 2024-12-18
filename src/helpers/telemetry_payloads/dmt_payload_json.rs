use crate::telemetry_payloads::parse_json_props::{
    get_bool_array_prop, get_int_number_prop, get_string_prop,
};
use crate::telemetry_payloads::telemetry_formats::TelemetryPackDMT;

pub fn get_raw_telemetry_pack_dmt(item: &serde_json::Value) -> Result<TelemetryPackDMT, String> {
    let telemetry = TelemetryPackDMT {
        timestamp: match get_string_prop(&item.get("timestamp")) {
            Ok(timestamp) => timestamp,
            Err(message) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("Invalid timestamp: {:?} {}", &item, message),
                );
                return Err(message);
            }
        },
        dev_id: match get_string_prop(&item.get("dev_id")) {
            Ok(dev_id) => dev_id,
            Err(message) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("Invalid dev_id: {:?} {}", &item, message),
                );
                return Err(message);
            }
        },
        samplingTime: get_int_number_prop(&item.get("samplingTime")).unwrap_or(1), // de quantos em quantos segundos o firmware lê os sensores e insere nos vetores.
        Feedback: match get_bool_array_prop(&item.get("Feedback")) {
            Ok(Feedback) => Feedback,
            Err(message) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("Invalid Feedback: {:?} {}", &item, message),
                );
                return Err(message);
            }
        },
        GMT: match get_int_number_prop(&item.get("GMT")) {
            Ok(gmt) => Some(gmt),
            Err(message) => Some(-3),
        },
    };

    return Ok(telemetry);
}
