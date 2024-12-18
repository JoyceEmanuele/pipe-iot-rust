use crate::telemetry_payloads::parse_json_props::{
    get_bool_array_prop, get_string_array_prop, get_string_prop,
};
use crate::telemetry_payloads::telemetry_formats::TelemetryPackDAL;

use super::parse_json_props::get_int_number_prop;

pub fn get_raw_telemetry_pack_dal(item: &serde_json::Value) -> Result<TelemetryPackDAL, String> {
    let telemetry = TelemetryPackDAL {
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
        State: match get_string_prop(&item.get("State")) {
            Ok(State) => State,
            Err(message) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("Invalid State: {:?} {}", &item, message),
                );
                return Err(message);
            }
        },
        Mode: match get_string_array_prop(&item.get("Mode")) {
            Ok(Mode) => Mode,
            Err(message) => {
                crate::LOG
                    .append_log_tag_msg("ERROR", &format!("Invalid Mode: {:?} {}", &item, message));
                return Err(message);
            }
        },
        Relays: match get_bool_array_prop(&item.get("Relays")) {
            Ok(Relays) => Relays,
            Err(message) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("Invalid Relays: {:?} {}", &item, message),
                );
                return Err(message);
            }
        },
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
