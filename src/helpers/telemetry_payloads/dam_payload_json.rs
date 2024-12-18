use crate::telemetry_payloads::parse_json_props::{get_int_number_prop, get_string_prop};
use crate::telemetry_payloads::telemetry_formats::TelemetryRawDAM_v1;

pub fn get_raw_telemetry_pack_dam(item: &serde_json::Value) -> Result<TelemetryRawDAM_v1, String> {
    println!("{}", item);
    let telemetry = TelemetryRawDAM_v1 {
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
        Mode: match get_string_prop(&item.get("Mode")) {
            Ok(Mode) => Mode,
            Err(message) => {
                crate::LOG
                    .append_log_tag_msg("ERROR", &format!("Invalid Mode: {:?} {}", &item, message));
                return Err(message);
            }
        },
        GMT: match get_int_number_prop(&item.get("GMT")) {
            Ok(gmt) => Some(gmt),
            Err(_message) => Some(-3),
        },
        Temperature: match get_string_prop(&item.get("Temperature")) {
            Ok(Temperature) => match Temperature.parse::<f64>() {
                Err(err) => {
                    crate::LOG.append_log_tag_msg(
                        "ERROR",
                        &format!("Invalid Temperature: {:?} {}", &item, err),
                    );
                    return Err(err.to_string());
                }
                Ok(temperature) => {
                    if temperature <= -99.0 {
                        None
                    } else {
                        Some(Temperature)
                    }
                }
            },
            Err(_message) => None, // { println!("Invalid Temperature"); return Err(message); }
        },
        Temperature_1: match get_string_prop(&item.get("Temperature_1")) {
            Ok(Temperature_1) => match Temperature_1.parse::<f64>() {
                Err(err) => {
                    crate::LOG.append_log_tag_msg(
                        "ERROR",
                        &format!("Invalid Temperature_1: {:?} {}", &item, err),
                    );
                    return Err(err.to_string());
                }
                Ok(temperature_1) => {
                    if temperature_1 <= -99.0 {
                        None
                    } else {
                        Some(Temperature_1)
                    }
                }
            },
            Err(_message) => None, // { println!("Invalid Temperature"); return Err(message); }
        },
    };
    return Ok(telemetry);
}
