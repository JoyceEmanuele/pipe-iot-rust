use crate::telemetry_payloads::parse_json_props::{
    get_bool_array_prop, get_float_number_array_prop, get_int_number_array_prop,
    get_int_number_prop, get_string_prop,
};
use crate::telemetry_payloads::telemetry_formats::TelemetryPackDAC_v2;

use super::parse_json_props::get_bool_prop;

pub fn get_raw_telemetry_pack_dac(item: &serde_json::Value) -> Result<TelemetryPackDAC_v2, String> {
    let telemetry = TelemetryPackDAC_v2 {
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
        samplingTime: get_int_number_prop(&item.get("samplingTime").or(item.get("sampling_time"))).unwrap_or(1),
        L1: match get_bool_array_prop(&item.get("L1")) {
            Ok(L1) => L1,
            Err(message) => {
                crate::LOG
                    .append_log_tag_msg("ERROR", &format!("Invalid L1: {:?} {}", &item, message));
                return Err(message);
            }
        },
        T0: match &item.get("T0") {
            None => {
                return Err(format!("Missing T0"));
            }
            Some(prop) => match get_float_number_array_prop(prop) {
                Ok(T0) => T0,
                Err(message) => {
                    crate::LOG.append_log_tag_msg(
                        "ERROR",
                        &format!("Invalid T0: {:?} {}", &item, message),
                    );
                    return Err(message);
                }
            },
        },
        T1: match &item.get("T1") {
            None => {
                return Err(format!("Missing T1"));
            }
            Some(prop) => match get_float_number_array_prop(prop) {
                Ok(T1) => T1,
                Err(message) => {
                    crate::LOG.append_log_tag_msg(
                        "ERROR",
                        &format!("Invalid T1: {:?} {}", &item, message),
                    );
                    return Err(message);
                }
            },
        },
        T2: match &item.get("T2") {
            None => {
                return Err(format!("Missing T2"));
            }
            Some(prop) => match get_float_number_array_prop(prop) {
                Ok(T2) => T2,
                Err(message) => {
                    crate::LOG.append_log_tag_msg(
                        "ERROR",
                        &format!("Invalid T2: {:?} {}", &item, message),
                    );
                    return Err(message);
                }
            },
        },
        P0: match &item.get("P0") {
            None => {
                return Err(format!("Missing P0"));
            }
            Some(prop) => match get_int_number_array_prop(prop) {
                Ok(P0) => P0,
                Err(message) => {
                    crate::LOG.append_log_tag_msg(
                        "ERROR",
                        &format!("Invalid P0: {:?} {}", &item, message),
                    );
                    return Err(message);
                }
            },
        },
        P1: match &item.get("P1") {
            None => {
                return Err(format!("Missing P1"));
            }
            Some(prop) => match get_int_number_array_prop(prop) {
                Ok(v) => v,
                Err(err) => {
                    return Err(format!("Invalid P1: {}", err));
                }
            },
        },
        State: match get_string_prop(&item.get("State")) {
            Ok(State) => Some(State),
            Err(_message) => None, // { println!("Invalid State"); return Err(message); }
        },
        Mode: match get_string_prop(&item.get("Mode")) {
            Ok(Mode) => Some(Mode),
            Err(_message) => None, // { println!("Invalid Mode"); return Err(message); }
        },
        GMT: match get_int_number_prop(&item.get("GMT")) {
            Ok(gmt) => Some(gmt),
            Err(message) => Some(-3),
        },
        saved_data: match get_bool_prop(&item.get("saved_data")) {
            Ok(saved_data) => Some(saved_data),
            Err(_message) => { None }
        },
    };
    if telemetry.T0.len() != telemetry.L1.len() {
        return Err(format!("Invalid T0 length"));
    }
    if telemetry.T1.len() != telemetry.L1.len() {
        return Err(format!("Invalid T1 length"));
    }
    if telemetry.T2.len() != telemetry.L1.len() {
        return Err(format!("Invalid T2 length"));
    }
    if telemetry.P0.len() != telemetry.L1.len() {
        return Err(format!("Invalid P0 length"));
    }
    if telemetry.P1.len() != telemetry.L1.len() {
        return Err(format!("Invalid P1 length"));
    }

    return Ok(telemetry);
}

// pub fn get_raw_telemetry_pack_dac_aws_format(item: &serde_json::Value) -> Result<TelemetryPackDAC_v2,String> {
//     let item = match item.get("telemetry") {
//         None => return Err("ERROR230".to_owned()),
//         Some (v) => v,
//     };
//     return get_raw_telemetry_pack_dac(item);
// }
