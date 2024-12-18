use crate::diel_hist_tables::BigQueryHistoryTable;
use crate::helpers::telemetry_payloads::parse_json_props::{
    get_bool_array_prop_2, get_float_number_array_prop, get_int_number_array_prop,
    get_int_number_prop,
};
use crate::lib_bigquery::client::RowBQ;
use crate::lib_bigquery::saver::{push_row_to_storage, push_rows_to_storage};
use crate::GlobalVars;
use chrono::DateTime;
use std::sync::atomic::Ordering;
use std::sync::Arc;

pub async fn save_telemetry_to_bigquery(
    topic: &str,
    payload: serde_json::Value,
    dev_id: String,
    pack_ts: i64,
    gmt: i64,
    globs: &Arc<GlobalVars>,
) {
    let table_name = match find_bigquery_table_name(globs, &dev_id) {
        Some(x) => x,
        None => {
            return;
        }
    };

    let is_dac_telemetry = dev_id.starts_with("DAC") && topic.starts_with("data/dac");
    let is_dut_telemetry = dev_id.starts_with("DUT") && topic.starts_with("data/dut");

    let rows_count;
    let result = {
        if is_dac_telemetry {
            let rows = match dividir_telemetria_dac(&payload, &dev_id, pack_ts, gmt) {
                Ok(x) => x,
                Err(err) => {
                    crate::LOG.append_log_tag_msg_v2(
                        "ERROR",
                        &format!("[36] {} {} {:?}", topic, err, payload),
                        false,
                    );
                    globs
                        .stats
                        .payloads_discarded
                        .fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            rows_count = rows.len();
            push_rows_to_storage(&globs.to_bigquery, table_name.to_owned(), rows).await
        } else if is_dut_telemetry {
            let rows = match dividir_telemetria_dut(&payload, &dev_id, pack_ts, gmt) {
                Ok(x) => x,
                Err(err) => {
                    crate::LOG.append_log_tag_msg_v2(
                        "ERROR",
                        &format!("[36] {} {} {:?}", topic, err, payload),
                        false,
                    );
                    globs
                        .stats
                        .payloads_discarded
                        .fetch_add(1, Ordering::Relaxed);
                    return;
                }
            };
            rows_count = rows.len();
            push_rows_to_storage(&globs.to_bigquery, table_name.to_owned(), rows).await
        } else {
            let timestamp_tz = timestamp_from_ts_tz(pack_ts, gmt);
            let row = RowBQ {
                payload: payload.to_string(),
                dev_id,
                timestamp: timestamp_tz,
            };
            rows_count = 1;
            push_row_to_storage(&globs.to_bigquery, table_name.to_owned(), row).await
        }
    };
    if let Err(err) = result {
        globs
            .stats
            .payloads_with_insert_error
            .fetch_add(1, Ordering::Relaxed);
        crate::LOG.append_log_tag_msg("ERROR", &format!("[25] {} {} {:?}", topic, err, payload));
    } else {
        globs
            .stats
            .bq_rows_inserted
            .fetch_add(rows_count, Ordering::Relaxed);
        globs
            .stats
            .bigquery_insertions
            .fetch_add(1, Ordering::Relaxed);
    }
}

fn find_bigquery_table_name(globs: &Arc<GlobalVars>, dev_id: &str) -> Option<String> {
    match &globs.configfile.gcp_dest_table {
        BigQueryHistoryTable::None => None,
        BigQueryHistoryTable::SingleTable(table_id) => Some(table_id.to_owned()),
        BigQueryHistoryTable::DevType => {
            // let is_valid = globs.valid_dev_type_checker.is_match(dev_id);
            let is_valid = globs.valid_dev_id_checker.is_match(dev_id);
            if !is_valid {
                return None;
            }
            return Some(format!("{}_telemetry", dev_id[..3].to_lowercase()));
        }
        BigQueryHistoryTable::DevGeneration => {
            let is_valid = globs.valid_dev_id_checker.is_match(dev_id);
            if !is_valid {
                return None;
            }
            return Some(format!("{}_telemetry", dev_id[..8].to_uppercase()));
        }
        BigQueryHistoryTable::DevId => {
            let is_valid = globs.valid_dev_id_checker.is_match(dev_id);
            if !is_valid {
                return None;
            }
            return Some(dev_id.to_uppercase());
        }
    }
}

fn timestamp_from_ts_tz(unix_ts: i64, gmt: i64) -> String {
    DateTime::from_timestamp(unix_ts - gmt * 3600, 0)
        .unwrap()
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string()
}

fn dividir_telemetria_dac(
    payload: &serde_json::Value,
    dev_id: &str,
    pack_ts: i64,
    gmt: i64,
) -> Result<Vec<RowBQ>, String> {
    let sampling_time = get_int_number_prop(&payload.get("samplingTime")).unwrap_or(1); // de quantos em quantos segundos o firmware lê os sensores e insere nos vetores.

    let L1 = match &payload.get("L1") {
        None => None,
        Some(prop) => match get_bool_array_prop_2(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid L1: {}", err));
            }
        },
    };
    let T0 = match &payload.get("T0") {
        None => None,
        Some(prop) => match get_float_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid T0: {}", err));
            }
        },
    };
    let T1 = match &payload.get("T1") {
        None => None,
        Some(prop) => match get_float_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid T1: {}", err));
            }
        },
    };
    let T2 = match &payload.get("T2") {
        None => None,
        Some(prop) => match get_float_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid T2: {}", err));
            }
        },
    };
    let P0 = match &payload.get("P0") {
        None => None,
        Some(prop) => match get_int_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid P0: {}", err));
            }
        },
    };
    let P1 = match &payload.get("P1") {
        None => None,
        Some(prop) => match get_int_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid P1: {}", err));
            }
        },
    };
    let Lcmp = match &payload.get("Lcmp") {
        None => None,
        Some(prop) => match get_int_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid Lcmp: {}", err));
            }
        },
    };
    let Lcut = match &payload.get("Lcut") {
        None => None,
        Some(prop) => match get_int_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid Lcut: {}", err));
            }
        },
    };
    let Levp = match &payload.get("Levp") {
        None => None,
        Some(prop) => match get_int_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid Levp: {}", err));
            }
        },
    };
    let Tamb = match &payload.get("Tamb") {
        None => None,
        Some(prop) => match get_float_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid Tamb: {}", err));
            }
        },
    };
    let Tsuc = match &payload.get("Tsuc") {
        None => None,
        Some(prop) => match get_float_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid Tsuc: {}", err));
            }
        },
    };
    let Tliq = match &payload.get("Tliq") {
        None => None,
        Some(prop) => match get_float_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid Tliq: {}", err));
            }
        },
    };
    let Psuc = match &payload.get("Psuc") {
        None => None,
        Some(prop) => match get_float_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid Psuc: {}", err));
            }
        },
    };
    let Pliq = match &payload.get("Pliq") {
        None => None,
        Some(prop) => match get_float_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid Pliq: {}", err));
            }
        },
    };
    let Tsc = match &payload.get("Tsc") {
        None => None,
        Some(prop) => match get_float_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid Tsc: {}", err));
            }
        },
    };
    let Tsh = match &payload.get("Tsh") {
        None => None,
        Some(prop) => match get_float_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid Tsh: {}", err));
            }
        },
    };

    let pack_len = L1
        .as_ref()
        .and_then(|v| Some(v.len()))
        .or_else(|| T0.as_ref().and_then(|v| Some(v.len())))
        .or_else(|| T1.as_ref().and_then(|v| Some(v.len())))
        .or_else(|| T2.as_ref().and_then(|v| Some(v.len())))
        .or_else(|| P0.as_ref().and_then(|v| Some(v.len())))
        .or_else(|| P1.as_ref().and_then(|v| Some(v.len())));

    let pack_len = match pack_len {
        Some(pack_len) => pack_len,
        None => {
            return Err(format!("DAC telemetry is not a pack"));
        }
    };

    if let Some(L1) = L1.as_ref() {
        if L1.len() != pack_len {
            return Err(format!("Invalid L1 length"));
        }
    }
    if let Some(T0) = T0.as_ref() {
        if T0.len() != pack_len {
            return Err(format!("Invalid T0 length"));
        }
    }
    if let Some(T1) = T1.as_ref() {
        if T1.len() != pack_len {
            return Err(format!("Invalid T1 length"));
        }
    }
    if let Some(T2) = T2.as_ref() {
        if T2.len() != pack_len {
            return Err(format!("Invalid T2 length"));
        }
    }
    if let Some(P0) = P0.as_ref() {
        if P0.len() != pack_len {
            return Err(format!("Invalid P0 length"));
        }
    }
    if let Some(P1) = P1.as_ref() {
        if P1.len() != pack_len {
            return Err(format!("Invalid P1 length"));
        }
    }
    if let Some(Lcmp) = Lcmp.as_ref() {
        if Lcmp.len() != pack_len {
            return Err(format!("Invalid Lcmp length"));
        }
    }
    if let Some(Lcut) = Lcut.as_ref() {
        if Lcut.len() != pack_len {
            return Err(format!("Invalid Lcut length"));
        }
    }
    if let Some(Levp) = Levp.as_ref() {
        if Levp.len() != pack_len {
            return Err(format!("Invalid Levp length"));
        }
    }
    if let Some(Tamb) = Tamb.as_ref() {
        if Tamb.len() != pack_len {
            return Err(format!("Invalid Tamb length"));
        }
    }
    if let Some(Tsuc) = Tsuc.as_ref() {
        if Tsuc.len() != pack_len {
            return Err(format!("Invalid Tsuc length"));
        }
    }
    if let Some(Tliq) = Tliq.as_ref() {
        if Tliq.len() != pack_len {
            return Err(format!("Invalid Tliq length"));
        }
    }
    if let Some(Psuc) = Psuc.as_ref() {
        if Psuc.len() != pack_len {
            return Err(format!("Invalid Psuc length"));
        }
    }
    if let Some(Pliq) = Pliq.as_ref() {
        if Pliq.len() != pack_len {
            return Err(format!("Invalid Pliq length"));
        }
    }
    if let Some(Tsc) = Tsc.as_ref() {
        if Tsc.len() != pack_len {
            return Err(format!("Invalid Tsc length"));
        }
    }
    if let Some(Tsh) = Tsh.as_ref() {
        if Tsh.len() != pack_len {
            return Err(format!("Invalid Tsh length"));
        }
    }

    let mut telemetry = payload.clone();

    let mut rows: Vec<RowBQ> = Vec::with_capacity(pack_len);
    for index in 0..pack_len {
        let remaining_steps = pack_len - index - 1;
        let telm_ts = pack_ts - ((remaining_steps as i64) * sampling_time);
        let timestamp_tz = timestamp_from_ts_tz(telm_ts, gmt);
        telemetry["timestamp"] = timestamp_from_ts_tz(telm_ts, 0).into();
        if let Some(L1) = L1.as_ref() {
            telemetry["L1"] = L1[index].into();
        }
        if let Some(T0) = T0.as_ref() {
            telemetry["T0"] = T0[index].into();
        }
        if let Some(T1) = T1.as_ref() {
            telemetry["T1"] = T1[index].into();
        }
        if let Some(T2) = T2.as_ref() {
            telemetry["T2"] = T2[index].into();
        }
        if let Some(P0) = P0.as_ref() {
            telemetry["P0"] = P0[index].into();
        }
        if let Some(P1) = P1.as_ref() {
            telemetry["P1"] = P1[index].into();
        }
        if let Some(Lcmp) = Lcmp.as_ref() {
            telemetry["Lcmp"] = Lcmp[index].into();
        }
        if let Some(Lcut) = Lcut.as_ref() {
            telemetry["Lcut"] = Lcut[index].into();
        }
        if let Some(Levp) = Levp.as_ref() {
            telemetry["Levp"] = Levp[index].into();
        }
        if let Some(Tamb) = Tamb.as_ref() {
            telemetry["Tamb"] = Tamb[index].into();
        }
        if let Some(Tsuc) = Tsuc.as_ref() {
            telemetry["Tsuc"] = Tsuc[index].into();
        }
        if let Some(Tliq) = Tliq.as_ref() {
            telemetry["Tliq"] = Tliq[index].into();
        }
        if let Some(Psuc) = Psuc.as_ref() {
            telemetry["Psuc"] = Psuc[index].into();
        }
        if let Some(Pliq) = Pliq.as_ref() {
            telemetry["Pliq"] = Pliq[index].into();
        }
        if let Some(Tsc) = Tsc.as_ref() {
            telemetry["Tsc"] = Tsc[index].into();
        }
        if let Some(Tsh) = Tsh.as_ref() {
            telemetry["Tsh"] = Tsh[index].into();
        }

        let row = RowBQ {
            payload: telemetry.to_string(),
            dev_id: dev_id.to_owned(),
            timestamp: timestamp_tz,
        };
        rows.push(row);
    }
    Ok(rows)
}

fn dividir_telemetria_dut(
    payload: &serde_json::Value,
    dev_id: &str,
    pack_ts: i64,
    gmt: i64,
) -> Result<Vec<RowBQ>, String> {
    let sampling_time = get_int_number_prop(&payload.get("samplingTime")).unwrap_or(5); // de quantos em quantos segundos o firmware lê os sensores e insere nos vetores.

    let Temperature = match &payload.get("Temperature") {
        None => None,
        Some(prop) => match get_float_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid Temperature: {}", err));
            }
        },
    };
    let Temperature_1 = match &payload.get("Temperature_1") {
        None => None,
        Some(prop) => match get_float_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid Temperature_1: {}", err));
            }
        },
    };
    let Tmp = match &payload.get("Tmp") {
        None => None,
        Some(prop) => match get_float_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid Tmp: {}", err));
            }
        },
    };
    let Humidity = match &payload.get("Humidity") {
        None => None,
        Some(prop) => match get_float_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid Humidity: {}", err));
            }
        },
    };
    let eCO2 = match &payload.get("eCO2") {
        None => None,
        Some(prop) => match get_int_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid eCO2: {}", err));
            }
        },
    };
    let raw_eCO2 = match &payload.get("raw_eCO2") {
        None => None,
        Some(prop) => match get_int_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid raw_eCO2: {}", err));
            }
        },
    };
    let TVOC = match &payload.get("TVOC") {
        None => None,
        Some(prop) => match get_int_number_array_prop(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid TVOC: {}", err));
            }
        },
    };
    let L1 = match &payload.get("L1") {
        None => None,
        Some(prop) => match get_bool_array_prop_2(prop) {
            Ok(x) => Some(x),
            Err(err) => {
                return Err(format!("Invalid L1: {}", err));
            }
        },
    };

    let pack_len = Temperature
        .as_ref()
        .and_then(|v| Some(v.len()))
        .or_else(|| Temperature_1.as_ref().and_then(|v| Some(v.len())))
        .or_else(|| Tmp.as_ref().and_then(|v| Some(v.len())))
        .or_else(|| Humidity.as_ref().and_then(|v| Some(v.len())))
        .or_else(|| eCO2.as_ref().and_then(|v| Some(v.len())))
        .or_else(|| raw_eCO2.as_ref().and_then(|v| Some(v.len())))
        .or_else(|| TVOC.as_ref().and_then(|v| Some(v.len())));

    let pack_len = match pack_len {
        Some(pack_len) => pack_len,
        None => {
            return Err(format!("DUT telemetry is not a pack"));
        }
    };

    if let Some(Temperature) = Temperature.as_ref() {
        if Temperature.len() != pack_len {
            return Err(format!("Invalid Temperature length"));
        }
    }
    if let Some(Temperature_1) = Temperature_1.as_ref() {
        if Temperature_1.len() != pack_len {
            return Err(format!("Invalid Temperature_1 length"));
        }
    }
    if let Some(Tmp) = Tmp.as_ref() {
        if Tmp.len() != pack_len {
            return Err(format!("Invalid Tmp length"));
        }
    }
    if let Some(Humidity) = Humidity.as_ref() {
        if Humidity.len() != pack_len {
            return Err(format!("Invalid Humidity length"));
        }
    }
    if let Some(eCO2) = eCO2.as_ref() {
        if eCO2.len() != pack_len {
            return Err(format!("Invalid eCO2 length"));
        }
    }
    if let Some(raw_eCO2) = raw_eCO2.as_ref() {
        if raw_eCO2.len() != pack_len {
            return Err(format!("Invalid raw_eCO2 length"));
        }
    }
    if let Some(TVOC) = TVOC.as_ref() {
        if TVOC.len() != pack_len {
            return Err(format!("Invalid TVOC length"));
        }
    }
    if let Some(L1) = L1.as_ref() {
        if L1.len() != pack_len {
            return Err(format!("Invalid L1 length"));
        }
    }

    let mut telemetry = payload.clone();

    let mut rows: Vec<RowBQ> = Vec::with_capacity(pack_len);
    for index in 0..pack_len {
        let remaining_steps = pack_len - index - 1;
        let telm_ts = pack_ts - ((remaining_steps as i64) * sampling_time);
        let timestamp_tz = timestamp_from_ts_tz(telm_ts, gmt);
        telemetry["timestamp"] = timestamp_from_ts_tz(telm_ts, 0).into();
        if let Some(Temperature) = Temperature.as_ref() {
            telemetry["Temperature"] = Temperature[index].into();
        }
        if let Some(Temperature_1) = Temperature_1.as_ref() {
            telemetry["Temperature_1"] = Temperature_1[index].into();
        }
        if let Some(Tmp) = Tmp.as_ref() {
            telemetry["Tmp"] = Tmp[index].into();
        }
        if let Some(Humidity) = Humidity.as_ref() {
            telemetry["Humidity"] = Humidity[index].into();
        }
        if let Some(eCO2) = eCO2.as_ref() {
            telemetry["eCO2"] = eCO2[index].into();
        }
        if let Some(raw_eCO2) = raw_eCO2.as_ref() {
            telemetry["raw_eCO2"] = raw_eCO2[index].into();
        }
        if let Some(TVOC) = TVOC.as_ref() {
            telemetry["TVOC"] = TVOC[index].into();
        }
        if let Some(L1) = L1.as_ref() {
            telemetry["L1"] = L1[index].into();
        }

        let row = RowBQ {
            payload: telemetry.to_string(),
            dev_id: dev_id.to_owned(),
            timestamp: timestamp_tz,
        };
        rows.push(row);
    }
    Ok(rows)
}
