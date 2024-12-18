use super::cache_files::build_part_file_name;
use crate::compression::compiler_DMA::DMATelemetryCompiler;
use crate::lib_http::response::{respond_http_json, respond_http_plain_text};
use crate::lib_http::types::HttpResponse;
use crate::telemetry_payloads::dma_payload_json::get_raw_telemetry_pack_dma;
use crate::telemetry_payloads::dma_telemetry::split_pack;
use crate::GlobalVars;
use chrono::{Duration, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;

pub async fn process_comp_command_dma(
    rpars: ReqParameters,
    globs: &Arc<GlobalVars>,
) -> Result<HttpResponse, String> {
    let rpars_serialized = serde_json::to_string(&rpars).unwrap();
    let dev_id = rpars.dev_id;
    let interval_length_s = rpars.interval_length_s;
    let ts_ini = rpars.ts_ini; // start datetime
    let i_ts_ini: i64 = rpars.i_ts_ini; // start timestamp (string)
    let ts_end = rpars.ts_end; // end datetime (end of the day selected)
    let i_ts_end = rpars.i_ts_end; // end timestamp (string)
    let timezone_offset = rpars.timezone_offset; // timezone filtered
    let open_end = rpars.open_end;
    let timezone_offset_string = timezone_offset
        .map(|v| v.to_string())
        .unwrap_or("None".to_string());

    // Create accumulators
    // Verificar na pasta se tem query pronta para o dia selecionado.
    let part_file_name = build_part_file_name(&dev_id, &ts_ini, &timezone_offset_string);
    let accs: DmaData = {
        let r = if rpars.avoid_cache {
            Err(String::from("Cache desabilitado"))
        } else {
            Ok(())
        }
        .and_then(|_| load_partial_query(&part_file_name, &rpars_serialized));
        match r {
            Ok(v) => v,
            Err(_err) => {
                let page_ts_ini = ts_ini.clone();
                let tcomp = DMATelemetryCompiler::new(rpars.interval_length_s);
                DmaData {
                    rpars: None,
                    page_ts_ini,
                    tcomp,
                    telemetryList: [].to_vec(),
                    timeOfTheLastTelemetry: "".to_string(),
                }
            }
        }
    };
    let page_ts_ini = accs.page_ts_ini;
    let mut tcomp = accs.tcomp;

    let mut table_name = {
        if (dev_id.len() == 12) && dev_id.to_uppercase().starts_with("DMA") {
            format!("{}XXXX_RAW", &dev_id.to_uppercase()[0..8])
        } else {
            String::new()
        }
    };

    for custom in &globs.configfile.CUSTOM_TABLE_NAMES_DMA {
        if dev_id.to_uppercase().starts_with(&custom.dev_prefix) {
            table_name = custom.table_name.to_owned();
            break;
        }
    }

    if table_name.len() == 0 {
        crate::LOG.append_log_tag_msg("WARN", &format!("Unknown DMA generation: {}", dev_id));
        return Ok(respond_http_json(200, "{}"));
    }

    let querier = crate::lib_dynamodb::query::QuerierDevIdTimestamp::new_diel_dev(
        table_name,
        dev_id.clone(),
        &globs.configfile.aws_config,
    );

    let mut found_invalid_payload = false;
    let mut is_first_of_the_day: bool = true;

    let currentDayQuery = NaiveDateTime::from_timestamp(i_ts_ini + 900, 0)
        .format("%d")
        .to_string();
    found_invalid_payload = false;
    let mut last_number_of_pulses: Option<i32> = None;
    let mut pulsesPerHour: HashMap<String, i32> = HashMap::new();
    let mut lastTelemetryTime: String = "".to_string();
    let result = querier
        .run(&ts_ini, &ts_end, &mut |items| {
            for i in 1..items.len() {
                let mut payload = match get_raw_telemetry_pack_dma(&items[i]) {
                    Ok(v) => v,
                    Err(err) => {
                        if !found_invalid_payload {
                            crate::LOG.append_log_tag_msg(
                                "WARN",
                                &format!("Ignoring invalid payload(s): {}", &err),
                            );
                        }
                        found_invalid_payload = true;
                        continue;
                    }
                };

                let result = split_pack(
                    &payload,
                    i_ts_ini + 900,
                    i_ts_end,
                    &mut |telemetry, index| {
                        tcomp.AdcPontos(telemetry, index);
                    },
                );

                match result {
                    Ok(()) => {}
                    Err(err) => {
                        if !found_invalid_payload {
                            crate::LOG.append_log_tag_msg(
                                "WARN",
                                &format!("Ignoring invalid payload(s): {} {:?}", &err, items[i]),
                            );
                        }
                        found_invalid_payload = true;
                        continue;
                    }
                };

                let payload_pulses = match payload.pulses {
                    Some(v) => v,
                    None => {
                        // Ignorar telemetria sem valor
                        continue;
                    }
                };

                if currentDayQuery == payload.timestamp[8..10] && last_number_of_pulses.is_some() {
                    payload.pulses = Some(payload_pulses - last_number_of_pulses.unwrap());

                    lastTelemetryTime = payload.timestamp.clone();

                    pulsesPerHour
                        .entry(format!("{}:00", payload.timestamp[11..13].to_string()))
                        .and_modify(|data| *data += payload.pulses.unwrap_or(0))
                        .or_insert(payload.pulses.unwrap_or(0));

                    is_first_of_the_day = false;
                }

                last_number_of_pulses = Some(payload_pulses);
            }
            return Ok(());
        })
        .await;

    let mut provision_error = false;
    if let Err(err) = result {
        if err.starts_with("ProvisionedThroughputExceeded:") {
            provision_error = true;
        } else if err.starts_with("ResourceNotFound:") {
            crate::LOG.append_log_tag_msg("WARN", &format!("Table not found for: {}", dev_id));
            return Ok(respond_http_json(200, "{}"));
        } else {
            return Ok(respond_http_plain_text(400, &format!("ERROR[117] {}", err)));
        }
    }

    let labels = vec![
        "00:00", "01:00", "02:00", "03:00", "04:00", "05:00", "06:00", "07:00", "08:00", "09:00",
        "10:00", "11:00", "12:00", "13:00", "14:00", "15:00", "16:00", "17:00", "18:00", "19:00",
        "20:00", "21:00", "22:00", "23:00",
    ];
    let mut formattedPulses: Vec<TelemetryPerTime> = Vec::new();

    for label in labels {
        formattedPulses.push(TelemetryPerTime {
            time: label.to_string(),
            pulses: match pulsesPerHour.get(&label.to_string()) {
                Some(v) => *v as i32,
                None => 0,
            },
        });
    }

    let mut dma_query_data = DmaData {
        rpars: Some(serde_json::from_str(&rpars_serialized).unwrap()),
        page_ts_ini,
        tcomp,
        timeOfTheLastTelemetry: lastTelemetryTime.to_string(),
        telemetryList: formattedPulses.clone(),
    };

    if (!rpars.avoid_cache && accs.rpars.is_some() && (interval_length_s > 3000)) {
        let serialized = serde_json::to_string(&dma_query_data).unwrap();
        match std::fs::File::create(&part_file_name) {
            Err(err) => {
                crate::LOG.append_log_tag_msg("ERROR", &format!("create failed: {}", err));
            }
            Ok(mut file) => {
                if let Err(err) = file.write_all(serialized.as_bytes()) {
                    crate::LOG.append_log_tag_msg("ERROR", &format!("write failed: {}", err));
                }
            }
        };
    }

    let period_data = match dma_query_data.tcomp.CheckClosePeriod(if open_end {
        dma_query_data.tcomp.last_index + 1
    } else {
        isize::try_from(interval_length_s).unwrap()
    }) {
        Err(err) => {
            crate::LOG.append_log_tag_msg("ERROR", &format!("{}", err));
            return Ok(respond_http_plain_text(400, "ERROR[120] CheckClosePeriod"));
        }
        Ok(v) => match v {
            Some(v) => v,
            None => {
                return Ok(respond_http_json(200, "{}"));
            }
        },
    };

    let data = serde_json::json!({
      "TelemetryList": formattedPulses,
      "timeOfTheLastTelemetry": lastTelemetryTime,
      "hoursOnline": period_data.hoursOnline,
      "provision_error": provision_error
    });

    return Ok(respond_http_json(200, &data.to_string()));
}

pub fn parse_parameters(parsed: &serde_json::Value) -> Result<ReqParameters, HttpResponse> {
    let dev_id = match parsed["dev_id"].as_str() {
        Some(v) => v,
        None => {
            return Err(respond_http_plain_text(400, "Missing dev_id"));
        }
    };
    if dev_id.len() < 9 {
        return Err(respond_http_plain_text(400, "ERROR[169] dev_id.len() < 9"));
    }

    let interval_length_s = 24 * 60 * 60;

    let mut ts_ini = match parsed["day"].as_str() {
        Some(v) => v,
        None => {
            return Err(respond_http_plain_text(400, "Missing day"));
        }
    };

    let timezone_offset: Option<i64> = parsed["timezoneOffset"].as_i64();

    let mut i_ts_ini =
        match NaiveDateTime::parse_from_str(&format!("{}T00:00:00", ts_ini), "%Y-%m-%dT%H:%M:%S") {
            Err(err) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("{} {}", &format!("{}T00:00:00", ts_ini), err),
                );
                return Err(respond_http_plain_text(400, "Error parsing Date"));
            }
            Ok(date) => date.timestamp(),
        };

    let ts_ini_aux = NaiveDateTime::from_timestamp(i_ts_ini, 0)
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string();
    ts_ini = &ts_ini_aux;

    i_ts_ini -= 900;
    let new_ts_ini = NaiveDateTime::from_timestamp(i_ts_ini, 0)
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string();
    let i_ts_end = i_ts_ini + interval_length_s + 900;
    let ts_end = NaiveDateTime::from_timestamp(i_ts_end, 0)
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string();

    let open_end = parsed["open_end"].as_bool().unwrap_or(false);
    let avoid_cache = parsed["avoid_cache"].as_bool().unwrap_or(false);

    return Ok(ReqParameters {
        dev_id: dev_id.to_string(),
        interval_length_s,
        ts_ini: new_ts_ini,
        i_ts_ini,
        i_ts_end,
        ts_end,
        open_end,
        avoid_cache,
        timezone_offset,
    });
}

fn load_partial_query(path: &str, rpars_serialized: &str) -> Result<DmaData, String> {
    let serialized = match std::fs::read_to_string(path) {
        Ok(v) => v,
        Err(err) => return Err(format!("{}", err)),
    };
    // let serialized = serde_json::to_string(&(&page_ts_ini, &fchk, &tcomp, &fcomp)).unwrap();
    let r = match serde_json::from_str::<DmaData>(&serialized) {
        Err(err) => return Err(format!("{}", err)),
        Ok(v) => v,
    };
    let rpars_check = serde_json::to_string(&r.rpars).unwrap();
    if rpars_check == rpars_serialized {
        return Ok(r);
    } else {
        return Err("Query is not the same".to_string());
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct ReqParameters {
    pub dev_id: String,
    pub interval_length_s: i64,
    pub ts_ini: String,
    pub i_ts_ini: i64,
    pub i_ts_end: i64,
    pub ts_end: String,
    pub open_end: bool,
    pub avoid_cache: bool,
    pub timezone_offset: Option<i64>,
}

#[derive(Serialize, Deserialize, Clone)]
struct TelemetryPerTime {
    time: String,
    pulses: i32,
}

#[derive(Serialize, Deserialize)]
struct DmaData {
    pub rpars: Option<ReqParameters>,
    pub timeOfTheLastTelemetry: String,
    pub telemetryList: Vec<TelemetryPerTime>,
    pub page_ts_ini: String,
    pub tcomp: DMATelemetryCompiler,
}
