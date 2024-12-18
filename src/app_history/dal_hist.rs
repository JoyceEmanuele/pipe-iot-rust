use super::cache_files::build_part_file_name;
use crate::compression::compiler_DAL::DALTelemetryCompiler;
use crate::lib_http::response::{respond_http_json, respond_http_plain_text};
use crate::lib_http::types::HttpResponse;
use crate::telemetry_payloads::dal_payload_json::get_raw_telemetry_pack_dal;
use crate::telemetry_payloads::dal_telemetry::split_pack;
use crate::GlobalVars;
use chrono::{Duration, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::io::Write;
use std::sync::Arc;

pub async fn process_comp_command_dal(
    rpars: ReqParameters,
    globs: &Arc<GlobalVars>,
) -> Result<HttpResponse, String> {
    let rpars_serialized = serde_json::to_string(&rpars).unwrap();
    let dev_id = rpars.dev_id;
    let interval_length_s = rpars.interval_length_s;
    let ts_ini = rpars.ts_ini;
    let i_ts_ini = rpars.i_ts_ini;
    let i_ts_end = rpars.i_ts_end;
    let ts_end = rpars.ts_end;
    let open_end = rpars.open_end;
    let timezone_offset = rpars.timezone_offset;
    let timezone_offset_string = timezone_offset
        .map(|v| v.to_string())
        .unwrap_or("None".to_string());
    // Create accumulators
    // Verificar na pasta se tem query pronta para o dia selecionado.
    let part_file_name = build_part_file_name(&dev_id, &ts_ini, &timezone_offset_string);
    let accs: Accumulators = {
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
                let tcomp = DALTelemetryCompiler::new(rpars.interval_length_s);
                Accumulators {
                    rpars: None,
                    page_ts_ini,
                    tcomp,
                    offset: timezone_offset,
                }
            }
        }
    };
    let page_ts_ini = accs.page_ts_ini;
    let mut tcomp = accs.tcomp;

    let mut table_name = {
        if (dev_id.len() == 12) && dev_id.to_uppercase().starts_with("DAL") {
            format!("{}XXXX_RAW", &dev_id.to_uppercase()[0..8])
        } else {
            String::new()
        }
    };

    for custom in &globs.configfile.CUSTOM_TABLE_NAMES_DAL {
        if dev_id.to_uppercase().starts_with(&custom.dev_prefix) {
            table_name = custom.table_name.to_owned();
            break;
        }
    }

    if table_name.len() == 0 {
        crate::LOG.append_log_tag_msg("WARN", &format!("Unknown DAL generation: {}", dev_id));
        return Ok(respond_http_json(200, "{}"));
    }

    let querier = crate::lib_dynamodb::query::QuerierDevIdTimestamp::new_diel_dev(
        table_name,
        dev_id.clone(),
        &globs.configfile.aws_config,
    );

    let mut found_invalid_payload = false;
    let result = querier
        .run(&ts_ini, &ts_end, &mut |items| {
            for item in items {
                let payload = match get_raw_telemetry_pack_dal(&item) {
                    Ok(v) => v,
                    Err(err) => {
                        if !found_invalid_payload {
                            crate::LOG.append_log_tag_msg(
                                "WARN",
                                &format!("Ignoring invalid payload(s): {} {:?}", &err, item),
                            );
                        }
                        found_invalid_payload = true;
                        continue;
                    }
                };
                let result = split_pack(&payload, i_ts_ini, i_ts_end, &mut |telemetry, index| {
                    tcomp.AdcPontos(telemetry, index);
                });
                match result {
                    Ok(()) => {}
                    Err(err) => {
                        if !found_invalid_payload {
                            crate::LOG.append_log_tag_msg(
                                "WARN",
                                &format!("Ignoring invalid payload(s): {} {:?}", &err, item),
                            );
                        }
                        found_invalid_payload = true;
                        continue;
                    }
                };
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
            return Ok(respond_http_plain_text(400, &format!("ERROR[78] {}", err)));
        }
    }

    let mut accs = Accumulators {
        rpars: Some(serde_json::from_str(&rpars_serialized).unwrap()),
        page_ts_ini,
        tcomp,
        offset: timezone_offset,
    };

    if (!rpars.avoid_cache) && accs.rpars.is_some() && (interval_length_s > 3000) {
        let serialized = serde_json::to_string(&accs).unwrap();
        match std::fs::File::create(&part_file_name) {
            Err(err) => {
                crate::LOG.append_log_tag_msg("ERROR", &format!("create failed: {}", err));
            }
            Ok(mut file) => {
                match file.write_all(serialized.as_bytes()) {
                    Err(err) => {
                        crate::LOG.append_log_tag_msg("ERROR", &format!("write failed: {}", err));
                    }
                    Ok(_) => {}
                };
            }
        };
    }

    let period_data = match accs.tcomp.CheckClosePeriod(if open_end {
        accs.tcomp.last_index + 1
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

    let mut data = serde_json::json!({});
    data["Mode"] = period_data.Mode.into();
    data["Relays"] = period_data.Relays.into();
    data["Feedback"] = period_data.Feedback.into();
    data["hoursOnline"] = period_data.hoursOnline.into();

    data["provision_error"] = provision_error.into();

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
        return Err(respond_http_plain_text(400, "dev_id.len() < 9"));
    }

    let (interval_length_s, mut ts_ini) = {
        if let Some(day) = parsed["day"].as_str() {
            let interval_length_s = 24 * 60 * 60;
            let ts_ini = format!("{}T00:00:00", day);
            (interval_length_s, ts_ini)
        } else {
            let interval_length_s = match parsed["interval_length_s"].as_i64() {
                Some(v) => v,
                None => {
                    return Err(respond_http_plain_text(400, "Missing interval_length_s"));
                }
            };
            let ts_ini = match parsed["ts_ini"].as_str() {
                Some(v) => v,
                None => {
                    return Err(respond_http_plain_text(400, "Missing ts_ini"));
                }
            };
            (interval_length_s, ts_ini.to_owned())
        }
    };

    let max_interval = 7 * 24 * 60 * 60; // 7 days
    let min_interval = 1 * 60; // 1 minute
    if interval_length_s > max_interval {
        return Err(respond_http_plain_text(400, "Interval too long"));
    }
    if interval_length_s < min_interval {
        return Err(respond_http_plain_text(400, "Invalid interval"));
    }
    if interval_length_s >= min_interval && interval_length_s <= max_interval {
    } else {
        return Err(respond_http_plain_text(400, "Invalid interval"));
    }

    let timezone_offset: Option<i64> = parsed["timezoneOffset"].as_i64();

    let i_ts_ini = match NaiveDateTime::parse_from_str(&ts_ini, "%Y-%m-%dT%H:%M:%S") {
        Err(err) => {
            crate::LOG.append_log_tag_msg("ERROR", &format!("{} {}", &ts_ini, err));
            return Err(respond_http_plain_text(400, "Error parsing Date"));
        }
        Ok(date) => date.timestamp(),
    };

    ts_ini = NaiveDateTime::from_timestamp(i_ts_ini, 0)
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string();

    let i_ts_end = i_ts_ini + interval_length_s;
    let ts_end = NaiveDateTime::from_timestamp(i_ts_end, 0)
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string();

    let open_end = parsed["open_end"].as_bool().unwrap_or(false);
    let avoid_cache = parsed["avoid_cache"].as_bool().unwrap_or(false);

    return Ok(ReqParameters {
        dev_id: dev_id.to_string(),
        interval_length_s,
        ts_ini: ts_ini.to_string(),
        i_ts_ini,
        i_ts_end,
        ts_end,
        open_end,
        avoid_cache,
        timezone_offset,
    });
}

fn load_partial_query(path: &str, rpars_serialized: &str) -> Result<Accumulators, String> {
    let serialized = match std::fs::read_to_string(path) {
        Ok(v) => v,
        Err(err) => return Err(format!("{}", err)),
    };
    // let serialized = serde_json::to_string(&(&page_ts_ini, &fchk, &tcomp, &fcomp)).unwrap();
    let accs = match serde_json::from_str::<Accumulators>(&serialized) {
        Err(err) => return Err(format!("{}", err)),
        Ok(v) => v,
    };
    let rpars_check = serde_json::to_string(&accs.rpars).unwrap();
    if rpars_check == rpars_serialized {
        return Ok(accs);
    } else {
        return Err("Query is not the same".to_string());
    }
}

#[derive(Serialize, Deserialize, Debug)]
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

#[derive(Serialize, Deserialize)]
struct Accumulators {
    pub rpars: Option<ReqParameters>,
    pub page_ts_ini: String,
    pub tcomp: DALTelemetryCompiler,
    pub offset: Option<i64>,
}
