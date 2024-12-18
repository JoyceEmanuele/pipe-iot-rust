use super::cache_files::build_part_file_name;
use crate::compression::compiler_DUT::DUTTelemetryCompiler;
use crate::l1_virtual::dut_l1::l1_calc::create_l1_calculator;
use crate::lib_http::response::{respond_http_json, respond_http_plain_text};
use crate::lib_http::types::HttpResponse;
use crate::telemetry_payloads::dut_payload_json::get_raw_telemetry_pack_dut;
use crate::telemetry_payloads::dut_telemetry::{split_pack, HwInfoDUT};
use crate::GlobalVars;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::io::Write;
use std::sync::Arc;

pub async fn process_comp_command_dut(
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
    let offset_temp = rpars.offset_temp;
    let dev = HwInfoDUT {
        temperature_offset: offset_temp,
    };
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
                let tcomp = DUTTelemetryCompiler::new();
                Accumulators {
                    rpars: None,
                    page_ts_ini,
                    tcomp,
                    timezone_offset,
                }
            }
        }
    };
    let page_ts_ini = accs.page_ts_ini;
    let mut tcomp = accs.tcomp;

    let mut table_name = {
        if (dev_id.len() == 12) && dev_id.to_uppercase().starts_with("DUT") {
            format!("{}XXXX_RAW", &dev_id.to_uppercase()[0..8])
        } else {
            String::new()
        }
    };

    for custom in &globs.configfile.CUSTOM_TABLE_NAMES_DUT {
        if dev_id.to_uppercase().starts_with(&custom.dev_prefix) {
            table_name = custom.table_name.to_owned();
            break;
        }
    }

    if table_name.len() == 0 {
        crate::LOG.append_log_tag_msg("WARN", &format!("Unknown DUT generation: {}", dev_id));
        return Ok(respond_http_json(200, "{}"));
    }

    let mut dut_l1_calc = create_l1_calculator(&dev);

    let querier = crate::lib_dynamodb::query::QuerierDevIdTimestamp::new_diel_dev(
        table_name,
        dev_id.clone(),
        &globs.configfile.aws_config,
    );
    let mut found_invalid_payload = false;
    let result = querier
        .run(&ts_ini, &ts_end, &mut |items| {
            for item in items {
                let payload = match get_raw_telemetry_pack_dut(&item) {
                    Ok(v) => v,
                    Err(err) => {
                        // return Ok(respond_http_plain_text(400, &format!("ERROR[130] {}", err)));
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
                let result = split_pack(
                    &payload,
                    i_ts_ini,
                    i_ts_end,
                    &mut dut_l1_calc,
                    &mut |telemetry, index| {
                        tcomp.AdcPontos(telemetry, index);
                    },
                    &dev,
                );
                match result {
                    Ok(()) => {}
                    Err(err) => {
                        // return Err(format!("ERROR[136] {}", err));
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
            return Ok(respond_http_plain_text(400, &format!("ERROR[117] {}", err)));
        }
    }

    let mut accs = Accumulators {
        rpars: Some(serde_json::from_str(&rpars_serialized).unwrap()),
        page_ts_ini,
        tcomp,
        timezone_offset,
    };
    if (!rpars.avoid_cache) && accs.rpars.is_some() && (interval_length_s > 3000) {
        let serialized = serde_json::to_string(&accs).unwrap();
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

    // return this.CheckClosePeriod(index ? (index + 1) : interval_length_s);
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

    let data = serde_json::json!({
      "Temp": period_data.Temp,
      "Temp1": period_data.Temp1,
      "Hum": period_data.Hum,
      "State": period_data.State,
      "Mode": period_data.Mode,
      "eCO2": period_data.e_co2,
      "TVOC": period_data.tvoc,
      "L1": period_data.l1,
      "hoursOnL1": period_data.hoursOnL1,
      "hoursOffL1": period_data.hoursOffL1,
      "provision_error": provision_error,
      "numDeparts": period_data.numDeparts,
      "hoursOnline": period_data.hoursOnline,
    });
    // data["Temp"] = period_data.Temp.into();
    // data["Hum"] = period_data.Hum.into();
    // data["State"] = period_data.State.into();
    // data["Mode"] = period_data.Mode.into();
    // data["eCO2"] = serde_json::Value::from(period_data.e_co2);
    // data["TVOC"] = serde_json::Value::from(period_data.tvoc);
    // data["provision_error"] = provision_error.into();

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

    let i_ts_ini =
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

    let i_ts_end = i_ts_ini + interval_length_s;
    let ts_end = NaiveDateTime::from_timestamp(i_ts_end + 120, 0)
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string();

    let open_end = parsed["open_end"].as_bool().unwrap_or(false);
    let avoid_cache = parsed["avoid_cache"].as_bool().unwrap_or(false);
    let timezone_offset: Option<i64> = parsed["timezoneOffset"].as_i64();
    let offset_temp = parsed["offset_temp"].as_f64().unwrap_or(0.0);

    return Ok(ReqParameters {
        dev_id: dev_id.to_string(),
        interval_length_s,
        ts_ini: ts_ini.to_string(),
        i_ts_ini,
        i_ts_end,
        ts_end,
        open_end,
        avoid_cache,
        offset_temp,
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
    pub offset_temp: f64,
    pub timezone_offset: Option<i64>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Accumulators {
    pub rpars: Option<ReqParameters>,
    pub page_ts_ini: String,
    pub tcomp: DUTTelemetryCompiler,
    pub timezone_offset: Option<i64>,
}
