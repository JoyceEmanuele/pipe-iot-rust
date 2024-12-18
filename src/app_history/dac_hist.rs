use super::cache_files::build_part_file_name;
use crate::compression::compiler_DAC::DACTelemetryCompiler;
use crate::lib_http::response::{respond_http_json, respond_http_plain_text};
use crate::lib_http::types::HttpResponse;
use crate::telemetry_payloads::dac_payload_json::get_raw_telemetry_pack_dac;
use crate::telemetry_payloads::dac_telemetry::{split_pack, HwInfoDAC, T_sensor_cfg, T_sensors};
use crate::telemetry_payloads::dac_tsh_tsc::{calculateSubResf, calculateSupAq, FluidInterpData};
use crate::telemetry_payloads::telemetry_formats::TelemetryDAC_v3_calcs;
use crate::GlobalVars;
use chrono::{Duration, NaiveDateTime};
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::io::Write;
use std::sync::Arc;

pub async fn process_comp_command_dac_v2(
    rpars: ReqParameters,
    globs: &Arc<GlobalVars>,
) -> Result<HttpResponse, String> {
    let rpars_serialized = serde_json::to_string(&rpars).unwrap();
    let hw_cfg = rpars.hw_cfg;
    let dev_id = rpars.dev_id;
    let interval_length_s = rpars.interval_length_s;
    let ts_ini = rpars.ts_ini;
    let timezone_offset = rpars.timezone_offset;
    let timezone_offset_string = timezone_offset
        .map(|v| v.to_string())
        .unwrap_or("None".to_string());
    // Processa 15 minutos antes do período para preparar o estado do L1.
    // Não atualizamos i_ts_ini pois é usado para identificar os limites do gráfico
    // e não queremos que esses 15min vão para o gráfico.
    let ts_ini = {
        let ts = NaiveDateTime::parse_from_str(&ts_ini, "%Y-%m-%dT%H:%M:%S")
            .map_err(|e| e.to_string())?;

        let ts = ts - chrono::Duration::minutes(15);
        ts.format("%Y-%m-%dT%H:%M:%S").to_string()
    };

    let i_ts_ini = rpars.i_ts_ini;
    let i_ts_end = rpars.i_ts_end;
    let ts_end = rpars.ts_end;
    let open_end = rpars.open_end;

    let has_Psuc = hw_cfg.P0Psuc || hw_cfg.P1Psuc;
    let has_Pliq = hw_cfg.P0Pliq || hw_cfg.P1Pliq;

    // let Tamb_sensor = match hw_cfg.t_cfg {
    //   None => Some(T_sensors::T0),
    //   Some(v) => v.Tamb,
    // };
    // let mut req_values = Vec::<String>::new();
    // if (hw_cfg.P0Psuc || hw_cfg.P0Pliq) { req_values.push("P0"); }

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
                let tcomp = DACTelemetryCompiler::new(rpars.interval_length_s, &hw_cfg);
                Accumulators {
                    rpars: None,
                    page_ts_ini,
                    tcomp,
                    timezone_offset,
                }
            }
        }
    };
    let mut page_ts_ini = accs.page_ts_ini;
    // let mut fchk = accs.fchk;
    let mut tcomp = accs.tcomp;
    // let mut fcomp = accs.fcomp;

    let mut fluid_info = match &hw_cfg.fluid {
        None => None,
        Some(fluid) => FluidInterpData::for_fluid(fluid),
    };
    let mut calcs = if (has_Psuc || has_Pliq) && fluid_info.is_some() {
        Some(TelemetryDAC_v3_calcs {
            Tsh: None,
            Tsc: None,
        })
    } else {
        None
    };

    let mut table_name = {
        if (dev_id.len() == 12) && dev_id.to_uppercase().starts_with("DAC") {
            format!("{}XXXX_RAW", &dev_id.to_uppercase()[0..8])
        } else {
            String::new()
        }
    };

    for custom in &globs.configfile.CUSTOM_TABLE_NAMES_DAC {
        if dev_id.to_uppercase().starts_with(&custom.dev_prefix) {
            table_name = custom.table_name.to_owned();
            break;
        }
    }

    if table_name.len() == 0 {
        crate::LOG.append_log_tag_msg("WARN", &format!("Unknown DAC generation: {}", dev_id));
        return Ok(respond_http_json(200, "{}"));
    }
    let mut dac_state = crate::l1_virtual::dac_l1::dac_l1_calculator::create_l1_calculator(&hw_cfg);

    let querier = if table_name == "DAC20719XXXX_RAW" {
        crate::lib_dynamodb::query::QuerierDevIdTimestamp::new_custom(
            table_name,
            "dac_id".to_owned(),
            "timestamp".to_owned(),
            dev_id.clone(),
            &globs.configfile.aws_config,
        )
    } else {
        crate::lib_dynamodb::query::QuerierDevIdTimestamp::new_diel_dev(
            table_name,
            dev_id.clone(),
            &globs.configfile.aws_config,
        )
    };
    let mut found_invalid_payload = false;
    let result = querier
        .run(&ts_ini, &ts_end, &mut |items| {
            for item in items {
                let payload = match get_raw_telemetry_pack_dac(&item) {
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
                    &hw_cfg,
                    &mut dac_state,
                    &mut |telemetry, L1, L1fancoil, index| {
                        if let Some(calcs) = &mut calcs {
                            if let Some(viData) = &mut fluid_info {
                                if has_Psuc {
                                    calcs.Tsh = calculateSupAq(
                                        viData,
                                        &telemetry.Psuc,
                                        &telemetry.Tsuc,
                                        &telemetry.Lcmp,
                                    );
                                };
                                if has_Pliq {
                                    calcs.Tsc = calculateSubResf(
                                        viData,
                                        &telemetry.Pliq,
                                        &telemetry.Tliq,
                                        &telemetry.Lcmp,
                                    );
                                };
                            };
                        }
                        tcomp.AdcPontos(telemetry, index, &calcs, L1, L1fancoil, payload.samplingTime);
                    },
                );
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
        timezone_offset,
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
    data["Lcmp"] = period_data.Lcmp.into();
    data["Tamb"] = period_data.Tamb.into();
    data["Tsuc"] = period_data.Tsuc.into();
    data["Tliq"] = period_data.Tliq.into();
    if has_Psuc {
        data["Psuc"] = period_data.Psuc.into();
    };
    if has_Pliq {
        data["Pliq"] = period_data.Pliq.into();
    };
    if hw_cfg.hasAutomation {
        data["Levp"] = period_data.Levp.into();
        data["Lcut"] = period_data.Lcut.into();
    }
    if calcs.is_some() {
        if has_Psuc {
            data["Tsh"] = period_data.Tsh.into();
        };
        if has_Pliq {
            data["Tsc"] = period_data.Tsc.into();
        };
    }
    data["State"] = period_data.State.into();
    data["Mode"] = period_data.Mode.into();

    if let Some(true) = hw_cfg.debug_L1_fancoil {
        data["L1raw"] = period_data.L1raw.into();
        data["L1fancoil"] = period_data.L1fancoil.into();
    }

    data["numDeparts"] = period_data.numDeparts.into();
    data["hoursOn"] = period_data.hoursOn.into();
    data["hoursOff"] = period_data.hoursOff.into();
    data["hoursBlocked"] = period_data.hoursBlocked.into();
    data["startLcmp"] = serde_json::json!(period_data.startLcmp);
    data["endLcmp"] = serde_json::json!(period_data.endLcmp);
    data["provision_error"] = provision_error.into();
    data["SavedData"] = period_data.savedData.into();
    data["first_saved_data_index"] = period_data.first_saved_data_index.into();

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

    let interval_length_s = match parsed["interval_length_s"].as_i64() {
        Some(v) => v,
        None => {
            return Err(respond_http_plain_text(400, "Missing interval_length_s"));
        }
    };

    let maxInterval = 7 * 24 * 60 * 60; // 7 days
    let minInterval = 1 * 60; // 1 minute
    if interval_length_s > maxInterval {
        return Err(respond_http_plain_text(400, "Interval too long"));
    }
    if interval_length_s < minInterval {
        return Err(respond_http_plain_text(400, "Invalid interval"));
    }
    if interval_length_s >= minInterval && interval_length_s <= maxInterval {
    } else {
        return Err(respond_http_plain_text(400, "Invalid interval"));
    }

    let mut ts_ini = match parsed["ts_ini"].as_str() {
        Some(v) => v,
        None => {
            return Err(respond_http_plain_text(400, "Missing ts_ini"));
        }
    };

    let i_ts_ini = match NaiveDateTime::parse_from_str(ts_ini, "%Y-%m-%dT%H:%M:%S") {
        Err(err) => {
            crate::LOG.append_log_tag_msg("ERROR", &format!("{} {}", ts_ini, err));
            return Err(respond_http_plain_text(400, "Error parsing Date"));
        }
        Ok(date) => date.timestamp(),
    };

    let ts_ini_aux = NaiveDateTime::from_timestamp(i_ts_ini, 0)
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string();
    ts_ini = &ts_ini_aux;

    let i_ts_end = i_ts_ini + interval_length_s;

    let ts_end = NaiveDateTime::from_timestamp(i_ts_end + 60, 0)
        .format("%Y-%m-%dT%H:%M:%S")
        .to_string();

    let hw_cfg = HwInfoDAC {
        isVrf: match parsed["isVrf"].as_bool() {
            Some(v) => v,
            None => {
                return Err(respond_http_plain_text(400, "Missing isVrf"));
            }
        },
        calculate_L1_fancoil: parsed["calculate_L1_fancoil"].as_bool().or(Some(false)),
        debug_L1_fancoil: parsed["debug_L1_fancoil"].as_bool().or(Some(false)),
        hasAutomation: match parsed["hasAutomation"].as_bool() {
            Some(v) => v,
            None => {
                return Err(respond_http_plain_text(400, "Missing hasAutomation"));
            }
        },
        P0Psuc: match parsed["P0Psuc"].as_bool() {
            Some(v) => v,
            None => {
                return Err(respond_http_plain_text(400, "Missing P0Psuc"));
            }
        },
        P1Psuc: match parsed["P1Psuc"].as_bool() {
            Some(v) => v,
            None => {
                return Err(respond_http_plain_text(400, "Missing P1Psuc"));
            }
        },
        P0Pliq: match parsed["P0Pliq"].as_bool() {
            Some(v) => v,
            None => {
                return Err(respond_http_plain_text(400, "Missing P0Pliq"));
            }
        },
        P1Pliq: match parsed["P1Pliq"].as_bool() {
            Some(v) => v,
            None => {
                return Err(respond_http_plain_text(400, "Missing P1Pliq"));
            }
        },
        P0multQuad: parsed["P0multQuad"].as_f64().unwrap_or(0.0),
        P0multLin: parsed["P0multLin"].as_f64().unwrap_or(0.0),
        P0ofst: parsed["P0ofst"].as_f64().unwrap_or(0.0),
        P1multQuad: parsed["P1multQuad"].as_f64().unwrap_or(0.0),
        P1multLin: parsed["P1multLin"].as_f64().unwrap_or(0.0),
        P1ofst: parsed["P1ofst"].as_f64().unwrap_or(0.0),
        fluid: parsed["fluid_type"].as_str().map(|v| v.to_owned()),
        t_cfg: match parsed["T0_T1_T2"].as_array() {
            None => None,
            Some(T0_T1_T2) => {
                if T0_T1_T2.len() != 3 {
                    return Err(respond_http_plain_text(400, "Invalid T0_T1_T2"));
                }
                let Tamb = if let Some("Tamb") = T0_T1_T2[0].as_str() {
                    Some(T_sensors::T0)
                } else if let Some("Tamb") = T0_T1_T2[1].as_str() {
                    Some(T_sensors::T1)
                } else if let Some("Tamb") = T0_T1_T2[2].as_str() {
                    Some(T_sensors::T2)
                } else {
                    None
                };
                let Tsuc = if let Some("Tsuc") = T0_T1_T2[0].as_str() {
                    Some(T_sensors::T0)
                } else if let Some("Tsuc") = T0_T1_T2[1].as_str() {
                    Some(T_sensors::T1)
                } else if let Some("Tsuc") = T0_T1_T2[2].as_str() {
                    Some(T_sensors::T2)
                } else {
                    None
                };
                let Tliq = if let Some("Tliq") = T0_T1_T2[0].as_str() {
                    Some(T_sensors::T0)
                } else if let Some("Tliq") = T0_T1_T2[1].as_str() {
                    Some(T_sensors::T1)
                } else if let Some("Tliq") = T0_T1_T2[2].as_str() {
                    Some(T_sensors::T2)
                } else {
                    None
                };
                Some(T_sensor_cfg { Tamb, Tsuc, Tliq })
            }
        },
        simulate_l1: parsed["virtualL1"].as_bool().unwrap_or(false),
        l1_psuc_offset: parsed["L1CalcCfg"]["psucOffset"].as_f64().unwrap_or(0.0),
        DAC_APPL: parsed["DAC_APPL"].as_str().map(|x| x.to_string()),
        DAC_TYPE: parsed["DAC_TYPE"].as_str().map(|x| x.to_string()),
    };

    if hw_cfg.P0Psuc || hw_cfg.P0Pliq {
        if parsed["P0multQuad"].as_f64().is_none() {
            return Err(respond_http_plain_text(400, "Missing P0multQuad"));
        }
        if parsed["P0multLin"].as_f64().is_none() {
            return Err(respond_http_plain_text(400, "Missing P0multLin"));
        }
        if parsed["P0ofst"].as_f64().is_none() {
            return Err(respond_http_plain_text(400, "Missing P0ofst"));
        }
    }
    if hw_cfg.P1Psuc || hw_cfg.P1Pliq {
        if parsed["P1multQuad"].as_f64().is_none() {
            return Err(respond_http_plain_text(400, "Missing P1multQuad"));
        }
        if parsed["P1multLin"].as_f64().is_none() {
            return Err(respond_http_plain_text(400, "Missing P1multLin"));
        }
        if parsed["P1ofst"].as_f64().is_none() {
            return Err(respond_http_plain_text(400, "Missing P1ofst"));
        }
    }

    let open_end = parsed["open_end"].as_bool().unwrap_or(false);
    let avoid_cache = parsed["avoid_cache"].as_bool().unwrap_or(false);
    let timezone_offset: Option<i64> = parsed["timezoneOffset"].as_i64();
    return Ok(ReqParameters {
        hw_cfg,
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

// #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
// pub enum RequiredVarsDAC {
//   L1only,
//   L1_Tamb,
//   All,
// }
// #[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
// pub struct RequiredSensorsDAC {
//   pub L1: bool,
//   pub State: bool,
//   pub Mode: bool,
//   pub T0: bool,
//   pub T1: bool,
//   pub T2: bool,
//   pub P0: bool,
//   pub P1: bool,
// }

#[derive(Serialize, Deserialize, Debug)]
pub struct ReqParameters {
    pub hw_cfg: HwInfoDAC,
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
    // pub fchk: DACFaultsChecker,
    pub tcomp: DACTelemetryCompiler,
    // pub fcomp: DACFaultsCompiler,
    pub timezone_offset: Option<i64>,
}
