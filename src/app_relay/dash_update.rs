use super::{global_vars::ConversionVars, state_persistence};
use crate::telemetry_payloads::{
    dac_telemetry::{HwInfoDAC, T_sensor_cfg, T_sensors},
    dri_telemetry::HwInfoDRI,
    dut_telemetry::HwInfoDUT,
};
use crate::ConfigFile;
use crate::GlobalVars;
use serde_json::json;
use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

pub enum DevHwConfig {
    DAC(HwInfoDAC, Vec<u8>), // O segundo elemento é algum tipo de tokem usado para comparar de forma eficiente dois HwInfoDAC e dizer se são iguais ou diferentes.
    DUT(HwInfoDUT, Vec<u8>), // O segundo elemento é algum tipo de tokem usado para comparar de forma eficiente dois HwInfoDUT e dizer se são iguais ou diferentes.
    DRI(HwInfoDRI),
    Other,
}

/** Tarefa busca no API-Server as configurações dos dispositivos e mantém atualizado no GlobalVars */
pub async fn run_task(globs: &Arc<GlobalVars>, conv_vars: &Mutex<ConversionVars>) {
    let mut last_update: Option<std::time::Instant> = None;
    let mut need_update = false;
    loop {
        need_update = need_update || globs.need_update_configs.load(Ordering::Relaxed);
        if !need_update {
            match last_update {
                Some(last_update) => {
                    // Se fizer mais de 1 hora que não atualiza, solicita atualização
                    need_update = last_update.elapsed() > Duration::from_secs(3600);
                }
                None => {
                    // Se ainda não atualizou nenhum vez, solicita.
                    need_update = true;
                }
            }
        }
        if need_update {
            globs.need_update_configs.store(false, Ordering::Relaxed);
            let result = make_cfg_update_request(&globs, &conv_vars).await;
            match result {
                Ok(()) => {
                    last_update = Some(std::time::Instant::now());
                    *globs.configs_ready.lock().await = true;
                    need_update = false;
                }
                Err(err) => {
                    crate::LOG.append_log_tag_msg("ERROR[196]", &err);
                }
            };
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }
}

pub fn parse_dash_update(
    parsed: &serde_json::Value,
    devs: &mut HashMap<String, DevHwConfig>,
) -> Result<(), String> {
    let removed = &parsed["removed"];
    if !removed.is_null() {
        if !removed.is_array() {
            return Err("Invalid dash response [55]".to_string());
        }
        let removed = match removed.as_array() {
            Some(v) => v,
            None => return Err("Invalid dash response [59]".to_string()),
        };
        for item in removed {
            let dev_id = match item.as_str() {
                None => return Err("Invalid dash response [68]".to_string()),
                Some(v) => v,
            };
            devs.remove_entry(dev_id);
        }
    }

    let list_dris = &parsed["dris"];
    if !list_dris.is_array() {
        return Err("Invalid dash response [60]".to_string());
    }
    let list_dris = match list_dris.as_array() {
        Some(v) => v,
        None => return Err("Invalid dash response [72]".to_string()),
    };

    for i in 0..list_dris.len() {
        let row = &list_dris[i];

        let dri_id = match row["DRI_ID"].as_str() {
            None => return Err("Invalid dash response [73]".to_string()),
            Some(v) => v,
        };

        let hw_dri_cfg = match parse_dri_cfg(&row) {
            Err(err) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("Error parsing DRI cfg {} {}", dri_id, err),
                );
                devs.remove_entry(dri_id);
                continue;
            }
            Ok(v) => v,
        };

        devs.insert(dri_id.to_owned(), DevHwConfig::DRI(hw_dri_cfg));
    }

    let list_duts = &parsed["duts"];
    if !list_duts.is_array() {
        return Err("Invalid dash response [60]".to_string());
    }
    let list_duts = match list_duts.as_array() {
        Some(v) => v,
        None => return Err("Invalid dash response [72]".to_string()),
    };

    for i in 0..list_duts.len() {
        let row = &list_duts[i];

        let dut_id = match row["DUT_ID"].as_str() {
            None => return Err("Invalid dash response [73]".to_string()),
            Some(v) => v,
        };

        let hw_dut_cfg = match parse_dut_cfg(&row) {
            Err(err) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("Error parsing DUT cfg {} {}", dut_id, err),
                );
                devs.remove_entry(dut_id);
                continue;
            }
            Ok(v) => v,
        };

        let hw_cfg_serialized = match state_persistence::serialize_state_obj(&hw_dut_cfg) {
            Ok(v) => v,
            Err(err) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("Error serializing device cfg {} {}", dut_id, err),
                );
                devs.remove_entry(dut_id);
                continue;
            }
        };
        devs.insert(
            dut_id.to_owned(),
            DevHwConfig::DUT(hw_dut_cfg, hw_cfg_serialized),
        );
    }

    let list = &parsed["dacs"];
    if !list.is_array() {
        return Err("Invalid dash response [60]".to_string());
    }
    let list = match list.as_array() {
        Some(v) => v,
        None => return Err("Invalid dash response [72]".to_string()),
    };

    for i in 0..list.len() {
        let row = &list[i];
        // {
        //     DAC_ID: string;
        //     DAC_TYPE: string;
        //     DAC_APPL: string;
        //     FLUID_TYPE: string;
        //     hasAutomation: boolean;
        //     isVrf: boolean;
        //     P0Psuc: boolean;
        //     P1Psuc: boolean;
        //     P0Pliq: boolean;
        //     P1Pliq: boolean;
        //     P0multQuad: number;
        //     P0multLin: number;
        //     P0ofst: number;
        //     P1multQuad: number;
        //     P1multLin: number;
        //     P1ofst: number;
        //     forwardTo: [string];
        //     T0_T1_T2: [string, string, string];
        //     L1CalcCfg: { psucOffset: number, }
        // }
        let dac_id = match row["DAC_ID"].as_str() {
            None => return Err("Invalid dash response [73]".to_string()),
            Some(v) => v,
        };

        let hw_cfg = match parse_dac_cfg(row) {
            Err(err) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("Error parsing DAC cfg {} {}", dac_id, err),
                );
                devs.remove_entry(dac_id);
                continue;
            }
            Ok(v) => v,
        };

        // // integrações através de tópico no broker MQTT
        // forward_to: match row["forwardTo"].as_array() {
        //     None => None,
        //     Some(forward_to) => {
        //         if (forward_to.len() == 0) {
        //             None
        //         } else {
        //             // Cria estrutura de encaminhamentos e popula
        //         }
        //     }
        // },

        let hw_cfg_serialized = match state_persistence::serialize_state_obj(&hw_cfg) {
            Ok(v) => v,
            Err(err) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("Error serializing device cfg {} {}", dac_id, err),
                );
                devs.remove_entry(dac_id);
                continue;
            }
        };
        devs.insert(
            dac_id.to_owned(),
            DevHwConfig::DAC(hw_cfg, hw_cfg_serialized),
        );
    }

    // let tz = chrono::FixedOffset::east(-3 * 3600);
    // globs.last_update = Some(chrono::Utc::now().with_timezone(&tz));
    return Ok(());
}

fn parse_dac_cfg(row: &serde_json::Value) -> Result<HwInfoDAC, String> {
    let hw_cfg = HwInfoDAC {
        isVrf: match row["isVrf"].as_bool() {
            Some(v) => v,
            None => {
                return Err("Missing isVrf".to_owned());
            }
        },
        calculate_L1_fancoil: row["calculate_L1_fancoil"].as_bool(),
        debug_L1_fancoil: row["debug_L1_fancoil"].as_bool(),
        hasAutomation: match row["hasAutomation"].as_bool() {
            Some(v) => v,
            None => {
                return Err("Missing hasAutomation".to_owned());
            }
        },
        P0Psuc: match row["P0Psuc"].as_bool() {
            Some(v) => v,
            None => {
                return Err("Missing P0Psuc".to_owned());
            }
        },
        P1Psuc: match row["P1Psuc"].as_bool() {
            Some(v) => v,
            None => {
                return Err("Missing P1Psuc".to_owned());
            }
        },
        P0Pliq: match row["P0Pliq"].as_bool() {
            Some(v) => v,
            None => {
                return Err("Missing P0Pliq".to_owned());
            }
        },
        P1Pliq: match row["P1Pliq"].as_bool() {
            Some(v) => v,
            None => {
                return Err("Missing P1Pliq".to_owned());
            }
        },
        P0multQuad: match row["P0multQuad"].as_f64() {
            Some(v) => v,
            None => 0.0,
        },
        P0multLin: match row["P0multLin"].as_f64() {
            Some(v) => v,
            None => 0.0,
        },
        P0ofst: match row["P0ofst"].as_f64() {
            Some(v) => v,
            None => 0.0,
        },
        P1multQuad: match row["P1multQuad"].as_f64() {
            Some(v) => v,
            None => 0.0,
        },
        P1multLin: match row["P1multLin"].as_f64() {
            Some(v) => v,
            None => 0.0,
        },
        P1ofst: match row["P1ofst"].as_f64() {
            Some(v) => v,
            None => 0.0,
        },
        fluid: match row["FLUID_TYPE"].as_str() {
            Some(v) => Some(v.to_owned()),
            None => None,
        },
        t_cfg: match row["T0_T1_T2"].as_array() {
            None => None,
            Some(t0_t1_t2) => {
                if t0_t1_t2.len() != 3 {
                    return Err("Invalid T0_T1_T2".to_owned());
                }
                let t_amb = if let Some("Tamb") = t0_t1_t2[0].as_str() {
                    Some(T_sensors::T0)
                } else if let Some("Tamb") = t0_t1_t2[1].as_str() {
                    Some(T_sensors::T1)
                } else if let Some("Tamb") = t0_t1_t2[2].as_str() {
                    Some(T_sensors::T2)
                } else {
                    None
                };
                let t_suc = if let Some("Tsuc") = t0_t1_t2[0].as_str() {
                    Some(T_sensors::T0)
                } else if let Some("Tsuc") = t0_t1_t2[1].as_str() {
                    Some(T_sensors::T1)
                } else if let Some("Tsuc") = t0_t1_t2[2].as_str() {
                    Some(T_sensors::T2)
                } else {
                    None
                };
                let t_liq = if let Some("Tliq") = t0_t1_t2[0].as_str() {
                    Some(T_sensors::T0)
                } else if let Some("Tliq") = t0_t1_t2[1].as_str() {
                    Some(T_sensors::T1)
                } else if let Some("Tliq") = t0_t1_t2[2].as_str() {
                    Some(T_sensors::T2)
                } else {
                    None
                };
                Some(T_sensor_cfg {
                    Tamb: t_amb,
                    Tsuc: t_suc,
                    Tliq: t_liq,
                })
            }
        },
        simulate_l1: row["virtualL1"].as_bool().unwrap_or(false),
        l1_psuc_offset: row["L1CalcCfg"]["psucOffset"].as_f64().unwrap_or(0.0),
        DAC_APPL: row["DAC_APPL"].as_str().map(|x| x.to_string()),
        DAC_TYPE: row["DAC_TYPE"].as_str().map(|x| x.to_string()),
    };

    if hw_cfg.P0Psuc || hw_cfg.P0Pliq {
        if row["P0multQuad"].as_f64().is_none() {
            return Err("Missing P0multQuad".to_string());
        }
        if row["P0multLin"].as_f64().is_none() {
            return Err("Missing P0multLin".to_string());
        }
        if row["P0ofst"].as_f64().is_none() {
            return Err("Missing P0ofst".to_string());
        }
    }
    if hw_cfg.P1Psuc || hw_cfg.P1Pliq {
        if row["P1multQuad"].as_f64().is_none() {
            return Err("Missing P1multQuad".to_string());
        }
        if row["P1multLin"].as_f64().is_none() {
            return Err("Missing P1multLin".to_string());
        }
        if row["P1ofst"].as_f64().is_none() {
            return Err("Missing P1ofst".to_string());
        }
    }

    return Ok(hw_cfg);
}

fn parse_dut_cfg(row: &serde_json::Value) -> Result<HwInfoDUT, String> {
    let hw_cfg = HwInfoDUT {
        temperature_offset: match row["TEMPERATURE_OFFSET"].as_f64() {
            Some(v) => v,
            None => 0.0,
        },
    };

    return Ok(hw_cfg);
}

fn parse_dri_cfg(row: &serde_json::Value) -> Result<HwInfoDRI, String> {
    let hw_cfg = HwInfoDRI {
        formulas: match row.get("FORMULAS") {
            Some(v) => json_to_hashmap(v.clone()),
            None => None,
        },
    };

    return Ok(hw_cfg);
}

fn json_to_hashmap(json: serde_json::Value) -> Option<HashMap<String, String>> {
    let mut lookup: HashMap<String, serde_json::Value> = serde_json::from_value(json).unwrap();
    let mut map = HashMap::new();
    for key in lookup.clone().keys() {
        let (k, v) = lookup.remove_entry(key).unwrap_or_default();
        map.insert(k, v.as_str().unwrap_or_default().to_string());
    }
    Some(map)
}

pub async fn make_cfg_http_req(configfile: &ConfigFile) -> Result<reqwest::Response, String> {
    crate::LOG.append_log_tag_msg("info", "Solicitando update de configurações");
    let body = json!({});

    let stats_url = format!(
        "{}/diel-internal/bgtasks/getDevsCfg",
        configfile.apiserver_internal_api
    );
    let client = reqwest::Client::new();
    let res = client
        .post(&stats_url)
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let response_status = res.status();

    // let addr = format!("http://{}", globs.configfile.STATS_SERVER_HTTP);
    // let mut request = HttpRequest{
    //     method: "GET".to_owned(),
    //     path: "/get_hw_cfg".to_owned(),
    //     headers: HashMap::new(),
    //     content: body.to_string().into_bytes(),
    // };
    // let response = lib_http::request::do_http_request(&addr, &request)
    //     .await?;
    // let response_bytes = &response.content;

    if response_status != reqwest::StatusCode::OK {
        let response_bytes = res.bytes().await.map_err(|e| e.to_string())?;
        let packet_payload = std::str::from_utf8(&response_bytes).map_err(|e| e.to_string())?;
        return Err(format!(
            "Invalid cfg_update response: {} {} {}",
            stats_url, response_status, packet_payload
        ));
    }

    Ok(res)
}
pub async fn make_cfg_update_request(
    globs: &Arc<GlobalVars>,
    conv_vars: &Mutex<ConversionVars>,
) -> Result<(), String> {
    let res = make_cfg_http_req(&globs.configfile).await?;
    let response_bytes = res.bytes().await.map_err(|e| e.to_string())?;
    let packet_payload = std::str::from_utf8(&response_bytes).map_err(|e| e.to_string())?;
    let packet_payload =
        serde_json::from_str::<serde_json::Value>(packet_payload).map_err(|e| e.to_string())?;

    let mut conv_vars = conv_vars.lock().await;

    parse_dash_update(&packet_payload, &mut conv_vars.devs)?;

    crate::LOG.append_log_tag_msg("info", "Update de configurações realizado");

    Ok(())
}
