use super::dash_update::DevHwConfig;
use super::redis_connection::{get_dev_state_redis, save_dev_state_redis};
use super::state_persistence::serialize_state_obj;
use crate::l1_virtual::dac_l1::dac_l1_calculator;
use crate::l1_virtual::dut_l1::l1_calc as dut_l1_calculator;
use crate::telemetry_payloads::dri::chiller_carrier_hx::{
    convert_chiller_carrier_hx_payload, TelemetryDriChillerCarrierHX,
};
use crate::telemetry_payloads::dri::chiller_carrier_xa::{
    convert_chiller_carrier_xa_payload, TelemetryDriChillerCarrierXA,
};
use crate::telemetry_payloads::dri::chiller_carrier_xa_hvar::{
    convert_chiller_carrier_xa_hvar_payload, TelemetryDriChillerCarrierXAHvar,
};
use crate::telemetry_payloads::dri::vav_fancoil::convert_vav_and_fancoil_payload;
use crate::telemetry_payloads::dri_telemetry::{HwInfoDRI, TelemetryDri};
use crate::telemetry_payloads::energy::dme::TelemetryDME;
use crate::telemetry_payloads::telemetry_formats::TelemetryPackDutV2Full;
use crate::telemetry_payloads::{
    dac_payload_json, dac_telemetry, dal_payload_json::get_raw_telemetry_pack_dal, dal_telemetry,
    dmt_payload_json, dmt_telemetry, dut_telemetry, energy::dme,
};
use crate::GlobalVars;
use serde_cbor;
use std::str::FromStr;
use std::sync::Arc;

pub enum PayloadConversionResult {
    WithoutConversion,            // Encaminhar o payload sem conversão
    Converted(serde_json::Value), // Encaminhar a versão convertida
    IgnorePayload,                // Não encaminhar o payload
    Error(String),                // Houve erro no processamento do payload
}
use PayloadConversionResult::*;

pub async fn convert_data_payload(
    payload_json: serde_json::Value,
    payload_str: &str,
    dev_id: &str,
    globs: &Arc<GlobalVars>,
) -> PayloadConversionResult {
    match &dev_id[0..3] {
        "DAC" => {
            return process_data_dac(dev_id, payload_json, payload_str, globs)
                .await
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DUT" => {
            return process_data_dut(dev_id, payload_json, payload_str, globs)
                .await
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DMA" => {
            return process_data_dma(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DMT" => {
            return process_data_dmt(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DAM" => {
            return process_data_dam(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DAL" => {
            return process_data_dal(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DRI" => {
            return process_data_dri(dev_id, payload_json, payload_str, globs)
                .await
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        _ => {}
    };

    match &dev_id[0..3].to_uppercase()[..] {
        "DAC" => {
            return process_data_dac(dev_id, payload_json, payload_str, globs)
                .await
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DUT" => {
            return process_data_dut(dev_id, payload_json, payload_str, globs)
                .await
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DMA" => {
            return process_data_dma(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DMT" => {
            return process_data_dmt(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DAM" => {
            return process_data_dam(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DAL" => {
            return process_data_dal(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DRI" => {
            return process_data_dri(dev_id, payload_json, payload_str, globs)
                .await
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        _ => {}
    };

    return PayloadConversionResult::WithoutConversion;
}

pub fn convert_control_payload(
    payload_json: serde_json::Value,
    topic: &str,
    payload_str: &str,
    dev_id: &str,
    globs: &Arc<GlobalVars>,
) -> PayloadConversionResult {
    if dev_id.len() < 3 {
        return PayloadConversionResult::IgnorePayload;
    }

    match &dev_id[0..3] {
        "DAC" => {
            return process_control_dac(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DUT" => {
            return process_control_dut(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DMA" => {
            return process_control_dma(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DMT" => {
            return process_control_dmt(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DAM" => {
            return process_control_dam(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DAL" => {
            return process_control_dal(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DRI" => return PayloadConversionResult::WithoutConversion,
        _ => {}
    };

    match &dev_id[0..3].to_uppercase()[..] {
        "DAC" => {
            return process_control_dac(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DUT" => {
            return process_control_dut(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DMA" => {
            return process_control_dma(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DMT" => {
            return process_control_dmt(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DAM" => {
            return process_control_dam(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DAL" => {
            return process_control_dal(dev_id, payload_json, payload_str, globs)
                .unwrap_or_else(|err| PayloadConversionResult::Error(err))
        }
        "DRI" => return PayloadConversionResult::WithoutConversion,
        _ => {}
    };

    return PayloadConversionResult::WithoutConversion;
}

async fn process_data_dac(
    dev_id: &str,
    mut payload_json: serde_json::Value,
    payload_str: &str,
    globs: &Arc<GlobalVars>,
) -> Result<PayloadConversionResult, String> {
    globs.stats.msgsz_dac.on_data(payload_str.len());

    let dac_state_db: Option<Vec<u8>> = get_dev_state_redis(&dev_id, globs).await?;

    let (payload_obj, dac_state_bytes) = {
        let payload_obj = match dac_payload_json::get_raw_telemetry_pack_dac(&payload_json) {
            Ok(v) => v,
            Err(err) => {
                return Err(format!("Ignoring invalid payload(s): {err}"));
            }
        };

        let mut conv_vars = globs.conv_vars.lock().await;
        let conv = &mut *conv_vars;
        let devs = &conv.devs;

        // Identifica a configuração de hardware mais recente associada ao dispositivo
        let default_cfg_serialized: Option<Vec<u8>>;
        let (hw_cfg, latest_cfg_token) = match devs.get(dev_id) {
            Some(DevHwConfig::DAC(hw_cfg, latest_cfg_token)) => (hw_cfg, latest_cfg_token),
            _ => {
                // Invalid dev hw cfg, should use default
                // return;
                default_cfg_serialized = Some(serialize_state_obj(&globs.default_dac_hw)?);
                (
                    &globs.default_dac_hw,
                    default_cfg_serialized.as_ref().unwrap(),
                )
            }
        };

        // Se o token no banco for igual, usa o estado do banco, caso contrário cria um novo estado.
        let mut dac_db_state: DacDbState = match dac_state_db {
            None => DacDbState {
                cfg_token: latest_cfg_token.to_owned(),
                state: dac_l1_calculator::create_l1_calculator(&hw_cfg),
            },
            Some(dac_state_db) => {
                match serde_cbor::from_reader::<DacDbState, &[u8]>(dac_state_db.as_slice()) {
                    Ok(dac_state_db) => {
                        if dac_state_db.cfg_token.eq(latest_cfg_token) {
                            dac_state_db
                        } else {
                            DacDbState {
                                cfg_token: latest_cfg_token.to_owned(),
                                state: dac_l1_calculator::create_l1_calculator(&hw_cfg),
                            }
                        }
                    }
                    Err(err) => {
                        crate::LOG.append_log_tag_msg(
                            "ERROR",
                            &format!("Could not parse persisted device information: {}", err),
                        );
                        DacDbState {
                            cfg_token: latest_cfg_token.to_owned(),
                            state: dac_l1_calculator::create_l1_calculator(&hw_cfg),
                        }
                    }
                }
            }
        };

        let payload_obj =
            match dac_telemetry::convert_payload(&payload_obj, &hw_cfg, &mut dac_db_state.state) {
                Ok(v) => v,
                Err(err) => {
                    return Err(format!(
                        "Ignoring invalid payload(s): {} {:?}",
                        &err, payload_json
                    ));
                }
            };

        let mut dac_state_bytes: Vec<u8> = Vec::new();
        serde_cbor::to_writer(&mut dac_state_bytes, &dac_db_state)
            .map_err(|err| format!("[266] {err}"))?;

        (payload_obj, dac_state_bytes)
    };

    save_dev_state_redis(&dev_id, globs, dac_state_bytes).await?;

    payload_json["Lcmp"] = serde_json::json!(payload_obj.Lcmp);
    if payload_obj.Lcut.is_some() {
        payload_json["Lcut"] = serde_json::json!(payload_obj.Lcut);
    }
    if payload_obj.Levp.is_some() {
        payload_json["Levp"] = serde_json::json!(payload_obj.Levp);
    }
    if payload_obj.Tamb.is_some() {
        payload_json["Tamb"] = serde_json::json!(payload_obj.Tamb);
    }
    if payload_obj.Tsuc.is_some() {
        payload_json["Tsuc"] = serde_json::json!(payload_obj.Tsuc);
    }
    if payload_obj.Tliq.is_some() {
        payload_json["Tliq"] = serde_json::json!(payload_obj.Tliq);
    }
    if payload_obj.Psuc.is_some() {
        payload_json["Psuc"] = serde_json::json!(payload_obj.Psuc);
    }
    if payload_obj.Pliq.is_some() {
        payload_json["Pliq"] = serde_json::json!(payload_obj.Pliq);
    }
    if payload_obj.Mode.is_some() {
        payload_json["Mode"] = serde_json::json!(payload_obj.Mode);
    }
    if payload_obj.State.is_some() {
        payload_json["State"] = serde_json::json!(payload_obj.State);
    }
    if payload_obj.Tsc.is_some() {
        payload_json["Tsc"] = serde_json::json!(payload_obj.Tsc);
    }
    if payload_obj.Tsh.is_some() {
        payload_json["Tsh"] = serde_json::json!(payload_obj.Tsh);
    }
    match payload_json.as_object_mut() {
        Some(payload_json) => {
            payload_json.remove("L1");
            payload_json.remove("T0");
            payload_json.remove("T1");
            payload_json.remove("T2");
            // payload_json.remove("P0");
            // payload_json.remove("P1");
            payload_json.remove("package_id");
        }
        None => {}
    };

    Ok(Converted(payload_json))
}

async fn process_data_dut(
    dev_id: &str,
    payload_json: serde_json::Value,
    payload_str: &str,
    globs: &Arc<GlobalVars>,
) -> Result<PayloadConversionResult, String> {
    globs.stats.msgsz_dut.on_data(payload_str.len());

    let dut_state_db: Option<Vec<u8>> = get_dev_state_redis(&dev_id, globs).await?;

    let (payload_json, dac_state_bytes) = {
        let payload_obj: TelemetryPackDutV2Full = match serde_json::from_str(payload_str) {
            Ok(v) => v,
            Err(err) => {
                return Err(format!("Ignoring invalid payload: {}", &err));
            }
        };

        let mut conv_vars = globs.conv_vars.lock().await;
        let conv = &mut *conv_vars;
        let devs = &conv.devs;

        // Identifica a configuração de hardware mais recente associada ao dispositivo
        let default_cfg_serialized: Option<Vec<u8>>;
        let (hw_cfg, latest_cfg_token) = match devs.get(dev_id) {
            Some(DevHwConfig::DUT(hw_cfg, latest_cfg_token)) => (hw_cfg, latest_cfg_token),
            _ => {
                // Invalid dev hw cfg, should use default
                // return;
                default_cfg_serialized = Some(serialize_state_obj(&globs.default_dut_hw)?);
                (
                    &globs.default_dut_hw,
                    default_cfg_serialized.as_ref().unwrap(),
                )
            }
        };

        // Se o token no banco for igual, usa o estado do banco, caso contrário cria um novo estado.
        let mut dut_db_state: DutDbState = match dut_state_db {
            None => DutDbState {
                cfg_token: latest_cfg_token.to_owned(),
                state: dut_l1_calculator::DutStateInfo::create_default(hw_cfg),
            },
            Some(dut_state_db) => {
                match serde_cbor::from_reader::<DutDbState, &[u8]>(dut_state_db.as_slice()) {
                    Ok(dut_state_db) => {
                        if dut_state_db.cfg_token.eq(latest_cfg_token) {
                            dut_state_db
                        } else {
                            DutDbState {
                                cfg_token: latest_cfg_token.to_owned(),
                                state: dut_l1_calculator::DutStateInfo::create_default(hw_cfg),
                            }
                        }
                    }
                    Err(err) => {
                        crate::LOG.append_log_tag_msg(
                            "ERROR",
                            &format!("Could not parse persisted device information: {}", err),
                        );
                        DutDbState {
                            cfg_token: latest_cfg_token.to_owned(),
                            state: dut_l1_calculator::DutStateInfo::create_default(hw_cfg),
                        }
                    }
                }
            }
        };

        let mut payload_obj =
            match dut_telemetry::convert_payload(payload_obj, hw_cfg, &mut dut_db_state.state) {
                Ok(v) => v,
                Err(err) => {
                    return Err(format!("Ignoring invalid payload: {}", &err));
                }
            };

        let mut dut_state_bytes: Vec<u8> = Vec::new();
        serde_cbor::to_writer(&mut dut_state_bytes, &dut_db_state)
            .map_err(|err| format!("[400] {err}"))?;

        if payload_obj.mode == Some("AUTO") {
            payload_obj.mode = Some("Auto");
        }

        let payload_json = serde_json::to_value(&payload_obj)
            .expect("Erro ao converter a telemetria de json para string");

        (payload_json, dut_state_bytes)
    };

    save_dev_state_redis(&dev_id, globs, dac_state_bytes).await?;

    return Ok(Converted(payload_json));
}

fn process_data_dmt(
    dev_id: &str,
    payload_json: serde_json::Value,
    payload_str: &str,
    globs: &Arc<GlobalVars>,
) -> Result<PayloadConversionResult, String> {
    globs.stats.msgsz_dmt.on_data(payload_str.len());
    let payload_obj = match dmt_payload_json::get_raw_telemetry_pack_dmt(&payload_json) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Ignoring invalid payload: {}", &err));
        }
    };

    let payload_obj = match dmt_telemetry::convert_payload(&payload_obj) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Ignoring invalid payload: {}", &err));
        }
    };

    let payload_json = serde_json::to_value(&payload_obj)
        .expect("Erro ao converter a telemetria de json para string");
    return Ok(Converted(payload_json));
}

fn process_data_dma(
    dev_id: &str,
    payload_json: serde_json::Value,
    payload_str: &str,
    globs: &Arc<GlobalVars>,
) -> Result<PayloadConversionResult, String> {
    globs.stats.msgsz_dma.on_data(payload_str.len());
    return Ok(WithoutConversion);
}

async fn process_data_dri(
    dev_id: &str,
    payload_json: serde_json::Value,
    payload_str: &str,
    globs: &Arc<GlobalVars>,
) -> Result<PayloadConversionResult, String> {
    let saved_data = payload_json["saved_data"].as_bool();
    if saved_data.is_some() && saved_data.unwrap() {
        // return Error(format!("DRI payload is an offline saved data: {}", payload_str));
        return Ok(IgnorePayload);
    }

    let conv_vars = globs.conv_vars.lock().await;
    let hw_cfg = match conv_vars.devs.get(dev_id) {
        Some(DevHwConfig::DRI(hw_cfg)) => hw_cfg,
        _ => {
            // Invalid dev hw cfg, should use default
            // return;
            &globs.default_dri_hw
        }
    };

    let payload_type = match payload_json["type"].as_str() {
        Some(v) => v,
        None => {
            return Err(format!("DRI payload does not have 'type' key"));
        }
    };

    let is_dme_payload = dme::DME_METERS.contains(&payload_type);
    if is_dme_payload {
        return process_data_dri_type_dme(payload_str, dev_id, hw_cfg, payload_type);
    }

    let is_vav_fancoil_payload =
        payload_type.starts_with("VAV") || payload_type.starts_with("FANCOIL");
    if is_vav_fancoil_payload {
        return process_data_dri_type_vav_fancoil(payload_str, dev_id, hw_cfg, payload_type);
    }

    let is_chiller_carrier_30xa_hvar = payload_type == "CHILLER-CARRIER-30XAB-HVAR";
    if is_chiller_carrier_30xa_hvar {
        return process_data_dri_type_chiller_carrier_30xa_hvar(
            payload_str,
            dev_id,
            hw_cfg,
            payload_type,
        );
    }

    let is_chiller_carrier_xa_payload = payload_type.starts_with("CHILLER-CARRIER-30XA");
    if is_chiller_carrier_xa_payload {
        return process_data_dri_type_chiller_carrier_xa(payload_str, dev_id, hw_cfg, payload_type);
    }

    let is_chiller_carrier_hx_payload = payload_type == "CHILLER-CARRIER-30HXE"
        || payload_type == "CHILLER-CARRIER-30GXE"
        || payload_type == "CHILLER-CARRIER-30HXF"
        || payload_str.contains("CHIL_S_S");
    if is_chiller_carrier_hx_payload {
        return process_data_dri_type_chiller_carrier_hx(payload_str, dev_id, hw_cfg, payload_type);
    }

    return Ok(WithoutConversion);
}
fn process_data_dri_type_dme(
    payload_str: &str,
    dev_id: &str,
    hw_cfg: &HwInfoDRI,
    payload_type: &str,
) -> Result<PayloadConversionResult, String> {
    let payload_obj: TelemetryDME = match serde_json::from_str(payload_str) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Ignoring invalid payload(s): {}", &err));
        }
    };

    let payload_obj = match dme::convert_dme_payload(payload_obj, hw_cfg) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Ignoring invalid payload(s): {}", &err));
        }
    };

    let telemetry_obj = match serde_json::json!(payload_obj) {
        serde_json::Value::Object(m) => {
            let mut m = m;
            m.insert(
                "dev_id".to_string(),
                serde_json::Value::String(dev_id.to_string()),
            );
            m.insert(
                "type".to_string(),
                serde_json::Value::String(payload_type.to_string()),
            );
            serde_json::Value::Object(m)
        }
        v => v,
    };

    return Ok(Converted(telemetry_obj));
}
fn process_data_dri_type_vav_fancoil(
    payload_str: &str,
    dev_id: &str,
    hw_cfg: &HwInfoDRI,
    payload_type: &str,
) -> Result<PayloadConversionResult, String> {
    let payload_obj: TelemetryDri = match serde_json::from_str(payload_str) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Ignoring invalid payload: {}", &err));
        }
    };

    let payload_obj = match convert_vav_and_fancoil_payload(payload_obj, hw_cfg) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Ignoring invalid payload: {}", &err));
        }
    };

    let telemetry_obj = match serde_json::json!(payload_obj) {
        serde_json::Value::Object(m) => {
            let mut m = m;
            m.insert(
                "dev_id".to_string(),
                serde_json::Value::String(dev_id.to_string()),
            );
            m.insert(
                "type".to_string(),
                serde_json::Value::String(payload_type.to_string()),
            );
            serde_json::Value::Object(m)
        }
        v => v,
    };

    return Ok(Converted(telemetry_obj));
}
fn process_data_dri_type_chiller_carrier_xa(
    payload_str: &str,
    dev_id: &str,
    hw_cfg: &HwInfoDRI,
    payload_type: &str,
) -> Result<PayloadConversionResult, String> {
    let payload_obj: TelemetryDriChillerCarrierXA = match serde_json::from_str(payload_str) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Ignoring invalid payload: {}", &err));
        }
    };

    let payload_obj = match convert_chiller_carrier_xa_payload(payload_obj, hw_cfg) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Ignoring invalid payload: {}", &err));
        }
    };

    let telemetry_obj = match serde_json::json!(payload_obj) {
        serde_json::Value::Object(m) => {
            let mut m = m;
            m.insert(
                "dev_id".to_string(),
                serde_json::Value::String(dev_id.to_string()),
            );
            m.insert(
                "type".to_string(),
                serde_json::Value::String(payload_type.to_string()),
            );
            serde_json::Value::Object(m)
        }
        v => v,
    };

    return Ok(Converted(telemetry_obj));
}
fn process_data_dri_type_chiller_carrier_hx(
    payload_str: &str,
    dev_id: &str,
    hw_cfg: &HwInfoDRI,
    payload_type: &str,
) -> Result<PayloadConversionResult, String> {
    let payload_obj: TelemetryDriChillerCarrierHX = match serde_json::from_str(payload_str) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Ignoring invalid payload: {}", &err));
        }
    };

    let payload_obj = match convert_chiller_carrier_hx_payload(payload_obj, hw_cfg) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Ignoring invalid payload: {}", &err));
        }
    };

    let telemetry_obj = match serde_json::json!(payload_obj) {
        serde_json::Value::Object(m) => {
            let mut m = m;
            m.insert(
                "dev_id".to_string(),
                serde_json::Value::String(dev_id.to_string()),
            );
            m.insert(
                "type".to_string(),
                serde_json::Value::String(payload_type.to_string()),
            );
            serde_json::Value::Object(m)
        }
        v => v,
    };

    return Ok(Converted(telemetry_obj));
}
fn process_data_dri_type_chiller_carrier_30xa_hvar(
    payload_str: &str,
    dev_id: &str,
    hw_cfg: &HwInfoDRI,
    payload_type: &str,
) -> Result<PayloadConversionResult, String> {
    let payload_obj = match serde_json::from_str::<TelemetryDriChillerCarrierXAHvar>(payload_str) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Ignoring invalid payload: {}", &err));
        }
    };

    let payload_obj = match convert_chiller_carrier_xa_hvar_payload(payload_obj, hw_cfg) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Ignoring invalid payload: {}", &err));
        }
    };

    let telemetry_obj = match serde_json::json!(payload_obj) {
        serde_json::Value::Object(m) => {
            let mut m = m;
            m.insert(
                "dev_id".to_string(),
                serde_json::Value::String(dev_id.to_string()),
            );
            m.insert(
                "type".to_string(),
                serde_json::Value::String(payload_type.to_string()),
            );
            serde_json::Value::Object(m)
        }
        v => v,
    };

    return Ok(Converted(telemetry_obj));
}

fn process_data_dam(
    dev_id: &str,
    mut payload_json: serde_json::Value,
    payload_str: &str,
    globs: &Arc<GlobalVars>,
) -> Result<PayloadConversionResult, String> {
    globs.stats.msgsz_dam.on_data(payload_str.len());

    {
        let modifed_value = validate_string_temperature(&payload_json["Temperature"], payload_str);
        if let Some(val) = modifed_value {
            payload_json["Temperature"] = val;
        }
    }
    {
        let modifed_value =
            validate_string_temperature(&payload_json["Temperature_1"], payload_str);
        if let Some(val) = modifed_value {
            payload_json["Temperature_1"] = val;
        }
    }

    return Ok(Converted(payload_json));
}

fn process_data_dal(
    dev_id: &str,
    payload_json: serde_json::Value,
    payload_str: &str,
    globs: &Arc<GlobalVars>,
) -> Result<PayloadConversionResult, String> {
    globs.stats.msgsz_dal.on_data(payload_str.len());

    let payload_obj = match get_raw_telemetry_pack_dal(&payload_json) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Ignoring invalid payload: {}", &err));
        }
    };

    let payload_obj = match dal_telemetry::convert_payload(&payload_obj) {
        Ok(v) => v,
        Err(err) => {
            return Err(format!("Ignoring invalid payload: {}", &err));
        }
    };

    let payload_json = serde_json::to_value(&payload_obj)
        .expect("Erro ao converter a telemetria de json para string");
    return Ok(Converted(payload_json));
}

fn process_control_dut(
    dev_id: &str,
    mut payload_json: serde_json::Value,
    payload_str: &str,
    globs: &Arc<GlobalVars>,
) -> Result<PayloadConversionResult, String> {
    globs.stats.msgsz_dut.on_control(payload_str.len());

    let mut modified = false;

    if let Some("temperature-control-state") = payload_json["msgtype"].as_str() {
        match payload_json["mode"].as_i64() {
            Some(0) => {
                payload_json["ctrl_mode"] = "0_NO_CONTROL".into();
                modified = true;
            }
            Some(1) => {
                payload_json["ctrl_mode"] = "1_CONTROL".into();
                modified = true;
            }
            Some(2) => {
                payload_json["ctrl_mode"] = "2_SOB_DEMANDA".into();
                modified = true;
            }
            Some(3) => {
                payload_json["ctrl_mode"] = "3_BACKUP".into();
                modified = true;
            }
            Some(4) => {
                payload_json["ctrl_mode"] = "4_BLOCKED".into();
                modified = true;
            }
            Some(5) => {
                payload_json["ctrl_mode"] = "5_BACKUP_CONTROL".into();
                modified = true;
            }
            Some(6) => {
                payload_json["ctrl_mode"] = "6_BACKUP_CONTROL_V2".into();
                modified = true;
            }
            Some(7) => {
                payload_json["ctrl_mode"] = "7_FORCED".into();
                modified = true;
            }
            Some(8) => {
                payload_json["ctrl_mode"] = "8_ECO_2".into();
                modified = true;
            }
            _ => {}
        };
        if let Some(temperature_i64) = payload_json["temperature"].as_i64() {
            payload_json["setpoint"] = temperature_i64.into();
            modified = true;
        } else if let Some(temperature_str) = payload_json["temperature"].as_str() {
            if let Ok(temperature_i64) = i64::from_str(temperature_str) {
                payload_json["setpoint"] = temperature_i64.into();
                modified = true;
            }
        }
    }

    if modified {
        return Ok(Converted(payload_json));
    } else {
        return Ok(WithoutConversion);
    }
}

fn process_control_dma(
    dev_id: &str,
    payload_json: serde_json::Value,
    payload_str: &str,
    globs: &Arc<GlobalVars>,
) -> Result<PayloadConversionResult, String> {
    globs.stats.msgsz_dma.on_control(payload_str.len());
    Ok(WithoutConversion)
}

fn process_control_dac(
    dev_id: &str,
    payload_json: serde_json::Value,
    payload_str: &str,
    globs: &Arc<GlobalVars>,
) -> Result<PayloadConversionResult, String> {
    globs.stats.msgsz_dac.on_control(payload_str.len());
    Ok(WithoutConversion)
}

fn process_control_dmt(
    dev_id: &str,
    payload_json: serde_json::Value,
    payload_str: &str,
    globs: &Arc<GlobalVars>,
) -> Result<PayloadConversionResult, String> {
    globs.stats.msgsz_dmt.on_control(payload_str.len());
    Ok(WithoutConversion)
}

fn process_control_dam(
    dev_id: &str,
    payload_json: serde_json::Value,
    payload_str: &str,
    globs: &Arc<GlobalVars>,
) -> Result<PayloadConversionResult, String> {
    globs.stats.msgsz_dam.on_control(payload_str.len());
    Ok(WithoutConversion)
}

fn process_control_dal(
    dev_id: &str,
    payload_json: serde_json::Value,
    payload_str: &str,
    globs: &Arc<GlobalVars>,
) -> Result<PayloadConversionResult, String> {
    globs.stats.msgsz_dal.on_control(payload_str.len());
    Ok(WithoutConversion)
}

fn validate_string_temperature(
    json_prop: &serde_json::Value,
    payload_str: &str,
) -> Option<serde_json::Value> {
    match json_prop.as_str() {
        None => None,
        Some(temperature_str) => match temperature_str.parse::<f64>() {
            Err(_err) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("Invalid DAM temperature [626]: {}", payload_str),
                );
                Some(serde_json::Value::Null)
            }
            Ok(temperature_f64) => {
                if temperature_f64 <= -99.0 {
                    Some(serde_json::Value::Null)
                } else {
                    Some(serde_json::json!(temperature_f64))
                }
            }
        },
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct DacDbState {
    pub cfg_token: Vec<u8>,
    pub state: dac_l1_calculator::L1Calculator,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct DutDbState {
    pub cfg_token: Vec<u8>,
    pub state: dut_l1_calculator::DutStateInfo,
}

/*
    Se duas instâncias tiverem configs diferentes (que pode acontecer por versão de código diferente) as duas vão ficar zerando as configs toda hora.
*/
