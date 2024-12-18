use serde_json::Map;

pub fn merge_processed_values(
    payload_json: &mut serde_json::Value,
    processed_payload: Option<serde_json::Value>,
    dev_id: &str,
) {
    let Some(processed_payload) = &processed_payload else {
        return;
    };
    match &dev_id[0..3] {
        "DAC" => {
            merge_dac_values(payload_json, processed_payload);
            return;
        }
        "DUT" => {
            merge_dut_values(payload_json, processed_payload);
            return;
        }
        "DRI" => {
            merge_dri_values(payload_json, processed_payload);
            return;
        }
        "DMA" => {
            return;
        }
        "DMT" => {
            return;
        }
        "DAM" => {
            return;
        }
        "DAL" => {
            return;
        }
        _ => {}
    };
    match &dev_id[0..3].to_uppercase()[..] {
        "DAC" => {
            merge_dac_values(payload_json, processed_payload);
        }
        "DUT" => {
            merge_dut_values(payload_json, processed_payload);
        }
        "DRI" => {
            merge_dri_values(payload_json, processed_payload);
        }
        _ => {}
    };
}

fn merge_dac_values(payload_json: &mut serde_json::Value, processed_payload: &serde_json::Value) {
    if processed_payload["Lcmp"].is_array() {
        payload_json["Lcmp"] = processed_payload["Lcmp"].clone();
    }
    if processed_payload["Lcut"].is_array() {
        payload_json["Lcut"] = processed_payload["Lcut"].clone();
    }
    if processed_payload["Levp"].is_array() {
        payload_json["Levp"] = processed_payload["Levp"].clone();
    }
    if processed_payload["Tamb"].is_array() {
        payload_json["Tamb"] = processed_payload["Tamb"].clone();
    }
    if processed_payload["Tsuc"].is_array() {
        payload_json["Tsuc"] = processed_payload["Tsuc"].clone();
    }
    if processed_payload["Tliq"].is_array() {
        payload_json["Tliq"] = processed_payload["Tliq"].clone();
    }
    if processed_payload["Psuc"].is_array() {
        payload_json["Psuc"] = processed_payload["Psuc"].clone();
    }
    if processed_payload["Pliq"].is_array() {
        payload_json["Pliq"] = processed_payload["Pliq"].clone();
    }
    if processed_payload["Tsc"].is_array() {
        payload_json["Tsc"] = processed_payload["Tsc"].clone();
    }
    if processed_payload["Tsh"].is_array() {
        payload_json["Tsh"] = processed_payload["Tsh"].clone();
    }
}

fn merge_dut_values(payload_json: &mut serde_json::Value, processed_payload: &serde_json::Value) {
    if processed_payload["L1"].is_array() {
        payload_json["L1"] = processed_payload["L1"].clone();
    }
}

fn merge_dri_values(payload_json: &mut serde_json::Value, processed_payload: &serde_json::Value) {
    let mut orig_raw: Map<String, serde_json::Value> = Map::new();
    let payload_obj = payload_json.as_object_mut().unwrap();
    let processed_obj = processed_payload.as_object().unwrap();

    put_calculated(payload_obj, &mut orig_raw, processed_obj, "ALM");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CAPA_T");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CAPB_T");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CAP_T");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CHIL_OCC");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CHIL_S_S");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "COND_EWT");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "COND_LWT");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "COND_SP");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "COOL_EWT");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "COOL_LWT");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPA1_CUR");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPA1_DGT");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPA1_OP");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPA1_TMP");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPA2_CUR");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPA2_DGT");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPA2_OP");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPA2_TMP");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPB1_CUR");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPB1_DGT");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPB1_OP");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPB1_TMP");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPB2_CUR");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPB2_DGT");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPB2_OP");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CPB2_TMP");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CP_A1");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CP_A2");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CP_B1");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CP_B2");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CTRL_PNT");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "CTRL_TYP");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "DEM_LIM");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "DOP_A1");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "DOP_A2");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "DOP_B1");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "DOP_B2");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "DP_A");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "DP_B");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "EMSTOP");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "EXV_A");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "EXV_B");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "FanStatus");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "Fanspeed");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "HR_CP_A");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "HR_CP_A1");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "HR_CP_A2");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "HR_CP_B");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "HR_CP_B1");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "HR_CP_B2");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "HR_MACH");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "HR_MACH_B");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "LAG_LIM");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "Lock");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "Mode");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "OAT");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "OP_A");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "OP_B");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "SCT_A");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "SCT_B");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "SLC_HM");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "SLT_A");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "SLT_B");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "SP");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "SP_A");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "SP_B");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "SP_OCC");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "SST_A");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "SST_B");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "STATUS");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "Setpoint");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "TempAmb");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "ThermOn");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "ValveOn");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "alarm_1");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "alarm_2");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "alarm_3");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "alarm_4");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "alarm_5");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "demanda");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "demanda_ap");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "demanda_at");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "demanda_med_at");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "en_ap_tri");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "en_at_tri");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "en_re_tri");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "erro");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "fp");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "fp_a");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "fp_b");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "fp_c");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "freq");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "i_a");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "i_b");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "i_c");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "pot_ap_a");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "pot_ap_b");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "pot_ap_c");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "pot_ap_tri");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "pot_at_a");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "pot_at_b");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "pot_at_c");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "pot_at_tri");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "pot_re_a");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "pot_re_b");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "pot_re_c");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "pot_re_tri");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "record_date");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "v_a");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "v_ab");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "v_b");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "v_bc");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "v_c");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "v_ca");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "v_tri_ll");
    put_calculated(payload_obj, &mut orig_raw, processed_obj, "v_tri_ln");

    if !orig_raw.is_empty() {
        payload_obj.insert("orig_raw".to_owned(), serde_json::Value::Object(orig_raw));
    }
}

fn put_calculated(
    payload_obj: &mut Map<String, serde_json::Value>,
    orig_raw: &mut Map<String, serde_json::Value>,
    processed_obj: &Map<String, serde_json::Value>,
    property: &str,
) {
    if let Some(cal_val) = processed_obj.get(property) {
        if let Some(orig_val) = payload_obj.remove(property) {
            orig_raw.insert(property.to_owned(), orig_val);
        }
        payload_obj.insert(property.to_owned(), cal_val.to_owned());
    }
}
