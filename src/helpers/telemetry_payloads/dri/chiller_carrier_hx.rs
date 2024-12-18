use super::super::dri_telemetry::{
    ChillerParametersChangesHist, DriChillerCarrierChangeParams, HwInfoDRI,
};
use crate::telemetry_payloads::energy::padronized::calculateFormulas;
use chrono::{Local, NaiveDateTime, Timelike};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TelemetryDriChillerCarrierHX<'a> {
    pub dev_id: Cow<'a, String>,
    pub timestamp: Cow<'a, String>,
    #[serde(rename = "type")]
    pub dev_type: Cow<'a, String>,
    pub CHIL_S_S: Option<i16>,
    pub ALM: Option<i16>,
    pub alarm_1: Option<i16>,
    pub alarm_2: Option<i16>,
    pub alarm_3: Option<i16>,
    pub alarm_4: Option<i16>,
    pub alarm_5: Option<i16>,
    pub CAP_T: Option<i16>,
    pub DEM_LIM: Option<i16>,
    pub LAG_LIM: Option<i16>,
    pub SP: Option<i16>,
    pub CTRL_PNT: Option<i16>,
    pub EMSTOP: Option<i16>,
    pub CP_A1: Option<i16>,
    pub CP_A2: Option<i16>,
    pub CAPA_T: Option<i16>,
    pub DP_A: Option<i16>,
    pub SP_A: Option<i16>,
    pub SCT_A: Option<i16>,
    pub SST_A: Option<i16>,
    pub CP_B1: Option<i16>,
    pub CP_B2: Option<i16>,
    pub CAPB_T: Option<i16>,
    pub DP_B: Option<i16>,
    pub SP_B: Option<i16>,
    pub SCT_B: Option<i16>,
    pub SST_B: Option<i16>,
    pub COND_LWT: Option<i16>,
    pub COND_EWT: Option<i16>,
    pub COOL_LWT: Option<i16>,
    pub COOL_EWT: Option<i16>,
    pub CPA1_OP: Option<i16>,
    pub CPA2_OP: Option<i16>,
    pub DOP_A1: Option<i16>,
    pub DOP_A2: Option<i16>,
    pub CPA1_DGT: Option<i16>,
    pub CPA2_DGT: Option<i16>,
    pub EXV_A: Option<i16>,
    pub HR_CP_A1: Option<i16>,
    pub HR_CP_A2: Option<i16>,
    pub CPA1_TMP: Option<i16>,
    pub CPA2_TMP: Option<i16>,
    pub CPA1_CUR: Option<i16>,
    pub CPA2_CUR: Option<i16>,
    pub CPB1_OP: Option<i16>,
    pub CPB2_OP: Option<i16>,
    pub DOP_B1: Option<i16>,
    pub DOP_B2: Option<i16>,
    pub CPB1_DGT: Option<i16>,
    pub CPB2_DGT: Option<i16>,
    pub EXV_B: Option<i16>,
    pub HR_CP_B1: Option<i16>,
    pub HR_CP_B2: Option<i16>,
    pub CPB1_TMP: Option<i16>,
    pub CPB2_TMP: Option<i16>,
    pub CPB1_CUR: Option<i16>,
    pub CPB2_CUR: Option<i16>,
    pub COND_SP: Option<i16>,
    pub CHIL_OCC: Option<i16>,
    pub STATUS: Option<i16>,
    pub formulas: Option<HashMap<String, String>>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DriChillerCarrierHXTelemetry {
    pub timestamp: String,
    pub CHIL_S_S: Option<f64>,
    pub ALM: Option<f64>,
    pub alarm_1: Option<f64>,
    pub alarm_2: Option<f64>,
    pub alarm_3: Option<f64>,
    pub alarm_4: Option<f64>,
    pub alarm_5: Option<f64>,
    pub CAP_T: Option<f64>,
    pub DEM_LIM: Option<f64>,
    pub LAG_LIM: Option<f64>,
    pub SP: Option<f64>,
    pub CTRL_PNT: Option<f64>,
    pub EMSTOP: Option<f64>,
    pub CP_A1: Option<f64>,
    pub CP_A2: Option<f64>,
    pub CAPA_T: Option<f64>,
    pub DP_A: Option<f64>,
    pub SP_A: Option<f64>,
    pub SCT_A: Option<f64>,
    pub SST_A: Option<f64>,
    pub CP_B1: Option<f64>,
    pub CP_B2: Option<f64>,
    pub CAPB_T: Option<f64>,
    pub DP_B: Option<f64>,
    pub SP_B: Option<f64>,
    pub SCT_B: Option<f64>,
    pub SST_B: Option<f64>,
    pub COND_LWT: Option<f64>,
    pub COND_EWT: Option<f64>,
    pub COOL_LWT: Option<f64>,
    pub COOL_EWT: Option<f64>,
    pub CPA1_OP: Option<f64>,
    pub CPA2_OP: Option<f64>,
    pub DOP_A1: Option<f64>,
    pub DOP_A2: Option<f64>,
    pub CPA1_DGT: Option<f64>,
    pub CPA2_DGT: Option<f64>,
    pub EXV_A: Option<f64>,
    pub HR_CP_A1: Option<f64>,
    pub HR_CP_A2: Option<f64>,
    pub CPA1_TMP: Option<f64>,
    pub CPA2_TMP: Option<f64>,
    pub CPA1_CUR: Option<f64>,
    pub CPA2_CUR: Option<f64>,
    pub CPB1_OP: Option<f64>,
    pub CPB2_OP: Option<f64>,
    pub DOP_B1: Option<f64>,
    pub DOP_B2: Option<f64>,
    pub CPB1_DGT: Option<f64>,
    pub CPB2_DGT: Option<f64>,
    pub EXV_B: Option<f64>,
    pub HR_CP_B1: Option<f64>,
    pub HR_CP_B2: Option<f64>,
    pub CPB1_TMP: Option<f64>,
    pub CPB2_TMP: Option<f64>,
    pub CPB1_CUR: Option<f64>,
    pub CPB2_CUR: Option<f64>,
    pub COND_SP: Option<f64>,
    pub CHIL_OCC: Option<f64>,
    pub STATUS: Option<f64>,
    pub record_date: Option<String>,
}

impl<'a> TryFrom<TelemetryDriChillerCarrierHX<'a>> for DriChillerCarrierHXTelemetry {
    type Error = String;
    fn try_from(
        value: TelemetryDriChillerCarrierHX,
    ) -> Result<DriChillerCarrierHXTelemetry, String> {
        let tel = json!(value);

        let result = DriChillerCarrierHXTelemetry {
            timestamp: value.timestamp.to_string(),
            CHIL_S_S: match value.CHIL_S_S {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CHIL_S_S",
                    value.CHIL_S_S.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            ALM: match value.ALM {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "ALM",
                    value.ALM.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            alarm_1: match value.alarm_1 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "alarm_1",
                    value.alarm_1.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            alarm_2: match value.alarm_2 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "alarm_2",
                    value.alarm_2.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            alarm_3: match value.alarm_3 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "alarm_3",
                    value.alarm_3.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            alarm_4: match value.alarm_4 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "alarm_4",
                    value.alarm_4.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            alarm_5: match value.alarm_5 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "alarm_5",
                    value.alarm_5.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CAP_T: match value.CAP_T {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CAP_T",
                    value.CAP_T.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DEM_LIM: match value.DEM_LIM {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DEM_LIM",
                    value.DEM_LIM.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            LAG_LIM: match value.LAG_LIM {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "LAG_LIM",
                    value.LAG_LIM.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SP: match value.SP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SP",
                    value.SP.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CTRL_PNT: match value.CTRL_PNT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CTRL_PNT",
                    value.CTRL_PNT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            EMSTOP: match value.EMSTOP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "EMSTOP",
                    value.EMSTOP.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CP_A1: match value.CP_A1 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CP_A1",
                    value.CP_A1.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CP_A2: match value.CP_A2 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CP_A2",
                    value.CP_A2.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CAPA_T: match value.CAPA_T {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CAPA_T",
                    value.CAPA_T.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DP_A: match value.DP_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DP_A",
                    value.DP_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SP_A: match value.SP_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SP_A",
                    value.SP_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SCT_A: match value.SCT_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SCT_A",
                    value.SCT_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SST_A: match value.SST_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SST_A",
                    value.SST_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CP_B1: match value.CP_B1 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CP_B1",
                    value.CP_B1.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CP_B2: match value.CP_B2 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CP_B2",
                    value.CP_B2.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CAPB_T: match value.CAPB_T {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CAPB_T",
                    value.CAPB_T.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DP_B: match value.DP_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DP_B",
                    value.DP_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SP_B: match value.SP_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SP_B",
                    value.SP_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SCT_B: match value.SCT_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SCT_B",
                    value.SCT_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SST_B: match value.SST_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SST_B",
                    value.SST_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            COND_LWT: match value.COND_LWT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "COND_LWT",
                    value.COND_LWT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            COND_EWT: match value.COND_EWT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "COND_EWT",
                    value.COND_EWT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            COOL_LWT: match value.COOL_LWT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "COOL_LWT",
                    value.COOL_LWT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            COOL_EWT: match value.COOL_EWT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "COOL_EWT",
                    value.COOL_EWT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPA1_OP: match value.CPA1_OP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPA1_OP",
                    value.CPA1_OP.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPA2_OP: match value.CPA2_OP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPA2_OP",
                    value.CPA2_OP.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DOP_A1: match value.DOP_A1 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DOP_A1",
                    value.DOP_A1.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DOP_A2: match value.DOP_A2 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DOP_A2",
                    value.DOP_A2.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPA1_DGT: match value.CPA1_DGT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPA1_DGT",
                    value.CPA1_DGT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPA2_DGT: match value.CPA2_DGT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPA2_DGT",
                    value.CPA2_DGT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            EXV_A: match value.EXV_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "EXV_A",
                    value.EXV_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            HR_CP_A1: match value.HR_CP_A1 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "HR_CP_A1",
                    value.HR_CP_A1.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            HR_CP_A2: match value.HR_CP_A2 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "HR_CP_A2",
                    value.HR_CP_A2.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPA1_TMP: match value.CPA1_TMP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPA1_TMP",
                    value.CPA1_TMP.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPA2_TMP: match value.CPA2_TMP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPA2_TMP",
                    value.CPA2_TMP.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPA1_CUR: match value.CPA1_CUR {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPA1_CUR",
                    value.CPA1_CUR.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPA2_CUR: match value.CPA2_CUR {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPA2_CUR",
                    value.CPA2_CUR.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPB1_OP: match value.CPB1_OP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPB1_OP",
                    value.CPB1_OP.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPB2_OP: match value.CPB2_OP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPB2_OP",
                    value.CPB2_OP.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DOP_B1: match value.DOP_B1 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DOP_B1",
                    value.DOP_B1.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DOP_B2: match value.DOP_B2 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DOP_B2",
                    value.DOP_B2.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPB1_DGT: match value.CPB1_DGT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPB1_DGT",
                    value.CPB1_DGT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPB2_DGT: match value.CPB2_DGT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPB2_DGT",
                    value.CPB2_DGT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            EXV_B: match value.EXV_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "EXV_B",
                    value.EXV_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            HR_CP_B1: match value.HR_CP_B1 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "HR_CP_B1",
                    value.HR_CP_B1.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            HR_CP_B2: match value.HR_CP_B2 {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "HR_CP_B2",
                    value.HR_CP_B2.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPB1_TMP: match value.CPB1_TMP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPB1_TMP",
                    value.CPB1_TMP.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPB2_TMP: match value.CPB2_TMP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPB2_TMP",
                    value.CPB2_TMP.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPB1_CUR: match value.CPB1_CUR {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPB1_CUR",
                    value.CPB1_CUR.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CPB2_CUR: match value.CPB2_CUR {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CPB2_CUR",
                    value.CPB2_CUR.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            COND_SP: match value.COND_SP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "COND_SP",
                    value.COND_SP.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CHIL_OCC: match value.CHIL_OCC {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CHIL_OCC",
                    value.CHIL_OCC.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            STATUS: match value.STATUS {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "STATUS",
                    value.STATUS.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            record_date: None,
        };
        Ok(result)
    }
}

pub fn convert_chiller_carrier_hx_payload<'a>(
    mut payload: TelemetryDriChillerCarrierHX<'a>,
    dev: &'a HwInfoDRI,
) -> Result<DriChillerCarrierHXTelemetry, String> {
    if dev.formulas.is_some() {
        payload.formulas = dev.formulas.clone();
    }
    return payload.try_into();
}

impl DriChillerCarrierHXTelemetry {
    pub fn group_telemetries(
        device_code: &str,
        telemetries: Vec<DriChillerCarrierHXTelemetry>,
        hour_graphic: bool,
    ) -> (
        HashMap<NaiveDateTime, Vec<DriChillerCarrierHXTelemetry>>,
        Vec<ChillerParametersChangesHist>,
    ) {
        let mut grouped_telemetries: HashMap<NaiveDateTime, Vec<DriChillerCarrierHXTelemetry>> =
            HashMap::new();
        let mut last_telemetry = DriChillerCarrierChangeParams::new(Local::now().naive_local());
        let mut params_changes_hist = Vec::<ChillerParametersChangesHist>::new();
        let interval = if hour_graphic { 60 } else { 10 };

        for (index, telemetry) in telemetries.iter().enumerate() {
            let timestamp =
                NaiveDateTime::parse_from_str(&telemetry.timestamp, "%Y-%m-%dT%H:%M:%S")
                    .unwrap_or_default();
            let rounded_minute = (timestamp.minute() / interval) * interval;
            let rounded_timestamp = timestamp
                .date()
                .and_hms(timestamp.hour(), rounded_minute, 0);
            grouped_telemetries
                .entry(rounded_timestamp)
                .or_insert_with(Vec::new)
                .push(telemetry.clone());

            let iterate_telemetry = DriChillerCarrierChangeParams {
                timestamp,
                CHIL_S_S: telemetry.CHIL_S_S,
                ALM: telemetry.ALM,
                EMSTOP: telemetry.EMSTOP,
                STATUS: telemetry.STATUS,
                CHIL_OCC: telemetry.CHIL_OCC,
                CP_A1: telemetry.CP_A1,
                CP_A2: telemetry.CP_A2,
                CP_B1: telemetry.CP_B1,
                CP_B2: telemetry.CP_B2,
            };

            if index == 0 || index == telemetries.len() - 1 {
                // primeira telemetria ou última, salvar dados
                Self::salve_all_params_change(
                    device_code,
                    iterate_telemetry,
                    &mut params_changes_hist,
                );
            } else {
                Self::verify_and_save_params_changes(
                    device_code,
                    iterate_telemetry,
                    last_telemetry.clone(),
                    &mut params_changes_hist,
                );
            }

            last_telemetry.timestamp = timestamp;
            last_telemetry.CHIL_S_S = telemetry.CHIL_S_S;
            last_telemetry.ALM = telemetry.ALM;
            last_telemetry.EMSTOP = telemetry.EMSTOP;
            last_telemetry.STATUS = telemetry.STATUS;
            last_telemetry.CHIL_OCC = telemetry.CHIL_OCC;
            last_telemetry.CP_A1 = telemetry.CP_A1;
            last_telemetry.CP_A2 = telemetry.CP_A2;
            last_telemetry.CP_B1 = telemetry.CP_B1;
            last_telemetry.CP_B2 = telemetry.CP_B2;
        }

        (grouped_telemetries, params_changes_hist)
    }

    pub fn calculate_group_averages(
        grouped_telemetries: &HashMap<NaiveDateTime, Vec<DriChillerCarrierHXTelemetry>>,
    ) -> Vec<DriChillerCarrierHXTelemetry> {
        let mut group_averages: Vec<DriChillerCarrierHXTelemetry> = Vec::new();

        for (time_interval, telemetry_array) in grouped_telemetries {
            let mut total_values: HashMap<&str, (f64, usize)> = HashMap::new();

            for telemetry in telemetry_array {
                for (field, value) in &[
                    ("CAP_T", telemetry.CAP_T),
                    ("DEM_LIM", telemetry.DEM_LIM),
                    ("LAG_LIM", telemetry.LAG_LIM),
                    ("SP", telemetry.SP),
                    ("CTRL_PNT", telemetry.CTRL_PNT),
                    ("CAPA_T", telemetry.CAPA_T),
                    ("DP_A", telemetry.DP_A),
                    ("SP_A", telemetry.SP_A),
                    ("SCT_A", telemetry.SCT_A),
                    ("SST_A", telemetry.SST_A),
                    ("CAPB_T", telemetry.CAPB_T),
                    ("DP_B", telemetry.DP_B),
                    ("SP_B", telemetry.SP_B),
                    ("SCT_B", telemetry.SCT_B),
                    ("SST_B", telemetry.SST_B),
                    ("COND_LWT", telemetry.COND_LWT),
                    ("COND_EWT", telemetry.COND_EWT),
                    ("COOL_LWT", telemetry.COOL_LWT),
                    ("COOL_EWT", telemetry.COOL_EWT),
                    ("CPA1_OP", telemetry.CPA1_OP),
                    ("CPA2_OP", telemetry.CPA2_OP),
                    ("DOP_A1", telemetry.DOP_A1),
                    ("DOP_A2", telemetry.DOP_A2),
                    ("CPA1_DGT", telemetry.CPA1_DGT),
                    ("CPA2_DGT", telemetry.CPA2_DGT),
                    ("EXV_A", telemetry.EXV_A),
                    ("HR_CP_A1", telemetry.HR_CP_A1),
                    ("HR_CP_A2", telemetry.HR_CP_A2),
                    ("CPA1_TMP", telemetry.CPA1_TMP),
                    ("CPA2_TMP", telemetry.CPA2_TMP),
                    ("CPA1_CUR", telemetry.CPA1_CUR),
                    ("CPA2_CUR", telemetry.CPA2_CUR),
                    ("CPB1_OP", telemetry.CPB1_OP),
                    ("CPB2_OP", telemetry.CPB2_OP),
                    ("DOP_B1", telemetry.DOP_B1),
                    ("DOP_B2", telemetry.DOP_B2),
                    ("CPB1_DGT", telemetry.CPB1_DGT),
                    ("CPB2_DGT", telemetry.CPB2_DGT),
                    ("EXV_B", telemetry.EXV_B),
                    ("HR_CP_B1", telemetry.HR_CP_B1),
                    ("HR_CP_B2", telemetry.HR_CP_B2),
                    ("CPB1_TMP", telemetry.CPB1_TMP),
                    ("CPB2_TMP", telemetry.CPB2_TMP),
                    ("CPB1_CUR", telemetry.CPB1_CUR),
                    ("CPB2_CUR", telemetry.CPB2_CUR),
                    ("COND_SP", telemetry.COND_SP),
                ] {
                    if let Some(v) = value {
                        let (sum, count_val) = total_values.entry(field).or_insert((0.0, 0));
                        *sum += v;
                        *count_val += 1;
                    }
                }
            }
            let mut averaged_telemetry =
                DriChillerCarrierHXTelemetry::new(time_interval.to_string());
            let date_clone = &averaged_telemetry.timestamp.clone();
            averaged_telemetry.record_date =
                Some([&date_clone[..10], "T", &date_clone[11..]].concat());

            for (field, (total, field_count)) in total_values {
                let avg = total / field_count as f64;
                let avg_rounded = (avg * 100.0).round() / 100.0;
                averaged_telemetry.set_field_average(field, avg_rounded);
            }

            group_averages.push(averaged_telemetry);
        }

        group_averages
    }

    fn verify_and_save_params_changes(
        device_code: &str,
        iterate_telemetry: DriChillerCarrierChangeParams,
        last_telemetry: DriChillerCarrierChangeParams,
        history_vec: &mut Vec<ChillerParametersChangesHist>,
    ) {
        Self::save_if_changed(
            "CHIL_S_S",
            last_telemetry.CHIL_S_S,
            iterate_telemetry.CHIL_S_S,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "EMSTOP",
            last_telemetry.EMSTOP,
            iterate_telemetry.EMSTOP,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "ALM",
            last_telemetry.ALM,
            iterate_telemetry.ALM,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "STATUS",
            last_telemetry.STATUS,
            iterate_telemetry.STATUS,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "CHIL_OCC",
            last_telemetry.CHIL_OCC,
            iterate_telemetry.CHIL_OCC,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "CP_A1",
            last_telemetry.CP_A1,
            iterate_telemetry.CP_A1,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "CP_A2",
            last_telemetry.CP_A2,
            iterate_telemetry.CP_A2,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "CP_B1",
            last_telemetry.CP_B1,
            iterate_telemetry.CP_B1,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "CP_B2",
            last_telemetry.CP_B2,
            iterate_telemetry.CP_B2,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
    }

    fn save_if_changed(
        param_name: &str,
        last_value: Option<f64>,
        new_value: Option<f64>,
        timestamp: NaiveDateTime,
        device_code: &str,
        history_vec: &mut Vec<ChillerParametersChangesHist>,
    ) {
        if let Some(new_val) = new_value {
            if last_value != new_value {
                Self::save_param_change_hist(
                    timestamp,
                    device_code,
                    param_name,
                    Some(new_val),
                    history_vec,
                );
            }
        }
    }

    fn save_param_change_hist(
        day: NaiveDateTime,
        device_code: &str,
        parameter_name: &str,
        parameter_value: Option<f64>,
        history_vec: &mut Vec<ChillerParametersChangesHist>,
    ) {
        if let Some(value) = parameter_value {
            let history = ChillerParametersChangesHist {
                device_code: String::from(device_code),
                parameter_name: parameter_name.to_string(),
                record_date: day,
                parameter_value: value.round() as i32,
            };

            history_vec.push(history);
        }
    }

    fn salve_all_params_change(
        device_code: &str,
        last_telemetry: DriChillerCarrierChangeParams,
        history_vec: &mut Vec<ChillerParametersChangesHist>,
    ) {
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "CHIL_S_S",
            last_telemetry.CHIL_S_S,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "EMSTOP",
            last_telemetry.EMSTOP,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "ALM",
            last_telemetry.ALM,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "STATUS",
            last_telemetry.STATUS,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "CHIL_OCC",
            last_telemetry.CHIL_OCC,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "CP_A2",
            last_telemetry.CP_A2,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "CP_A1",
            last_telemetry.CP_A1,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "CP_B1",
            last_telemetry.CP_B1,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "CP_B2",
            last_telemetry.CP_B2,
            history_vec,
        );
    }

    fn set_field_average(&mut self, field: &str, value: f64) {
        match field {
            "CAP_T" => self.CAP_T = Some(value),
            "DEM_LIM" => self.DEM_LIM = Some(value),
            "LAG_LIM" => self.LAG_LIM = Some(value),
            "SP" => self.SP = Some(value),
            "CTRL_PNT" => self.CTRL_PNT = Some(value),
            "CAPA_T" => self.CAPA_T = Some(value),
            "DP_A" => self.DP_A = Some(value),
            "SP_A" => self.SP_A = Some(value),
            "SCT_A" => self.SCT_A = Some(value),
            "SST_A" => self.SST_A = Some(value),
            "CAPB_T" => self.CAPB_T = Some(value),
            "DP_B" => self.DP_B = Some(value),
            "SP_B" => self.SP_B = Some(value),
            "SCT_B" => self.SCT_B = Some(value),
            "SST_B" => self.SST_B = Some(value),
            "COND_LWT" => self.COND_LWT = Some(value),
            "COND_EWT" => self.COND_EWT = Some(value),
            "COOL_LWT" => self.COOL_LWT = Some(value),
            "COOL_EWT" => self.COOL_EWT = Some(value),
            "CPA1_OP" => self.CPA1_OP = Some(value),
            "CPA2_OP" => self.CPA2_OP = Some(value),
            "DOP_A1" => self.DOP_A1 = Some(value),
            "DOP_A2" => self.DOP_A2 = Some(value),
            "CPA1_DGT" => self.CPA1_DGT = Some(value),
            "CPA2_DGT" => self.CPA2_DGT = Some(value),
            "EXV_A" => self.EXV_A = Some(value),
            "HR_CP_A1" => self.HR_CP_A1 = Some(value),
            "HR_CP_A2" => self.HR_CP_A2 = Some(value),
            "CPA1_TMP" => self.CPA1_TMP = Some(value),
            "CPA2_TMP" => self.CPA2_TMP = Some(value),
            "CPA1_CUR" => self.CPA1_CUR = Some(value),
            "CPA2_CUR" => self.CPA2_CUR = Some(value),
            "CPB1_OP" => self.CPB1_OP = Some(value),
            "CPB2_OP" => self.CPB2_OP = Some(value),
            "DOP_B1" => self.DOP_B1 = Some(value),
            "DOP_B2" => self.DOP_B2 = Some(value),
            "CPB1_DGT" => self.CPB1_DGT = Some(value),
            "CPB2_DGT" => self.CPB2_DGT = Some(value),
            "EXV_B" => self.EXV_B = Some(value),
            "HR_CP_B1" => self.HR_CP_B1 = Some(value),
            "HR_CP_B2" => self.HR_CP_B2 = Some(value),
            "CPB1_TMP" => self.CPB1_TMP = Some(value),
            "CPB2_TMP" => self.CPB2_TMP = Some(value),
            "CPB1_CUR" => self.CPB1_CUR = Some(value),
            "CPB2_CUR" => self.CPB2_CUR = Some(value),
            "COND_SP" => self.COND_SP = Some(value),
            _ => (),
        }
    }

    fn new(timestamp: String) -> Self {
        Self {
            timestamp,
            CHIL_S_S: None,
            ALM: None,
            alarm_1: None,
            alarm_2: None,
            alarm_3: None,
            alarm_4: None,
            alarm_5: None,
            CAP_T: None,
            DEM_LIM: None,
            LAG_LIM: None,
            SP: None,
            CTRL_PNT: None,
            EMSTOP: None,
            CAPA_T: None,
            DP_A: None,
            SP_A: None,
            SCT_A: None,
            SST_A: None,
            CAPB_T: None,
            DP_B: None,
            SP_B: None,
            SCT_B: None,
            SST_B: None,
            COND_LWT: None,
            COND_EWT: None,
            COOL_LWT: None,
            COOL_EWT: None,
            CPA1_OP: None,
            CPA2_OP: None,
            DOP_A1: None,
            DOP_A2: None,
            CPA1_DGT: None,
            CPA2_DGT: None,
            EXV_A: None,
            HR_CP_A1: None,
            HR_CP_A2: None,
            CPA1_TMP: None,
            CPA2_TMP: None,
            CPA1_CUR: None,
            CPA2_CUR: None,
            CPB1_OP: None,
            CPB2_OP: None,
            DOP_B1: None,
            DOP_B2: None,
            CPB1_DGT: None,
            CPB2_DGT: None,
            EXV_B: None,
            HR_CP_B1: None,
            HR_CP_B2: None,
            CPB1_TMP: None,
            CPB2_TMP: None,
            CPB1_CUR: None,
            CPB2_CUR: None,
            COND_SP: None,
            CHIL_OCC: None,
            CP_A1: None,
            CP_A2: None,
            CP_B1: None,
            CP_B2: None,
            STATUS: None,
            record_date: None,
        }
    }
}
