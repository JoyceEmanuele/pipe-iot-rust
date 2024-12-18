use super::super::dri_telemetry::{ChillerParametersChangesHist, HwInfoDRI};
use crate::telemetry_payloads::energy::padronized::calculateFormulas;
use chrono::{Local, NaiveDateTime, Timelike};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::borrow::Cow;
use std::collections::HashMap;
use std::convert::TryFrom;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DriChillerCarrierXAHvarTelemetry {
    pub timestamp: NaiveDateTime,
    pub GENUNIT_UI: Option<f64>,
    pub CTRL_TYP: Option<f64>,
    pub STATUS: Option<f64>,
    pub ALM: Option<f64>,
    pub SP_OCC: Option<f64>,
    pub CHIL_S_S: Option<f64>,
    pub CHIL_OCC: Option<f64>,
    pub CAP_T: Option<f64>,
    pub DEM_LIM: Option<f64>,
    pub TOT_CURR: Option<f64>,
    pub CTRL_PNT: Option<f64>,
    pub OAT: Option<f64>,
    pub COOL_EWT: Option<f64>,
    pub COOL_LWT: Option<f64>,
    pub EMSTOP: Option<f64>,
    pub CIRCA_AN_UI: Option<f64>,
    pub CAPA_T: Option<f64>,
    pub DP_A: Option<f64>,
    pub SP_A: Option<f64>,
    pub ECON_P_A: Option<f64>,
    pub OP_A: Option<f64>,
    pub DOP_A: Option<f64>,
    pub CURREN_A: Option<f64>,
    pub CP_TMP_A: Option<f64>,
    pub DGT_A: Option<f64>,
    pub ECO_TP_A: Option<f64>,
    pub SCT_A: Option<f64>,
    pub SST_A: Option<f64>,
    pub SST_B: Option<f64>,
    pub SUCT_T_A: Option<f64>,
    pub EXV_A: Option<f64>,
    pub CIRCB_AN_UI: Option<f64>,
    pub CAPB_T: Option<f64>,
    pub DP_B: Option<f64>,
    pub SP_B: Option<f64>,
    pub ECON_P_B: Option<f64>,
    pub OP_B: Option<f64>,
    pub DOP_B: Option<f64>,
    pub CURREN_B: Option<f64>,
    pub CP_TMP_B: Option<f64>,
    pub DGT_B: Option<f64>,
    pub ECO_TP_B: Option<f64>,
    pub SCT_B: Option<f64>,
    pub SUCT_T_B: Option<f64>,
    pub EXV_B: Option<f64>,
    pub CIRCC_AN_UI: Option<f64>,
    pub CAPC_T: Option<f64>,
    pub DP_C: Option<f64>,
    pub SP_C: Option<f64>,
    pub ECON_P_C: Option<f64>,
    pub OP_C: Option<f64>,
    pub DOP_C: Option<f64>,
    pub CURREN_C: Option<f64>,
    pub CP_TMP_C: Option<f64>,
    pub DGT_C: Option<f64>,
    pub ECO_TP_C: Option<f64>,
    pub SCT_C: Option<f64>,
    pub SST_C: Option<f64>,
    pub SUCT_T_C: Option<f64>,
    pub EXV_C: Option<f64>,
    pub record_date: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TelemetryDriChillerCarrierXAHvar<'a> {
    pub dev_id: Cow<'a, String>,
    pub timestamp: Cow<'a, String>,
    #[serde(rename = "type")]
    pub dev_type: Cow<'a, String>,
    pub CAP_T: Option<i16>,
    pub CHIL_OCC: Option<i16>,
    pub CHIL_S_S: Option<i16>,
    pub COOL_EWT: Option<i16>,
    pub COOL_LWT: Option<i16>,
    pub CTRL_PNT: Option<i16>,
    pub CTRL_TYP: Option<i16>,
    pub DEM_LIM: Option<i16>,
    pub DP_A: Option<i16>,
    pub DP_B: Option<i16>,
    pub EMSTOP: Option<i16>,
    pub OAT: Option<i16>,
    pub OP_A: Option<i16>,
    pub OP_B: Option<i16>,
    pub SCT_A: Option<i16>,
    pub SCT_B: Option<i16>,
    pub SP_A: Option<i16>,
    pub SP_B: Option<i16>,
    pub SP_OCC: Option<i16>,
    pub SST_A: Option<i16>,
    pub SST_B: Option<i16>,
    pub STATUS: Option<i16>,
    pub GENUNIT_UI: Option<i16>,
    pub ALM: Option<i16>,
    pub TOT_CURR: Option<i16>,
    pub CIRCA_AN_UI: Option<i16>,
    pub CAPA_T: Option<i16>,
    pub ECON_P_A: Option<i16>,
    pub DOP_A: Option<i16>,
    pub CURREN_A: Option<i16>,
    pub CP_TMP_A: Option<i16>,
    pub DGT_A: Option<i16>,
    pub ECO_TP_A: Option<i16>,
    pub SUCT_T_A: Option<i16>,
    pub EXV_A: Option<i16>,
    pub CIRCB_AN_UI: Option<i16>,
    pub CAPB_T: Option<i16>,
    pub ECON_P_B: Option<i16>,
    pub DOP_B: Option<i16>,
    pub CURREN_B: Option<i16>,
    pub CP_TMP_B: Option<i16>,
    pub DGT_B: Option<i16>,
    pub ECO_TP_B: Option<i16>,
    pub SUCT_T_B: Option<i16>,
    pub EXV_B: Option<i16>,
    pub CIRCC_AN_UI: Option<i16>,
    pub CAPC_T: Option<i16>,
    pub DP_C: Option<i16>,
    pub SP_C: Option<i16>,
    pub ECON_P_C: Option<i16>,
    pub OP_C: Option<i16>,
    pub DOP_C: Option<i16>,
    pub CURREN_C: Option<i16>,
    pub CP_TMP_C: Option<i16>,
    pub DGT_C: Option<i16>,
    pub ECO_TP_C: Option<i16>,
    pub SCT_C: Option<i16>,
    pub SST_C: Option<i16>,
    pub SUCT_T_C: Option<i16>,
    pub EXV_C: Option<i16>,
    pub formulas: Option<HashMap<String, String>>,
}

impl<'a> TryFrom<TelemetryDriChillerCarrierXAHvar<'a>> for DriChillerCarrierXAHvarTelemetry {
    type Error = String;
    fn try_from(
        value: TelemetryDriChillerCarrierXAHvar,
    ) -> Result<DriChillerCarrierXAHvarTelemetry, String> {
        let tel = json!(value);
        let result = DriChillerCarrierXAHvarTelemetry {
            timestamp: NaiveDateTime::parse_from_str(value.timestamp.as_ref(), "%Y-%m-%dT%H:%M:%S")
                .map_err(|e| e.to_string())?,
            GENUNIT_UI: match value.GENUNIT_UI {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "GENUNIT_UI",
                    value.GENUNIT_UI.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SUCT_T_B: match value.SUCT_T_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SUCT_T_B",
                    value.SUCT_T_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SUCT_T_C: match value.SUCT_T_C {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SUCT_T_C",
                    value.SUCT_T_C.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            TOT_CURR: match value.TOT_CURR {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "TOT_CURR",
                    value.TOT_CURR.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SP_C: match value.SP_C {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SP_C",
                    value.SP_C.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SST_C: match value.SST_C {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SST_C",
                    value.SST_C.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SUCT_T_A: match value.SUCT_T_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SUCT_T_A",
                    value.SUCT_T_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            EXV_C: match value.EXV_C {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "EXV_C",
                    value.EXV_C.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            OP_C: match value.OP_C {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "OP_C",
                    value.OP_C.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            SCT_C: match value.SCT_C {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SCT_C",
                    value.SCT_C.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            ECO_TP_C: match value.ECO_TP_C {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "ECO_TP_C",
                    value.ECO_TP_C.unwrap() as f64,
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
            ECON_P_C: match value.ECON_P_C {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "ECON_P_C",
                    value.ECON_P_C.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            ECO_TP_A: match value.ECO_TP_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "ECO_TP_A",
                    value.ECO_TP_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            ECO_TP_B: match value.ECO_TP_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "ECO_TP_B",
                    value.ECO_TP_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DP_C: match value.DP_C {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DP_C",
                    value.DP_C.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            ECON_P_A: match value.ECON_P_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "ECON_P_A",
                    value.ECON_P_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            ECON_P_B: match value.ECON_P_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "ECON_P_B",
                    value.ECON_P_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DOP_A: match value.DOP_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DOP_A",
                    value.DOP_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DOP_B: match value.DOP_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DOP_B",
                    value.DOP_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DOP_C: match value.DOP_C {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DOP_C",
                    value.DOP_C.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DGT_A: match value.DGT_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DGT_A",
                    value.DGT_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DGT_B: match value.DGT_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DGT_B",
                    value.DGT_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            DGT_C: match value.DGT_C {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "DGT_C",
                    value.DGT_C.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CURREN_A: match value.CURREN_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CURREN_A",
                    value.CURREN_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CURREN_B: match value.CURREN_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CURREN_B",
                    value.CURREN_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CURREN_C: match value.CURREN_C {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CURREN_C",
                    value.CURREN_C.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CP_TMP_B: match value.CP_TMP_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CP_TMP_B",
                    value.CP_TMP_B.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CP_TMP_A: match value.CP_TMP_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CP_TMP_A",
                    value.CP_TMP_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CP_TMP_C: match value.CP_TMP_C {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CP_TMP_C",
                    value.CP_TMP_C.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CIRCA_AN_UI: match value.CIRCA_AN_UI {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CIRCA_AN_UI",
                    value.CIRCA_AN_UI.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CIRCB_AN_UI: match value.CIRCB_AN_UI {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CIRCB_AN_UI",
                    value.CIRCB_AN_UI.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CIRCC_AN_UI: match value.CIRCC_AN_UI {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CIRCC_AN_UI",
                    value.CIRCC_AN_UI.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            CTRL_TYP: match value.CTRL_TYP {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CTRL_TYP",
                    value.CTRL_TYP.unwrap() as f64,
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
            CAPC_T: match value.CAPC_T {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "CAPC_T",
                    value.CAPC_T.unwrap() as f64,
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
            SP_OCC: match value.SP_OCC {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "SP_OCC",
                    value.SP_OCC.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
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
            OAT: match value.OAT {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "OAT",
                    value.OAT.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            OP_A: match value.OP_A {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "OP_A",
                    value.OP_A.unwrap() as f64,
                    &tel,
                    false,
                )),
            },
            OP_B: match value.OP_B {
                None => None,
                Some(-1) => None,
                _ => Some(calculateFormulas(
                    "OP_B",
                    value.OP_B.unwrap() as f64,
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
            record_date: None,
        };
        Ok(result)
    }
}

pub fn convert_chiller_carrier_xa_hvar_payload<'a>(
    mut payload: TelemetryDriChillerCarrierXAHvar<'a>,
    dev: &'a HwInfoDRI,
) -> Result<DriChillerCarrierXAHvarTelemetry, String> {
    if dev.formulas.is_some() {
        payload.formulas = dev.formulas.clone();
    }
    return payload.try_into();
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DriChillerCarrierXAHvarChangeParams {
    pub timestamp: NaiveDateTime,
    pub CTRL_TYP: Option<f64>,
    pub STATUS: Option<f64>,
    pub ALM: Option<f64>,
    pub SP_OCC: Option<f64>,
    pub CHIL_S_S: Option<f64>,
    pub CHIL_OCC: Option<f64>,
    pub DEM_LIM: Option<f64>,
    pub EMSTOP: Option<f64>,
}

impl DriChillerCarrierXAHvarChangeParams {
    pub fn new(timestamp: NaiveDateTime) -> Self {
        Self {
            timestamp,
            STATUS: None,
            CHIL_S_S: None,
            CHIL_OCC: None,
            CTRL_TYP: None,
            ALM: None,
            DEM_LIM: None,
            SP_OCC: None,
            EMSTOP: None,
        }
    }
}

impl DriChillerCarrierXAHvarTelemetry {
    pub fn group_telemetries(
        device_code: &str,
        telemetries: Vec<DriChillerCarrierXAHvarTelemetry>,
        hour_graphic: bool,
    ) -> (
        HashMap<NaiveDateTime, Vec<DriChillerCarrierXAHvarTelemetry>>,
        Vec<ChillerParametersChangesHist>,
    ) {
        let mut grouped_telemetries: HashMap<NaiveDateTime, Vec<DriChillerCarrierXAHvarTelemetry>> =
            HashMap::new();
        let mut last_telemetry =
            DriChillerCarrierXAHvarChangeParams::new(Local::now().naive_local());
        let mut params_changes_hist = Vec::<ChillerParametersChangesHist>::new();
        let interval = if hour_graphic { 60 } else { 10 };

        for (index, telemetry) in telemetries.iter().enumerate() {
            let timestamp = telemetry.timestamp;
            let rounded_minute = (timestamp.minute() / interval) * interval;
            let rounded_timestamp = timestamp
                .date()
                .and_hms(timestamp.hour(), rounded_minute, 0);
            grouped_telemetries
                .entry(rounded_timestamp)
                .or_insert_with(Vec::new)
                .push(telemetry.clone());

            let iterate_telemetry = DriChillerCarrierXAHvarChangeParams {
                timestamp,
                STATUS: telemetry.STATUS,
                CHIL_S_S: telemetry.CHIL_S_S,
                CHIL_OCC: telemetry.CHIL_OCC,
                CTRL_TYP: telemetry.CTRL_TYP,
                ALM: telemetry.ALM,
                DEM_LIM: telemetry.DEM_LIM,
                SP_OCC: telemetry.SP_OCC,
                EMSTOP: telemetry.EMSTOP,
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
            last_telemetry.STATUS = telemetry.STATUS;
            last_telemetry.CHIL_S_S = telemetry.CHIL_S_S;
            last_telemetry.CHIL_OCC = telemetry.CHIL_OCC;
            last_telemetry.CTRL_TYP = telemetry.CTRL_TYP;
            last_telemetry.ALM = telemetry.ALM;
            last_telemetry.DEM_LIM = telemetry.DEM_LIM;
            last_telemetry.SP_OCC = telemetry.SP_OCC;
            last_telemetry.EMSTOP = telemetry.EMSTOP;
        }

        (grouped_telemetries, params_changes_hist)
    }

    pub fn calculate_group_averages(
        grouped_telemetries: &HashMap<NaiveDateTime, Vec<DriChillerCarrierXAHvarTelemetry>>,
    ) -> Vec<DriChillerCarrierXAHvarTelemetry> {
        let mut group_averages: Vec<DriChillerCarrierXAHvarTelemetry> = Vec::new();

        for (time_interval, telemetry_array) in grouped_telemetries {
            let mut total_values: HashMap<&str, (f64, usize)> = HashMap::new();

            for telemetry in telemetry_array {
                for (field, value) in &[
                    ("CAP_T", telemetry.CAP_T),
                    ("COOL_EWT", telemetry.COOL_EWT),
                    ("COOL_LWT", telemetry.COOL_LWT),
                    ("CTRL_PNT", telemetry.CTRL_PNT),
                    ("DP_A", telemetry.DP_A),
                    ("DP_B", telemetry.DP_B),
                    ("OAT", telemetry.OAT),
                    ("OP_A", telemetry.OP_A),
                    ("OP_B", telemetry.OP_B),
                    ("SCT_A", telemetry.SCT_A),
                    ("SCT_B", telemetry.SCT_B),
                    ("SP_A", telemetry.SP_A),
                    ("SP_B", telemetry.SP_B),
                    ("SST_A", telemetry.SST_A),
                    ("SST_B", telemetry.SST_B),
                    ("GENUNIT_UI", telemetry.GENUNIT_UI),
                    ("TOT_CURR", telemetry.TOT_CURR),
                    ("EMSTOP", telemetry.EMSTOP),
                    ("CIRCA_AN_UI", telemetry.CIRCA_AN_UI),
                    ("CAPA_T", telemetry.CAPA_T),
                    ("ECON_P_A", telemetry.ECON_P_A),
                    ("DOP_A", telemetry.DOP_A),
                    ("CURREN_A", telemetry.CURREN_A),
                    ("CP_TMP_A", telemetry.CP_TMP_A),
                    ("DGT_A", telemetry.DGT_A),
                    ("ECO_TP_A", telemetry.ECO_TP_A),
                    ("SUCT_T_A", telemetry.SUCT_T_A),
                    ("EXV_A", telemetry.EXV_A),
                    ("CIRCB_AN_UI", telemetry.CIRCB_AN_UI),
                    ("CAPB_T", telemetry.CAPB_T),
                    ("ECON_P_B", telemetry.ECON_P_B),
                    ("DOP_B", telemetry.DOP_B),
                    ("CURREN_B", telemetry.CURREN_B),
                    ("CP_TMP_B", telemetry.CP_TMP_B),
                    ("DGT_B", telemetry.DGT_B),
                    ("ECO_TP_B", telemetry.ECO_TP_B),
                    ("SUCT_T_B", telemetry.SUCT_T_B),
                    ("EXV_B", telemetry.EXV_B),
                    ("CIRCC_AN_UI", telemetry.CIRCC_AN_UI),
                    ("CAPC_T", telemetry.CAPC_T),
                    ("DP_C", telemetry.DP_C),
                    ("SP_C", telemetry.SP_C),
                    ("ECON_P_C", telemetry.ECON_P_C),
                    ("OP_C", telemetry.OP_C),
                    ("DOP_C", telemetry.DOP_C),
                    ("CURREN_C", telemetry.CURREN_C),
                    ("CP_TMP_C", telemetry.CP_TMP_C),
                    ("DGT_C", telemetry.DGT_C),
                    ("ECO_TP_C", telemetry.ECO_TP_C),
                    ("SCT_C", telemetry.SCT_C),
                    ("SST_C", telemetry.SST_C),
                    ("SUCT_T_C", telemetry.SUCT_T_C),
                    ("EXV_C", telemetry.EXV_C),
                ] {
                    if let Some(v) = value {
                        let (sum, count_val) = total_values.entry(field).or_insert((0.0, 0));
                        *sum += v;
                        *count_val += 1;
                    }
                }
            }
            let mut averaged_telemetry = DriChillerCarrierXAHvarTelemetry::new(*time_interval);
            let date_clone = averaged_telemetry.timestamp;

            averaged_telemetry.record_date =
                Some(date_clone.format("%Y-%m-%dT%H:%M:%S").to_string());

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
        iterate_telemetry: DriChillerCarrierXAHvarChangeParams,
        last_telemetry: DriChillerCarrierXAHvarChangeParams,
        history_vec: &mut Vec<ChillerParametersChangesHist>,
    ) {
        Self::save_if_changed(
            "STATUS",
            last_telemetry.STATUS,
            iterate_telemetry.STATUS,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "CHIL_S_S",
            last_telemetry.CHIL_S_S,
            iterate_telemetry.CHIL_S_S,
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
            "CTRL_TYP",
            last_telemetry.CTRL_TYP,
            iterate_telemetry.CTRL_TYP,
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
            "DEM_LIM",
            last_telemetry.DEM_LIM,
            iterate_telemetry.DEM_LIM,
            iterate_telemetry.timestamp,
            device_code,
            history_vec,
        );
        Self::save_if_changed(
            "SP_OCC",
            last_telemetry.SP_OCC,
            iterate_telemetry.SP_OCC,
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
        last_telemetry: DriChillerCarrierXAHvarChangeParams,
        history_vec: &mut Vec<ChillerParametersChangesHist>,
    ) {
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
            "CHIL_S_S",
            last_telemetry.CHIL_S_S,
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
            "CTRL_TYP",
            last_telemetry.CTRL_TYP,
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
            "DEM_LIM",
            last_telemetry.DEM_LIM,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "SP_OCC",
            last_telemetry.SP_OCC,
            history_vec,
        );
        Self::save_param_change_hist(
            last_telemetry.timestamp,
            device_code,
            "EMSTOP",
            last_telemetry.EMSTOP,
            history_vec,
        );
    }

    fn set_field_average(&mut self, field: &str, value: f64) {
        match field {
            "GENUNIT_UI" => self.GENUNIT_UI = Some(value),
            "CAP_T" => self.CAP_T = Some(value),
            "TOT_CURR" => self.TOT_CURR = Some(value),
            "CTRL_PNT" => self.CTRL_PNT = Some(value),
            "OAT" => self.OAT = Some(value),
            "COOL_EWT" => self.COOL_EWT = Some(value),
            "COOL_LWT" => self.COOL_LWT = Some(value),
            "CIRCA_AN_UI" => self.CIRCA_AN_UI = Some(value),
            "CAPA_T" => self.CAPA_T = Some(value),
            "DP_A" => self.DP_A = Some(value),
            "SP_A" => self.SP_A = Some(value),
            "ECON_P_A" => self.ECON_P_A = Some(value),
            "OP_A" => self.OP_A = Some(value),
            "DOP_A" => self.DOP_A = Some(value),
            "CURREN_A" => self.CURREN_A = Some(value),
            "CP_TMP_A" => self.CP_TMP_A = Some(value),
            "DGT_A" => self.DGT_A = Some(value),
            "ECO_TP_A" => self.ECO_TP_A = Some(value),
            "SCT_A" => self.SCT_A = Some(value),
            "SST_A" => self.SST_A = Some(value),
            "SST_B" => self.SST_B = Some(value),
            "SUCT_T_A" => self.SUCT_T_A = Some(value),
            "EXV_A" => self.EXV_A = Some(value),
            "CIRCB_AN_UI" => self.CIRCB_AN_UI = Some(value),
            "CAPB_T" => self.CAPB_T = Some(value),
            "DP_B" => self.DP_B = Some(value),
            "SP_B" => self.SP_B = Some(value),
            "ECON_P_B" => self.ECON_P_B = Some(value),
            "OP_B" => self.OP_B = Some(value),
            "DOP_B" => self.DOP_B = Some(value),
            "CURREN_B" => self.CURREN_B = Some(value),
            "CP_TMP_B" => self.CP_TMP_B = Some(value),
            "DGT_B" => self.DGT_B = Some(value),
            "ECO_TP_B" => self.ECO_TP_B = Some(value),
            "SCT_B" => self.SCT_B = Some(value),
            "SUCT_T_B" => self.SUCT_T_B = Some(value),
            "EXV_B" => self.EXV_B = Some(value),
            "CIRCC_AN_UI" => self.CIRCC_AN_UI = Some(value),
            "CAPC_T" => self.CAPC_T = Some(value),
            "DP_C" => self.DP_C = Some(value),
            "SP_C" => self.SP_C = Some(value),
            "ECON_P_C" => self.ECON_P_C = Some(value),
            "OP_C" => self.OP_C = Some(value),
            "DOP_C" => self.DOP_C = Some(value),
            "CURREN_C" => self.CURREN_C = Some(value),
            "CP_TMP_C" => self.CP_TMP_C = Some(value),
            "DGT_C" => self.DGT_C = Some(value),
            "ECO_TP_C" => self.ECO_TP_C = Some(value),
            "SCT_C" => self.SCT_C = Some(value),
            "SST_C" => self.SST_C = Some(value),
            "SUCT_T_C" => self.SUCT_T_C = Some(value),
            "EXV_C" => self.EXV_C = Some(value),
            _ => (),
        }
    }

    fn new(timestamp: NaiveDateTime) -> Self {
        Self {
            timestamp,
            CAP_T: None,
            CHIL_OCC: None,
            CHIL_S_S: None,
            COOL_EWT: None,
            COOL_LWT: None,
            CTRL_PNT: None,
            CTRL_TYP: None,
            DEM_LIM: None,
            DP_A: None,
            DP_B: None,
            EMSTOP: None,
            OAT: None,
            OP_A: None,
            OP_B: None,
            SCT_A: None,
            SCT_B: None,
            SP_A: None,
            SP_B: None,
            SP_OCC: None,
            SST_A: None,
            SST_B: None,
            STATUS: None,
            GENUNIT_UI: None,
            ALM: None,
            TOT_CURR: None,
            CIRCA_AN_UI: None,
            CAPA_T: None,
            ECON_P_A: None,
            DOP_A: None,
            CURREN_A: None,
            CP_TMP_A: None,
            DGT_A: None,
            ECO_TP_A: None,
            SUCT_T_A: None,
            EXV_A: None,
            CIRCB_AN_UI: None,
            CAPB_T: None,
            ECON_P_B: None,
            DOP_B: None,
            CURREN_B: None,
            CP_TMP_B: None,
            DGT_B: None,
            ECO_TP_B: None,
            SUCT_T_B: None,
            EXV_B: None,
            CIRCC_AN_UI: None,
            CAPC_T: None,
            DP_C: None,
            SP_C: None,
            ECON_P_C: None,
            OP_C: None,
            DOP_C: None,
            CURREN_C: None,
            CP_TMP_C: None,
            DGT_C: None,
            ECO_TP_C: None,
            SCT_C: None,
            SST_C: None,
            SUCT_T_C: None,
            EXV_C: None,
            record_date: None,
        }
    }
}
