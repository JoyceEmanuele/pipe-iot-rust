use std::collections::HashMap;

use crate::telemetry_payloads::{
    dut_telemetry::HwInfoDUT,
    telemetry_formats::{TelemetryDUT_v3, TelemetryDUTv2},
    temprt_value_checker::DutTemperaturesChecker,
};

use super::temp_difference::TempDiffL1;

pub trait DutL1Calculator: Send + Sync {
    fn calc_l1(
        &mut self,
        payload: &TelemetryDUTv2,
        cfg: &HwInfoDUT,
    ) -> Result<Option<bool>, String>;

    fn calc_l1_tel_v3(
        &mut self,
        payload: &TelemetryDUT_v3,
        sampling_time: i64,
        cfg: &HwInfoDUT,
    ) -> Result<Option<bool>, String> {
        let p = TelemetryDUTv2 {
            temp: payload.Temp,
            temp_1: payload.Temp1,
            hum: payload.Hum,
            e_co2: payload.eCO2,
            tvoc: payload.tvoc,
            timestamp: payload.timestamp,
            sampling_time,
            state: payload.State.as_deref(),
            mode: payload.Mode.as_deref(),
            gmt: payload.GMT.clone(),
        };
        self.calc_l1(&p, cfg)
    }
}

pub fn create_l1_calculator(_cfg: &HwInfoDUT) -> L1Calculator {
    L1Calculator::TempDiffL1(Box::new(TempDiffL1::new()))
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct DutStateInfo {
    pub l1_calc: L1Calculator,
    pub t_checker: DutTemperaturesChecker,
}

impl DutStateInfo {
    pub fn create_default(hw_info: &HwInfoDUT) -> DutStateInfo {
        let l1_calc = create_l1_calculator(hw_info);
        let t_checker = DutTemperaturesChecker::new();
        DutStateInfo { l1_calc, t_checker }
    }
}

#[derive(Default)]
pub struct DutStateList(HashMap<String, DutStateInfo>);

impl DutStateList {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get_or_create(&mut self, dev_id: &str, hw_info: &HwInfoDUT) -> &mut DutStateInfo {
        self.0
            .entry(dev_id.into())
            .or_insert_with(|| DutStateInfo::create_default(hw_info))
    }

    pub fn replace_with(&mut self, dev_id: &str, hw_info: &HwInfoDUT) -> Option<DutStateInfo> {
        let state_info = DutStateInfo::create_default(hw_info);
        self.0.insert(dev_id.into(), state_info)
    }

    pub fn contains(&self, dev_id: &str) -> bool {
        self.0.contains_key(dev_id)
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum L1Calculator {
    TempDiffL1(Box<TempDiffL1>),
}
impl DutL1Calculator for L1Calculator {
    fn calc_l1(
        &mut self,
        payload: &TelemetryDUTv2,
        cfg: &HwInfoDUT,
    ) -> Result<Option<bool>, String> {
        match self {
            L1Calculator::TempDiffL1(v) => v.calc_l1(payload, cfg),
        }
    }
}
