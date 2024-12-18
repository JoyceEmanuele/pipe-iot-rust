use serde::{Deserialize, Serialize};

use super::{
    dac_l1_calculator::DacL1Calculator, temperature_only_general::TsucDependentL1,
    temperature_only_self::TemperatureOnlySelf,
};

#[derive(Debug, Serialize, Deserialize)]
pub enum TemperatureOnlyCalc {
    General(TsucDependentL1),
    SelfHVAC(TemperatureOnlySelf),
}

impl DacL1Calculator for TemperatureOnlyCalc {
    fn calc_l1(
        &mut self,
        building_tel: &crate::telemetry_payloads::telemetry_formats::TelemetryDAC_v3,
        full_tel: &crate::telemetry_payloads::telemetry_formats::TelemetryDACv2,
        cfg: &crate::telemetry_payloads::dac_telemetry::HwInfoDAC,
    ) -> Result<Option<bool>, String> {
        match self {
            Self::General(x) => x.calc_l1(building_tel, full_tel, cfg),
            Self::SelfHVAC(x) => x.calc_l1(building_tel, full_tel, cfg),
        }
    }
}
