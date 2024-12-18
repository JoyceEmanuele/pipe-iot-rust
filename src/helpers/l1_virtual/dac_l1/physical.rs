use crate::telemetry_payloads::{
    dac_telemetry::HwInfoDAC,
    telemetry_formats::{TelemetryDAC_v3, TelemetryDACv2},
};

use super::dac_l1_calculator::DacL1Calculator;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct PhysicalL1;

impl DacL1Calculator for PhysicalL1 {
    #[inline]
    fn calc_l1(
        &mut self,
        _building_tel: &TelemetryDAC_v3,
        tel: &TelemetryDACv2,
        _cfg: &HwInfoDAC,
    ) -> Result<Option<bool>, String> {
        Ok(tel.l1)
    }
}
