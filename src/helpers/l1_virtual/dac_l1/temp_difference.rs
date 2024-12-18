use crate::telemetry_payloads::{
    dac_telemetry::HwInfoDAC,
    telemetry_formats::{TelemetryDAC_v3, TelemetryDACv2},
};

use super::dac_l1_calculator::DacL1Calculator;

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct TemperatureDifferenceL1;

impl DacL1Calculator for TemperatureDifferenceL1 {
    fn calc_l1(
        &mut self,
        building_tel: &TelemetryDAC_v3,
        _tel: &TelemetryDACv2,
        _cfg: &HwInfoDAC,
    ) -> Result<Option<bool>, String> {
        let tamb = building_tel.Tamb;
        let tsuc = building_tel.Tsuc;

        Ok(tamb.zip(tsuc).map(|(ta, ts)| ta - ts >= 5.0))
    }
}
