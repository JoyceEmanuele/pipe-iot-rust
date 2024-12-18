use std::collections::HashMap;

use crate::telemetry_payloads::dac_telemetry::HwInfoDAC;

use super::dac_l1_calculator::{create_l1_calculator, L1Calculator};

pub struct DACStateList(HashMap<String, L1Calculator>);
impl DACStateList {
    pub fn new() -> Self {
        Self(HashMap::default())
    }

    #[must_use]
    pub(crate) fn get_or_create(&mut self, dac_id: &str, hw_info: &HwInfoDAC) -> &mut L1Calculator {
        self.0
            .entry(dac_id.into())
            .or_insert_with(|| create_l1_calculator(hw_info))
    }

    pub(crate) fn replace_with(
        &mut self,
        dac_id: &str,
        hw_info: &HwInfoDAC,
    ) -> Option<L1Calculator> {
        let new_calc = create_l1_calculator(hw_info);
        self.0.insert(dac_id.into(), new_calc)
    }

    pub fn contains(&self, dac_id: &str) -> bool {
        self.0.contains_key(dac_id)
    }
}

impl Default for DACStateList {
    fn default() -> Self {
        Self::new()
    }
}
