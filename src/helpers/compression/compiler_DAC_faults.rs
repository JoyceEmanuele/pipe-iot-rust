use crate::compression::compiler_common::SingleVariableCompiler;
use falhas_repentinas::utils::telemetry_formats::TelemetryDACv3;
use serde::{Deserialize, Serialize};
use std::iter::FromIterator;
use std::collections::HashMap;
use falhas_repentinas::{
    fault_detectors::dac_faults_checker::DACFaultsChecker,
    utils::faults::DacInfo,
};

#[derive(Serialize, Deserialize)]
pub struct DACFaultsCompiler {
    faults_compilers: HashMap<String, SingleVariableCompiler>,
    faults_checker: DACFaultsChecker,
    period: isize,
}
impl DACFaultsCompiler {
    pub fn new(dac_info: DacInfo) -> Self {
        let fc = DACFaultsChecker::new(dac_info);
        let faults = fc.get_valid_faults().into_iter().map(|x| x.to_string()).collect::<Vec<_>>();

        let mut compilers = HashMap::new();
        compilers.reserve(faults.len());

        for f in faults {
            compilers.insert(f, SingleVariableCompiler::create());
        }

        Self {
            faults_compilers: compilers,
            faults_checker: fc,
            period: -1,
        }
    }

    pub fn on_telemetry(&mut self, tel: &TelemetryDACv3, index : isize) -> Result<(), String> {
        let faults = self.faults_checker.check_faults(tel, None);
        let report = match faults {
            Ok(report) => report,
            Err(e) => {
                crate::LOG.append_log_tag_msg("ERROR", &format!("ERROR DACFaultsCompiler {}", e));
                return Err(e);
            }
        };
        self.period = index;

        // let faults_names = report
        //     .fault_status_vec
        //     // .health
        //     .iter()
        //     .filter_map(|f| match &f.health_index {
        //         HealthIndex::Green
        //         | HealthIndex::Yellow
        //         | HealthIndex::Orange
        //         | HealthIndex::Red => Some(f.fault_id.as_str()),
        //         HealthIndex::NoFault => None,
        //         HealthIndex::SistemaRestabelecido => None,
        //     })
        //     .collect::<Vec<_>>();

        let f = self.faults_checker.get_detected_faults();


        for (name, detected) in f {
            let compiler = self.faults_compilers.get_mut(name).ok_or_else(|| {
                "DACFaultsChecker retornou falha que não está no compiler!".to_string()
            })?;

            compiler.adc_ponto(index, if detected { "1" } else { "0" }, 15);
        }


        Ok(())
    }

    pub const fn current_period(&self) -> isize {
        self.period
    }

    pub fn close_period(self, periodLength: isize) -> HashMap<String, String> {
        HashMap::from_iter(
            self.faults_compilers
                .into_iter()
                .map(|(name, mut compiler)| (name, compiler.fechar_vetor_completo(periodLength))),
        )
    }
}
