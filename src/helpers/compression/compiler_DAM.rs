use super::common_func::calcular_tempo_online;
use super::compiler_common::{SingleVariableCompiler, SingleVariableCompilerFloat};
use crate::telemetry_payloads::telemetry_formats::TelemetryRawDAM_v1;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug)]
pub struct DAMTelemetryCompiler {
    pub lastIndex: isize,
    pub vState: SingleVariableCompiler,
    pub vMode: SingleVariableCompiler,
    pub vTemperature: SingleVariableCompilerFloat,
    pub vTemperature_1: SingleVariableCompilerFloat,
}

impl DAMTelemetryCompiler {
    pub fn new() -> DAMTelemetryCompiler {
        return DAMTelemetryCompiler {
            lastIndex: -1,
            vState: SingleVariableCompiler::create(),
            vMode: SingleVariableCompiler::create(),
            vTemperature: SingleVariableCompilerFloat::create(5, 1, 0.2),
            vTemperature_1: SingleVariableCompilerFloat::create(5, 1, 0.2),
        };
    }

    pub fn AdcPontos(&mut self, telemetry: &TelemetryRawDAM_v1, index: isize) {
        if index <= self.lastIndex {
            return;
        }
        self.lastIndex = index;
        self.vState.adc_ponto(index, &telemetry.State, 150);
        self.vMode.adc_ponto(index, &telemetry.Mode, 150);
        self.vTemperature.adc_ponto_float(
            index,
            match &telemetry.Temperature {
                Some(t) => f64::from_str(t).ok(),
                None => None,
            },
            150,
        );
        self.vTemperature_1.adc_ponto_float(
            index,
            match &telemetry.Temperature_1 {
                Some(t) => f64::from_str(t).ok(),
                None => None,
            },
            150,
        );
    }

    pub fn CheckClosePeriod(
        &mut self,
        periodLength: isize,
    ) -> Result<Option<CompiledPeriod>, String> {
        if self.vState.had_error() {
            return Err("There was an error compiling the data".to_owned());
        }

        if self.vState.is_empty() {
            return Ok(None);
        }

        let vecState = self.vState.fechar_vetor_completo(periodLength);
        let vecMode = self.vMode.fechar_vetor_completo(periodLength);
        let vecTemperature = self.vTemperature.fechar_vetor_completo(periodLength);
        let vecTemperature_1 = self.vTemperature_1.fechar_vetor_completo(periodLength);

        let hoursOnline = calcular_tempo_online(&vecState);

        return Ok(Some(CompiledPeriod {
            State: vecState,
            Mode: vecMode,
            Temperature: vecTemperature,
            Temperature_1: vecTemperature_1,
            hoursOnline,
        }));
    }
}

pub struct CompiledPeriod {
    pub State: String,
    pub Mode: String,
    pub hoursOnline: f64,
    pub Temperature: String,
    pub Temperature_1: String,
}
