use crate::compression::compiler_common::{SingleVariableCompiler, SingleVariableCompilerFloat};
use crate::telemetry_payloads::telemetry_formats::TelemetryDMA;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug)]
pub struct DMATelemetryCompiler {
    #[serde(rename = "lastIndex")]
    pub last_index: isize,

    #[serde(rename = "vPulses")]
    pub v_pulses: SingleVariableCompilerFloat,
    #[serde(rename = "vMode")]
    pub v_mode: SingleVariableCompiler,
    #[serde(rename = "vOperationMode")]
    pub v_operationMode: SingleVariableCompilerFloat,
}

impl DMATelemetryCompiler {
    pub fn new(period_length: i64) -> DMATelemetryCompiler {
        return DMATelemetryCompiler {
            last_index: -1,
            v_pulses: SingleVariableCompilerFloat::create(5, 1, 0.2),
            v_mode: SingleVariableCompiler::create(),
            v_operationMode: SingleVariableCompilerFloat::create(5, 1, 0.2),
        };
    }

    pub fn AdcPontos(&mut self, telemetry: &TelemetryDMA, index: isize) {
        if index <= self.last_index {
            return;
        }

        let tolerance_time = isize::try_from(match telemetry.samplingTime {
            Some(v) => v * 2 + 20,
            None => 60,
        })
        .unwrap();

        self.last_index = index;
        self.v_pulses.adc_ponto_float(
            index,
            match telemetry.pulses {
                Some(v) => Some(f64::from(v)),
                None => None,
            },
            tolerance_time,
        );
        self.v_mode.adc_ponto(
            index,
            match &telemetry.mode {
                Some(v) => v,
                None => "",
            },
            tolerance_time,
        );
        self.v_operationMode.adc_ponto_float(
            index,
            match telemetry.operation_mode {
                Some(v) => Some(f64::from(v)),
                None => None,
            },
            tolerance_time,
        );
    }

    pub fn CheckClosePeriod(
        &mut self,
        periodLength: isize,
    ) -> Result<Option<CompiledPeriod>, String> {
        if self.v_pulses.had_error() {
            return Err("There was an error compiling the data".to_owned());
        }
        let _periodData: Option<CompiledPeriod> = None;

        if self.v_pulses.is_empty() {
            return Ok(None);
        }

        let vecPulses = self.v_pulses.fechar_vetor_completo(periodLength);
        let vecMode = self.v_mode.fechar_vetor_completo(periodLength);
        let vecOperationMode = self.v_operationMode.fechar_vetor_completo(periodLength);

        let hoursOnline = calcular_tempo_online(&vecMode);

        return Ok(Some(CompiledPeriod {
            Pulses: vecPulses,
            Mode: vecMode,
            OperationMode: vecOperationMode,
            hoursOnline,
        }));
    }
}

fn calcular_tempo_online(vecTelemetry: &str) -> f64 {
    // "*200,3.5*300,2,2*30,*100"
    let mut hoursOnline = 0.0;
    if vecTelemetry.is_empty() {
        return hoursOnline;
    }
    let imax = usize::try_from(vecTelemetry.len() - 1).unwrap();
    let mut i: usize = 0;
    let mut ival: i64 = -1;
    let mut iast: i64 = -1;
    let mut value;
    loop {
        if ival < 0 {
            ival = i as i64;
        }
        if i > imax || vecTelemetry.as_bytes()[i] == b',' {
            let duration: isize;
            if iast < 0 {
                value = &vecTelemetry[(ival as usize)..i];
                duration = 1;
            } else {
                value = &vecTelemetry[(ival as usize)..(iast as usize)];
                duration = isize::from_str(&vecTelemetry[((iast + 1) as usize)..i]).unwrap();
            }
            // println!("v: '" + value + "' x " + duration + endl)
            if !value.is_empty() {
                hoursOnline += (duration as f64) / 3600.0;
            }
            ival = -1;
            iast = -1;
        } else if vecTelemetry.as_bytes()[i] == b'*' {
            iast = i as i64;
        }
        if i > imax {
            break;
        }
        i += 1;
    }
    return hoursOnline;
}

pub struct CompiledPeriod {
    pub Pulses: String,
    pub Mode: String,
    pub OperationMode: String,
    pub hoursOnline: f64,
}
