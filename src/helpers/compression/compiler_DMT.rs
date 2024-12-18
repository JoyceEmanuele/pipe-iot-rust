use crate::compression::compiler_common::SingleVariableCompiler;
use crate::telemetry_payloads::telemetry_formats::TelemetryDMT;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug)]
pub struct DMTTelemetryCompiler {
    #[serde(rename = "lastIndex")]
    pub last_index: isize,

    #[serde(rename = "vF1")]
    pub v_f1: SingleVariableCompiler,
    #[serde(rename = "vF2")]
    pub v_f2: SingleVariableCompiler,
    #[serde(rename = "vF3")]
    pub v_f3: SingleVariableCompiler,
    #[serde(rename = "vF4")]
    pub v_f4: SingleVariableCompiler,
}

impl DMTTelemetryCompiler {
    pub fn new(period_length: i64) -> DMTTelemetryCompiler {
        return DMTTelemetryCompiler {
            last_index: -1,
            v_f1: SingleVariableCompiler::create(),
            v_f2: SingleVariableCompiler::create(),
            v_f3: SingleVariableCompiler::create(),
            v_f4: SingleVariableCompiler::create(),
        };
    }

    pub fn AdcPontos(&mut self, telemetry: &TelemetryDMT, index: isize) {
        if index <= self.last_index {
            return;
        }
        self.last_index = index;
        self.v_f1.adc_ponto(
            index,
            match telemetry.F1 {
                Some(false) => "0",
                Some(true) => "1",
                None => "",
            },
            150,
        );
        self.v_f2.adc_ponto(
            index,
            match telemetry.F2 {
                Some(false) => "0",
                Some(true) => "1",
                None => "",
            },
            150,
        );
        self.v_f3.adc_ponto(
            index,
            match telemetry.F3 {
                Some(false) => "0",
                Some(true) => "1",
                None => "",
            },
            150,
        );
        self.v_f4.adc_ponto(
            index,
            match telemetry.F4 {
                Some(false) => "0",
                Some(true) => "1",
                None => "",
            },
            150,
        );
    }

    pub fn CheckClosePeriod(
        &mut self,
        periodLength: isize,
    ) -> Result<Option<CompiledPeriod>, String> {
        if self.v_f1.had_error() {
            return Err("There was an error compiling the data".to_owned());
        }
        let _periodData: Option<CompiledPeriod> = None;

        if self.v_f1.is_empty() {
            return Ok(None);
        }

        let vecF1 = self.v_f1.fechar_vetor_completo(periodLength);
        let vecF2 = self.v_f2.fechar_vetor_completo(periodLength);
        let vecF3 = self.v_f3.fechar_vetor_completo(periodLength);
        let vecF4 = self.v_f4.fechar_vetor_completo(periodLength);

        let hoursOnline = calcular_tempo_online(&vecF1);

        return Ok(Some(CompiledPeriod {
            F1: vecF1,
            F2: vecF2,
            F3: vecF3,
            F4: vecF4,
            hoursOnline,
        }));
    }
}

fn calcular_tempo_online(vecF: &str) -> f64 {
    // "*200,3.5*300,2,2*30,*100"
    let mut hoursOnline = 0.0;
    if vecF.is_empty() {
        return hoursOnline;
    }
    let imax = usize::try_from(vecF.len() - 1).unwrap();
    let mut i: usize = 0;
    let mut ival: i64 = -1;
    let mut iast: i64 = -1;
    let mut value;
    loop {
        if ival < 0 {
            ival = i as i64;
        }
        if i > imax || vecF.as_bytes()[i] == b',' {
            let duration: isize;
            if iast < 0 {
                value = &vecF[(ival as usize)..i];
                duration = 1;
            } else {
                value = &vecF[(ival as usize)..(iast as usize)];
                duration = isize::from_str(&vecF[((iast + 1) as usize)..i]).unwrap();
            }
            // println!("v: '" + value + "' x " + duration + endl)
            if !value.is_empty() {
                hoursOnline += (duration as f64) / 3600.0;
            }
            ival = -1;
            iast = -1;
        } else if vecF.as_bytes()[i] == b'*' {
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
    pub F1: String,
    pub F2: String,
    pub F3: String,
    pub F4: String,
    pub hoursOnline: f64,
}
