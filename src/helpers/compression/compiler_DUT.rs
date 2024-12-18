use super::compiler_common::{
    SingleVariableCompiler, SingleVariableCompilerBuilder, SingleVariableCompilerFloat,
};
use crate::telemetry_payloads::telemetry_formats::TelemetryDUT_v3;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug)]
pub struct DUTTelemetryCompiler {
    #[serde(rename = "lastIndex")]
    pub last_index: isize,
    #[serde(rename = "vTemp")]
    pub v_temp: SingleVariableCompilerFloat,
    #[serde(rename = "vTemp1")]
    pub v_temp1: SingleVariableCompilerFloat,
    #[serde(rename = "vHum")]
    pub v_hum: SingleVariableCompilerFloat,
    #[serde(rename = "vCO2")]
    pub v_eco2: SingleVariableCompilerFloat,
    #[serde(rename = "vTVOC")]
    pub v_tvoc: SingleVariableCompilerFloat,
    #[serde(rename = "vState")]
    pub v_state: SingleVariableCompiler,
    #[serde(rename = "vMode")]
    pub v_mode: SingleVariableCompiler,
    #[serde(rename = "vL1")]
    pub v_l1: SingleVariableCompiler,
}

impl DUTTelemetryCompiler {
    pub fn new() -> DUTTelemetryCompiler {
        return DUTTelemetryCompiler {
            last_index: -1,
            v_temp: SingleVariableCompilerFloat::create(5, 1, 0.2),
            v_temp1: SingleVariableCompilerFloat::create(5, 1, 0.2),
            v_hum: SingleVariableCompilerFloat::create(5, 1, 0.2),
            v_state: SingleVariableCompiler::create(),
            v_eco2: SingleVariableCompilerFloat::create(1, 25, 0.2),
            v_tvoc: SingleVariableCompilerFloat::create(1, 25, 0.2),
            v_mode: SingleVariableCompiler::create(),
            v_l1: SingleVariableCompilerBuilder::new()
                .with_min_run_length(5)
                .build_common(),
        };
    }

    pub fn AdcPontos(&mut self, telemetry: &TelemetryDUT_v3, index: isize) {
        if index <= self.last_index {
            return;
        }
        self.last_index = index;
        let l1 = match telemetry.l1 {
            Some(true) => "1",
            Some(false) => "0",
            None => "",
        };
        self.v_temp.adc_ponto_float(index, telemetry.Temp, 180);
        self.v_temp1.adc_ponto_float(index, telemetry.Temp1, 180);
        self.v_hum.adc_ponto_float(index, telemetry.Hum, 180);
        self.v_eco2
            .adc_ponto_float(index, telemetry.eCO2.map(f64::from), 180);
        self.v_tvoc
            .adc_ponto_float(index, telemetry.tvoc.map(f64::from), 180);
        SingleVariableCompiler::adc_ponto(
            &mut self.v_state,
            index,
            match &telemetry.State {
                Some(v) => v,
                None => "",
            },
            180,
        );
        SingleVariableCompiler::adc_ponto(
            &mut self.v_mode,
            index,
            match &telemetry.Mode {
                Some(v) => v,
                None => "",
            },
            180,
        );
        self.v_l1.adc_ponto(index, l1, 180);
    }

    pub fn CheckClosePeriod(
        &mut self,
        periodLength: isize,
    ) -> Result<Option<CompiledPeriod>, String> {
        if self.v_temp.had_error() || self.v_eco2.had_error() {
            return Err("There was an error compiling the data".to_owned());
        }

        if self.v_temp.is_empty() && self.v_eco2.is_empty() {
            return Ok(None);
        }

        let vecTemp = self.v_temp.fechar_vetor_completo(periodLength);
        let vecTemp1 = self.v_temp1.fechar_vetor_completo(periodLength);
        let vecHum = self.v_hum.fechar_vetor_completo(periodLength);
        let vecState = self.v_state.fechar_vetor_completo(periodLength);
        let vecMode = self.v_mode.fechar_vetor_completo(periodLength);
        let vec_l1 = self.v_l1.fechar_vetor_completo(periodLength);

        let hasL1 = &vec_l1 != "*86400";
        let mut hoursOnline = 0.0;
        let mut hoursOnL1 = 0.0;
        let mut hoursOffL1 = 0.0;
        let mut numDeparts = 0;

        if hasL1 {
            let (hoursOnlineL1, hoursOfflineL1, numDepartsL1) =
                calcular_estatisticas_dut_duo(&vec_l1);
            hoursOnline = hoursOnlineL1 + hoursOfflineL1;
            hoursOnL1 = hoursOnlineL1;
            hoursOffL1 = hoursOfflineL1;
            numDeparts = numDepartsL1;
        } else {
            hoursOnline = calcular_tempo_online(&vecTemp, &vecHum);
        }

        return Ok(Some(CompiledPeriod {
            Temp: vecTemp,
            Temp1: vecTemp1,
            Hum: vecHum,
            e_co2: self.v_eco2.fechar_vetor_completo(periodLength),
            tvoc: self.v_tvoc.fechar_vetor_completo(periodLength),
            State: vecState,
            Mode: vecMode,
            hoursOnline,
            hoursOnL1,
            hoursOffL1,
            l1: vec_l1,
            numDeparts,
        }));
    }
}

fn calcular_estatisticas_dut_duo(vecL1: &str) -> (f64, f64, isize) {
    // "*200,3.5*300,2,2*30,*100"
    let mut hoursOnline = 0.0;
    let mut hoursOffline = 0.0;
    let mut numDeparts: isize = 0;

    if vecL1.is_empty() {
        return (hoursOnline, hoursOffline, numDeparts);
    }
    let imax = usize::try_from(vecL1.len() - 1).unwrap();
    let mut i: usize = 0;
    let mut ival: i64 = -1;
    let mut iast: i64 = -1;
    let mut value;
    let mut lastValue = "";

    loop {
        if ival < 0 {
            ival = i as i64;
        }
        if i > imax || vecL1.as_bytes()[i] == b',' {
            let duration: isize;
            if iast < 0 {
                value = &vecL1[(ival as usize)..i];
                duration = 1;
            } else {
                value = &vecL1[(ival as usize)..(iast as usize)];
                duration = isize::from_str(&vecL1[((iast + 1) as usize)..i]).unwrap();
            }
            if value == "1" {
                hoursOnline += (duration as f64) / 3600.0;
                if lastValue == "0" {
                    numDeparts += 1
                };
            }
            if value == "0" {
                hoursOffline += (duration as f64) / 3600.0;
            }
            if !value.is_empty() {
                lastValue = value
            };
            ival = -1;
            iast = -1;
        } else if vecL1.as_bytes()[i] == b'*' {
            iast = i as i64;
        }
        if i > imax {
            break;
        }
        i += 1;
    }
    return (hoursOnline, hoursOffline, numDeparts);
}

pub fn calcular_tempo_online(vecTemp: &str, vecHum: &str) -> (f64) {
    let mut hoursOnline = 0.0;

    if vecTemp.is_empty() && vecHum.is_empty() {
        return hoursOnline;
    }

    if !vecTemp.is_empty() {
        let imax = usize::try_from(vecTemp.len() - 1).unwrap();
        let mut i: usize = 0;
        let mut ival: i64 = -1;
        let mut iast: i64 = -1;
        let mut value;
        loop {
            if ival < 0 {
                ival = i as i64;
            }
            if i > imax || vecTemp.as_bytes()[i] == b',' {
                let duration: isize;
                if iast < 0 {
                    value = &vecTemp[(ival as usize)..i];
                    duration = 1;
                } else {
                    value = &vecTemp[(ival as usize)..(iast as usize)];
                    duration = isize::from_str(&vecTemp[((iast + 1) as usize)..i]).unwrap();
                }
                if !value.is_empty() {
                    hoursOnline += (duration as f64) / 3600.0;
                }
                ival = -1;
                iast = -1;
            } else if vecTemp.as_bytes()[i] == b'*' {
                iast = i as i64;
            }
            if i > imax {
                break;
            }
            i += 1;
        }
    }

    if !vecHum.is_empty() && hoursOnline == 0. {
        let imax = usize::try_from(vecHum.len() - 1).unwrap();
        let mut i: usize = 0;
        let mut ival: i64 = -1;
        let mut iast: i64 = -1;
        let mut value;
        loop {
            if ival < 0 {
                ival = i as i64;
            }
            if i > imax || vecHum.as_bytes()[i] == b',' {
                let duration: isize;
                if iast < 0 {
                    value = &vecHum[(ival as usize)..i];
                    duration = 1;
                } else {
                    value = &vecHum[(ival as usize)..(iast as usize)];
                    duration = isize::from_str(&vecHum[((iast + 1) as usize)..i]).unwrap();
                }
                if !value.is_empty() {
                    hoursOnline += (duration as f64) / 3600.0;
                }
                ival = -1;
                iast = -1;
            } else if vecHum.as_bytes()[i] == b'*' {
                iast = i as i64;
            }
            if i > imax {
                break;
            }
            i += 1;
        }
    }

    return hoursOnline;
}

pub struct CompiledPeriod {
    pub Temp: String,
    pub Temp1: String,
    pub Hum: String,
    pub e_co2: String,
    pub tvoc: String,
    pub State: String,
    pub Mode: String,
    pub l1: String,
    pub hoursOnline: f64,
    pub hoursOnL1: f64,
    pub hoursOffL1: f64,
    pub numDeparts: isize,
}
