use crate::compression::compiler_common::SingleVariableCompiler;
use crate::telemetry_payloads::telemetry_formats::TelemetryDAL;
use serde::{Deserialize, Serialize};
use std::str::FromStr;

#[derive(Serialize, Deserialize, Debug)]
pub struct DALTelemetryCompiler {
    #[serde(rename = "lastIndex")]
    pub last_index: isize,

    #[serde(rename = "vMode")]
    pub v_mode: Vec<SingleVariableCompiler>,
    #[serde(rename = "vRelays")]
    pub v_relays: Vec<SingleVariableCompiler>,
    #[serde(rename = "vFeedback")]
    pub v_feedback: Vec<SingleVariableCompiler>,
}

impl DALTelemetryCompiler {
    pub fn new(period_length: i64) -> DALTelemetryCompiler {
        return DALTelemetryCompiler {
            last_index: -1,
            v_mode: Vec::new(),
            v_relays: Vec::new(),
            v_feedback: Vec::new(),
        };
    }

    pub fn AdcPontos(&mut self, telemetry: &TelemetryDAL, index: isize) {
        if index <= self.last_index {
            return;
        }
        self.last_index = index;
        for (mIndex, mValue) in telemetry.Mode.clone().into_iter().enumerate() {
            if self.v_mode.get(mIndex).is_none() {
                self.v_mode.push(SingleVariableCompiler::create())
            }
            let position = self.v_mode.get_mut(mIndex).unwrap();
            position.adc_ponto(index, &mValue, 90);
        }
        for (rIndex, rValue) in telemetry.Relays.clone().into_iter().enumerate() {
            if self.v_relays.get(rIndex).is_none() {
                self.v_relays.push(SingleVariableCompiler::create())
            }
            let position = self.v_relays.get_mut(rIndex).unwrap();
            position.adc_ponto(
                index,
                match rValue {
                    Some(false) => "0",
                    Some(true) => "1",
                    None => "",
                },
                90,
            );
        }
        for (fIndex, fValue) in telemetry.Feedback.clone().into_iter().enumerate() {
            if self.v_feedback.get(fIndex).is_none() {
                self.v_feedback.push(SingleVariableCompiler::create())
            }
            let position = self.v_feedback.get_mut(fIndex).unwrap();
            position.adc_ponto(
                index,
                match fValue {
                    Some(false) => "0",
                    Some(true) => "1",
                    None => "",
                },
                90,
            );
        }
    }

    pub fn CheckClosePeriod(
        &mut self,
        periodLength: isize,
    ) -> Result<Option<CompiledPeriod>, String> {
        let mut error = false;
        let mut empty = false;
        self.v_relays.iter().for_each(|x| {
            if x.had_error() {
                error = true;
            }
            if x.is_empty() {
                empty = true;
            }
        });
        if error {
            return Err("There was an error compiling the data".to_owned());
        }
        if empty {
            return Ok(None);
        }

        let mut vecMode = Vec::new();
        for mut mValue in self.v_mode.iter_mut() {
            let vecValue = mValue.fechar_vetor_completo(periodLength);
            vecMode.push(vecValue);
        }

        let mut vecRelays = Vec::new();
        for mut rValue in self.v_relays.iter_mut() {
            let vecValue = rValue.fechar_vetor_completo(periodLength);
            vecRelays.push(vecValue);
        }

        let mut vecFeedback = Vec::new();
        for mut fValue in self.v_feedback.iter_mut() {
            let vecValue = fValue.fechar_vetor_completo(periodLength);
            vecFeedback.push(vecValue);
        }

        let modeTelemetries = if vecMode.len() > 0 { &vecMode[0] } else { "" };
        let hoursOnline = calcular_tempo_online(modeTelemetries);

        return Ok(Some(CompiledPeriod {
            Mode: vecMode,
            Relays: vecRelays,
            Feedback: vecFeedback,
            hoursOnline,
        }));
    }
}

fn calcular_tempo_online(vecMode: &str) -> f64 {
    // "*200,3.5*300,2,2*30,*100"
    let mut hoursOnline = 0.0;
    if vecMode.is_empty() {
        return hoursOnline;
    }
    let imax = usize::try_from(vecMode.len() - 1).unwrap();
    let mut i: usize = 0;
    let mut ival: i64 = -1;
    let mut iast: i64 = -1;
    let mut value;
    loop {
        if ival < 0 {
            ival = i as i64;
        }
        if i > imax || vecMode.as_bytes()[i] == b',' {
            let duration: isize;
            if iast < 0 {
                value = &vecMode[(ival as usize)..i];
                duration = 1;
            } else {
                value = &vecMode[(ival as usize)..(iast as usize)];
                duration = isize::from_str(&vecMode[((iast + 1) as usize)..i]).unwrap();
            }
            // println!("v: '" + value + "' x " + duration + endl)
            if !value.is_empty() {
                hoursOnline += (duration as f64) / 3600.0;
            }
            ival = -1;
            iast = -1;
        } else if vecMode.as_bytes()[i] == b'*' {
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
    pub Mode: Vec<String>,
    pub Relays: Vec<String>,
    pub Feedback: Vec<String>,
    pub hoursOnline: f64,
}
