use crate::compression::compiler_common::{SingleVariableCompiler, SingleVariableCompilerFloat};
use crate::telemetry_payloads::dac_telemetry::HwInfoDAC;
use crate::telemetry_payloads::telemetry_formats::{TelemetryDAC_v3, TelemetryDAC_v3_calcs};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::convert::TryFrom;
use std::iter::FromIterator;
use std::str::FromStr;

use super::compiler_common::SingleVariableCompilerBuilder;

#[derive(Serialize, Deserialize, Debug)]
pub struct DACTelemetryCompiler {
    #[serde(rename = "lastIndex")]
    pub last_index: isize,
    #[serde(rename = "vLcmp")]
    pub v_lcmp: SingleVariableCompiler,
    #[serde(rename = "vLevp")]
    pub v_levp: SingleVariableCompiler,
    #[serde(rename = "vLcut")]
    pub v_lcut: SingleVariableCompiler,
    pub v_l1raw: SingleVariableCompiler,
    pub v_l1fancoil: SingleVariableCompiler,
    #[serde(rename = "vTamb")]
    pub v_tamb: SingleVariableCompilerFloat,
    #[serde(rename = "vTsuc")]
    pub v_tsuc: SingleVariableCompilerFloat,
    #[serde(rename = "vTliq")]
    pub v_tliq: SingleVariableCompilerFloat,
    #[serde(rename = "vPsuc")]
    pub v_psuc: SingleVariableCompilerFloat,
    #[serde(rename = "vPliq")]
    pub v_pliq: SingleVariableCompilerFloat,
    #[serde(rename = "vTsc")]
    pub v_tsc: SingleVariableCompilerFloat,
    #[serde(rename = "vTsh")]
    pub v_tsh: SingleVariableCompilerFloat,
    #[serde(rename = "vState")]
    pub v_state: SingleVariableCompiler,
    #[serde(rename = "vMode")]
    pub v_mode: SingleVariableCompiler,
    pub v_saved_data: SingleVariableCompiler,
    pub first_saved_data_index: Option<isize>,
}

impl DACTelemetryCompiler {
    pub fn new(period_length: i64, cfg: &HwInfoDAC) -> DACTelemetryCompiler {
        let min_run = if cfg.isVrf || cfg.simulate_l1 {
            60isize
        } else {
            1isize
        };
        return DACTelemetryCompiler {
            last_index: -1,
            v_lcmp: SingleVariableCompilerBuilder::new()
                .with_min_run_length(min_run)
                .build_common(),
            v_levp: SingleVariableCompilerBuilder::new()
                .with_min_run_length(min_run)
                .build_common(),
            v_lcut: SingleVariableCompilerBuilder::new()
                .with_min_run_length(min_run)
                .build_common(),
            v_l1raw: SingleVariableCompiler::create(),
            v_l1fancoil: SingleVariableCompiler::create(),
            v_tamb: SingleVariableCompilerFloat::create(
                5,
                1,
                if period_length > 10000 { 0.1 } else { 0.3 },
            ),
            v_tsuc: SingleVariableCompilerFloat::create(
                5,
                1,
                if period_length > 10000 { 0.1 } else { 0.3 },
            ),
            v_tliq: SingleVariableCompilerFloat::create(
                5,
                1,
                if period_length > 10000 { 0.1 } else { 0.3 },
            ),
            v_psuc: SingleVariableCompilerFloat::create(
                5,
                1,
                if period_length > 10000 { 0.09 } else { 0.2 },
            ),
            v_pliq: SingleVariableCompilerFloat::create(
                5,
                1,
                if period_length > 10000 { 0.09 } else { 0.2 },
            ),
            v_tsc: SingleVariableCompilerFloat::create(
                5,
                1,
                if period_length > 10000 { 0.075 } else { 0.2 },
            ),
            v_tsh: SingleVariableCompilerFloat::create(
                5,
                1,
                if period_length > 10000 { 0.075 } else { 0.2 },
            ),
            v_state: SingleVariableCompiler::create(),
            v_mode: SingleVariableCompiler::create(),
            v_saved_data: SingleVariableCompiler::create(),
            first_saved_data_index: None,
        };
    }

    pub fn AdcPontos(
        &mut self,
        telemetry: &TelemetryDAC_v3,
        index: isize,
        calcs: &Option<TelemetryDAC_v3_calcs>,
        L1: Option<bool>,
        L1fancoil: Option<bool>,
        tolerance_time: i64,
    ) {
        if index <= self.last_index {
            return;
        }

        let sampling_time = if telemetry.saved_data == Some(true) {
            if tolerance_time < 15 { 15 } else { tolerance_time }
          } else {
              15
          } as isize;

        self.last_index = index;
        self.v_lcmp.adc_ponto(
            index,
            match telemetry.Lcmp {
                Some(false) => "0",
                Some(true) => "1",
                None => "",
            },
            sampling_time,
        );
        self.v_levp.adc_ponto(
            index,
            match telemetry.Levp {
                Some(false) => "0",
                Some(true) => "1",
                None => "",
            },
            sampling_time,
        );
        self.v_lcut.adc_ponto(
            index,
            match telemetry.Lcut {
                Some(false) => "0",
                Some(true) => "1",
                None => "",
            },
            sampling_time,
        );
        self.v_l1raw.adc_ponto(
            index,
            match L1 {
                Some(false) => "0",
                Some(true) => "1",
                None => "",
            },
            sampling_time,
        );
        self.v_l1fancoil.adc_ponto(
            index,
            match L1fancoil {
                Some(false) => "0",
                Some(true) => "1",
                None => "",
            },
            sampling_time,
        );
        self.v_tamb.adc_ponto_float(index, telemetry.Tamb, sampling_time);
        self.v_tsuc.adc_ponto_float(index, telemetry.Tsuc, sampling_time);
        self.v_tliq.adc_ponto_float(index, telemetry.Tliq, sampling_time);
        self.v_psuc.adc_ponto_float(index, telemetry.Psuc, sampling_time);
        self.v_pliq.adc_ponto_float(index, telemetry.Pliq, sampling_time);
        self.v_state.adc_ponto(
            index,
            match &telemetry.State {
                Some(v) => v,
                None => "",
            },
            sampling_time,
        );
        self.v_mode.adc_ponto(
            index,
            match &telemetry.Mode {
                Some(v) => v,
                None => "",
            },
            sampling_time,
        );
        self.v_saved_data.adc_ponto(
            index,
            match telemetry.saved_data {
                Some(false) => "0",
                Some(true) => "1",
                None => "0",
            },
            sampling_time,
        );
        if let Some(calcs) = calcs {
            self.v_tsc.adc_ponto_float(index, calcs.Tsc, sampling_time);
            self.v_tsh.adc_ponto_float(index, calcs.Tsh, sampling_time);
        }

        if self.first_saved_data_index.is_none() && telemetry.saved_data.unwrap_or(false) {
            self.first_saved_data_index = Some(index);
        }
    }

    pub fn CheckClosePeriod(
        &mut self,
        periodLength: isize,
    ) -> Result<Option<CompiledPeriod>, String> {
        if self.v_lcmp.had_error() {
            return Err("There was an error compiling the data".to_owned());
        }
        let _periodData: Option<CompiledPeriod> = None;

        if self.v_lcmp.is_empty() {
            return Ok(None);
        }

        let vecLcmp = self.v_lcmp.fechar_vetor_completo(periodLength);
        let vecLevp = self.v_levp.fechar_vetor_completo(periodLength);
        let vecLcut = self.v_lcut.fechar_vetor_completo(periodLength);
        let vecL1raw = self.v_l1raw.fechar_vetor_completo(periodLength);
        let vecL1fancoil = self.v_l1fancoil.fechar_vetor_completo(periodLength);
        let vecTamb = self.v_tamb.fechar_vetor_completo(periodLength);
        let vecTsuc = self.v_tsuc.fechar_vetor_completo(periodLength);
        let vecTliq = self.v_tliq.fechar_vetor_completo(periodLength);
        let vecPsuc = self.v_psuc.fechar_vetor_completo(periodLength);
        let vecPliq = self.v_pliq.fechar_vetor_completo(periodLength);
        let vecTsc = self.v_tsc.fechar_vetor_completo(periodLength);
        let vecTsh = self.v_tsh.fechar_vetor_completo(periodLength);
        let vecState = self.v_state.fechar_vetor_completo(periodLength);
        let vecMode = self.v_mode.fechar_vetor_completo(periodLength);
        let vecSaveData = self.v_saved_data.fechar_vetor_completo(periodLength);
        let (numDeparts, hoursOn, hoursOff, iStartLcmp, iEndLcmp) =
            CalcularEstatisticasUso(&vecLcmp);
        let (_, hoursLevp, _, _, _) = CalcularEstatisticasUso(&vecLevp);
        let hoursBlocked = if hoursLevp > hoursOn {
            (hoursLevp - hoursOn)
        } else {
            0.0
        };
        return Ok(Some(CompiledPeriod {
            Lcmp: vecLcmp,
            Levp: vecLevp,
            Lcut: vecLcut,
            L1raw: vecL1raw,
            L1fancoil: vecL1fancoil,
            Tamb: vecTamb,
            Tsuc: vecTsuc,
            Tliq: vecTliq,
            Psuc: vecPsuc,
            Pliq: vecPliq,
            Tsc: vecTsc,
            Tsh: vecTsh,
            State: vecState,
            Mode: vecMode,
            numDeparts,
            hoursOn,
            hoursOff,
            hoursBlocked,
            savedData: vecSaveData,
            startLcmp: if iStartLcmp > 0 {
                Some(usize::try_from(iStartLcmp).unwrap())
            } else {
                None
            },
            endLcmp: if iEndLcmp > 0 {
                Some(usize::try_from(iEndLcmp).unwrap())
            } else {
                None
            },
            first_saved_data_index: self.first_saved_data_index,
        }));
    }
}

fn CalcularEstatisticasUso(vecL1: &str) -> (isize, f64, f64, isize, isize) {
    // "*200,3.5*300,2,2*30,*100"
    let mut numDeparts: isize = 0;
    let mut hoursOn = 0.0;
    let mut hoursOff = 0.0;
    let mut iStartLcmp: isize = -1;
    let mut iEndLcmp: isize = -1;
    if vecL1.is_empty() {
        return (numDeparts, hoursOn, hoursOff, iStartLcmp, iEndLcmp);
    }
    let imax = usize::try_from(vecL1.len() - 1).unwrap();
    let mut i: usize = 0;
    let mut ival: i64 = -1;
    let mut iast: i64 = -1;
    let mut value;
    let mut lastValue = "";
    let mut acc_duration: isize = 0;
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
            // println!("v: '" + value + "' x " + duration + endl)
            if value == "1" {
                hoursOn += (duration as f64) / 3600.0;
                if lastValue == "0" {
                    numDeparts += 1
                };
                if iStartLcmp < 0 {
                    iStartLcmp = acc_duration;
                }
                iEndLcmp = acc_duration + duration;
            }
            if value == "0" {
                hoursOff += (duration as f64) / 3600.0;
            }
            if !value.is_empty() {
                lastValue = value
            };
            ival = -1;
            iast = -1;
            acc_duration += duration;
        } else if vecL1.as_bytes()[i] == b'*' {
            iast = i as i64;
        }
        if i > imax {
            break;
        }
        i += 1;
    }
    return (numDeparts, hoursOn, hoursOff, iStartLcmp, iEndLcmp);
}

pub struct CompiledPeriod {
    pub Lcmp: String,
    pub Psuc: String,
    pub Pliq: String,
    pub Tamb: String,
    pub Tsuc: String,
    pub Tliq: String,
    pub Levp: String,
    pub Lcut: String,
    pub Tsc: String,
    pub Tsh: String,
    pub State: String,
    pub Mode: String,
    pub L1raw: String,
    pub L1fancoil: String,
    pub numDeparts: isize,
    pub hoursOn: f64,
    pub hoursOff: f64,
    pub hoursBlocked: f64,
    pub startLcmp: Option<usize>,
    pub endLcmp: Option<usize>,
    pub savedData: String,
    pub first_saved_data_index: Option<isize>
}
