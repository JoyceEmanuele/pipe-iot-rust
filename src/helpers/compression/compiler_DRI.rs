use super::common_func::calcular_tempo_online;
use super::compiler_common::SingleVariableCompilerFloat;
use crate::telemetry_payloads::dri::{
    ccn::DriCCNTelemetry, vav_fancoil::DriVAVandFancoilTelemetry,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct DRICCNTelemetryCompiler {
    pub tel_interval: Option<isize>,
    #[serde(rename = "lastIndex")]
    pub last_index: isize,
    #[serde(rename = "vTemp")]
    pub v_temp: SingleVariableCompilerFloat,
    #[serde(rename = "vMachineStatus")]
    pub v_machine_status: SingleVariableCompilerFloat,
    #[serde(rename = "vMode")]
    pub v_mode: SingleVariableCompilerFloat,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DRIVAVandFancoilTelemetryCompiler {
    pub tel_interval: Option<isize>,
    #[serde(rename = "lastIndex")]
    pub last_index: isize,
    #[serde(rename = "vThermOn")]
    pub v_therm_on: SingleVariableCompilerFloat,
    #[serde(rename = "vFanspeed")]
    pub v_fanspeed: SingleVariableCompilerFloat,
    #[serde(rename = "vMode")]
    pub v_mode: SingleVariableCompilerFloat,
    #[serde(rename = "vSetpoint")]
    pub v_setpoint: SingleVariableCompilerFloat,
    #[serde(rename = "vLock")]
    pub v_lock: SingleVariableCompilerFloat,
    #[serde(rename = "vTempAmb")]
    pub v_temp_amb: SingleVariableCompilerFloat,
    #[serde(rename = "vValveOn")]
    pub v_valve_on: SingleVariableCompilerFloat,
    #[serde(rename = "vFanStatus")]
    pub v_fan_status: SingleVariableCompilerFloat,
}

impl DRICCNTelemetryCompiler {
    pub fn new(tel_interval: Option<isize>) -> DRICCNTelemetryCompiler {
        DRICCNTelemetryCompiler {
            tel_interval,
            last_index: -1,
            v_temp: SingleVariableCompilerFloat::create(1, 1, 1.0),
            v_machine_status: SingleVariableCompilerFloat::create(1, 1, 1.0),
            v_mode: SingleVariableCompilerFloat::create(1, 1, 1.0),
        }
    }

    pub fn AdcPontos(&mut self, telemetry: &DriCCNTelemetry, index: isize) {
        if index <= self.last_index {
            return;
        }
        self.last_index = index;
        let interval = match self.tel_interval {
            Some(v) if v < 300 => 300 + 10,
            Some(v) => (v * 2) + 10,
            None => 300 + 10,
        };
        self.v_temp
            .adc_ponto_float(index, telemetry.Setpoint.map(f64::from), interval);
        self.v_machine_status
            .adc_ponto_float(index, telemetry.Status.map(f64::from), interval);
        self.v_mode
            .adc_ponto_float(index, telemetry.Mode.map(f64::from), interval);
    }

    pub fn CheckClosePeriod(
        &mut self,
        periodLength: isize,
    ) -> Result<Option<DRICCNCompiledPeriod>, String> {
        if self.v_temp.had_error() {
            return Err("There was an error compiling the data".to_owned());
        }

        if self.v_temp.is_empty() {
            return Ok(None);
        }

        let vecTemp = self.v_temp.fechar_vetor_completo(periodLength);
        let vecMachineStatus = self.v_machine_status.fechar_vetor_completo(periodLength);
        let vecMode = self.v_mode.fechar_vetor_completo(periodLength);

        let hoursOnline = calcular_tempo_online(&vecTemp);

        Ok(Some(DRICCNCompiledPeriod {
            Setpoint: vecTemp,
            Status: vecMachineStatus,
            Mode: vecMode,
            hoursOnline,
        }))
    }
}

impl DRIVAVandFancoilTelemetryCompiler {
    pub fn new(tel_interval: Option<isize>) -> DRIVAVandFancoilTelemetryCompiler {
        DRIVAVandFancoilTelemetryCompiler {
            tel_interval,
            last_index: -1,
            v_therm_on: SingleVariableCompilerFloat::create(1, 1, 1.0),
            v_fanspeed: SingleVariableCompilerFloat::create(1, 1, 1.0),
            v_mode: SingleVariableCompilerFloat::create(1, 1, 1.0),
            v_setpoint: SingleVariableCompilerFloat::create(10, 1, 1.0),
            v_lock: SingleVariableCompilerFloat::create(1, 1, 1.0),
            v_temp_amb: SingleVariableCompilerFloat::create(10, 1, 1.0),
            v_valve_on: SingleVariableCompilerFloat::create(1, 1, 1.0),
            v_fan_status: SingleVariableCompilerFloat::create(1, 1, 1.0),
        }
    }

    pub fn AdcPontos(&mut self, telemetry: &DriVAVandFancoilTelemetry, index: isize) {
        if index <= self.last_index {
            return;
        }
        let interval = match self.tel_interval {
            Some(v) if v < 300 => 300 + 10,
            Some(v) => (v * 2) + 10,
            None => 300 + 10,
        };
        self.last_index = index;

        self.v_therm_on
            .adc_ponto_float(index, telemetry.ThermOn, interval);
        self.v_fanspeed
            .adc_ponto_float(index, telemetry.Fanspeed, interval);
        self.v_mode.adc_ponto_float(index, telemetry.Mode, interval);
        self.v_setpoint
            .adc_ponto_float(index, telemetry.Setpoint, interval);
        self.v_lock.adc_ponto_float(index, telemetry.Lock, interval);
        self.v_temp_amb
            .adc_ponto_float(index, telemetry.TempAmb, interval);
        self.v_valve_on
            .adc_ponto_float(index, telemetry.ValveOn, interval);
        self.v_fan_status
            .adc_ponto_float(index, telemetry.FanStatus, interval);
    }

    pub fn CheckClosePeriod(
        &mut self,
        periodLength: isize,
    ) -> Result<Option<DRIVAVandFancoilCompiledPeriod>, String> {
        if self.v_setpoint.had_error() {
            return Err("There was an error compiling the data".to_owned());
        }

        if self.v_setpoint.is_empty() {
            return Ok(None);
        }

        let vecThermOn = self.v_therm_on.fechar_vetor_completo(periodLength);
        let vecFanspeed = self.v_fanspeed.fechar_vetor_completo(periodLength);
        let vecMode = self.v_mode.fechar_vetor_completo(periodLength);
        let vecSetpoint = self.v_setpoint.fechar_vetor_completo(periodLength);
        let vecLock = self.v_lock.fechar_vetor_completo(periodLength);
        let vecTempAmb = self.v_temp_amb.fechar_vetor_completo(periodLength);
        let vecValveOn = self.v_valve_on.fechar_vetor_completo(periodLength);
        let vecFanStatus = self.v_fan_status.fechar_vetor_completo(periodLength);

        let hoursOnline = calcular_tempo_online(&vecSetpoint);

        Ok(Some(DRIVAVandFancoilCompiledPeriod {
            ThermOn: vecThermOn,
            Fanspeed: vecFanspeed,
            Mode: vecMode,
            Setpoint: vecSetpoint,
            Lock: vecLock,
            TempAmb: vecTempAmb,
            ValveOn: vecValveOn,
            FanStatus: vecFanStatus,
            hoursOnline,
        }))
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DRICCNCompiledPeriod {
    pub Setpoint: String,
    pub Status: String,
    pub Mode: String,
    pub hoursOnline: f64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct DRIVAVandFancoilCompiledPeriod {
    pub ThermOn: String,
    pub Fanspeed: String,
    pub Mode: String,
    pub Setpoint: String,
    pub Lock: String,
    pub TempAmb: String,
    pub ValveOn: String,
    pub FanStatus: String,
    pub hoursOnline: f64,
}
