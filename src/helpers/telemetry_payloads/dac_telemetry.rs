use super::telemetry_formats::TelemetryDACv2;
use crate::l1_virtual::dac_l1::dac_l1_calculator::{DacL1Calculator, L1Calculator};
use crate::telemetry_payloads::dac_tsh_tsc;
use crate::telemetry_payloads::telemetry_formats::{
    TelemetryDAC_v3, TelemetryPackDAC_v2, TelemetryPackDAC_v3,
};
use chrono::Duration;
use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;

#[derive(Serialize, Deserialize, Debug)]
pub enum T_sensors {
    T0,
    T1,
    T2,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct T_sensor_cfg {
    pub Tamb: Option<T_sensors>,
    pub Tsuc: Option<T_sensors>,
    pub Tliq: Option<T_sensors>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct HwInfoDAC {
    pub isVrf: bool,
    pub calculate_L1_fancoil: Option<bool>,
    pub debug_L1_fancoil: Option<bool>,
    pub hasAutomation: bool,
    pub P0Psuc: bool,
    pub P1Psuc: bool,
    pub P0Pliq: bool,
    pub P1Pliq: bool,
    pub P0multQuad: f64,
    pub P1multQuad: f64,
    #[serde(alias = "P0mult")]
    pub P0multLin: f64,
    #[serde(alias = "P1mult")]
    pub P1multLin: f64,
    pub P0ofst: f64,
    pub P1ofst: f64,
    pub fluid: Option<String>,
    pub t_cfg: Option<T_sensor_cfg>,
    #[serde(rename = "simulateL1")]
    pub simulate_l1: bool,
    pub l1_psuc_offset: f64,
    pub DAC_APPL: Option<String>,
    pub DAC_TYPE: Option<String>,
}

pub fn split_pack(
    payload: &TelemetryPackDAC_v2,
    ts_ini: i64,
    ts_next: i64,
    dev: &HwInfoDAC,
    dac_state: &mut dyn DacL1Calculator,
    itemCallback: &mut dyn FnMut(&mut TelemetryDAC_v3, Option<bool>, Option<bool>, isize),
) -> Result<(), String> {
    if payload.T0.len() != payload.L1.len() {
        return Err(format!(
            "Incompatible length of T0 at {}",
            payload.timestamp
        ));
    }
    if payload.T1.len() != payload.L1.len() {
        return Err(format!(
            "Incompatible length of T1 at {}",
            payload.timestamp
        ));
    }
    if payload.T2.len() != payload.L1.len() {
        return Err(format!(
            "Incompatible length of T2 at {}",
            payload.timestamp
        ));
    }
    if payload.P0.len() != payload.L1.len() {
        return Err(format!(
            "Incompatible length of P0 at {}",
            payload.timestamp
        ));
    }
    if payload.P1.len() != payload.L1.len() {
        return Err(format!(
            "Incompatible length of P1 at {}",
            payload.timestamp
        ));
    }

    let pack_ts = match NaiveDateTime::parse_from_str(&payload.timestamp, "%Y-%m-%dT%H:%M:%S") {
        Err(_) => {
            crate::LOG.append_log_tag_msg("ERROR", &format!("Error parsing Date: {:?}", payload));
            return Err("Error parsing Date".to_owned());
        }
        Ok(date) => date.timestamp(),
    };
    let sampling_time: i64 = payload.samplingTime; // de quantos em quantos segundos o firmware lê os sensores e insere nos vetores.

    let mut telemetry = TelemetryDAC_v3 {
        Lcmp: None,
        Lcut: None,
        Levp: None,
        Tamb: None,
        Tsuc: None,
        Tliq: None,
        Psuc: None,
        Pliq: None,
        State: None,
        Mode: None,
        GMT: payload.GMT.to_owned(),
        saved_data: payload.saved_data,  
    };

    let mut remainingSteps = payload.L1.len();
    for _i in 0..payload.L1.len() {
        let telm_ts = pack_ts - ((remainingSteps as i64 - 1) * sampling_time);
        remainingSteps = checkSetTelemetryValues(
            dev,
            payload,
            &mut telemetry,
            telm_ts,
            dac_state,
            remainingSteps,
        );
        let index = payload.L1.len() - 1 - remainingSteps;
        let L1 = payload.L1[index];
        let L1fancoil = calc_L1_fancoil(&telemetry.Tsuc, &telemetry.Tliq);
        if telm_ts < ts_ini {
            continue;
        }
        if telm_ts >= ts_next {
            continue;
        }
        itemCallback(
            &mut telemetry,
            L1,
            L1fancoil,
            isize::try_from(telm_ts - ts_ini).unwrap(),
        );
    }
    return Ok(());
}

fn split_pack_unlimited(
    payload: &TelemetryPackDAC_v2,
    sampling_time: i64,
    dev: &HwInfoDAC,
    dac_state: &mut dyn DacL1Calculator,
    itemCallback: &mut dyn FnMut(&mut TelemetryDAC_v3, &str),
) -> Result<(), String> {
    if payload.T0.len() != payload.L1.len() {
        return Err(format!(
            "Incompatible length of T0 at {}",
            payload.timestamp
        ));
    }
    if payload.T1.len() != payload.L1.len() {
        return Err(format!(
            "Incompatible length of T1 at {}",
            payload.timestamp
        ));
    }
    if payload.T2.len() != payload.L1.len() {
        return Err(format!(
            "Incompatible length of T2 at {}",
            payload.timestamp
        ));
    }
    if payload.P0.len() != payload.L1.len() {
        return Err(format!(
            "Incompatible length of P0 at {}",
            payload.timestamp
        ));
    }
    if payload.P1.len() != payload.L1.len() {
        return Err(format!(
            "Incompatible length of P1 at {}",
            payload.timestamp
        ));
    }

    let pack_ts = match NaiveDateTime::parse_from_str(&payload.timestamp, "%Y-%m-%dT%H:%M:%S") {
        Err(_) => {
            crate::LOG.append_log_tag_msg("ERROR", &format!("Error parsing Date: {:?}", payload));
            return Err("Error parsing Date".to_owned());
        }
        Ok(date) => date.timestamp(),
    };
    let sampling_time: i64 = payload.samplingTime; // de quantos em quantos segundos o firmware lê os sensores e insere nos vetores.

    let mut telemetry = TelemetryDAC_v3 {
        Lcmp: None,
        Lcut: None,
        Levp: None,
        Tamb: None,
        Tsuc: None,
        Tliq: None,
        Psuc: None,
        Pliq: None,
        State: None,
        Mode: None,
        GMT: payload.GMT.to_owned(),
        saved_data: payload.saved_data,  
    };
    let mut remainingSteps = payload.L1.len();
    for _i in 0..payload.L1.len() {
        let telm_ts = pack_ts - ((remainingSteps as i64 - 1) * sampling_time);
        remainingSteps = checkSetTelemetryValues(
            dev,
            payload,
            &mut telemetry,
            telm_ts,
            dac_state,
            remainingSteps,
        );
        let timestamp =
            NaiveDateTime::from_timestamp(pack_ts - ((remainingSteps as i64) * sampling_time), 0)
                .format("%Y-%m-%dT%H:%M:%S")
                .to_string();
        itemCallback(&mut telemetry, &timestamp);
    }

    return Ok(());
}

pub fn convert_payload_v2(
    payload: &TelemetryPackDAC_v2,
    dev: &HwInfoDAC,
    dac_state: &mut Box<L1Calculator>,
) -> Result<TelemetryPackDAC_v3, String> {
    convert_payload(payload, dev, dac_state)
}

pub fn convert_payload(
    payload: &TelemetryPackDAC_v2,
    dev: &HwInfoDAC,
    dac_state: &mut L1Calculator,
) -> Result<TelemetryPackDAC_v3, String> {
    let pack_length = payload.L1.len();
    if payload.T0.len() != pack_length {
        return Err(format!(
            "Incompatible length of T0 at {}",
            payload.timestamp
        ));
    }
    if payload.T1.len() != pack_length {
        return Err(format!(
            "Incompatible length of T1 at {}",
            payload.timestamp
        ));
    }
    if payload.T2.len() != pack_length {
        return Err(format!(
            "Incompatible length of T2 at {}",
            payload.timestamp
        ));
    }
    if payload.P0.len() != pack_length {
        return Err(format!(
            "Incompatible length of P0 at {}",
            payload.timestamp
        ));
    }
    if payload.P1.len() != pack_length {
        return Err(format!(
            "Incompatible length of P1 at {}",
            payload.timestamp
        ));
    }

    let pack_ts = match NaiveDateTime::parse_from_str(&payload.timestamp, "%Y-%m-%dT%H:%M:%S") {
        Err(_) => {
            crate::LOG.append_log_tag_msg("ERROR", &format!("Error parsing Date: {:?}", payload));
            return Err("Error parsing Date".to_owned());
        }
        Ok(date) => date,
    };

    let has_Psuc = dev.P0Psuc || dev.P1Psuc;
    let has_Pliq = dev.P0Pliq || dev.P1Pliq;
    let _uses_P0 = dev.P0Psuc || dev.P0Pliq;
    let _uses_P1 = dev.P1Psuc || dev.P1Pliq;
    // let mut has_Tsc = dev.fluid.as_ref().filter(|_| has_Pliq).and_then(|fluid| dac_tsh_tsc::FluidInterpData::for_fluid(fluid));
    // let mut has_Tsh = dev.fluid.as_ref().filter(|_| has_Psuc).and_then(|fluid| dac_tsh_tsc::FluidInterpData::for_fluid(fluid));
    let mut has_Tsc = if has_Pliq && dev.fluid.is_some() {
        dac_tsh_tsc::FluidInterpData::for_fluid(dev.fluid.as_ref().unwrap())
    } else {
        None
    };
    let mut has_Tsh = if has_Psuc && dev.fluid.is_some() {
        dac_tsh_tsc::FluidInterpData::for_fluid(dev.fluid.as_ref().unwrap())
    } else {
        None
    };

    let mut telemetry_pack = TelemetryPackDAC_v3 {
        Lcmp: Vec::with_capacity(pack_length),
        Lcut: if dev.hasAutomation {
            Some(Vec::with_capacity(pack_length))
        } else {
            None
        },
        Levp: if dev.hasAutomation {
            Some(Vec::with_capacity(pack_length))
        } else {
            None
        },
        Tamb: Some(Vec::with_capacity(pack_length)),
        Tsuc: Some(Vec::with_capacity(pack_length)),
        Tliq: Some(Vec::with_capacity(pack_length)),
        Psuc: if has_Psuc {
            Some(Vec::with_capacity(pack_length))
        } else {
            None
        },
        Pliq: if has_Pliq {
            Some(Vec::with_capacity(pack_length))
        } else {
            None
        },
        Tsc: if has_Tsc.is_some() {
            Some(Vec::with_capacity(pack_length))
        } else {
            None
        },
        Tsh: if has_Tsh.is_some() {
            Some(Vec::with_capacity(pack_length))
        } else {
            None
        },
        State: None,
        Mode: None,
    };

    for index in 0..pack_length {
        let T0 = match payload.T0[index] {
            None => None,
            Some(T0) => {
                if (T0 <= -99.0) || (T0 >= 85.0) {
                    None
                } else {
                    Some(T0)
                }
            }
        };
        let T1 = match payload.T1[index] {
            None => None,
            Some(T1) => {
                if (T1 <= -99.0) || (T1 >= 85.0) {
                    None
                } else {
                    Some(T1)
                }
            }
        };
        let T2 = match payload.T2[index] {
            None => None,
            Some(T2) => {
                if (T2 <= -99.0) || (T2 >= 85.0) {
                    None
                } else {
                    Some(T2)
                }
            }
        };
        let P0 = match payload.P0[index] {
            None => None,
            Some(P0) => {
                if P0 == 0 {
                    None
                } else {
                    Some(P0)
                }
            }
        };
        let P1 = match payload.P1[index] {
            None => None,
            Some(P1) => {
                if P1 == 0 {
                    None
                } else {
                    Some(P1)
                }
            }
        };
        let mut Tamb = T0;
        let mut Tsuc = T1;
        let mut Tliq = T2;
        if let Some(t_cfg) = &dev.t_cfg {
            Tamb = match t_cfg.Tamb {
                None => None,
                Some(T_sensors::T0) => T0,
                Some(T_sensors::T1) => T1,
                Some(T_sensors::T2) => T2,
            };
            Tsuc = match t_cfg.Tsuc {
                None => None,
                Some(T_sensors::T0) => T0,
                Some(T_sensors::T1) => T1,
                Some(T_sensors::T2) => T2,
            };
            Tliq = match t_cfg.Tliq {
                None => None,
                Some(T_sensors::T0) => T0,
                Some(T_sensors::T1) => T1,
                Some(T_sensors::T2) => T2,
            };
        }

        let mut Psuc = None;
        let mut Pliq = None;
        if has_Psuc {
            Psuc = {
                if dev.P0Psuc {
                    P0.map(|P0| {
                        dev.P0multQuad * f64::from(P0) * f64::from(P0)
                            + f64::from(P0) * dev.P0multLin
                            + dev.P0ofst
                    })
                } else if dev.P1Psuc {
                    P1.map(|P1| {
                        dev.P1multQuad * f64::from(P1) * f64::from(P1)
                            + f64::from(P1) * dev.P1multLin
                            + dev.P1ofst
                    })
                } else {
                    None
                }
            };
            telemetry_pack
                .Psuc
                .as_mut()
                .unwrap()
                .push(Psuc.map(|v| (v * 10.).round() / 10.));
        }
        if has_Pliq {
            Pliq = {
                if dev.P0Pliq {
                    P0.map(|P0| {
                        dev.P0multQuad * f64::from(P0) * f64::from(P0)
                            + f64::from(P0) * dev.P0multLin
                            + dev.P0ofst
                    })
                } else if dev.P1Pliq {
                    P1.map(|P1| {
                        dev.P1multQuad * f64::from(P1) * f64::from(P1)
                            + f64::from(P1) * dev.P1multLin
                            + dev.P1ofst
                    })
                } else {
                    None
                }
            };
            telemetry_pack
                .Pliq
                .as_mut()
                .unwrap()
                .push(Pliq.map(|v| (v * 10.).round() / 10.));
        }

        telemetry_pack.Tamb.as_mut().unwrap().push(Tamb);
        telemetry_pack.Tsuc.as_mut().unwrap().push(Tsuc);
        telemetry_pack.Tliq.as_mut().unwrap().push(Tliq);

        let point_tel = TelemetryDACv2 {
            mode: payload.Mode.as_deref(),
            state: payload.State.as_deref(),
            t0: T0,
            t1: T1,
            t2: T2,
            p0: P0,
            p1: P1,
            l1: payload.L1[index],
            timestamp: pack_ts
                - Duration::seconds(
                    payload.samplingTime * i64::try_from(pack_length - index - 1).unwrap(),
                ),
            GMT: payload.GMT.clone(),
        };
        let building_tel = TelemetryDAC_v3 {
            Lcmp: None,
            Lcut: None,
            Levp: None,
            Mode: None,
            State: None,
            Tamb,
            Tsuc,
            Tliq,
            Psuc: Psuc.map(|v| (v * 10.).round() / 10.),
            Pliq: Pliq.map(|v| (v * 10.).round() / 10.),
            GMT: payload.GMT.clone(),
            saved_data: payload.saved_data,  
        };

        let l1 = dac_state
            .calc_l1(&building_tel, &point_tel, dev)
            .ok()
            .flatten()
            .map(u8::from);

        if dev.isVrf || dev.calculate_L1_fancoil.unwrap_or(false) {
            if dev.hasAutomation {
                match payload.State.as_deref() {
                    Some("Disabled") => {
                        telemetry_pack.Levp.as_mut().unwrap().push(None);
                        telemetry_pack.Lcut.as_mut().unwrap().push(Some(1));
                        telemetry_pack.Lcmp.push(Some(0));
                    }
                    Some("Enabled") => {
                        telemetry_pack.Levp.as_mut().unwrap().push(None);
                        telemetry_pack.Lcut.as_mut().unwrap().push(Some(0));
                        telemetry_pack.Lcmp.push(l1);
                    }
                    _ => {
                        telemetry_pack.Levp.as_mut().unwrap().push(None);
                        telemetry_pack.Lcut.as_mut().unwrap().push(None);
                        telemetry_pack.Lcmp.push(l1);
                    }
                }
                telemetry_pack.State = payload.State.as_ref().map(|v| v.to_owned());
                telemetry_pack.Mode = payload.Mode.as_ref().map(|v| v.to_owned());
            } else {
                telemetry_pack.Lcmp.push(l1);
            }
        } else if dev.hasAutomation {
            telemetry_pack.Levp.as_mut().unwrap().push(l1);
            match payload.State.as_deref() {
                Some("Disabled") => {
                    telemetry_pack.Lcut.as_mut().unwrap().push(Some(1));
                    telemetry_pack.Lcmp.push(Some(0));
                }
                Some("Enabled") => {
                    telemetry_pack.Lcut.as_mut().unwrap().push(Some(0));
                    telemetry_pack.Lcmp.push(l1);
                }
                _ => {
                    telemetry_pack.Lcut.as_mut().unwrap().push(None);
                    telemetry_pack.Lcmp.push(None);
                }
            }
            telemetry_pack.State = payload.State.as_ref().map(|v| v.to_owned());
            telemetry_pack.Mode = payload.Mode.as_ref().map(|v| v.to_owned());
        } else {
            telemetry_pack.Lcmp.push(l1);
        }

        if has_Psuc && has_Tsh.is_some() {
            let mut viData = has_Tsh.as_mut().unwrap();
            let Lcmp = &telemetry_pack.Lcmp[telemetry_pack.Lcmp.len() - 1];
            let Lcmp = if *Lcmp == Some(1) {
                Some(true)
            } else if *Lcmp == Some(0) {
                Some(false)
            } else {
                None
            };
            let Tsh = dac_tsh_tsc::calculateSupAq(&mut viData, &Psuc, &Tsuc, &Lcmp);
            telemetry_pack.Tsh.as_mut().unwrap().push(Tsh);
        }
        if has_Pliq && has_Tsc.is_some() {
            let mut viData = has_Tsc.as_mut().unwrap();
            let Lcmp = &telemetry_pack.Lcmp[telemetry_pack.Lcmp.len() - 1];
            let Lcmp = if *Lcmp == Some(1) {
                Some(true)
            } else if *Lcmp == Some(0) {
                Some(false)
            } else {
                None
            };
            let Tsc = dac_tsh_tsc::calculateSubResf(&mut viData, &Pliq, &Tliq, &Lcmp);
            telemetry_pack.Tsc.as_mut().unwrap().push(Tsc);
        }
    }

    return Ok(telemetry_pack);
}

fn calc_L1_fancoil(Tsuc: &Option<f64>, Tliq: &Option<f64>) -> Option<bool> {
    if let (Some(Tsuc), Some(Tliq)) = (Tsuc, Tliq) {
        Some((Tsuc - Tliq) >= 1.5)
    } else {
        None
    }
}

pub fn checkSetTelemetryValues(
    dev: &HwInfoDAC,
    payload: &TelemetryPackDAC_v2,
    telemetry: &mut TelemetryDAC_v3,
    current_ts: i64,
    dac_state: &mut dyn DacL1Calculator,
    mut remainingSteps: usize,
) -> usize {
    if remainingSteps == 0 {
        remainingSteps = payload.L1.len() - 1;
    } else {
        remainingSteps -= 1;
    }
    let index = payload.L1.len() - 1 - remainingSteps;

    if let Some(t_cfg) = &dev.t_cfg {
        let T0 = match payload.T0[index] {
            None => None,
            Some(T0) => {
                if (T0 <= -99.0) || (T0 >= 85.0) {
                    None
                } else {
                    Some(T0)
                }
            }
        };
        let T1 = match payload.T1[index] {
            None => None,
            Some(T1) => {
                if (T1 <= -99.0) || (T1 >= 85.0) {
                    None
                } else {
                    Some(T1)
                }
            }
        };
        let T2 = match payload.T2[index] {
            None => None,
            Some(T2) => {
                if (T2 <= -99.0) || (T2 >= 85.0) {
                    None
                } else {
                    Some(T2)
                }
            }
        };
        telemetry.Tamb = match t_cfg.Tamb {
            None => None,
            Some(T_sensors::T0) => T0,
            Some(T_sensors::T1) => T1,
            Some(T_sensors::T2) => T2,
        };
        telemetry.Tsuc = match t_cfg.Tsuc {
            None => None,
            Some(T_sensors::T0) => T0,
            Some(T_sensors::T1) => T1,
            Some(T_sensors::T2) => T2,
        };
        telemetry.Tliq = match t_cfg.Tliq {
            None => None,
            Some(T_sensors::T0) => T0,
            Some(T_sensors::T1) => T1,
            Some(T_sensors::T2) => T2,
        };
    } else {
        telemetry.Tsuc = match payload.T1[index] {
            None => None,
            Some(T1) => {
                if T1 <= -99.0 || T1 >= 85.0 {
                    None
                } else {
                    Some(T1)
                }
            }
        };
        telemetry.Tliq = match payload.T2[index] {
            None => None,
            Some(T2) => {
                if T2 <= -99.0 || T2 >= 85.0 {
                    None
                } else {
                    Some(T2)
                }
            }
        };
        telemetry.Tamb = match payload.T0[index] {
            None => None,
            Some(T0) => {
                if T0 <= -99.0 || T0 >= 85.0 {
                    None
                } else {
                    Some(T0)
                }
            }
        };
    }

    telemetry.Psuc = {
        if dev.P0Psuc {
            payload.P0[index]
                .map(|P0| {
                    dev.P0multQuad * f64::from(P0) * f64::from(P0)
                        + f64::from(P0) * dev.P0multLin
                        + dev.P0ofst
                })
                .map(|x| (10.0 * x).round() / 10.0)
        } else if dev.P1Psuc {
            payload.P1[index]
                .map(|P1| {
                    dev.P1multQuad * f64::from(P1) * f64::from(P1)
                        + f64::from(P1) * dev.P1multLin
                        + dev.P1ofst
                })
                .map(|x| (10.0 * x).round() / 10.0)
        } else {
            None
        }
    };
    telemetry.Pliq = {
        if dev.P0Pliq {
            payload.P0[index]
                .map(|P0| {
                    dev.P0multQuad * f64::from(P0) * f64::from(P0)
                        + f64::from(P0) * dev.P0multLin
                        + dev.P0ofst
                })
                .map(|x| (10.0 * x).round() / 10.0)
        } else if dev.P1Pliq {
            payload.P1[index]
                .map(|P1| {
                    dev.P1multQuad * f64::from(P1) * f64::from(P1)
                        + f64::from(P1) * dev.P1multLin
                        + dev.P1ofst
                })
                .map(|x| (10.0 * x).round() / 10.0)
        } else {
            None
        }
    };
    let GMT = match &telemetry.GMT {
        None => Some(-3),
        Some(v) => Some(*v),
    };

    let single_point_payload = TelemetryDACv2 {
        l1: payload.L1[index],
        timestamp: NaiveDateTime::from_timestamp_opt(current_ts, 0).unwrap(),
        GMT,
        t0: payload.T0[index],
        t1: payload.T1[index],
        t2: payload.T2[index],
        p0: payload.P0[index],
        p1: payload.P1[index],
        mode: payload.Mode.as_deref(),
        state: payload.State.as_deref(),
    };

    let l1 = dac_state
        .calc_l1(telemetry, &single_point_payload, dev)
        .ok()
        .flatten();

    if dev.hasAutomation {
        telemetry.Levp = l1;
        match payload.State.as_deref() {
            Some("Disabled") => {
                telemetry.Lcut = Some(true);
                telemetry.Lcmp = Some(false);
            }
            Some("Enabled") => {
                telemetry.Lcut = Some(false);
                telemetry.Lcmp = l1;
            }
            _ => {
                telemetry.Lcut = None;
                telemetry.Lcmp = None;
            }
        }
        telemetry.State = payload.State.as_ref().map(|v| v.to_owned());
        telemetry.Mode = payload.Mode.as_ref().map(|v| v.to_owned());
    } else {
        telemetry.Lcmp = l1;
        telemetry.Lcut = None;
        telemetry.Levp = None;
    }

    return remainingSteps;
}

pub fn parse_dac_payload(json_payload: &serde_json::Value) -> Result<TelemetryPackDAC_v2, String> {
    if !json_payload["dev_id"].is_string() {
        return Err("Missing dev_id".to_owned());
    }
    if !json_payload["timestamp"].is_string() {
        return Err("Missing timestamp".to_owned());
    }
    if !json_payload["L1"].is_array() {
        return Err("Missing L1".to_owned());
    };
    if !json_payload["T0"].is_array() {
        return Err("Missing T0".to_owned());
    };
    if !json_payload["T1"].is_array() {
        return Err("Missing T1".to_owned());
    };
    if !json_payload["T2"].is_array() {
        return Err("Missing T2".to_owned());
    };
    if !json_payload["P0"].is_array() {
        return Err("Missing P0".to_owned());
    };
    if !json_payload["P1"].is_array() {
        return Err("Missing P1".to_owned());
    };

    let mut payload = TelemetryPackDAC_v2 {
        timestamp: json_payload["timestamp"].as_str().unwrap().to_owned(),
        samplingTime: json_payload["samplingTime"]
            .as_i64()
            .or_else(|| json_payload["sampling_time"].as_i64())
            .unwrap_or(1),
        L1: vec![],
        T0: vec![],
        T1: vec![],
        T2: vec![],
        P0: vec![],
        P1: vec![],
        State: json_payload["State"].as_str().map(|v| v.to_owned()),
        Mode: json_payload["Mode"].as_str().map(|v| v.to_owned()),
        GMT: json_payload["GMT"].as_i64().map(|v| v.to_owned()),
        saved_data: json_payload["saved_data"].as_bool().map(|v| v.to_owned()),
    };

    for item in json_payload["L1"].as_array().unwrap().iter() {
        if item.is_null() {
            payload.L1.push(None);
        } else {
            payload.L1.push(match item.as_i64() {
                Some(v) => match v {
                    0 => Some(false),
                    1 => Some(true),
                    _ => {
                        return Err("Invalid L1 value".to_owned());
                    }
                },
                None => {
                    return Err("Invalid L1 value".to_owned());
                }
            });
        }
    }
    for item in json_payload["T0"].as_array().unwrap().iter() {
        if item.is_null() {
            payload.T0.push(None);
        } else {
            payload.T0.push(match item.as_f64() {
                Some(v) => Some(v),
                None => {
                    return Err("Invalid T0 value".to_owned());
                }
            });
        }
    }
    for item in json_payload["T1"].as_array().unwrap().iter() {
        if item.is_null() {
            payload.T1.push(None);
        } else {
            payload.T1.push(match item.as_f64() {
                Some(v) => Some(v),
                None => {
                    return Err("Invalid T1 value".to_owned());
                }
            });
        }
    }
    for item in json_payload["T2"].as_array().unwrap().iter() {
        if item.is_null() {
            payload.T2.push(None);
        } else {
            payload.T2.push(match item.as_f64() {
                Some(v) => Some(v),
                None => {
                    return Err("Invalid T2 value".to_owned());
                }
            });
        }
    }
    for item in json_payload["P0"].as_array().unwrap().iter() {
        if item.is_null() {
            payload.P0.push(None);
        } else {
            payload.P0.push(match item.as_i64() {
                Some(v) => Some(i16::try_from(v).unwrap()),
                None => {
                    return Err("Invalid P0 value".to_owned());
                }
            });
        }
    }
    for item in json_payload["P1"].as_array().unwrap().iter() {
        if item.is_null() {
            payload.P1.push(None);
        } else {
            payload.P1.push(match item.as_i64() {
                Some(v) => Some(i16::try_from(v).unwrap()),
                None => {
                    return Err("Invalid P1 value".to_owned());
                }
            });
        }
    }

    return Ok(payload);
}
