use super::telemetry_formats::TelemetryDUTv2;
use crate::l1_virtual::dut_l1::l1_calc::{DutL1Calculator, DutStateInfo, L1Calculator};
use crate::telemetry_payloads::telemetry_formats::{
    TelemetryDUT_v3, TelemetryPackDUT_v2, TelemetryPackDutV2Full,
};
use chrono::NaiveDateTime;
use std::convert::TryFrom;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct HwInfoDUT {
    pub temperature_offset: f64,
}

pub fn split_pack(
    payload: &TelemetryPackDUT_v2,
    ts_ini: i64,
    ts_next: i64,
    dut_state: &mut L1Calculator,
    itemCallback: &mut dyn FnMut(&TelemetryDUT_v3, isize),
    dev: &HwInfoDUT,
) -> Result<(), String> {
    let pack_length = payload
        .Temperature
        .as_ref()
        .map(|x| x.len())
        .or_else(|| payload.eCO2.as_ref().map(|x| x.len()))
        .or_else(|| payload.Temperature_1.as_ref().map(|x| x.len()))
        .unwrap_or(0);

    if let Some(Humidity) = &payload.Humidity {
        if Humidity.len() != pack_length {
            return Err("Incompatible length".to_owned());
        }
    }
    if let Some(Temperature) = &payload.Temperature {
        if Temperature.len() != pack_length {
            return Err("Incompatible length".to_owned());
        }
    }
    if let Some(Temperature_1) = &payload.Temperature_1 {
        if Temperature_1.len() != pack_length {
            return Err("Incompatible length".to_owned());
        }
    }
    if let Some(raw_eCO2) = &payload.raw_eCO2 {
        if raw_eCO2.len() != pack_length {
            return Err("Incompatible length".to_owned());
        }
    }
    if let Some(Tmp) = &payload.Tmp {
        if Tmp.len() != pack_length {
            return Err("Incompatible length".to_owned());
        }
    }

    let pack_ts = payload.timestamp.timestamp();
    let sampling_time: i64 = payload.samplingTime; // de quantos em quantos segundos o firmware lê os sensores e insere nos vetores.
    let GMT: Option<i64> = payload.GMT.clone();

    let mut telemetry = TelemetryDUT_v3 {
        timestamp: payload.timestamp,
        Temp: None,
        Temp1: None,
        Tmp: None,
        Hum: None,
        eCO2: None,
        raw_eCO2: None,
        tvoc: None,
        State: None,
        Mode: None,
        l1: None,
        GMT: payload.GMT,
    };

    if pack_length == 0 {
        // payload is automation only
        checkSetAutTelemetryValues(payload, &mut telemetry);
        if (pack_ts >= ts_ini) && (pack_ts < ts_next) {
            itemCallback(&telemetry, isize::try_from(pack_ts - ts_ini).unwrap());
        }
    } else {
        let mut remainingSteps = pack_length;
        let mut telm_ts;
        for _i in 0..pack_length {
            telemetry.timestamp = NaiveDateTime::from_timestamp_opt(
                pack_ts - ((remainingSteps as i64 - 1) * sampling_time),
                0,
            )
            .unwrap();
            remainingSteps =
                checkSetTelemetryValues(payload, &mut telemetry, dut_state, remainingSteps, dev);
            telm_ts = pack_ts - ((remainingSteps as i64) * sampling_time);
            if telm_ts < ts_ini {
                continue;
            }
            if telm_ts >= ts_next {
                continue;
            }
            telemetry.timestamp = NaiveDateTime::from_timestamp_opt(telm_ts, 0).unwrap();
            itemCallback(&telemetry, isize::try_from(telm_ts - ts_ini).unwrap());
        }
    }

    return Ok(());
}

pub fn convert_payload<'a>(
    mut payload: TelemetryPackDutV2Full<'a>,
    dev: &'a HwInfoDUT,
    dut_state_info: &mut DutStateInfo,
) -> Result<TelemetryPackDutV2Full<'a>, String> {
    // let pack_ts = match NaiveDateTime::parse_from_str(&payload.timestamp, "%Y-%m-%dT%H:%M:%S") {
    //   Err(_) => {
    //     println!("Error parsing Date:\n{:?}", payload);
    //     return Err("Error parsing Date".to_owned());
    //   },
    //   Ok (date) => date.timestamp(),
    // };

    let pack_length = if let Some(temp) = &payload.temp {
        temp.len()
    } else if let Some(e_co2) = &payload.e_co2 {
        e_co2.len()
    } else {
        return Err("No pack length!".to_owned());
    };

    if let Some(hum) = &payload.hum {
        if hum.len() != pack_length {
            return Err("Incompatible length".to_owned());
        }
    }
    if let Some(temp) = &payload.temp {
        if temp.len() != pack_length {
            return Err("Incompatible length".to_owned());
        }
    }
    if let Some(temp1) = &payload.temp1 {
        if temp1.len() != pack_length {
            return Err("Incompatible length".to_owned());
        }
    }
    if let Some(tmp) = &payload.tmp {
        if tmp.len() != pack_length {
            return Err("Incompatible length".to_owned());
        }
    }
    if let Some(e_co2) = &payload.e_co2 {
        if e_co2.len() != pack_length {
            return Err("Incompatible length".to_owned());
        }
    }
    if let Some(raw_e_co2) = &payload.raw_e_co2 {
        if raw_e_co2.len() != pack_length {
            return Err("Incompatible length".to_owned());
        }
    }
    if let Some(tvoc) = &payload.tvoc {
        if tvoc.len() != pack_length {
            return Err("Incompatible length".to_owned());
        }
    }

    let should_calc_l1 = payload.temp.is_some() && payload.temp1.is_some();
    if should_calc_l1 && (payload.l1.is_none() || payload.l1.as_ref().is_some_and(|x| x.is_empty()))
    {
        payload.l1 = Some(Vec::with_capacity(pack_length));
    }

    let ts_first = (payload.timestamp.timestamp_millis() / 1000)
        - ((pack_length as i64 - 1) * payload.sampling_time);

    for index in 0..pack_length {
        let timestamp = ts_first + (((index as i64) * payload.sampling_time) as i64);
        if let Some(arr) = payload.temp.as_mut() {
            if let Some(val) = arr[index] {
                arr[index] = dut_state_info
                    .t_checker
                    .t0
                    .check_value(val + dev.temperature_offset, timestamp);
            }
        }
        if let Some(arr) = payload.temp1.as_mut() {
            if let Some(val) = arr[index] {
                arr[index] = dut_state_info.t_checker.t0.check_value(val, timestamp);
            }
        }
        if let Some(arr) = payload.tmp.as_mut() {
            if let Some(val) = arr[index] {
                if (val <= -99.0) || (val >= 85.0) {
                    arr[index] = None;
                }
            }
        }
        if let Some(arr) = payload.hum.as_mut() {
            if let Some(val) = arr[index] {
                if val < 0.0 {
                    arr[index] = None;
                }
            }
        }
        if let Some(arr) = payload.e_co2.as_mut() {
            if let Some(val) = arr[index] {
                if val == -99 {
                    arr[index] = None;
                }
            }
        }
        if let Some(arr) = payload.raw_e_co2.as_mut() {
            if let Some(val) = arr[index] {
                if val == -99 {
                    arr[index] = None;
                }
            }
        }
        if let Some(arr) = payload.tvoc.as_mut() {
            if let Some(val) = arr[index] {
                if val == -99 {
                    arr[index] = None;
                }
            }
        }
        if should_calc_l1 {
            let payload_per_point = TelemetryDUTv2::from_full_tel(&payload, index, pack_length);
            let l1 = dut_state_info
                .l1_calc
                .calc_l1(&payload_per_point, dev)
                .ok()
                .flatten();
            if let Some(l1_vec) = payload.l1.as_mut().filter(|_| should_calc_l1) {
                l1_vec.push(l1);
            }
        }
    }

    return Ok(payload);
}

fn checkSetTelemetryValues(
    payload: &TelemetryPackDUT_v2,
    telemetry: &mut TelemetryDUT_v3,
    dut_l1_calc: &mut dyn DutL1Calculator,
    mut remainingSteps: usize,
    dev: &HwInfoDUT,
) -> usize {
    let offset_temp = dev.temperature_offset;
    let payload_length = payload
        .Temperature
        .as_ref()
        .map(|t| t.len())
        .or_else(|| payload.eCO2.as_ref().map(|e| e.len()))
        .or_else(|| payload.Temperature_1.as_ref().map(|e| e.len()));
    let payload_length = match payload_length {
        Some(x) => x,
        None => {
            crate::LOG.append_log_tag_msg(
                "ERROR",
                "Não foi possível identificar o número de leituras no payload.",
            );
            std::process::exit(2);
        }
    };

    if remainingSteps == 0 {
        remainingSteps = payload_length - 1;
    } else {
        remainingSteps -= 1;
    }
    let index = payload_length - 1 - remainingSteps;

    telemetry.Temp = match &payload.Temperature {
        None => None,
        Some(Temperature) => match Temperature.get(index) {
            None => None,
            Some(Temp) => Temp
                .filter(|t| *t > -99.0 && *t < 85.0)
                .map(|t| t + offset_temp),
        },
    };
    telemetry.Temp1 = match &payload.Temperature_1 {
        None => None,
        Some(Temperature_1) => match Temperature_1.get(index) {
            None => None,
            Some(Temp1) => Temp1.filter(|t| *t > -99.0 && *t < 85.0),
        },
    };
    telemetry.Tmp = match &payload.Tmp {
        None => None,
        Some(Tmp) => match Tmp.get(index) {
            None => None,
            Some(Tmp) => Tmp.filter(|t| *t > -99.0 && *t < 85.0),
        },
    };
    telemetry.Hum = match &payload.Humidity {
        None => None,
        Some(Humidity) => match Humidity.get(index) {
            None => None,
            Some(Hum) => Hum.filter(|h| *h >= 0.0),
        },
    };
    telemetry.eCO2 = match &payload.eCO2 {
        None => None,
        Some(eCO2) => match eCO2.get(index) {
            None => None,
            Some(eCO2) => eCO2.filter(|c| *c != -99),
        },
    };
    telemetry.raw_eCO2 = match &payload.raw_eCO2 {
        None => None,
        Some(raw_eCO2) => match raw_eCO2.get(index) {
            None => None,
            Some(raw_eCO2) => raw_eCO2.filter(|c| *c != -99),
        },
    };
    telemetry.tvoc = match &payload.tvoc {
        None => None,
        Some(tvoc) => match tvoc.get(index) {
            None => None,
            Some(tvoc) => tvoc.filter(|t| *t > -99),
        },
    };
    telemetry.State = payload.State.clone();
    telemetry.Mode = payload.Mode.as_ref().map(|mode| {
        if mode.as_str() == "AUTO" {
            "Auto".to_owned()
        } else {
            mode.to_owned()
        }
    });
    telemetry.GMT = match &telemetry.GMT {
        None => Some(-3),
        Some(v) => Some(*v),
    };
    telemetry.l1 = dut_l1_calc
        .calc_l1_tel_v3(telemetry, payload.samplingTime, dev)
        .ok()
        .flatten();
    return remainingSteps;
}

fn checkSetAutTelemetryValues(payload: &TelemetryPackDUT_v2, telemetry: &mut TelemetryDUT_v3) {
    telemetry.Temp = None;
    telemetry.Temp1 = None;
    telemetry.Hum = None;
    telemetry.State = payload.State.as_ref().map(|v| v.to_owned());
    telemetry.Mode = payload.Mode.as_ref().map(|v| v.to_owned());
    telemetry.GMT = payload.GMT.as_ref().map(|v| v.to_owned());
}
