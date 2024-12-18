use super::padronized::PadronizedEnergyTelemetry;
use crate::telemetry_payloads::dri_telemetry::HwInfoDRI;
use chrono::{Duration, NaiveDateTime, Timelike};
use serde::{
    de::{Error, Unexpected},
    Deserialize, Deserializer, Serialize,
};
use serde_json::Value;
use serde_with::{serde_as, DeserializeAs, SerializeAs};
use std::{borrow::Cow, collections::HashMap};
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TelemetryDME<'a> {
    pub dev_id: Cow<'a, String>,
    pub timestamp: Cow<'a, String>,
    #[serde(rename = "type")]
    pub dev_type: Option<Cow<'a, String>>,
    pub v_a: Option<f64>,
    pub v_b: Option<f64>,
    pub v_c: Option<f64>,
    pub v_ab: Option<f64>,
    pub v_bc: Option<f64>,
    pub v_ca: Option<f64>,
    pub i_a: Option<f64>,
    pub i_b: Option<f64>,
    pub i_c: Option<f64>,
    pub pot_at_a: Option<f64>,
    pub pot_at_b: Option<f64>,
    pub pot_at_c: Option<f64>,
    pub pot_ap_a: Option<f64>,
    pub pot_ap_b: Option<f64>,
    pub pot_ap_c: Option<f64>,
    pub pot_re_a: Option<f64>,
    pub pot_re_b: Option<f64>,
    pub pot_re_c: Option<f64>,
    pub v_tri_ln: Option<f64>,
    pub v_tri_ll: Option<f64>,
    pub pot_at_tri: Option<f64>,
    pub pot_ap_tri: Option<f64>,
    pub pot_re_tri: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<VerifyStringOrf64>")]
    pub en_at_tri: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<VerifyStringOrf64>")]
    pub en_re_tri: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<VerifyStringOrf64>")]
    pub en_ap_tri: Option<f64>,
    pub fp_a: Option<f64>,
    pub fp_b: Option<f64>,
    pub fp_c: Option<f64>,
    pub fp: Option<f64>,
    pub freq: Option<f64>,
    pub demanda: Option<f64>,
    pub demanda_at: Option<f64>,
    pub demanda_ap: Option<f64>,
    pub demanda_med_at: Option<f64>,
    pub erro: Option<f64>,

    pub CMN0: Option<f64>,
    pub CMN1: Option<f64>,
    pub CMN2: Option<f64>,
    pub CMN3: Option<f64>,
    pub CMN4: Option<f64>,
    pub CMN5: Option<f64>,
    pub CMN6: Option<f64>,
    pub CMN7: Option<f64>,
    pub CMN8: Option<f64>,
    pub CMN9: Option<f64>,
    pub CMN10: Option<f64>,
    pub CMN11: Option<f64>,
    pub CMN12: Option<f64>,
    pub CMN13: Option<f64>,
    pub CMN14: Option<f64>,
    pub CMN15: Option<f64>,
    pub CMN16: Option<f64>,
    pub CMN17: Option<f64>,
    pub CMN18: Option<f64>,
    pub CMN19: Option<f64>,
    pub CMN20: Option<f64>,
    pub CMN21: Option<f64>,
    pub CMN22: Option<f64>,
    pub CMN23: Option<f64>,
    pub CMN24: Option<f64>,
    pub CMN25: Option<f64>,
    pub CMN26: Option<f64>,
    pub CMN27: Option<f64>,
    pub CMN28: Option<f64>,
    pub CMN29: Option<f64>,
    pub CMN30: Option<f64>,
    pub CMN31: Option<f64>,
    pub CMN32: Option<f64>,
    pub CMN33: Option<f64>,
    pub CMN34: Option<f64>,
    pub CMN35: Option<f64>,
    pub CMN36: Option<f64>,
    pub CMN37: Option<f64>,
    pub CMN38: Option<f64>,
    pub CMN39: Option<f64>,
    pub CMN40: Option<f64>,
    pub CMN41: Option<f64>,
    pub formulas: Option<HashMap<String, String>>,
}

pub const DME_METERS: [&str; 12] = [
    "CG-ET330",
    "ABB-NEXUS-II",
    "ABB-ETE-30",
    "ABB-ETE-50",
    "CG-EM210",
    "KRON-MULT-K",
    "KRON-IKRON-03",
    "SCHNEIDER-ELETRIC-PM2100",
    "SCHNEIDER-ELECTRIC-PM210",
    "SCHNEIDER-ELECTRIC-PM9C",
    "KRON-MULT-K 05",
    "KRON-MULT-K 120",
];

pub fn convert_dme_payload<'a>(
    mut payload: TelemetryDME<'a>,
    dev: &'a HwInfoDRI,
) -> Result<PadronizedEnergyTelemetry, String> {
    if dev.formulas.is_some() {
        payload.formulas = dev.formulas.clone();
    }
    return payload.try_into();
}

fn f64_from_str<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    match Value::deserialize(deserializer) {
        Ok(Value::Number(result)) => result.as_f64().ok_or_else(|| {
            Error::invalid_type(Unexpected::Other(&result.to_string()), &"Tipo incorreto")
        }),

        Ok(Value::String(result)) => result
            .parse::<f64>()
            .map_err(|e| Error::invalid_value(Unexpected::Str(&result), &"Float em String")),
        Ok(wrong_value) => Err(Error::invalid_type(
            Unexpected::Other(&wrong_value.to_string()),
            &"Tipo não adequado",
        )),
        Err(err) => {
            print!("{err}");
            Err(err)
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
enum VerifyStringOrf64 {
    Temp1(Option<String>),
    Temp2(Option<f64>),
}

impl SerializeAs<f64> for VerifyStringOrf64 {
    fn serialize_as<S>(source: &f64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_f64(*source)
    }
}

impl<'de> DeserializeAs<'de, f64> for VerifyStringOrf64 {
    fn deserialize_as<D>(deserializer: D) -> Result<f64, D::Error>
    where
        D: Deserializer<'de>,
    {
        f64_from_str(deserializer)
    }
}

#[derive(Debug, Default, Serialize, Deserialize, Clone)]
pub struct EnergyDemandTelemetry {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub average_demand: Option<f64>,
    pub min_demand: Option<f64>,
    pub max_demand: Option<f64>,
    pub record_date: NaiveDateTime,
}

impl EnergyDemandTelemetry {
    pub fn set_average_demand(&mut self, value: f64) {
        self.average_demand = Some(value);
    }

    pub fn new(record_date: NaiveDateTime) -> Self {
        Self {
            record_date,
            average_demand: None,
            min_demand: None,
            max_demand: None,
        }
    }

    pub fn set_min_demand(&mut self, value: f64) {
        self.min_demand = Some(value);
    }

    pub fn set_max_demand(&mut self, value: f64) {
        self.max_demand = Some(value);
    }

    pub fn group_telemetries_by_interval(
        telemetries: Vec<PadronizedEnergyTelemetry>,
        hour_interval: bool,
    ) -> HashMap<NaiveDateTime, Vec<EnergyDemandTelemetry>> {
        let mut grouped_telemetries: HashMap<NaiveDateTime, Vec<EnergyDemandTelemetry>> =
            HashMap::new();
        let interval = if hour_interval { 60 } else { 15 };

        for telemetry in telemetries.iter() {
            if telemetry.timestamp.hour() == 0 && telemetry.timestamp.minute() < 15 {
                continue;
            }

            let adjusted_timestamp = telemetry
                .timestamp
                .checked_sub_signed(Duration::minutes(15))
                .unwrap_or(telemetry.timestamp);

            let rounded_minute = ((adjusted_timestamp.minute() / interval) * interval) as u32;
            let final_timestamp =
                adjusted_timestamp
                    .date()
                    .and_hms(adjusted_timestamp.hour(), rounded_minute, 0);

            let energy_demand_telemetry = EnergyDemandTelemetry {
                record_date: final_timestamp,
                average_demand: telemetry.demanda_med_at,
                min_demand: telemetry.demanda_med_at,
                max_demand: telemetry.demanda_med_at,
            };

            grouped_telemetries
                .entry(final_timestamp)
                .or_insert_with(Vec::new)
                .push(energy_demand_telemetry);
        }
        grouped_telemetries
    }

    pub fn calculate_group_telemetries(
        grouped_telemetries: &HashMap<NaiveDateTime, Vec<EnergyDemandTelemetry>>,
    ) -> Vec<EnergyDemandTelemetry> {
        let mut group_demands: Vec<EnergyDemandTelemetry> = Vec::new();

        for (time_interval, telemetry_array) in grouped_telemetries {
            let mut total_values: HashMap<&str, (f64, usize)> = HashMap::new();

            let mut min_demand = f64::MAX;
            let mut max_demand = f64::MIN;

            for telemetry in telemetry_array {
                if let Some(value) = telemetry.average_demand {
                    let (sum, count_val) = total_values.entry("average_demand").or_insert((0.0, 0));
                    *sum += value;
                    *count_val += 1;

                    if value < min_demand {
                        min_demand = value;
                    }
                    if value > max_demand {
                        max_demand = value;
                    }
                }
            }

            let mut grouped_telemetry = EnergyDemandTelemetry::new(*time_interval);

            if let Some((total, field_count)) = total_values.get("average_demand") {
                let avg = total / *field_count as f64;
                let avg_rounded = (avg * 100.0).round() / 100.0;
                grouped_telemetry.set_average_demand(avg_rounded);
            }

            if min_demand != f64::MAX {
                grouped_telemetry.set_min_demand(min_demand);
            }
            if max_demand != f64::MIN {
                grouped_telemetry.set_max_demand(max_demand);
            }

            if grouped_telemetry.average_demand.is_some() {
                group_demands.push(grouped_telemetry);
            }
        }

        group_demands
    }
}
