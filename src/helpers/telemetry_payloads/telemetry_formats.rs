use chrono::{Duration, NaiveDateTime};
use serde::{
    de::{Error, Unexpected},
    Deserialize, Deserializer, Serialize,
};
use serde_json::Value;
use serde_with::{serde_as, DeserializeAs, SerializeAs};

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryPackDAC_v2 {
    pub timestamp: String,
    pub samplingTime: i64,
    #[serde_as(as = "Vec<Option<BoolWrap>>")]
    pub L1: Vec<Option<bool>>,
    pub T0: Vec<Option<f64>>,
    pub T1: Vec<Option<f64>>,
    pub T2: Vec<Option<f64>>,
    pub P0: Vec<Option<i16>>,
    pub P1: Vec<Option<i16>>,
    pub State: Option<String>,
    pub Mode: Option<String>,
    pub GMT: Option<i64>,
    pub saved_data: Option<bool>,
}

pub struct TelemetryDACv2<'a> {
    pub timestamp: NaiveDateTime,
    pub l1: Option<bool>,
    pub t0: Option<f64>,
    pub t1: Option<f64>,
    pub t2: Option<f64>,
    pub p0: Option<i16>,
    pub p1: Option<i16>,
    pub state: Option<&'a str>,
    pub mode: Option<&'a str>,
    pub GMT: Option<i64>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryPackDACv2Full<'a> {
    pub dev_id: &'a str,
    pub bt_id: &'a str,
    pub timestamp: &'a str,
    #[serde_as(as = "Vec<Option<BoolWrap>>")]
    #[serde(rename = "L1")]
    pub l1: Vec<Option<bool>>,
    #[serde(rename = "T0")]
    pub t0: Vec<Option<f64>>,
    #[serde(rename = "T1")]
    pub t1: Vec<Option<f64>>,
    #[serde(rename = "T2")]
    pub t2: Vec<Option<f64>>,
    #[serde(rename = "P0")]
    pub p0: Vec<Option<i16>>,
    #[serde(rename = "P1")]
    pub p1: Vec<Option<i16>>,
    #[serde(rename = "State")]
    pub state: Option<&'a str>,
    #[serde(rename = "Mode")]
    pub mode: Option<&'a str>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryPackDACFullv3<'a> {
    pub dev_id: &'a str,
    pub bt_id: &'a str,
    pub timestamp: &'a str,
    pub Lcmp: Vec<Option<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Lcut: Option<Vec<Option<u8>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Levp: Option<Vec<Option<u8>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Tamb: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Tsuc: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Tliq: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Psuc: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Pliq: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Tsc: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Tsh: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub State: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Mode: Option<&'a str>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryPackDAC_v3 {
    pub Lcmp: Vec<Option<u8>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Lcut: Option<Vec<Option<u8>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Levp: Option<Vec<Option<u8>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Tamb: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Tsuc: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Tliq: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Psuc: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Pliq: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Tsc: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Tsh: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub State: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Mode: Option<String>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryPackDAC_v1 {
    #[serde_as(as = "Option<BoolWrap>")]
    pub L1: Option<bool>,
    pub T0: Option<f64>,
    pub T1: Option<f64>,
    pub T2: Option<f64>,
    pub P0: Option<i16>,
    pub P1: Option<i16>,
    pub State: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Default, Clone)]
pub struct TelemetryDAC_v3 {
    pub Lcmp: Option<bool>,
    pub Lcut: Option<bool>,
    pub Levp: Option<bool>,
    pub Tamb: Option<f64>,
    pub Tsuc: Option<f64>,
    pub Tliq: Option<f64>,
    pub Psuc: Option<f64>,
    pub Pliq: Option<f64>,
    pub State: Option<String>,
    pub Mode: Option<String>,
    pub GMT: Option<i64>,
    pub saved_data: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TelemetryDAC_v3_calcs {
    pub Tsh: Option<f64>,
    pub Tsc: Option<f64>,
}

const fn always5() -> i64 {
    5
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryPackDutV2Full<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub package_id: Option<i32>,
    pub dev_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bt_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "MAC")]
    pub mac: Option<&'a str>,
    pub timestamp: NaiveDateTime,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "Temperature")]
    #[serde_as(as = "Option<Vec<Option<VerifyStringOrf64>>>")]
    pub temp: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "Temperature_1")]
    pub temp1: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "Tmp")]
    #[serde_as(as = "Option<Vec<Option<VerifyStringOrf64>>>")]
    pub tmp: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "Humidity")]
    pub hum: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "eCO2")]
    pub e_co2: Option<Vec<Option<i16>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "raw_eCO2")]
    pub raw_e_co2: Option<Vec<Option<i16>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "TVOC")]
    pub tvoc: Option<Vec<Option<i16>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "State")]
    pub state: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "Mode")]
    pub mode: Option<&'a str>,
    #[serde(rename = "samplingTime")]
    #[serde(default = "always5")]
    pub sampling_time: i64,
    #[serde(default)]
    #[serde(rename = "L1")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub l1: Option<Vec<Option<bool>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "operation_mode")]
    pub operation_mode: Option<i8>,
    pub GMT: Option<i64>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryPackDUT_v2 {
    pub timestamp: NaiveDateTime,
    #[serde(default = "always5")]
    pub samplingTime: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<Vec<Option<VerifyStringOrf64>>>")]
    pub Temperature: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Temperature_1: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde_as(as = "Option<Vec<Option<VerifyStringOrf64>>>")]
    pub Tmp: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Humidity: Option<Vec<Option<f64>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eCO2: Option<Vec<Option<i16>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_eCO2: Option<Vec<Option<i16>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "TVOC")]
    pub tvoc: Option<Vec<Option<i16>>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub State: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub Mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub GMT: Option<i64>,
}

#[derive(Debug, Serialize)]
pub struct TelemetryDUTv2<'a> {
    pub timestamp: NaiveDateTime,
    pub sampling_time: i64,
    pub temp: Option<f64>,
    pub temp_1: Option<f64>,
    pub hum: Option<f64>,
    pub e_co2: Option<i16>,
    pub tvoc: Option<i16>,
    pub state: Option<&'a str>,
    pub mode: Option<&'a str>,
    pub gmt: Option<i64>,
}

impl<'a> TelemetryDUTv2<'a> {
    pub fn from_tel(t: &'a TelemetryPackDUT_v2, idx: usize, pack_len: usize) -> Self {
        Self {
            timestamp: t.timestamp
                - Duration::seconds(t.samplingTime * i64::try_from(pack_len - idx - 1).unwrap()),
            sampling_time: t.samplingTime,
            temp: t
                .Temperature
                .as_ref()
                .and_then(|t| t.get(idx).copied())
                .flatten(),
            temp_1: t
                .Temperature_1
                .as_ref()
                .and_then(|t| t.get(idx).copied())
                .flatten(),
            hum: t
                .Humidity
                .as_ref()
                .and_then(|t| t.get(idx).copied())
                .flatten(),
            e_co2: t.eCO2.as_ref().and_then(|t| t.get(idx).copied()).flatten(),
            tvoc: t.tvoc.as_ref().and_then(|t| t.get(idx).copied()).flatten(),
            state: t.State.as_deref(),
            mode: t.Mode.as_deref(),
            gmt: t.GMT.clone(),
        }
    }

    pub fn from_full_tel(t: &'a TelemetryPackDutV2Full, idx: usize, pack_len: usize) -> Self {
        Self {
            timestamp: t.timestamp
                - Duration::seconds(t.sampling_time * i64::try_from(pack_len - idx - 1).unwrap()),
            sampling_time: t.sampling_time,
            temp: t.temp.as_ref().and_then(|t| t.get(idx).copied()).flatten(),
            temp_1: t.temp1.as_ref().and_then(|t| t.get(idx).copied()).flatten(),
            hum: t.hum.as_ref().and_then(|t| t.get(idx).copied()).flatten(),
            e_co2: t.e_co2.as_ref().and_then(|t| t.get(idx).copied()).flatten(),
            tvoc: t.tvoc.as_ref().and_then(|t| t.get(idx).copied()).flatten(),
            state: t.state,
            mode: t.mode,
            gmt: t.GMT.clone(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TelemetryDUT_v3 {
    pub timestamp: NaiveDateTime,
    pub Temp: Option<f64>,
    pub Temp1: Option<f64>,
    pub Tmp: Option<f64>,
    pub Hum: Option<f64>,
    pub eCO2: Option<i16>,
    pub raw_eCO2: Option<i16>,
    pub tvoc: Option<i16>,
    pub State: Option<String>,
    pub Mode: Option<String>,
    pub l1: Option<bool>,
    pub GMT: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryRawDAM_v1 {
    pub timestamp: String,
    pub State: String,
    pub Mode: String,
    pub Temperature: Option<String>,
    pub Temperature_1: Option<String>,
    pub GMT: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TelemetryDMA {
    pub timestamp: String,
    pub pulses: Option<i32>,
    pub mode: Option<String>,
    pub operation_mode: Option<i16>,
    pub dev_id: String,
    pub samplingTime: Option<i16>,
    pub gmt: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TelemetryPackDMA {
    pub timestamp: String,
    pub dev_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pulses: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_mode: Option<i16>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub samplingTime: Option<i16>,
    pub GMT: Option<i64>,
}
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TelemetryPackDMT {
    pub timestamp: String,
    pub dev_id: String,
    pub samplingTime: i64,
    #[serde_as(as = "Vec<Option<BoolWrap>>")]
    pub Feedback: Vec<Option<bool>>,
    pub GMT: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TelemetryDMT {
    pub timestamp: String,
    pub F1: Option<bool>,
    pub F2: Option<bool>,
    pub F3: Option<bool>,
    pub F4: Option<bool>,
    pub dev_id: String,
    pub GMT: Option<i64>,
}

#[serde_as]
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TelemetryPackDAL {
    pub timestamp: String,
    pub dev_id: String,
    pub State: String,
    pub Mode: Vec<String>,
    #[serde_as(as = "Vec<Option<BoolWrap>>")]
    pub Feedback: Vec<Option<bool>>,
    #[serde_as(as = "Vec<Option<BoolWrap>>")]
    pub Relays: Vec<Option<bool>>,
    pub GMT: Option<i64>,
}

#[derive(Debug, Serialize, Deserialize, Default)]
pub struct TelemetryDAL {
    pub timestamp: String,
    pub dev_id: String,
    pub State: String,
    pub Mode: Vec<String>,
    pub Feedback: Vec<Option<bool>>,
    pub Relays: Vec<Option<bool>>,
    pub GMT: Option<i64>,
}

fn bool_from_int<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    match u8::deserialize(deserializer)? {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(Error::invalid_value(
            Unexpected::Unsigned(other as u64),
            &"zero or one",
        )),
    }
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
struct BoolWrap(bool);

impl SerializeAs<bool> for BoolWrap {
    fn serialize_as<S>(source: &bool, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bool(*source)
    }
}

impl<'de> DeserializeAs<'de, bool> for BoolWrap {
    fn deserialize_as<D>(deserializer: D) -> Result<bool, D::Error>
    where
        D: Deserializer<'de>,
    {
        bool_from_int(deserializer)
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
