use crate::lib_dynamodb::client::AWSConfig;
use crate::lib_http::response::respond_http_plain_text;
use crate::lib_http::types::HttpResponse;
use serde::{Deserialize, Serialize};

pub async fn export_dev_telemetries(
    rpars: ReqParameters,
    aws_config: &AWSConfig,
) -> Result<HttpResponse, String> {
    let querier = crate::lib_dynamodb::query::QuerierDevIdTimestamp::new_diel_dev(
        rpars.table_name.to_owned(),
        rpars.dev_id.to_owned(),
        aws_config,
    );

    let mut output = String::with_capacity(1_000_000);
    let result = querier
        .run(&rpars.ts_ini, &rpars.ts_end, &mut |items: Vec<
            serde_json::Value,
        >| {
            for item in items {
                output.push_str(&item.to_string());
                output.push_str("\n");
            }
            return Ok(());
        })
        .await;

    if let Err(err) = result {
        return Ok(respond_http_plain_text(400, &format!("ERROR[24] {}", err)));
    }

    return Ok(respond_http_plain_text(200, &output.to_string()));
}

pub fn parse_parameters(parsed: &serde_json::Value) -> Result<ReqParameters, HttpResponse> {
    let dev_id = match parsed["dev_id"].as_str() {
        Some(v) => v,
        None => {
            return Err(respond_http_plain_text(400, "Missing dev_id"));
        }
    };
    let table_name = match parsed["table_name"].as_str() {
        Some(v) => v,
        None => {
            return Err(respond_http_plain_text(400, "Missing table_name"));
        }
    };
    let day_ymd = match parsed["day_YMD"].as_str() {
        Some(v) => v,
        None => {
            return Err(respond_http_plain_text(400, "Missing day_YMD"));
        }
    };

    let ts_ini = format!("{}T00:00:00", day_ymd);
    let ts_end = format!("{}T24:00:00", day_ymd);

    return Ok(ReqParameters {
        dev_id: dev_id.to_owned(),
        table_name: table_name.to_owned(),
        ts_ini,
        ts_end,
    });
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ReqParameters {
    pub dev_id: String,
    pub table_name: String,
    pub ts_ini: String,
    pub ts_end: String,
}
