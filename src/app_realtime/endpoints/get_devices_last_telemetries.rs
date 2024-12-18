use crate::{
    global_vars::GlobalVars,
    lib_http::{
        response::respond_http_json_bytes,
        types::{HttpRequest, HttpResponse},
    },
};
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;

/*
  ['/diel-internal/realtime/getDevicesLastTelemetries']: (reqParams: {
    devIds?: string[]
  }) => {
    lastMessages: {
      [devId: string]: {
        ts: number // Timestamp do servidor da última vez que chegou mensagem do dispostivo
        topic?: TopicType // Tópico 'data/...' que foi usado, e não o tipo do dispositivo. O DMA por exemplo usa tópico de DUT.
        telemetry?: any // último JSON que chegou em tópico 'data/...'
      }
    }
  },

*/

#[derive(Deserialize)]
pub struct ParamsGetDevicesLastTelemetries {
    pub devIds: Option<Vec<String>>,
}

pub async fn get_devices_last_telemetries(
    req: &HttpRequest,
    globs: &Arc<GlobalVars>,
) -> Result<HttpResponse, String> {
    let req_params: ParamsGetDevicesLastTelemetries =
        serde_json::from_slice(&req.content).map_err(|e| e.to_string())?;

    let all_devs = globs.last_telemetry.read().await;
    let mut resp_devs = json!({});

    match req_params.devIds {
        None => {
            // let num_devs = all_devs.len();
            // let mut response = String::with_capacity(num_devs * 1000);
            for (dev_id, dev_info) in all_devs.iter() {
                let dev_info = dev_info.read().await;
                // response.push_str(&serde_json::to_string(&*dev_info).map_err(|err| format!("[48] {err}"))?);
                resp_devs[dev_id] =
                    serde_json::to_value(&*dev_info).map_err(|err| format!("[50] {err}"))?;
            }
        }
        Some(dev_ids) => {
            for dev_id in &dev_ids {
                if let Some(dev_info) = all_devs.get(dev_id) {
                    let dev_info = dev_info.read().await;
                    resp_devs[dev_id] =
                        serde_json::to_value(&*dev_info).map_err(|err| format!("[58] {err}"))?;
                };
            }
        }
    };

    let response = json!({
      "lastMessages": resp_devs,
    });

    let response = serde_json::to_vec(&response).map_err(|err| format!("[68] {err}"))?;
    Ok(respond_http_json_bytes(200, response))
}
