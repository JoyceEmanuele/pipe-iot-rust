use crate::{
    global_vars::GlobalVars,
    lib_http::{
        response::respond_http_json_bytes,
        types::{HttpRequest, HttpResponse},
    },
};
use serde::Deserialize;
use serde_json::json;
use std::sync::atomic::Ordering;
use std::sync::Arc;

/*
  ['/diel-internal/realtime/getDevicesLastTS']: (reqParams: {
    devIds?: string[]
  }) => {
    deviceLastTs: {
      [devId: string]: number // Timestamp do servidor da última vez que chegou mensagem do dispostivo
    }
  },

*/

#[derive(Deserialize)]
pub struct ParamsGetDevicesLastTS {
    pub devIds: Option<Vec<String>>,
}

pub async fn get_devices_last_ts(
    req: &HttpRequest,
    globs: &Arc<GlobalVars>,
) -> Result<HttpResponse, String> {
    let req_params: ParamsGetDevicesLastTS =
        serde_json::from_slice(&req.content).map_err(|e| e.to_string())?;

    let all_devs = globs.last_timestamp.read().await;
    let mut resp_devs = json!({});

    match req_params.devIds {
        None => {
            for (dev_id, dev_info) in all_devs.iter() {
                resp_devs[dev_id] = dev_info.load(Ordering::Relaxed).into();
            }
        }
        Some(dev_ids) => {
            for dev_id in &dev_ids {
                if let Some(dev_info) = all_devs.get(dev_id) {
                    resp_devs[dev_id] = dev_info.load(Ordering::Relaxed).into();
                };
            }
        }
    };

    let response = json!({
      "deviceLastTs": resp_devs,
    });

    let response = serde_json::to_vec(&response).map_err(|err| format!("[58] {err}"))?;
    Ok(respond_http_json_bytes(200, response))
}
