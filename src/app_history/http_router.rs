use super::cache_files::process_clear_cache;
use super::compiler_queues::MsgToCompilers;
use crate::app_history::compiler_queues::CompilationRequest;
use crate::app_history::{dri_hist, energy_hist, energy_stats};
use crate::lib_http::response::{respond_http_plain_text, send_response};
use crate::lib_http::types::{HttpRequest, HttpResponse};
use crate::GlobalVars;
use std::sync::Arc;
use tokio::net::TcpStream;

/* TODO:
 - Comando para acrescentar pacote de até 60000 segundos em um arquivo. (cria o arquivo se não existir)
 - Comando para listar os pacotes relativos a um período solicitado. (retorna uma lista de strings, vazia se o arquivo não existir)
 - Comando para retornar as telemetrias de um pacote, podendo passar parâmetros de filtragem e compactação
   - poderá ter filtro de intervalo, mas ainda não vou implementar
   - pode retornar pacotes de telemetria de minuto inteiro ou de hora inteira
   - pode retornar somente dados de L1 (Lcomando e Lcompressor) no formato "1*120,0*300,*50"
   - pode retornar dados de Temperatura e Pressão já convertidos, suavizados, em baixa resolução, tudo passado como parâmetro, no formato "1*120,0*300,*50"
*/

pub async fn on_http_req(
    req: HttpRequest,
    is_internal: bool,
    mut socket: TcpStream,
    globs: Arc<GlobalVars>,
) {
    // let parsed_url = Url::parse("http://example.com/?a=1&b=2&c=3").unwrap();
    // let hash_query: HashMap<_, _> = parsed_url.query_pairs().into_owned().collect();
    // hash_query.get("a") = "1"
    // parsed_url.path() = "/rust-lang/rust/issues"
    // parsed_url.path_segments().map(|c| c.collect::<Vec<_>>()) = vec!["rust-lang", "rust", "issues"]

    if !is_internal {
        // Requisições externas são uma nova funcionalidade. Só algumas rotas estão permitidas por enquanto.
        match &req.path[..] {
            "/" => {}                       // OK
            "/comp-dut" => {}               // OK
            "/comp-dac-v2" => {}            // OK
            "/comp-dam" => {}               // OK
            "/health_check" => {}           // OK
            "/export-dev-telemetries" => {} // OK
            _ => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("Invalid external request: {} {}", req.method, req.path),
                );
                let response =
                    respond_http_plain_text(404, &(String::new() + "Not found: " + &req.path));
                if let Err(err) = {
                    send_response(&mut socket, &response).await // socket_write
                } {
                    crate::LOG.append_log_tag_msg("ERROR[51]", &err.to_string());
                }
            }
        }
    }

    // Verificar se é um endpoint síncrono
    let response = match sync_routes(&req) {
        Ok(x) => x,
        Err(err) => Some(respond_http_plain_text(500, &err)),
    };
    if let Some(response) = response {
        if let Err(err) = {
            send_response(&mut socket, &response).await // socket_write
        } {
            crate::LOG.append_log_tag_msg("ERROR[66]", &err.to_string());
        }
        return;
    }

    // Os próximos endpoints são tarefas que devem ser enfileiradas
    match async_routes(&req, is_internal, &globs) {
        Ok((request, dev_id)) => {
            globs
                .to_compiler
                .send(MsgToCompilers::NewRequest(socket, request, dev_id))
                .await;
        }
        Err(response) => {
            // let response = respond_http_plain_text(500, &err);
            if let Err(err) = {
                send_response(&mut socket, &response).await // socket_write
            } {
                crate::LOG.append_log_tag_msg("ERROR[81]", &err.to_string());
            }
        }
    };
}

fn sync_routes(req: &HttpRequest) -> Result<Option<HttpResponse>, String> {
    match &req.path[..] {
        "/" => {
            return Ok(Some(respond_http_plain_text(200, "Olá")));
        }
        "/health_check" => {
            return Ok(Some(respond_http_plain_text(200, "Alive")));
        }
        "/clear-cache" => {
            let body_str = String::from_utf8_lossy(&req.content);
            let json_body =
                serde_json::from_str(&body_str).map_err(|e| format!("ERROR37: {}", e))?;
            return process_clear_cache(json_body).map(|r| Some(r));
        }
        _ => {
            return Ok(None);
        }
    };
}

fn async_routes(
    req: &HttpRequest,
    is_internal: bool,
    globs: &Arc<GlobalVars>,
) -> Result<(CompilationRequest, String), HttpResponse> {
    match &req.path[..] {
        "/comp-dri" => {
            let body_str = String::from_utf8_lossy(&req.content);
            let body = serde_json::from_str::<dri_hist::DriHistParams>(&body_str)
                .map_err(|e| respond_http_plain_text(400, &e.to_string()))?;
            let dev_id = body.dev_id.to_owned();
            return Ok((CompilationRequest::CompDri(body), dev_id));
        }
        "/comp-dut" => {
            let body_str = String::from_utf8_lossy(&req.content);
            let mut json_body: serde_json::Value = serde_json::from_str(&body_str)
                .map_err(|e| respond_http_plain_text(400, &format!("ERROR44: {}", e)))?;
            if !is_internal {
                if let (Some(token), Some(allowed_token)) = (
                    json_body["token"].as_str(),
                    &globs.configfile.EXTERNAL_REQUESTS_TOKEN,
                ) {
                    if token != allowed_token {
                        return Err(respond_http_plain_text(403, "Token inválido"));
                    }
                } else {
                    return Err(respond_http_plain_text(403, "Token não fornecido"));
                }
                json_body["avoid_cache"] = true.into();
            }
            let rpars = crate::app_history::dut_hist::parse_parameters(&json_body)?;
            let dev_id = rpars.dev_id.to_owned();
            return Ok((CompilationRequest::CompDut(rpars), dev_id));
        }
        "/comp-dma" => {
            let body_str = String::from_utf8_lossy(&req.content);
            let mut json_body: serde_json::Value = serde_json::from_str(&body_str)
                .map_err(|e| respond_http_plain_text(400, &format!("ERROR44: {}", e)))?;
            if !is_internal {
                if let (Some(token), Some(allowed_token)) = (
                    json_body["token"].as_str(),
                    &globs.configfile.EXTERNAL_REQUESTS_TOKEN,
                ) {
                    if token != allowed_token {
                        return Err(respond_http_plain_text(403, "Token inválido"));
                    }
                } else {
                    return Err(respond_http_plain_text(403, "Token não fornecido"));
                }
                json_body["avoid_cache"] = true.into();
            }
            let rpars = crate::app_history::dma_hist::parse_parameters(&json_body)?;
            let dev_id = rpars.dev_id.to_owned();
            return Ok((CompilationRequest::CompDma(rpars), dev_id));
        }
        "/comp-dmt" => {
            let body_str = String::from_utf8_lossy(&req.content);
            let mut json_body: serde_json::Value = serde_json::from_str(&body_str)
                .map_err(|e| respond_http_plain_text(400, &format!("ERROR44: {}", e)))?;
            if !is_internal {
                if let (Some(token), Some(allowed_token)) = (
                    json_body["token"].as_str(),
                    &globs.configfile.EXTERNAL_REQUESTS_TOKEN,
                ) {
                    if token != allowed_token {
                        return Err(respond_http_plain_text(403, "Token inválido"));
                    }
                } else {
                    return Err(respond_http_plain_text(403, "Token não fornecido"));
                }
                json_body["avoid_cache"] = true.into();
            }
            let rpars = crate::app_history::dmt_hist::parse_parameters(&json_body)?;
            let dev_id = rpars.dev_id.to_owned();
            return Ok((CompilationRequest::CompDmt(rpars), dev_id));
        }
        "/comp-dal" => {
            let body_str = String::from_utf8_lossy(&req.content);
            let mut json_body: serde_json::Value = serde_json::from_str(&body_str)
                .map_err(|e| respond_http_plain_text(400, &format!("ERROR44: {}", e)))?;
            if !is_internal {
                if let (Some(token), Some(allowed_token)) = (
                    json_body["token"].as_str(),
                    &globs.configfile.EXTERNAL_REQUESTS_TOKEN,
                ) {
                    if token != allowed_token {
                        return Err(respond_http_plain_text(403, "Token inválido"));
                    }
                } else {
                    return Err(respond_http_plain_text(403, "Token não fornecido"));
                }
                json_body["avoid_cache"] = true.into();
            }
            let rpars = crate::app_history::dal_hist::parse_parameters(&json_body)?;
            let dev_id = rpars.dev_id.to_owned();
            return Ok((CompilationRequest::CompDal(rpars), dev_id));
        }
        "/comp-dac-v2" => {
            let body_str = String::from_utf8_lossy(&req.content);
            let mut json_body: serde_json::Value = serde_json::from_str(&body_str)
                .map_err(|e| respond_http_plain_text(400, &format!("ERROR37: {}", e)))?;
            if !is_internal {
                if let (Some(token), Some(allowed_token)) = (
                    json_body["token"].as_str(),
                    &globs.configfile.EXTERNAL_REQUESTS_TOKEN,
                ) {
                    if token != allowed_token {
                        return Err(respond_http_plain_text(403, "Token inválido"));
                    }
                } else {
                    return Err(respond_http_plain_text(403, "Token não fornecido"));
                }
                json_body["avoid_cache"] = true.into();
            }
            let rpars = crate::app_history::dac_hist::parse_parameters(&json_body)?;
            let dev_id = rpars.dev_id.to_owned();
            return Ok((CompilationRequest::CompDacV2(rpars), dev_id));
        }
        "/comp-dam" => {
            let body_str = String::from_utf8_lossy(&req.content);
            let mut json_body: serde_json::Value = serde_json::from_str(&body_str)
                .map_err(|e| respond_http_plain_text(400, &format!("ERROR42: {}", e)))?;
            if !is_internal {
                if let (Some(token), Some(allowed_token)) = (
                    json_body["token"].as_str(),
                    &globs.configfile.EXTERNAL_REQUESTS_TOKEN,
                ) {
                    if token != allowed_token {
                        return Err(respond_http_plain_text(403, "Token inválido"));
                    }
                } else {
                    return Err(respond_http_plain_text(403, "Token não fornecido"));
                }
                json_body["avoid_cache"] = true.into();
            }
            let rpars = crate::app_history::dam_hist::parse_parameters(&json_body)?;
            let dev_id = rpars.dev_id.to_owned();
            return Ok((CompilationRequest::CompDam(rpars), dev_id));
        }
        "/energy-query" => {
            let body_str = String::from_utf8_lossy(&req.content);
            let body = serde_json::from_str::<energy_hist::EnergyHistParams>(&body_str)
                .map_err(|e| respond_http_plain_text(400, &e.to_string()))?;
            let dev_id = body.energy_device_id.to_owned();
            return Ok((CompilationRequest::EnergyQuery(body), dev_id));
        }
        "/energy-stats" => {
            let body_str = String::from_utf8_lossy(&req.content);
            let body = serde_json::from_str::<energy_stats::EnergyStatParams>(&body_str)
                .map_err(|e| respond_http_plain_text(400, &e.to_string()))?;
            let dev_id = body.energy_device_id.to_owned();
            return Ok((CompilationRequest::EnergyStats(body), dev_id));
        }
        "/export-dev-telemetries" => {
            // curl 'http://api.dielenergia.com:29547/export-dev-telemetries' --data-raw '{"token":"...","dev_id":"DRI008220235","table_name":"DRI00822XXXX_RAW","day_YMD":"2023-03-21"}' > 2023-03-21-DRI008220235.txt
            let body_str = std::str::from_utf8(&req.content)
                .map_err(|e| respond_http_plain_text(400, &format!("ERROR154: {}", e)))?;
            let json_body: serde_json::Value = serde_json::from_str(&body_str)
                .map_err(|e| respond_http_plain_text(400, &format!("ERROR155: {}", e)))?;
            if !is_internal {
                if let (Some(token), Some(allowed_token)) = (
                    json_body["token"].as_str(),
                    &globs.configfile.EXTERNAL_REQUESTS_TOKEN,
                ) {
                    if token != allowed_token {
                        return Err(respond_http_plain_text(403, "Token inválido"));
                    }
                } else {
                    return Err(respond_http_plain_text(403, "Token não fornecido"));
                }
            }
            let rpars = crate::app_history::dev_export::parse_parameters(&json_body)?;
            let dev_id = rpars.dev_id.to_owned();
            return Ok((CompilationRequest::ExportDevTelemetries(rpars), dev_id));
        }
        _ => {
            crate::LOG.append_log_tag_msg(
                "ERROR",
                &format!("Invalid request: {} {}", req.method, req.path),
            );
            return Err(respond_http_plain_text(
                404,
                &(String::new() + "Not found: " + &req.path),
            ));
        }
    }
}
