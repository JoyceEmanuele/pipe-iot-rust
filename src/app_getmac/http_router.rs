use crate::lib_http::response::{build_http_response, respond_http_plain_text, send_response};
use crate::lib_http::types::{HttpRequest, HttpResponse};
use crate::GlobalVars;
use std::sync::Arc;
use tokio::net::TcpStream;

pub async fn on_http_req(
    req: HttpRequest,
    _is_internal: bool,
    mut socket: TcpStream,
    globs: Arc<GlobalVars>,
) {
    let response = match &req.path[..] {
        "/service-getmac/health_check" => respond_http_plain_text(200, "Alive"),
        "/service-getmac/get_devs_macs" => get_devs_macs(&req, &globs)
            .await
            .unwrap_or_else(|err| respond_http_plain_text(400, &err)),
        "/service-getmac/get_dev_mac" => get_dev_mac(&req, &globs)
            .await
            .unwrap_or_else(|err| respond_http_plain_text(400, &err)),
        _ => {
            crate::LOG.append_log_tag_msg(
                "ERROR",
                &format!("Invalid request: {} {}", req.method, req.path),
            );
            respond_http_plain_text(404, &format!("Not found: {}", &req.path))
        }
    };
    if let Err(err) = {
        send_response(&mut socket, &response).await // socket_write
    } {
        crate::LOG.append_log_tag_msg("ERROR[33]", &err.to_string());
    }
}

async fn get_devs_macs(req: &HttpRequest, globs: &Arc<GlobalVars>) -> Result<HttpResponse, String> {
    let body = std::str::from_utf8(&req.content).map_err(|e| e.to_string())?;
    let resposta = super::fetch_mac::processar(globs, body).await;
    match resposta {
        Ok(csv) => Ok(build_http_response(
            200,
            csv.into_bytes(),
            "text/csv; charset=UTF-8",
        )),
        Err(err) => Ok(build_http_response(
            400,
            err.into_bytes(),
            "text/plain; charset=UTF-8",
        )),
    }
}

async fn get_dev_mac(req: &HttpRequest, globs: &Arc<GlobalVars>) -> Result<HttpResponse, String> {
    let body = std::str::from_utf8(&req.content)
        .map_err(|e| e.to_string())?
        .trim();
    let resposta = super::fetch_mac::verificar_dev(body, &globs).await;
    match resposta {
        Ok(mac) => Ok(build_http_response(
            200,
            mac.into_bytes(),
            "text/csv; charset=UTF-8",
        )),
        Err(err) => Ok(build_http_response(
            400,
            err.into_bytes(),
            "text/plain; charset=UTF-8",
        )),
    }
}
