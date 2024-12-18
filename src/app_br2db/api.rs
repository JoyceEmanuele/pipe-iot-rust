use crate::lib_http::response::{build_http_response, respond_http_plain_text, send_response};
use crate::lib_http::types::{HttpRequest, HttpResponse};
use crate::GlobalVars;
use regex::Regex;
use std::sync::Arc;
use tokio::net::TcpStream;

pub async fn on_http_req(
    req: HttpRequest,
    _is_internal: bool,
    mut socket: TcpStream,
    _globs: Arc<GlobalVars>,
) {
    // pub async fn on_http_req(req: &HttpRequest) -> Result<HttpResponse, String> {
    let response = match &req.path[..] {
        "/health_check" => respond_http_plain_text(200, "Alive"),
        "/status-charts-v1" => build_status_charts_v1(&req)
            .await
            .unwrap_or_else(|err| respond_http_plain_text(400, &err)),
        _ => respond_http_plain_text(404, "Not found"),
    };
    // let response = match response {
    //     Ok(v) => v,
    //     Err(err) => respond_http_plain_text(500, &err),
    // };
    // if let Err(err) = send_response(&mut socket_reader.stream, &response).await {
    if let Err(err) = send_response(&mut socket, &response).await {
        crate::LOG.append_log_tag_msg("ERROR", &format!("ERR80: {}", err));
    }
}

async fn build_status_charts_v1(req: &HttpRequest) -> Result<HttpResponse, String> {
    let body = std::str::from_utf8(&req.content).map_err(|e| e.to_string())?;
    let body: serde_json::Value = serde_json::from_str(body).map_err(|e| e.to_string())?;

    let day = body["day"]
        .as_str()
        .ok_or("Faltou parâmetro 'day'")
        .map_err(|e| e.to_string())?;
    let t_start = body["t_start"].as_str().unwrap_or("00:00:00");
    let t_end = body["t_end"].as_str().unwrap_or("24:00:00");
    let ts_start = &format!("{}T{}", day, t_start)[..];
    let ts_end = &format!("{}T{}", day, t_end)[..];

    let re = Regex::new(r"^\d{4}-\d{2}-\d{2}$").expect("ERRO 24");
    if !re.is_match(day) {
        return Err(format!("Data inválida: {}", day));
    }
    // "2022-06-01T00:25:16"-0300 {...}
    let re = Regex::new(r#"^"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}"-0300 "#).expect("ERRO 33");
    let mut resultado: Vec<u8> = Vec::new();
    let filename = crate::LOG.stats_file_name_for_day(day);
    if let Ok(contents) = std::fs::read_to_string(filename) {
        for line in contents.split('\n') {
            if !re.is_match(line) {
                continue;
            }
            let line_ts = &line[1..20];
            if line_ts < ts_start {
                continue;
            }
            if line_ts > ts_end {
                continue;
            }

            let ts_hora = &line[12..20];
            let json = &line[27..];
            resultado.extend_from_slice(ts_hora.as_bytes());
            resultado.push(b'\t');
            resultado.extend_from_slice(json.as_bytes());
            resultado.push(b'\n');
        }
    }
    return Ok(build_http_response(
        200,
        resultado,
        "text/plain; charset=UTF-8",
    ));
}
