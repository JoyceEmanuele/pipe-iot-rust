use super::buffer::SocketReader;
use super::request::read_socket_http_request;
use super::types::HttpRequest;
use crate::GlobalVars;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;

pub async fn run_service_result<F, Fut>(
    bind_addr: String,
    globs: Arc<GlobalVars>,
    on_http_req: &'static F,
) -> Result<(), String>
where
    F: Fn(HttpRequest, bool, TcpStream, Arc<GlobalVars>) -> Fut,
    Fut: Future<Output = ()>,
{
    // let bind_addr = "127.0.0.1:46878"; // configfile::LISTEN_SOCKET_HIST
    let listener = tokio::net::TcpListener::bind(&bind_addr)
        .await
        .map_err(|err| format!("Error binding to TCP port: {}", err))?;
    crate::LOG.append_log_tag_msg("INFO", &format!("Awaiting HTTP clients on {}", bind_addr));

    loop {
        let (socket, _) = match listener.accept().await {
            Ok(v) => v,
            Err(err) => {
                crate::LOG.append_log_tag_msg(
                    "ERROR",
                    &format!("Error getting incoming TCP stream: {}", err),
                );
                continue;
            }
        };
        // println!("Cliente HTTP conectado {}", socket.peer_addr().expect("Error getting peer address"));
        handle_http_request(socket, &globs, on_http_req).await;
        // tokio::spawn(handle_http_request(socket));
    }
}

async fn tcp_connection_establisher<F, Fut>(
    host_uri: &str,
    globs: Arc<GlobalVars>,
    on_http_req: &'static F,
) where
    F: Fn(HttpRequest, bool, TcpStream, Arc<GlobalVars>) -> Fut,
    Fut: Future<Output = ()>,
{
    // "127.0.0.1:8080"
    loop {
        let socket = loop {
            crate::LOG.append_log_tag_msg("INFO", "Connecting to API-Server");
            match TcpStream::connect(host_uri).await {
                Ok(socket) => {
                    break socket;
                }
                Err(err) => {
                    crate::LOG.append_log_tag_msg(
                        "ERROR",
                        &format!("Error connecting to API-Server: {}", err),
                    );
                    tokio::time::sleep(Duration::from_secs(120)).await;
                    continue;
                }
            };
        };
        crate::LOG.append_log_tag_msg("DEBUG", "Connected to API-Server");
        handle_http_request(socket, &globs, on_http_req).await;
    }
}

async fn handle_http_request<F, Fut>(
    socket: tokio::net::TcpStream,
    globs: &Arc<GlobalVars>,
    on_http_req: &'static F,
) where
    F: Fn(HttpRequest, bool, TcpStream, Arc<GlobalVars>) -> Fut,
    Fut: Future<Output = ()>,
{
    // TODO: start a new thread to handle the request, but wait for it to finish before accepting a new request. This is to handle panics.
    let origin: String = socket
        .peer_addr()
        .expect("Error getting peer address")
        .to_string();
    // let (socket_read, socket_write) = tokio::io::split(socket);
    let mut socket_reader = SocketReader::new(socket, 1000);
    let req = match read_socket_http_request(&mut socket_reader, Some(2_000_000)).await {
        Ok(v) => v,
        Err(err) => {
            // send_response(&mut socket_reader.stream, &respond_http_plain_text(500, &err)).await;
            crate::LOG.append_log_tag_msg("ERROR", &format!("Connection ended: {}", err));
            return;
        }
    };
    crate::LOG.append_log_tag_msg(
        "INFO",
        &format!(
            "DBG request {} {} {} {}",
            req.method,
            req.path,
            origin,
            String::from_utf8_lossy(&req.content)
        ),
    );
    let socket = socket_reader.get_socket();
    let is_internal = origin.starts_with("127.0.0.1:");
    on_http_req(req, is_internal, socket, globs.clone()).await;
}
