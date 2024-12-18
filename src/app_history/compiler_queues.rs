use std::collections::HashSet;
use std::sync::Arc;

use crate::app_history::{
    dac_hist, dal_hist, dam_hist, dev_export, dma_hist, dmt_hist, dri_hist, dut_hist, energy_hist,
    energy_stats,
};
use crate::lib_http::response::{
    respond_http_json_serializable, respond_http_plain_text, send_response,
};
use crate::lib_http::types::HttpResponse;
use crate::{lib_essential_thread, GlobalVars};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

pub enum CompilationRequest {
    CompDacV2(dac_hist::ReqParameters),
    CompDut(dut_hist::ReqParameters),
    CompDam(dam_hist::ReqParameters),
    CompDma(dma_hist::ReqParameters),
    CompDmt(dmt_hist::ReqParameters),
    CompDal(dal_hist::ReqParameters),
    CompDri(dri_hist::DriHistParams),
    EnergyQuery(energy_hist::EnergyHistParams),
    EnergyStats(energy_stats::EnergyStatParams),
    ExportDevTelemetries(dev_export::ReqParameters),
}

pub enum MsgToCompilers {
    NewRequest(TcpStream, CompilationRequest, String),
    CompilationDone(String),
}
pub async fn task_queue_manager(
    mut receiver: mpsc::Receiver<MsgToCompilers>,
    globs: Arc<GlobalVars>,
) {
    let mut queue: Vec<(TcpStream, CompilationRequest, String)> = Vec::new();
    let mut tasks_running: HashSet<String> = HashSet::new();
    let mut n_req: usize = 0;
    loop {
        match receiver.recv().await.expect("Erro ao receber do mpsc") {
            MsgToCompilers::NewRequest(socket, request, dev_id) => {
                // Adiciona a requisição na fila
                queue.push((socket, request, dev_id));
            }
            MsgToCompilers::CompilationDone(dev_id) => {
                // Nothing to do here, it will be checked on the next lines
                tasks_running.remove(&dev_id);
            }
        };
        // Verifica se tem requisições na fila;
        // Se estiver rodando muitas já, dá continue para esperar alguma terminar;
        // Pega a próxima da fila e põe para rodar. Pode ser que mesmo que tenha vaga já exista uma tarefa para o mesmo dev_id e aí espera liberar uma vaga de interesse.
        if (!queue.is_empty()) && (tasks_running.len() < 5) {
            for i in 0..queue.len() {
                if !tasks_running.contains(&queue[i].2) {
                    n_req += 1;
                    let (mut socket, request, dev_id) = queue.remove(i);
                    let globs = globs.clone();
                    tasks_running.insert(dev_id.clone());
                    std::thread::spawn(move || {
                        let rt = tokio::runtime::Builder::new_current_thread()
                            .enable_all()
                            .build()
                            .expect("Error creating tokio runtime");
                        rt.block_on(async move {
                            crate::LOG.append_log_tag_msg(
                                "info",
                                &format!("Iniciando compilação [{}] {}", n_req, dev_id),
                            );
                            let response = match executar_requisicao(request, &globs).await {
                                Ok(v) => v,
                                Err(err) => respond_http_plain_text(500, &err),
                            };
                            if let Err(err) = {
                                send_response(&mut socket, &response).await // socket_write
                            } {
                                crate::LOG.append_log_tag_msg("ERROR[67]", &err.to_string());
                            }
                            crate::LOG.append_log_tag_msg(
                                "info",
                                &format!("Concluindo compilação [{}] {}", n_req, dev_id),
                            );
                            globs
                                .to_compiler
                                .send(MsgToCompilers::CompilationDone(dev_id))
                                .await
                                .expect("UNEXPECTED-53");
                            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                        })
                    });
                    break;
                }
            }
        }
    }
}

async fn executar_requisicao(
    request: CompilationRequest,
    globs: &Arc<GlobalVars>,
) -> Result<HttpResponse, String> {
    match request {
        CompilationRequest::CompDacV2(body) => {
            dac_hist::process_comp_command_dac_v2(body, globs).await
        }
        CompilationRequest::CompDut(body) => dut_hist::process_comp_command_dut(body, globs).await,
        CompilationRequest::CompDam(body) => dam_hist::process_comp_command_dam(body, globs).await,
        CompilationRequest::CompDma(body) => dma_hist::process_comp_command_dma(body, globs).await,
        CompilationRequest::CompDmt(body) => dmt_hist::process_comp_command_dmt(body, globs).await,
        CompilationRequest::CompDal(body) => dal_hist::process_comp_command_dal(body, globs).await,
        CompilationRequest::ExportDevTelemetries(json_body) => {
            dev_export::export_dev_telemetries(json_body, &globs.configfile.aws_config).await
        }
        CompilationRequest::CompDri(body) => body
            .process_query(globs)
            .await
            .map(|results| respond_http_json_serializable(200, results)),
        CompilationRequest::EnergyQuery(body) => body
            .process_query(globs)
            .await
            .map(|results| respond_http_json_serializable(200, results)),
        CompilationRequest::EnergyStats(body) => body
            .process(globs)
            .await
            .map(|results| respond_http_json_serializable(200, results)),
    }
}
