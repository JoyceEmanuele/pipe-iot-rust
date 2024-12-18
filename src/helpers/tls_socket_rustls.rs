use rumqttc::tokio_rustls::rustls::{ClientConfig, RootCertStore};
use std::fs::File;
use std::io::BufReader;
use std::sync::Arc;

pub fn create_client_config(ca_path: &str) -> Result<ClientConfig, String> {
    // Utilizado para abrir uma conexão TLS com o broker
    let root_store = load_ca_file_for_broker(ca_path)?;
    let root_store = Arc::new(root_store);
    let client_config = ClientConfig::builder()
        // .with_safe_defaults()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    // let client_config = Arc::new(client_config);
    Ok(client_config)
}

fn load_ca_file_for_broker(ca_path: &str) -> Result<RootCertStore, String> {
    let mut root_store = RootCertStore::empty();
    // let ca_path = &configfile.BROKER_TLS_CA_PUBLIC_CERT;
    // let ca_path = configfile.broker_config.ca_cert.as_ref().expect("Certificado CA não informado");
    let ca_file = File::open(&ca_path);
    let ca_file = ca_file.map_err(|err| format!("CaFileNotFound {}\n{}", ca_path, err))?;
    let ca_file = &mut BufReader::new(ca_file);
    let ca_certs = rustls_pemfile::certs(ca_file);

    for c in ca_certs {
        let c = c.map_err(|e| format!("ERR202 {}", e))?;
        root_store
            .add(c)
            // .add(&Certificate(c.to_vec()))
            .map_err(|e| format!("ERR222 {}", e))?;
    }
    Ok(root_store)
}
