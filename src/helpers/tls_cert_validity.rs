pub fn check_certificate_validity(path: &str) -> String {
	// use x509_parser::prelude::*;
	let file_contents = match std::fs::read(path) {
			Ok(v) => v,
			Err(err) => {
					return format!("Error reading the file: {}", err);
			}
	};
	let (_, pem) = match x509_parser::pem::parse_x509_pem(&file_contents) {
			Ok(v) => v,
			Err(err) => {
					return format!("Error parsing the pem certificate: {}", err);
			}
	};
	let cert = match pem.parse_x509() {
			Ok(v) => v,
			Err(err) => {
					return format!("Error parsing the x509 certificate: {}", err);
			}
	};
	// pub fn time_to_expiration(&self) -> Option<std::time::Duration>
	let duration = match cert.tbs_certificate.validity.time_to_expiration() {
			Some(v) => v,
			None => {
					return "Could not get certificate validity".to_owned();
			}
	};
	return format!(
			"Certificate valid for: {} days",
			duration.as_seconds_f32() / 60. / 60. / 24.
	);
}

/*

{
  globs.certs_vld.insert(
    "ca_public_cert".to_owned(),
    check_certificate_validity(&globs.configfile.BROKER_TLS_CA_PUBLIC_CERT),
  );
}

*/