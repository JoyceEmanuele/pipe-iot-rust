use tokio::io::AsyncReadExt;

pub fn parse_item_head(
    buffer: &[u8],
    buf_len: usize,
    prefix: &[u8],
) -> Result<Option<(usize, usize, Vec<usize>)>, String> {
    // \n[2+3+5]B:L1:false
    const MAX_HEADER_SIZE: usize = 20;
    let mut cursor = 0;
    let mut parts = vec![0];

    while cursor < buf_len {
        if cursor >= buf_len {
            return Ok(None);
        }
        if cursor >= MAX_HEADER_SIZE {
            return Err("Invalid length".to_owned());
        }
        if cursor < prefix.len() {
            if buffer[cursor] != prefix[cursor] {
                return Err(format!(
                    "Invalid data at prefix[{}] => {}",
                    cursor,
                    String::from_utf8_lossy(buffer)
                ));
            }
            cursor += 1;
            continue;
        }
        if cursor == prefix.len() {
            if buffer[cursor] != b'[' {
                return Err(format!(
                    "Invalid data: [{}] != '[' => {}",
                    cursor, buffer[cursor]
                ));
            }
            cursor += 1;
            continue;
        }
        if buffer[cursor] == b']' {
            break;
        }
        if buffer[cursor] == b'+' {
            parts.push(0);
            cursor += 1;
            continue;
        }
        if buffer[cursor] >= b'0' && buffer[cursor] <= b'9' {
            let p = parts.len() - 1;
            parts[p] = parts[p] * 10 + (buffer[cursor] - b'0') as usize;
            cursor += 1;
            continue;
        }
        return Err(format!(
            "Invalid data at [{}] => {}",
            cursor,
            String::from_utf8_lossy(buffer)
        ));
    }

    let mut p_len = 0;
    for part_len in &parts {
        p_len += part_len;
    }
    let h_end = cursor;
    let h_len = h_end + 1;
    let p_end = h_len + p_len;
    return Ok(Some((h_len, p_end, parts)));
}

pub async fn read_socket_package(
    socket: &mut tokio::net::TcpStream,
) -> Result<(Vec<u8>, Vec<usize>), String> {
    let mut buf1 = [0u8; 30];
    let bytes_read = match socket.read(&mut buf1).await {
        Ok(bytes_read) => {
            if bytes_read == 0 {
                // println!("P170 - Não houve resposta");
                return Err(format!("Nenhum byte lido"));
            }
            bytes_read
        }
        Err(err) => {
            // println!("P176 - Não foi possível ler a resposta: {}", err);
            return Err(format!("{}", err));
        }
    };
    let (h_len, p_end, parts) = {
        match parse_item_head(&buf1[..bytes_read], bytes_read, b"\n") {
            Ok(Some(v)) => v,
            Ok(None) => {
                return Err(format!("Erro interno: cabeçalho fracionado"));
            }
            Err(err) => {
                return Err(format!("{}", err));
            }
        }
    };
    let p_len = p_end - h_len;
    let mut pacote = vec![0u8; p_len];
    let ja_lido = bytes_read - h_len;
    for i in 0..ja_lido {
        pacote[i] = buf1[h_len + i];
    }
    socket
        .read_exact(&mut pacote[ja_lido..])
        .await
        .map_err(|err| format!("{}", err))?;

    return Ok((pacote, parts));
}

pub fn split_package_parts<'a>(pacote: &'a [u8], parts: &[usize]) -> Vec<&'a [u8]> {
    let mut parts2: Vec<&[u8]> = Vec::with_capacity(parts.len());
    let mut last_part = 0;
    for part in parts {
        parts2.push(&pacote[last_part..*part]);
        last_part = *part;
    }
    return parts2;
}
