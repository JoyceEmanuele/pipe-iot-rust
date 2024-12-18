use std::str::FromStr;

pub fn calcular_tempo_online(vecTemp: &str) -> (f64) {
    let mut hoursOnline = 0.0;

    if vecTemp.is_empty() {
        return hoursOnline;
    }
    let imax = usize::try_from(vecTemp.len() - 1).unwrap();
    let mut i: usize = 0;
    let mut ival: i64 = -1;
    let mut iast: i64 = -1;
    let mut value;
    loop {
        if ival < 0 {
            ival = i as i64;
        }
        if i > imax || vecTemp.as_bytes()[i] == b',' {
            let duration: isize;
            if iast < 0 {
                value = &vecTemp[(ival as usize)..i];
                duration = 1;
            } else {
                value = &vecTemp[(ival as usize)..(iast as usize)];
                duration = isize::from_str(&vecTemp[((iast + 1) as usize)..i]).unwrap();
            }
            if !value.is_empty() {
                hoursOnline += (duration as f64) / 3600.0;
            }
            ival = -1;
            iast = -1;
        } else if vecTemp.as_bytes()[i] == b'*' {
            iast = i as i64;
        }
        if i > imax {
            break;
        }
        i += 1;
    }
    return hoursOnline;
}
