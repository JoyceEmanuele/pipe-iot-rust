#[derive(serde::Serialize, serde::Deserialize)]
pub struct DutTemperaturesChecker {
    pub t0: TemperatureChecker,
    pub t1: TemperatureChecker,
}

impl DutTemperaturesChecker {
    pub fn new() -> Self {
        DutTemperaturesChecker {
            t0: TemperatureChecker::new(),
            t1: TemperatureChecker::new(),
        }
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct TemperatureChecker {
    pub empty: bool,
    pub last_value: f64,
    pub last_timestamp: i64,
}

fn round(x: f64, decimals: u32) -> f64 {
    let y = 10i32.pow(decimals) as f64;
    (x * y).round() / y
}

impl TemperatureChecker {
    pub fn new() -> Self {
        return TemperatureChecker {
            empty: true,
            last_value: 0.0, // o valor 0 não vai ser usado pois empty=true
            last_timestamp: 0,
        };
    }

    pub fn check_value(&mut self, v: f64, timestamp_ms: i64) -> Option<f64> {
        // Esta função deve verificar se a leitura do sensor é um valor absurdo que deve ser ignorado.

        // Se o valor lido for None nem tem o que verificar, só retorna None mesmo.
        // let mut v = match v {
        //   Some(v) => v,
        //   None => { return None },
        // };

        // Alguns valores enviados pelo firmware significam erro:
        if (v <= -99.0) || (v >= 85.0) {
            return None;
        }

        let mut invalido = false;
        // Como a verificação é baseada nos últimos valores, ela só é feita se existir um valor anterior
        if self.empty {
            self.last_value = v;
            self.last_timestamp = timestamp_ms;
            self.empty = false;
        } else {
            // O que se verifica é se teve uma variação muito grande de temperatura em um intervalo curto, delta_ts é o tamanho do intervalo.
            let delta_ts = ((timestamp_ms - self.last_timestamp).abs() as f64 / 1000.0).round();
            if (1.0 <= delta_ts) && (delta_ts < 10.0) {
                // Entre 1 e 10 segundos
                let delta_v_per_s = (v - self.last_value).abs() / delta_ts; // Variação em °C por segundo
                if delta_v_per_s >= 1.0 {
                    // variação máxima permitida (ex: 1°C/segundo)
                    invalido = true;
                }
                self.last_value = 0.1 * v + 0.9 * self.last_value;
                self.last_timestamp = timestamp_ms;
            } else {
                // Como o intervalo entre as leituras foi grande, pega a leitura atual como referência
                self.last_value = v;
                self.last_timestamp = timestamp_ms;
            }
        }

        if invalido {
            return None;
        }
        return Some(round(v, 1));
    }
}
