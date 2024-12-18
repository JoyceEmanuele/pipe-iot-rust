use std::collections::HashMap;

pub struct LogInfo {
    pub telemetrySaved_c: HashMap<String, bool>, // tableName
    pub devError_c: HashMap<String, chrono::DateTime<chrono::Utc>>, // devId
    pub topicError_c: HashMap<String, chrono::DateTime<chrono::Utc>>, // topic
}
impl LogInfo {
    pub fn telemetrySaved(
        self: &mut Self,
        devId: &str,
        timestamp1: &str,
        timestamp2: &str,
        tableName: &str,
    ) {
        if let Some(true) = self.telemetrySaved_c.get(tableName) {
            return;
        }
        self.telemetrySaved_c.insert(tableName.to_owned(), true);
        crate::LOG.append_log_tag_msg(
            "INFO",
            &format!(
                "Telemetry received from '{}' {}{} saved in '{}'",
                devId, timestamp1, timestamp2, tableName
            ),
        );
    }

    pub fn devError(
        self: &mut Self,
        errorType: &str,
        devId: &str,
        topic: &str,
        payload: &serde_json::Value,
        err: &str,
    ) {
        if let Some(ts) = self.devError_c.get(devId) {
            if (ts > &chrono::Utc::now()) {
                return;
            }
        }
        self.devError_c.insert(
            devId.to_owned(),
            chrono::Utc::now() + chrono::Duration::hours(1),
        );
        crate::LOG.append_log_tag_msg(
            "ERROR",
            &format!(
                "{}: topic '{}' payload: {} {}",
                errorType,
                topic,
                payload.to_string(),
                err
            ),
        );
    }

    pub fn topicError(
        self: &mut Self,
        errorType: &str,
        topic: &str,
        payload: &serde_json::Value,
        err: &str,
    ) {
        if let Some(ts) = self.topicError_c.get(topic) {
            if (ts > &chrono::Utc::now()) {
                return;
            }
        }
        self.topicError_c.insert(
            topic.to_owned(),
            chrono::Utc::now() + chrono::Duration::minutes(10),
        );
        crate::LOG.append_log_tag_msg(
            "ERROR",
            &format!(
                "{}: topic '{}' payload: {} {}",
                errorType,
                topic,
                payload.to_string(),
                err
            ),
        );
    }
}
