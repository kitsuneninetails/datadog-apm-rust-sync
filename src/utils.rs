use crate::{
    models::{log_record::LogRecord, logging_config::LoggingConfig, trace_command::TraceCommand},
    TraceId, COMMAND_CHANNEL_RECEIVER, UNIQUEID_COUNTER,
};
use chrono::Utc;
use std::sync::atomic::Ordering;

pub(crate) fn filter_log(log_config: &LoggingConfig, record: LogRecord) {
    let mod_skip = record
        .module
        .as_ref()
        .map(|m: &String| {
            log_config
                .mod_filter
                .iter()
                .any(|filter| m.contains(*filter))
        })
        .unwrap_or(false);
    let body_skip = record
        .message
        .as_ref()
        .map(|message| log_config.body_filter.iter().any(|f| message.contains(*f)))
        .unwrap_or(false);
    if !mod_skip && !body_skip {
        let log_body = build_log_body(&record);
        match (record.trace_id, record.span_id) {
            (Some(tr), Some(sp)) => {
                // Both trace and span are active on this thread
                println!(
                    "{time} {level} [trace-id:{traceid} span-id:{spanid}] [{module}] {body}",
                    time = record.time.format(log_config.time_format.as_ref()),
                    traceid = tr,
                    spanid = sp,
                    level = record.level,
                    module = record.module.clone().unwrap_or("-".to_string()),
                    body = log_body
                );
            }
            _ => {
                // Both trace and span are not active on this thread
                println!(
                    "{time} {level} [{module}] {body}",
                    time = record.time.format(log_config.time_format.as_ref()),
                    level = record.level,
                    module = record.module.clone().unwrap_or("-".to_string()),
                    body = log_body
                );
            }
        }
    }
}

fn build_log_body(record: &LogRecord) -> String {
    #[cfg(not(feature = "json"))]
    {
        record.message.clone().unwrap_or_default()
    }
    #[cfg(feature = "json")]
    {
        let message = record.message.clone();
        if record.key_values.is_empty() {
            message.unwrap_or_else(|| String::new())
        } else {
            let mut body = std::collections::HashMap::new();
            if let Some(msg_str) = message {
                body.insert("message".to_string(), msg_str);
            }
            for (k, v) in &record.key_values {
                body.insert(k.clone(), v.clone());
            }
            serde_json::to_string(&body).unwrap_or_else(|_| "".to_string())
        }
    }
}

pub fn send_trace(trace_id: TraceId) -> Result<(), String> {
    let command = TraceCommand::SendTrace(trace_id);
    COMMAND_CHANNEL_RECEIVER
        .0
        .send(command)
        .map(|_| ())
        .map_err(|_| "Send Trace Failed".to_string())
}

pub fn log_level_to_trace_level(level: log::Level) -> tracing::Level {
    use log::Level::*;
    match level {
        Error => tracing::Level::ERROR,
        Warn => tracing::Level::WARN,
        Info => tracing::Level::INFO,
        Debug => tracing::Level::DEBUG,
        Trace => tracing::Level::TRACE,
    }
}

// Format
// |                       6 bytes                       |      2 bytes    |
// +--------+--------+--------+--------+--------+--------+--------+--------+
// |     number of milliseconds since epoch (1970)       | static counter  |
// +--------+--------+--------+--------+--------+--------+--------+--------+
// 0        8        16       24       32       40       48       56       64
//
// This will hold up to the year 10,000 before it cycles.
pub fn create_unique_id64() -> u64 {
    let millis_since_epoch = (Utc::now().timestamp_millis() << 16) as u64;
    millis_since_epoch + UNIQUEID_COUNTER.fetch_add(1, Ordering::Relaxed) as u64
}

#[cfg(feature = "json")]
pub(crate) struct KeyValueMap(std::collections::HashMap<String, String>);

#[cfg(feature = "json")]
impl<'kvs> log::kv::VisitSource<'kvs> for KeyValueMap {
    fn visit_pair(
        &mut self,
        key: log::kv::Key<'kvs>,
        value: log::kv::Value<'kvs>,
    ) -> Result<(), log::kv::Error> {
        self.0.insert(key.to_string(), value.to_string());
        Ok(())
    }
}

#[cfg(feature = "json")]
pub(crate) fn build_logger_key_value_map<'a>(
    record: &log::Record<'a>,
) -> std::collections::HashMap<String, String> {
    let mut visitor = KeyValueMap(std::collections::HashMap::new());
    let visit_result = record.key_values().visit(&mut visitor);
    if let Err(e) = visit_result {
        println!("Error building key value map: {:?}", e);
    }

    visitor.0
}

#[macro_export]
macro_rules! http_info_span {
    ($name:literal, $trace_id:expr) => {
        http_info_span!($name, $trace_id,)
    };
    ($name:literal, $trace_id:expr, $($fields:tt)*) => {
        span!(tracing::Level::INFO, $name,
            trace_id = $trace_id,
            http.url = field::Empty,
            http.method = field::Empty,
            http.status_code = field::Empty,
            error.etype = field::Empty,
            error.message = field::Empty,
            resource = field::Empty,
            span.type = field::Empty,
            $($fields)*
        )
    };
}

#[macro_export]
macro_rules! add_span_tags {
    ($($fields:tt)*) => {
        record_all!(tracing::Span::current(), $($fields)*)
    };
}
