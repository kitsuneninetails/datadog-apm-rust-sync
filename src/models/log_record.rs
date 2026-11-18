use crate::{SpanId, TraceId};
use chrono::{DateTime, Utc};

#[derive(Clone, Debug)]
pub(crate) struct LogRecord {
    pub(crate) trace_id: Option<TraceId>,
    pub(crate) span_id: Option<SpanId>,
    pub(crate) level: tracing::Level,
    pub(crate) time: DateTime<Utc>,
    pub(crate) message: Option<String>,
    pub(crate) module: Option<String>,
    #[cfg(feature = "json")]
    pub(crate) key_values: std::collections::HashMap<String, String>,
}
