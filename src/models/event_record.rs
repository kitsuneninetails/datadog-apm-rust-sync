use crate::{SpanId, TraceId};
use chrono::{DateTime, Utc};
use std::collections::HashMap;

#[derive(Clone, Debug)]
pub(crate) struct EventRecord {
    pub(crate) trace_id: Option<TraceId>,
    pub(crate) span_id: Option<SpanId>,
    #[cfg(feature = "json")]
    pub(crate) fields: HashMap<String, String>,
    pub(crate) time: DateTime<Utc>,
    pub(crate) level: tracing::Level,
    pub(crate) module: Option<String>,
    pub(crate) message: Option<String>,
}

impl EventRecord {
    pub fn new(
        trace_id: Option<SpanId>,
        span_id: Option<SpanId>,
        mut fields: HashMap<String, String>,
        time: DateTime<Utc>,
        level: tracing::Level,
        module: Option<String>,
    ) -> Self {
        let message = fields.remove("message");
        EventRecord {
            trace_id,
            span_id,
            #[cfg(feature = "json")]
            fields,
            time,
            level,
            module,
            message,
        }
    }
}
