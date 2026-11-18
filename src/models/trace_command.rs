use crate::{
    models::{
        event_record::EventRecord, log_record::LogRecord, span_record_data::SpanExtensionData,
    },
    TimeInNanos, TraceId,
};
#[derive(Clone, Debug)]
pub(crate) enum TraceCommand {
    Log(LogRecord),
    CloseSpan(TimeInNanos, SpanExtensionData),
    SendTrace(TraceId),
    Event(EventRecord),
    DebugGetSpanState(TraceId),
}
