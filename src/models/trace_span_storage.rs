use crate::{models::span::Span, TraceId};
use std::collections::HashMap;

pub(crate) struct TraceSpanStorage {
    spans_for_trace: HashMap<TraceId, Vec<Span>>,
}

impl TraceSpanStorage {
    pub fn new() -> Self {
        TraceSpanStorage {
            spans_for_trace: HashMap::new(),
        }
    }

    /// Add a span for the given trace.
    pub fn add_span_for_trace(&mut self, trace_id: TraceId, span: Span) {
        self.spans_for_trace.entry(trace_id).or_default().push(span);
    }

    /// Drain the spans and return the vector for the given trace.  If the trace
    /// doesn't exist, return an empty vector.
    pub fn drain_spans_for_trace(&mut self, trace_id: TraceId) -> Vec<Span> {
        self.spans_for_trace
            .remove(&trace_id)
            .unwrap_or_default()
            .into_iter()
            .fold(Vec::new(), |mut ret, span| {
                ret.push(span);
                ret
            })
    }

    /// Return the spans for the trace for debugging purposes.
    pub(crate) fn get_spans_for_trace(&self, trace_id: TraceId) -> Option<Vec<Span>> {
        self.spans_for_trace.get(&trace_id).cloned()
    }
}
