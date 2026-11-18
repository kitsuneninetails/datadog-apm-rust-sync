use tracing::field::Visit;

use crate::{models::hashmap_visitor::HashMapVisitor, SpanId, TraceId};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub(crate) struct SpanExtensionData {
    pub id: SpanId,
    pub trace_id: Option<TraceId>,
    pub parent_id: Option<TraceId>,
    pub name: String,
    pub target: String,
    pub start: u64,
    pub tags: HashMap<String, String>,
}

impl SpanExtensionData {
    pub fn new(id: SpanId, name: String, target: String, start: u64) -> Self {
        SpanExtensionData {
            id,
            trace_id: None,
            parent_id: None,
            name,
            target,
            start,
            tags: HashMap::new(),
        }
    }

    pub fn fill_data(&mut self, values: impl SpanExtensionDataRecordable) {
        let mut new_span_visitor = HashMapVisitor::new();
        values.record_visit(&mut new_span_visitor);

        if let Some(id) = new_span_visitor
            .fields
            .remove("trace_id")
            .and_then(|s| s.parse::<u64>().ok())
        {
            self.trace_id = Some(id);
        }

        if let Some(parent_id) = new_span_visitor
            .fields
            .remove("parent_id")
            .and_then(|s| s.parse::<u64>().ok())
        {
            self.parent_id = Some(parent_id);
        }

        if let Some(name) = new_span_visitor.fields.remove("name") {
            self.name = name;
        }
        self.tags.extend(new_span_visitor.fields);
    }
}

pub trait SpanExtensionDataRecordable {
    fn record_visit(&self, visitor: &mut dyn Visit);
}

impl SpanExtensionDataRecordable for &tracing::span::Attributes<'_> {
    fn record_visit(&self, visitor: &mut dyn Visit) {
        self.record(visitor);
    }
}

impl SpanExtensionDataRecordable for &tracing::span::Record<'_> {
    fn record_visit(&self, visitor: &mut dyn Visit) {
        self.record(visitor);
    }
}
