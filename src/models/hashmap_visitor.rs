use std::collections::HashMap;

pub(crate) struct HashMapVisitor {
    pub(crate) fields: HashMap<String, String>,
}

impl HashMapVisitor {
    pub fn new() -> Self {
        // Event/Span vectors should never have more than ten fields.
        HashMapVisitor {
            fields: HashMap::new(),
        }
    }
    fn add_value(&mut self, field: &tracing::field::Field, value: String) {
        self.fields.insert(field.name().to_string(), value);
    }
}

impl tracing::field::Visit for HashMapVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.add_value(field, value.to_string());
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.add_value(field, value.to_string());
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.add_value(field, value.to_string());
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.add_value(field, value.to_string());
    }
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        self.add_value(field, format!("{:?}", value));
    }
}
