use chrono::{DateTime, Duration, Utc};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Span {
    pub id: u64,
    pub trace_id: u64,
    pub name: String,
    pub resource: String,
    pub parent_id: Option<u64>,
    pub start: DateTime<Utc>,
    pub duration: Duration,
    pub sql: Option<SqlInfo>,
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct SqlInfo {
    pub query: String,
    pub rows: String,
    pub db: String,
}
