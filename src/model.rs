use std::collections::HashMap;
use chrono::{Duration, DateTime, Utc};

#[derive(Debug, Clone)]
pub struct Span {
    pub id: u64,
    pub trace_id: u64,
    pub name: String,
    pub resource: String,
    pub parent_id: Option<u64>,
    pub start: DateTime<Utc>,
    pub duration: Duration,
    pub error: Option<ErrorInfo>,
    pub http: Option<HttpInfo>,
    pub sql: Option<SqlInfo>,
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ErrorInfo {
    pub r#type: String,
    pub msg: String,
    pub stack: String,
}

#[derive(Debug, Clone)]
pub struct HttpInfo {
    pub url: String,
    pub status_code: String,
    pub method: String,
}

#[derive(Debug, Clone)]
pub struct SqlInfo {
    pub query: String,
    pub rows: String,
    pub db: String,
}
