use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Span {
    pub id: u64,
    pub trace_id: u64,
    pub name: String,
    pub target: String,
    pub parent_id: Option<u64>,
    pub start: u64,
    pub duration: u64,
    pub sql: Option<SqlInfo>,
    pub tags: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct SqlInfo {
    pub query: String,
    pub rows: String,
    pub db: String,
}
