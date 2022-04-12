use crate::{client::ApmConfig, model::Span};
use serde::Serialize;
use std::collections::HashMap;

const SAMPLING_PRIORITY_KEY: &str = "_sampling_priority_v1";
const ANALYTICS_SAMPLE_RATE_KEY: &str = "_dd1.sr.eausr";
const _SAMPLE_RATE_METRIC_KEY: &str = "_sample_rate";
const _SAMPLING_AGENT_DECISION: &str = "_dd.agent_psr";
const _SAMPLING_RULE_DECISION: &str = "_dd.rule_psr";
const _SAMPLING_LIMIT_DECISION: &str = "_dd.limit_psr";

fn fill_meta(span: &Span, env: Option<String>) -> HashMap<String, String> {
    let mut meta = HashMap::new();
    if let Some(env) = env {
        meta.insert("env".to_string(), env);
    }
    if let Some(sql) = &span.sql {
        meta.insert("sql.query".to_string(), sql.query.clone());
        meta.insert("sql.rows".to_string(), sql.rows.clone());
        meta.insert("sql.db".to_string(), sql.db.clone());
    }
    for (key, value) in &span.tags {
        meta.insert(key.to_string(), value.to_string());
    }
    meta
}

fn fill_metrics(apm_config: &ApmConfig) -> HashMap<String, f64> {
    let mut metrics = HashMap::new();
    if apm_config.apm_enabled {
        metrics.insert(
            SAMPLING_PRIORITY_KEY.to_string(),
            apm_config.sample_priority,
        );
        metrics.insert(
            ANALYTICS_SAMPLE_RATE_KEY.to_string(),
            apm_config.sample_rate,
        );
    }
    metrics
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct RawSpan {
    service: String,
    name: String,
    resource: String,
    trace_id: u64,
    span_id: u64,
    parent_id: Option<u64>,
    start: u64,
    duration: u64,
    error: i32,
    meta: HashMap<String, String>,
    metrics: HashMap<String, f64>,
    r#type: String,
}

impl RawSpan {
    pub fn from_span(
        span: &Span,
        service: &str,
        env: &Option<String>,
        cfg: &ApmConfig,
    ) -> RawSpan {
        let http_enabled = span.tags.contains_key("http.url");
        let is_error = span.tags.contains_key("error.message");
        RawSpan {
            service: service.to_string(),
            trace_id: span.trace_id,
            span_id: span.id,
            name: span.name.clone(),
            resource: span.resource.clone(),
            parent_id: span.parent_id,
            start: span.start.timestamp_nanos() as u64,
            duration: span.duration.num_nanoseconds().unwrap_or(0) as u64,
            error: if is_error { 1 } else { 0 },
            r#type: if http_enabled { "custom" } else { "web" }.to_string(),
            meta: fill_meta(span, env.clone()),
            metrics: fill_metrics(cfg),
        }
    }
}
