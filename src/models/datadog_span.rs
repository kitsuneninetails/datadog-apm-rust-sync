use crate::models::{apm_config::ApmConfig, span::Span};
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
pub struct DatadogSpan {
    pub(crate) service: String,
    pub(crate) name: String,
    pub(crate) resource: String,
    pub(crate) trace_id: u64,
    pub(crate) span_id: u64,
    pub(crate) parent_id: Option<u64>,
    pub(crate) start: u64,
    pub(crate) duration: u64,
    pub(crate) error: i32,
    pub(crate) meta: HashMap<String, String>,
    pub(crate) metrics: HashMap<String, f64>,
    pub(crate) r#type: String,
}

impl DatadogSpan {
    pub fn from_span(mut span: Span, service: &str, env: &Option<String>, cfg: &ApmConfig) -> Self {
        let http_enabled = span.tags.contains_key("http.url");
        let is_error = span.tags.contains_key("error.message");
        let resource = span
            .tags
            .remove("resource")
            .or_else(|| {
                span.tags.get("http.url").map(|url| {
                    format!(
                        "{} {url}",
                        span.tags.get("http.method").unwrap_or(&"?".to_string())
                    )
                })
            })
            .unwrap_or(span.target.clone());
        let span_type = span
            .tags
            .remove("span.type")
            .unwrap_or_else(|| if http_enabled { "web" } else { "custom" }.to_string());
        DatadogSpan {
            service: service.to_string(),
            trace_id: span.trace_id,
            span_id: span.id,
            name: span.name.clone(),
            resource,
            parent_id: span.parent_id,
            start: span.start,
            duration: span.duration,
            error: if is_error { 1 } else { 0 },
            r#type: span_type,
            meta: fill_meta(&span, env.clone()),
            metrics: fill_metrics(cfg),
        }
    }
}
