use hyper::method::Method;

use hyper::header::{ContentLength, Headers};
use log::{error, trace, warn, Level as LogLevel, Log, Record};
use serde_json::to_string;
use std::sync::{mpsc, Arc, Mutex, RwLock};

use crate::{api::RawSpan, model::Span, LogRecord};
use std::collections::{HashMap, LinkedList};

/// Configuration settings for the client.
#[derive(Clone, Debug)]
pub struct Config {
    /// Datadog apm service name
    pub service: String,
    /// Datadog apm environment
    pub env: Option<String>,
    /// Datadog agent host/ip, defaults to `localhost`.
    pub host: String,
    /// Datadog agent port, defaults to `8196`.
    pub port: String,
    /// Optional Logging Config to also set this tracer as the main logger
    pub logging_config: Option<LoggingConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            env: None,
            host: "localhost".to_string(),
            port: "8126".to_string(),
            service: "".to_string(),
            logging_config: None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct LoggingConfig {
    pub level: LogLevel,
    pub time_format: String,
    pub mod_filter: Vec<&'static str>,
    pub body_filter: Vec<&'static str>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        LoggingConfig {
            level: LogLevel::Info,
            time_format: "%Y-%m-%d %H:%M:%S%z".to_string(),
            mod_filter: Vec::new(),
            body_filter: Vec::new(),
        }
    }
}

#[derive(Clone, Debug)]
enum SpanState {
    SpanStart(u64, u64),
    SpanEnd(Span),
    Log(LogRecord),
}

#[derive(Clone, Debug)]
struct SpanStack {
    completed_spans: Vec<Span>,
    current_span_stack: LinkedList<u64>,
}

impl SpanStack {
    fn new() -> Self {
        SpanStack {
            completed_spans: vec![],
            current_span_stack: LinkedList::new(),
        }
    }

    // Open a span by pushing the ID on the "current" span stack.
    fn start_span(&mut self, id: u64) {
        self.current_span_stack.push_back(id);
    }

    // Move span to "completed" and return the current span ID of the top of the stack (or None
    // if it's empty and time to remove the trace).  Unless already set, automatically set the
    // span's parent if there are still "current" spans after popping the current one (indicating
    // that the next on the stack is the current span's "parent").
    fn end_span(&mut self, span: Span) -> Option<u64> {
        if self.current_span_stack.pop_back().is_some() {
            let add_span = Span {
                parent_id: span
                    .parent_id
                    .or_else(|| self.current_span_stack.back().map(|i| *i)),
                ..span
            };
            self.completed_spans.push(add_span);
        }
        self.current_span_stack.back().map(|id| *id)
    }
}

struct SpanStorage {
    inner: RwLock<HashMap<u64, SpanStack>>,
    last_span_id: Option<(u64, u64)>,
}

impl SpanStorage {
    fn new() -> Self {
        SpanStorage {
            inner: RwLock::new(HashMap::new()),
            last_span_id: None,
        }
    }

    // Either start a new trace with the span's trace ID (if there is no span already
    // pushed for that trace ID), or push the span on the "current" stack of spans for that
    // trace ID.
    fn start_span(&mut self, trace_id: u64, span_id: u64) {
        let mut inner = self.inner.write().unwrap();
        if let Some(ref mut ss) = inner.get_mut(&trace_id) {
            ss.start_span(span_id);
        } else {
            let mut new_stack = SpanStack::new();
            new_stack.start_span(span_id);
            inner.insert(trace_id, new_stack);
        }
        self.last_span_id = Some((trace_id, span_id));
    }

    // Check if there's a trace for this span's trace ID.  If so, pop the span (which will send
    // the trace if there are no more spans).  If the current span list is empty after the
    // pop, then pop the entire SpanStack and return it (consuming so we can free memory).
    fn end_span(&mut self, span: Span) -> Option<SpanStack> {
        let trace_id = span.trace_id;
        self.last_span_id = {
            let mut inner = self.inner.write().unwrap();
            if let Some(ref mut ss) = inner.get_mut(&trace_id) {
                ss.end_span(span)
            } else {
                return None;
            }
        }
        .map(|id| (trace_id, id));

        if self.last_span_id.is_none() {
            self.inner.write().unwrap().remove(&trace_id)
        } else {
            None
        }
    }

    fn current_span_id(&self) -> Option<(u64, u64)> {
        self.last_span_id
    }
}

fn trace_server_loop(
    client: DdAgentClient,
    buffer_receiver: mpsc::Receiver<SpanState>,
    log_config: Option<LoggingConfig>,
) {
    let mut storage = SpanStorage::new();

    loop {
        let client = client.clone();

        match buffer_receiver.try_recv() {
            Ok(SpanState::SpanStart(trace_id, span_id)) => {
                storage.start_span(trace_id, span_id);
            }
            Ok(SpanState::SpanEnd(info)) => {
                if let Some(stack) = storage.end_span(info) {
                    client.send(stack);
                }
            }
            Ok(SpanState::Log(record)) => {
                if let Some(ref lc) = log_config {
                    let skip = record
                        .module
                        .as_ref()
                        .map(|m| {
                            lc.mod_filter
                                .iter()
                                .filter(|f| m.contains(*f))
                                .next()
                                .is_some()
                        })
                        .unwrap_or(false);
                    let body_skip = lc
                        .body_filter
                        .iter()
                        .filter(|f| record.msg_str.contains(*f))
                        .next()
                        .is_some();
                    if !skip && !body_skip {
                        if let Some((traceid, spanid)) = storage.current_span_id() {
                            println!(
                                "{time} {level} [trace-id:{traceid} span-id:{spanid}] [{module}] {body}",
                                time = record.time.format(lc.time_format.as_ref()),
                                traceid = traceid,
                                spanid = spanid,
                                level = record.level,
                                module = record.module.unwrap_or("-".to_string()),
                                body = record.msg_str
                            );
                        } else {
                            println!(
                                "{time} {level} [{module}] {body}",
                                time = record.time.format(lc.time_format.as_ref()),
                                level = record.level,
                                module = record.module.unwrap_or("-".to_string()),
                                body = record.msg_str
                            );
                        }
                    }
                }
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                warn!("Tracing channel disconnected, exiting");
                return;
            }
            _ => {}
        }
    }
}

#[derive(Debug, Clone)]
pub struct DatadogTracing {
    buffer_sender: Arc<Mutex<mpsc::Sender<SpanState>>>,
    log_config: Option<LoggingConfig>,
}

impl DatadogTracing {
    pub fn new(config: Config) -> DatadogTracing {
        let (buffer_sender, buffer_receiver) = mpsc::channel();

        let client = DdAgentClient {
            env: config.env,
            service: config.service,
            endpoint: format!("http://{}:{}/v0.3/traces", config.host, config.port),
            http_client: Arc::new(hyper::Client::new()),
        };

        let log_config = config.logging_config.clone();
        std::thread::spawn(move || {
            trace_server_loop(client, buffer_receiver, log_config);
        });

        let tracer = DatadogTracing {
            buffer_sender: Arc::new(Mutex::new(buffer_sender)),
            log_config: config.logging_config,
        };

        if let Some(ref lc) = tracer.log_config {
            let _ = log::set_boxed_logger(Box::new(tracer.clone()));
            log::set_max_level(lc.level.to_level_filter());
        }
        tracer
    }

    pub fn start_span(&self, trace_id: u64, span_id: u64) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(SpanState::SpanStart(trace_id, span_id))
            .map(|_| ())
            .map_err(|_| ())
    }

    pub fn end_span(&self, info: Span) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(SpanState::SpanEnd(info))
            .map(|_| ())
            .map_err(|_| ())
    }

    pub fn send_log(&self, record: LogRecord) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(SpanState::Log(record))
            .map(|_| ())
            .map_err(|_| ())
    }
}

impl Log for DatadogTracing {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        if let Some(ref lc) = self.log_config {
            metadata.level() <= lc.level
        } else {
            false
        }
    }

    fn log(&self, record: &Record) {
        if let Some(ref lc) = self.log_config {
            if record.level() <= lc.level {
                let now = chrono::Utc::now();
                let msg_str = format!("{}", record.args());
                let log_rec = LogRecord {
                    level: record.level(),
                    time: now,
                    module: record.module_path().map(|s| s.to_string()),
                    msg_str,
                };
                self.send_log(log_rec).unwrap_or_else(|_| ());
            }
        }
    }

    fn flush(&self) {}
}

#[derive(Debug, Clone)]
struct DdAgentClient {
    env: Option<String>,
    endpoint: String,
    service: String,
    http_client: Arc<hyper::Client>,
}

impl DdAgentClient {
    fn send(self, stack: SpanStack) {
        let spans: Vec<Vec<RawSpan>> = vec![stack
            .completed_spans
            .into_iter()
            .map(|s| RawSpan::from_span(&s, &self.service, &self.env))
            .collect()];
        match to_string(&spans) {
            Err(e) => warn!("Couldn't encode payload for datadog: {:?}", e),
            Ok(payload) => {
                trace!("Sending to localhost agent payload: {:?}", payload);

                let mut headers = Headers::new();
                headers.set(ContentLength(payload.len() as u64));
                headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                headers.set_raw("X-Datadog-Trace-Count", vec![b"1".to_vec()]);
                let req = self
                    .http_client
                    .request(Method::Post, &self.endpoint)
                    .body(payload.as_str());

                match req.send() {
                    Ok(resp) if resp.status.is_success() => {
                        trace!("Sent to localhost agent: {:?}", resp)
                    }
                    Ok(resp) => error!("error from datadog agent: {:?}", resp),
                    Err(err) => error!("error sending traces to datadog: {:?}", err),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{HttpInfo, Span};
    use log::{debug, info, Level};
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};

    use rand::Rng;

    #[test]
    fn test_send_trace() {
        let config = Config {
            service: String::from("datadog_apm_test"),
            env: Some("staging-01".into()),
            logging_config: Some(LoggingConfig {
                level: Level::Debug,
                mod_filter: vec!["hyper", "mime"],
                ..LoggingConfig::default()
            }),
            ..Default::default()
        };
        let client = DatadogTracing::new(config);
        let mut rng = rand::thread_rng();
        let trace_id = rng.gen::<u64>();
        let parent_span_id = rng.gen::<u64>();
        debug!("Test before span start (should have no span-id and trace-id info");
        let pspan = Span {
            id: parent_span_id.clone(),
            trace_id: trace_id.clone(),
            name: String::from("request"),
            resource: String::from("/home/v3"),
            start: SystemTime::now(),
            duration: Duration::from_secs(2),
            parent_id: None,
            http: Some(HttpInfo {
                url: String::from("/home/v3/2?trace=true"),
                method: String::from("GET"),
                status_code: String::from("200"),
            }),
            error: None,
            sql: None,
            tags: HashMap::new(),
        };
        client.start_span(pspan.trace_id, pspan.id).unwrap();

        debug!("Test in span (should have trace-id and span-id)");
        let span = Span {
            id: rng.gen::<u64>(),
            trace_id,
            name: String::from("request_subspan"),
            resource: String::from("/home/v3"),
            start: SystemTime::now(),
            duration: Duration::from_secs(2),
            parent_id: Some(parent_span_id),
            http: Some(HttpInfo {
                url: String::from("/home/v3/2?trace=true"),
                method: String::from("GET"),
                status_code: String::from("200"),
            }),
            error: None,
            sql: None,
            tags: HashMap::new(),
        };
        client.start_span(span.trace_id, span.id).unwrap();

        info!("Test in subspan (should have trace-id and span-id)");

        trace!("Should not print because below log level");

        client.end_span(span).unwrap();

        error!(
            "Test after subspan end, but still in parent span (should have trace-id and span-id)"
        );
        client.end_span(pspan).unwrap();

        debug!("Test after last span end (should have no span-id and trace-id info");
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }
}
