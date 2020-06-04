use chrono::{DateTime, Duration, Utc};
use hyper::method::Method;

use crate::{api::RawSpan, model::Span};
use hyper::header::{ContentLength, Headers};
use log::{error, trace, warn, Level as LogLevel, Log, Record};
use serde_json::to_string;
use std::{
    cell::RefCell,
    collections::{HashMap, LinkedList},
    sync::{mpsc, Arc, Mutex, RwLock},
};

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
    /// Turn on tracing
    pub enable_tracing: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            env: None,
            host: "localhost".to_string(),
            port: "8126".to_string(),
            service: "".to_string(),
            logging_config: None,
            enable_tracing: false,
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
struct LogRecord {
    pub trace_id: Option<u64>,
    pub span_id: Option<u64>,
    pub level: log::Level,
    pub time: DateTime<Utc>,
    pub msg_str: String,
    pub module: Option<String>,
}

#[derive(Clone, Debug)]
enum TraceCommand {
    Log(LogRecord),
    NewSpan(NewSpanData),
    Enter(u64, u64),
    Exit(u64, u64),
    CloseSpan(u64),
    Event(u64, Vec<(String, String)>),
}

#[derive(Debug, Clone)]
struct NewSpanData {
    pub trace_id: u64,
    pub parent_id: Option<u64>,
    pub id: u64,
    pub name: String,
    pub resource: String,
    pub start: DateTime<Utc>,
}

#[derive(Clone, Debug)]
struct SpanCollection {
    completed_spans: Vec<Span>,
    current_spans: LinkedList<Span>,
}

impl SpanCollection {
    fn new() -> Self {
        SpanCollection {
            completed_spans: vec![],
            current_spans: LinkedList::new(),
        }
    }

    // Open a span by inserting the span into the "current" span map by ID.
    fn start_span(&mut self, span: Span) {
        self.current_spans.push_back(span);
    }

    // Move span to "completed" based on ID.  Return if there are current spans left or not/
    fn end_span(&mut self) -> bool {
        self.current_spans.pop_back().map(|span| {
            self.completed_spans.push(Span {
                duration: Utc::now().signed_duration_since(span.start),
                ..span
            })
        });
        self.current_spans.is_empty()
    }

    fn err_span(&mut self, error: crate::model::ErrorInfo) {
        self.current_spans.pop_back().map(|span| {
            self.current_spans.push_back(Span {
                error: Some(error),
                ..span
            })
        });
    }

    fn add_http(&mut self, http: crate::model::HttpInfo) {
        self.current_spans.pop_back().map(|span| {
            self.current_spans.push_back(Span {
                http: Some(http),
                ..span
            })
        });
    }

    fn add_tag(&mut self, k: String, v: String) {
        self.current_spans.pop_back().map(|span| {
            let mut tags = span.tags;
            tags.insert(k, v);
            self.current_spans.push_back(Span { tags, ..span })
        });
    }

    fn drain(&mut self) -> Vec<Span> {
        self.completed_spans.drain(..).collect()
    }
}

struct SpanStorage {
    traces: HashMap<u64, SpanCollection>,
}

impl SpanStorage {
    fn new() -> Self {
        SpanStorage {
            traces: HashMap::new(),
        }
    }

    // Either start a new trace with the span's trace ID (if there is no span already
    // pushed for that trace ID), or push the span on the "current" stack of spans for that
    // trace ID.
    fn start_span(&mut self, span: Span) {
        let trace_id = span.trace_id;
        if let Some(ss) = self.traces.get_mut(&trace_id) {
            ss.start_span(span);
        } else {
            let mut new_ss = SpanCollection::new();
            new_ss.start_span(span);
            self.traces.insert(trace_id, new_ss);
        }
    }

    /// End a span and possibly return the drained trace data if it was the last span on the stack
    fn end_span(&mut self, trace_id: u64) -> Option<Vec<Span>> {
        if let Some(ref mut ss) = self.traces.get_mut(&trace_id) {
            if ss.end_span() {
                Some(ss.drain())
            } else {
                None
            }
        } else {
            None
        }
    }

    /// Record an error event on a span
    fn span_record_error(&mut self, trace_id: u64, error: crate::model::ErrorInfo) {
        if let Some(ref mut ss) = self.traces.get_mut(&trace_id) {
            ss.err_span(error)
        }
    }

    /// Record HTTP info onto a span
    fn span_record_http(&mut self, trace_id: u64, http: crate::model::HttpInfo) {
        if let Some(ref mut ss) = self.traces.get_mut(&trace_id) {
            ss.add_http(http)
        }
    }

    /// Record tag info onto a span
    fn span_record_tag(&mut self, trace_id: u64, key: String, value: String) {
        if let Some(ref mut ss) = self.traces.get_mut(&trace_id) {
            ss.add_tag(key, value)
        }
    }
}

fn trace_server_loop(
    client: DdAgentClient,
    buffer_receiver: mpsc::Receiver<TraceCommand>,
    log_config: Option<LoggingConfig>,
) {
    let storage = RwLock::new(SpanStorage::new());

    loop {
        let client = client.clone();

        match buffer_receiver.try_recv() {
            Ok(TraceCommand::Log(record)) => {
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
                        match (record.trace_id, record.span_id) {
                            (Some(tr), Some(sp)) => {
                                println!(
                                    "{time} {level} [trace-id:{traceid} span-id:{spanid}] [{module}] {body}",
                                    time = record.time.format(lc.time_format.as_ref()),
                                    traceid = tr,
                                    spanid = sp,
                                    level = record.level,
                                    module = record.module.unwrap_or("-".to_string()),
                                    body = record.msg_str
                                );
                            }
                            _ => {
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
            }
            Ok(TraceCommand::NewSpan(data)) => {
                storage.write().unwrap().start_span(Span {
                    id: data.id,
                    trace_id: data.trace_id,
                    tags: HashMap::new(),
                    parent_id: data.parent_id,
                    start: data.start,
                    name: data.name,
                    resource: data.resource,
                    http: None,
                    sql: None,
                    error: None,
                    duration: Duration::seconds(0),
                });
            }
            Ok(TraceCommand::Enter(_trace_id, _id)) => {}
            Ok(TraceCommand::Exit(_trace_id, _id)) => {}
            Ok(TraceCommand::Event(trace_id, event)) => {
                fn to_error_info(
                    msg: Option<String>,
                    t: Option<String>,
                    st: Option<String>,
                ) -> Option<crate::model::ErrorInfo> {
                    if msg.is_some() || t.is_some() || st.is_some() {
                        Some(crate::model::ErrorInfo {
                            msg: msg.unwrap_or(String::new()),
                            r#type: t.unwrap_or(String::new()),
                            stack: st.unwrap_or(String::new()),
                        })
                    } else {
                        None
                    }
                }
                fn to_http_info(
                    u: Option<String>,
                    st: Option<String>,
                    m: Option<String>,
                ) -> Option<crate::model::HttpInfo> {
                    if u.is_some() || st.is_some() || m.is_some() {
                        Some(crate::model::HttpInfo {
                            url: u.unwrap_or(String::new()),
                            status_code: st.unwrap_or(String::new()),
                            method: m.unwrap_or(String::new()),
                        })
                    } else {
                        None
                    }
                }

                let mut evt_map: HashMap<String, String> = event.into_iter().collect();
                let http_evt = to_http_info(
                    evt_map.remove("http_url"),
                    evt_map.remove("http_status_code"),
                    evt_map.remove("http_method"),
                );

                let err_evt = to_error_info(
                    evt_map.remove("error_msg"),
                    evt_map.remove("error_type"),
                    evt_map.remove("error_stack"),
                );

                if let Some(h) = http_evt {
                    storage.write().unwrap().span_record_http(trace_id, h);
                }

                if let Some(e) = err_evt {
                    storage.write().unwrap().span_record_error(trace_id, e)
                }

                evt_map
                    .into_iter()
                    .for_each(|(k, v)| storage.write().unwrap().span_record_tag(trace_id, k, v));
            }
            Ok(TraceCommand::CloseSpan(trace_id)) => {
                let ret = storage.write().unwrap().end_span(trace_id);
                if let Some(stack) = ret {
                    client.send(stack);
                }
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                warn!("Tracing channel disconnected, exiting");
                return;
            }
            Err(_) => {}
        }
    }
}

#[derive(Debug, Clone)]
pub struct DatadogTracing {
    buffer_sender: Arc<Mutex<mpsc::Sender<TraceCommand>>>,
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
        if config.enable_tracing {
            tracing::subscriber::set_global_default(tracer.clone()).unwrap();
        }
        tracer
    }

    fn send_log(&self, record: LogRecord) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(TraceCommand::Log(record))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_new_span(&self, span: NewSpanData) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(TraceCommand::NewSpan(span))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_enter_span(&self, trace_id: u64, id: u64) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(TraceCommand::Enter(trace_id, id))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_exit_span(&self, trace_id: u64, id: u64) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(TraceCommand::Exit(trace_id, id))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_close_span(&self, trace_id: u64) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(TraceCommand::CloseSpan(trace_id))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_event(&self, trace_id: u64, event: Vec<(String, String)>) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(TraceCommand::Event(trace_id, event))
            .map(|_| ())
            .map_err(|_| ())
    }
}

fn log_level_to_trace_level(level: log::Level) -> tracing::Level {
    use log::Level::*;
    match level {
        Error => tracing::Level::ERROR,
        Warn => tracing::Level::WARN,
        Info => tracing::Level::INFO,
        Debug => tracing::Level::DEBUG,
        Trace => tracing::Level::TRACE,
    }
}

thread_local! {
    static TRACE_ID: RefCell<Option<u64>> = RefCell::new(None);
    static CURRENT_SPAN_ID: RefCell<LinkedList<u64>> = RefCell::new(LinkedList::new());
}

pub fn get_thread_trace_id() -> Option<u64> {
    TRACE_ID.with(|id| id.borrow().clone())
}

pub struct EventVisitor {
    fields: Vec<(String, String)>,
}

impl EventVisitor {
    fn new() -> Self {
        // Event vectors should never have more than five fields.
        EventVisitor {
            fields: Vec::with_capacity(5),
        }
    }
}

impl tracing::field::Visit for EventVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }
    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {
        // Do nothing
    }
}

impl tracing::Subscriber for DatadogTracing {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        match self.log_config {
            Some(ref lc) => log_level_to_trace_level(lc.level) >= *metadata.level(),
            None => false,
        }
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        let trace_id = TRACE_ID.with(|tr| match tr.try_borrow_mut() {
            Ok(mut tr_id) => {
                if let Some(t) = *tr_id {
                    t
                } else {
                    let new_trace_id = Utc::now().timestamp_nanos() as u64;
                    tr_id.replace(new_trace_id);
                    new_trace_id
                }
            }
            Err(_) => Utc::now().timestamp_nanos() as u64,
        });
        let span_id = Utc::now().timestamp_nanos() as u64 + 1;
        let new_span = NewSpanData {
            id: span_id,
            trace_id,
            parent_id: CURRENT_SPAN_ID.with(|spans| spans.borrow().back().map(|i| *i)),
            start: Utc::now(),
            resource: span.metadata().target().to_string(),
            name: span.metadata().name().to_string(),
        };
        self.send_new_span(new_span).unwrap_or(());
        tracing::span::Id::from_u64(span_id)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let mut new_evt_visitor = EventVisitor::new();
        event.record(&mut new_evt_visitor);
        TRACE_ID.with(|tr| {
            if let Some(ref trace_id) = *tr.borrow() {
                self.send_event(*trace_id, new_evt_visitor.fields)
                    .unwrap_or(());
            }
        });
    }

    fn enter(&self, span: &tracing::span::Id) {
        CURRENT_SPAN_ID.with(|spans| match spans.try_borrow_mut() {
            Ok(mut r) => r.push_back(span.into_u64()),
            Err(_) => {}
        });
        TRACE_ID.with(|tr| {
            if let Some(ref trace_id) = *tr.borrow() {
                self.send_enter_span(*trace_id, span.clone().into_u64())
                    .unwrap_or(());
            }
        });
    }

    fn exit(&self, span: &tracing::span::Id) {
        CURRENT_SPAN_ID.with(|spans| match spans.try_borrow_mut() {
            Ok(mut r) => r.pop_back(),
            Err(_) => None,
        });
        TRACE_ID.with(|tr| {
            if let Some(ref trace_id) = *tr.borrow() {
                self.send_exit_span(*trace_id, span.clone().into_u64())
                    .unwrap_or(());
            }
        });
    }

    fn try_close(&self, _span: tracing::span::Id) -> bool {
        TRACE_ID.with(|tr| {
            if let Some(ref trace_id) = *tr.borrow() {
                self.send_close_span(*trace_id).unwrap_or(());
            }
        });
        false
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
                let log_rec = TRACE_ID.with(|tr| {
                    CURRENT_SPAN_ID.with(|sp| LogRecord {
                        span_id: sp.borrow().back().clone().map(|i| *i),
                        trace_id: tr.borrow().clone(),
                        level: record.level(),
                        time: now,
                        module: record.module_path().map(|s| s.to_string()),
                        msg_str,
                    })
                });
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
    fn send(self, stack: Vec<Span>) {
        let spans: Vec<Vec<RawSpan>> = vec![stack
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
    use log::{debug, info, Level};
    use tracing::event;

    #[tracing::instrument]
    async fn traced_func(id: u32) {
        debug!("Performing some function for id={}", id);
        debug!("Current trace ID: {}", get_thread_trace_id().unwrap());
        long_call(id).await;
    }

    #[tracing::instrument]
    async fn traced_error_func(id: u32) {
        debug!("Performing some function for id={}", id);
        long_call(id).await;
        event!(
            tracing::Level::ERROR,
            error_type = "",
            error_msg = "Test error"
        );
        event!(
            tracing::Level::ERROR,
            http_url = "http://test.test/",
            http_status_code = "400",
            http_method = "GET"
        );
        event!(
            tracing::Level::ERROR,
            custom_tag = "good",
            custom_tag2 = "test"
        );
    }

    #[tracing::instrument]
    async fn traced_error_func_single_event(id: u32) {
        debug!("Performing some function for id={}", id);
        long_call(id).await;
        event!(
            tracing::Level::ERROR,
            error_type = "",
            error_msg = "Test error",
            http_url = "http://test.test/",
            http_status_code = "400",
            http_method = "GET",
            custom_tag = "good",
            custom_tag2 = "test"
        );
    }

    #[tracing::instrument]
    async fn long_call(id: u32) {
        debug!("Waiting on I/O {}", id);
        sleep_call();
        info!("I/O Finished {}", id);
    }

    #[tracing::instrument]
    fn sleep_call() {
        debug!("Long call");
        std::thread::sleep(std::time::Duration::from_millis(2000));
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_rust_tracing() {
        let config = Config {
            service: String::from("datadog_apm_test"),
            env: Some("staging-01".into()),
            logging_config: Some(LoggingConfig {
                level: Level::Debug,
                mod_filter: vec!["hyper", "mime"],
                ..LoggingConfig::default()
            }),
            enable_tracing: true,
            ..Default::default()
        };
        let _client = DatadogTracing::new(config);

        let f1 = tokio::spawn(async move { traced_func(1).await });
        let f2 = tokio::spawn(async move { traced_func(2).await });
        let f3 = tokio::spawn(async move { traced_error_func(3).await });
        let f4 = tokio::spawn(async move { traced_error_func_single_event(4).await });

        let (r1, r2, r3, r4) = tokio::join!(f1, f2, f3, f4);
        r1.unwrap();
        r2.unwrap();
        r3.unwrap();
        r4.unwrap();

        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }
}
