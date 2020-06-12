use chrono::{DateTime, Duration, Utc};
use hyper::method::Method;

use crate::{api::RawSpan, model::Span};
use hyper::header::{ContentLength, Headers};
use log::{error, trace, warn, Level as LogLevel, Log, Record};
use serde_json::to_string;
use std::{
    cell::RefCell,
    collections::{HashMap, VecDeque},
    str::FromStr,
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
    CloseSpan(u64, u64),
    Event(u64, Vec<(String, String)>),
}

#[derive(Debug, Clone)]
struct NewSpanData {
    pub trace_id: u64,
    pub id: u64,
    pub name: String,
    pub resource: String,
    pub start: DateTime<Utc>,
}

#[derive(Clone, Debug)]
struct SpanCollection {
    completed_spans: Vec<Span>,
    parent_span: Span,
    current_spans: VecDeque<Span>,
    entered_spans: VecDeque<u64>,
}

impl SpanCollection {
    fn new(parent_span: Span) -> Self {
        SpanCollection {
            completed_spans: vec![],
            parent_span,
            current_spans: VecDeque::new(),
            entered_spans: VecDeque::new(),
        }
    }

    // Open a span by inserting the span into the "current" span map by ID.
    fn start_span(&mut self, span: Span) {
        let parent_id = Some(self.current_span_id().unwrap_or(self.parent_span.id));
        self.current_spans.push_back(Span { parent_id, ..span });
        trace!(
            "Start span: {:?}/{:?}",
            self.completed_spans
                .iter()
                .map(|i| i.id)
                .collect::<Vec<u64>>(),
            self.current_spans
                .iter()
                .map(|i| i.id)
                .collect::<Vec<u64>>()
        );
    }

    // Move span to "completed" based on ID.
    fn end_span(&mut self, span_id: u64) {
        let pos = self.current_spans.iter().rposition(|i| i.id == span_id);
        if let Some(i) = pos {
            self.current_spans.remove(i).map(|span| {
                self.completed_spans.push(Span {
                    duration: Utc::now().signed_duration_since(span.start),
                    ..span
                })
            });
        }
        trace!(
            "End span: {:?}/{:?}",
            self.completed_spans
                .iter()
                .map(|i| i.id)
                .collect::<Vec<u64>>(),
            self.current_spans
                .iter()
                .map(|i| i.id)
                .collect::<Vec<u64>>()
        );
    }

    // Enter a span (mark it on stack)
    fn enter_span(&mut self, span_id: u64) {
        self.entered_spans.push_back(span_id);
        trace!("Enter span: {:?}", self.entered_spans);
    }

    // Exit a span (pop from stack)
    fn exit_span(&mut self, span_id: u64) {
        let pos = self.entered_spans.iter().rposition(|i| *i == span_id);
        if let Some(i) = pos {
            self.entered_spans.remove(i);
        }
        trace!("Exit span: {:?}", self.entered_spans);
    }

    /// Get the id, if present, of the most current span for this trace
    fn current_span_id(&self) -> Option<u64> {
        self.entered_spans.back().map(|i| *i)
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

    fn drain_current(mut self) -> Self {
        self.current_spans
            .drain(..)
            .collect::<Vec<Span>>()
            .into_iter()
            .for_each(|span| {
                self.completed_spans.push(Span {
                    duration: Utc::now().signed_duration_since(span.start),
                    ..span
                })
            });
        self
    }

    fn drain(self) -> Vec<Span> {
        let parent_span = Span {
            duration: Utc::now().signed_duration_since(self.parent_span.start.clone()),
            ..self.parent_span.clone()
        };
        let mut ret = self
            .drain_current()
            .completed_spans
            .drain(..)
            .collect::<Vec<Span>>();
        ret.push(parent_span);
        ret
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
    // trace ID.  If "parent" is true, that means we need a parent span pushed for this to
    // represent the entire trace.
    fn start_span(&mut self, span: Span) {
        let trace_id = span.trace_id;
        if let Some(ss) = self.traces.get_mut(&trace_id) {
            ss.start_span(span);
        } else {
            let parent_span_id = Utc::now().timestamp_nanos() as u64 + 1;
            let parent_span = Span {
                id: parent_span_id,
                parent_id: None,
                name: format!("{}-traceparent", trace_id),
                ..span.clone()
            };

            let mut new_ss = SpanCollection::new(parent_span);
            new_ss.start_span(span);

            self.traces.insert(trace_id, new_ss);
        }
    }

    /// End a span and possibly return the drained trace data if it was the last span on the stack
    fn end_span(&mut self, trace_id: u64, span_id: u64) {
        if let Some(ref mut ss) = self.traces.get_mut(&trace_id) {
            ss.end_span(span_id);
        }
    }

    /// Enter a span for trace, and keep track so that new spans get the correct parent
    fn enter_span(&mut self, trace_id: u64, span_id: u64) {
        if let Some(ref mut ss) = self.traces.get_mut(&trace_id) {
            ss.enter_span(span_id);
        }
    }

    /// Exit a span for trace, and keep track so that new spans get the correct parent
    fn exit_span(&mut self, trace_id: u64, span_id: u64) {
        if let Some(ref mut ss) = self.traces.get_mut(&trace_id) {
            ss.exit_span(span_id);
        }
    }

    /// Drain the span collection for this trace so we can send the trace through to Datadog,
    /// This effectively ends the trace.  Any new spans on this trace ID will have the same
    /// trace ID, but have a new parent span (and a new trace line in Datadog).
    fn drain_completed(&mut self, trace_id: u64) -> Vec<Span> {
        if let Some(ss) = self.traces.remove(&trace_id) {
            ss.drain()
        } else {
            vec![]
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

    /// Get the id, if present, of the most current span for the given thread
    fn current_span_id(&self, trace_id: u64) -> Option<u64> {
        self.traces.get(&trace_id).and_then(|s| s.current_span_id())
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

        match buffer_receiver.recv() {
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
                        match record.trace_id.and_then(|tr| {
                            storage
                                .read()
                                .unwrap()
                                .current_span_id(tr)
                                .map(|sp| (tr, sp))
                        }) {
                            Some((tr, sp)) => {
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
                trace!("NEW SPAN: {:?}", data);
                storage.write().unwrap().start_span(Span {
                    id: data.id,
                    trace_id: data.trace_id,
                    tags: HashMap::new(),
                    parent_id: None,
                    start: data.start,
                    name: data.name,
                    resource: data.resource,
                    http: None,
                    sql: None,
                    error: None,
                    duration: Duration::seconds(0),
                });
            }
            Ok(TraceCommand::Enter(trace_id, span_id)) => {
                trace!("ENTER SPAN: {}/{}", trace_id, span_id);
                storage.write().unwrap().enter_span(trace_id, span_id);
            }
            Ok(TraceCommand::Exit(trace_id, span_id)) => {
                trace!("EXIT SPAN: {}/{}", trace_id, span_id);
                storage.write().unwrap().exit_span(trace_id, span_id);
            }
            Ok(TraceCommand::Event(trace_id, event)) => {
                trace!("EVENT: {}/{:?}", trace_id, event);

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
                if let Some(st) = evt_map.remove("send_trace") {
                    if bool::from_str(st.as_str()).unwrap_or(false) {
                        let send_vec = storage.write().unwrap().drain_completed(trace_id);
                        if !send_vec.is_empty() {
                            client.send(send_vec);
                        }
                    }
                }
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
            Ok(TraceCommand::CloseSpan(trace_id, span_id)) => {
                trace!("CLOSE SPAN: {}/{}", trace_id, span_id);
                storage.write().unwrap().end_span(trace_id, span_id);
            }
            Err(_) => {
                warn!("Tracing channel disconnected, exiting");
                return;
            }
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
            tracing::subscriber::set_global_default(tracer.clone()).unwrap_or_else(|_| {
                warn!(
                    "Global subscriber has already been set!  \
                           This should only be set once in the executable."
                )
            });
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

    fn send_close_span(&self, trace_id: u64, span_id: u64) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(TraceCommand::CloseSpan(trace_id, span_id))
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
        Error => tracing::Level::INFO,
        Warn => tracing::Level::INFO,
        Info => tracing::Level::INFO,
        Debug => tracing::Level::DEBUG,
        Trace => tracing::Level::TRACE,
    }
}

thread_local! {
    static TRACE_ID: RefCell<u64> = RefCell::new(Utc::now().timestamp_nanos() as u64);
}

pub fn new_trace_id() -> u64 {
    TRACE_ID.with(|tr| match tr.try_borrow_mut() {
        Ok(mut tr_id) => {
            let new_trace_id = Utc::now().timestamp_nanos() as u64;
            *tr_id = new_trace_id;
            new_trace_id
        }
        Err(_) => 0u64,
    })
}

pub fn get_thread_trace_id() -> u64 {
    TRACE_ID.with(|id| *id.borrow())
}

pub struct EventVisitor {
    fields: Vec<(String, String)>,
    pub new_event: bool,
}

impl EventVisitor {
    fn new() -> Self {
        // Event vectors should never have more than five fields.
        EventVisitor {
            fields: Vec::with_capacity(5),
            new_event: false,
        }
    }
}

impl tracing::field::Visit for EventVisitor {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "send_trace" && bool::from_str(value).unwrap_or(false) {
            self.new_event = true;
        }
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if field.name() == "send_trace" && value {
            self.new_event = true;
        }
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "send_trace" && bool::from_str(&value.to_string()).unwrap_or(false) {
            self.new_event = true;
        }
        self.fields
            .push((field.name().to_string(), value.to_string()));
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        if field.name() == "send_trace" && bool::from_str(&value.to_string()).unwrap_or(false) {
            self.new_event = true;
        }
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
        let trace_id = get_thread_trace_id();
        let span_id = Utc::now().timestamp_nanos() as u64 + 1;
        let new_span = NewSpanData {
            id: span_id,
            trace_id,
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
        let trace_id = get_thread_trace_id();
        self.send_event(trace_id, new_evt_visitor.fields)
            .unwrap_or(());
        if new_evt_visitor.new_event {
            new_trace_id();
        }
    }

    fn enter(&self, span: &tracing::span::Id) {
        let trace_id = get_thread_trace_id();
        self.send_enter_span(trace_id, span.clone().into_u64())
            .unwrap_or(());
    }

    fn exit(&self, span: &tracing::span::Id) {
        let trace_id = get_thread_trace_id();
        self.send_exit_span(trace_id, span.clone().into_u64())
            .unwrap_or(());
    }

    fn try_close(&self, span: tracing::span::Id) -> bool {
        let trace_id = get_thread_trace_id();
        self.send_close_span(trace_id, span.into_u64())
            .unwrap_or(());
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
                let log_rec = LogRecord {
                    trace_id: Some(get_thread_trace_id()),
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
    fn send(self, stack: Vec<Span>) {
        trace!("Sending spans: {:?}", stack);
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

    #[tracing::instrument]
    async fn traced_func_no_send(id: u32) {
        debug!("Performing some function for id={}", id);
        debug!("Current trace ID: {}", get_thread_trace_id());
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
            custom_tag2 = "test",
            send_trace = true
        );
    }

    #[tracing::instrument]
    async fn traced_error_func_single_event(id: u32) {
        debug!("Performing some function for id={}", id);
        long_call(id).await;
        event!(
            tracing::Level::ERROR,
            send_trace = true,
            error_type = "",
            error_msg = "Test error",
            http_url = "http://test.test/",
            http_status_code = "400",
            http_method = "GET",
            custom_tag = "good",
            custom_tag2 = "test"
        );
    }

    fn trace_config() {
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
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_trace_one_func_stack() {
        trace_config();
        let f1 = tokio::spawn(async move {
            traced_func_no_send(1).await;
            event!(tracing::Level::INFO, send_trace = true);
        });

        f1.await.unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_parallel_two_threads_two_traces() {
        trace_config();
        let f1 = tokio::spawn(async move {
            traced_func_no_send(1).await;
            event!(tracing::Level::INFO, send_trace = true);
        });
        let f2 = tokio::spawn(async move {
            traced_func_no_send(2).await;
            event!(tracing::Level::INFO, send_trace = true);
        });

        let (r1, r2) = tokio::join!(f1, f2);
        r1.unwrap();
        r2.unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_error_span() {
        trace_config();
        let f3 = tokio::spawn(async move {
            traced_error_func(3).await;
        });
        f3.await.unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_error_span_as_single_event() {
        trace_config();
        let f4 = tokio::spawn(async move {
            traced_error_func_single_event(4).await;
        });
        f4.await.unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_two_funcs_in_one_span() {
        trace_config();
        let f5 = tokio::spawn(async move {
            traced_func_no_send(5).await;
            traced_func_no_send(6).await;
            // Send both funcs under one parent span and one trace
            event!(tracing::Level::INFO, send_trace = true);
        });
        f5.await.unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_one_thread_two_funcs_serial_two_traces() {
        trace_config();
        let f7 = tokio::spawn(async move {
            traced_func_no_send(7).await;
            // Send on one trace and generate new trace ID
            event!(tracing::Level::INFO, send_trace = true);
            traced_func_no_send(8).await;
            // Send on second trace
            event!(tracing::Level::INFO, send_trace = true);
        });
        f7.await.unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }
}
