use chrono::{DateTime, Duration, TimeZone, Utc};
use hyper::method::Method;

use crate::{api::RawSpan, model::Span};
use hyper::header::{ContentLength, Headers};
use lazy_static::lazy_static;
use log::{error, trace, warn, Level as LogLevel, Log, Record};
use rand::Rng;
use serde_json::to_string;
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU32, AtomicU8, Ordering},
        mpsc, Arc, Mutex, RwLock,
    },
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
    /// APM Config to set up APM Analytics (default is to disable)
    pub apm_config: ApmConfig,
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
            apm_config: ApmConfig::default(),
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
pub struct ApmConfig {
    pub apm_enabled: bool,
    pub sample_priority: f64,
    pub sample_rate: f64,
}

impl Default for ApmConfig {
    fn default() -> Self {
        ApmConfig {
            apm_enabled: false,
            sample_rate: 0f64,
            sample_priority: 0f64,
        }
    }
}

type TimeInNanos = u64;
type ThreadId = u32;
type TraceId = u64;
type SpanId = u64;

#[derive(Clone, Debug)]
struct LogRecord {
    pub thread_id: ThreadId,
    pub level: log::Level,
    pub time: DateTime<Utc>,
    pub msg_str: String,
    pub module: Option<String>,
}

#[derive(Clone, Debug)]
enum TraceCommand {
    Log(LogRecord),
    NewSpan(TimeInNanos, NewSpanData),
    Enter(TimeInNanos, ThreadId, SpanId),
    Exit(TimeInNanos, SpanId),
    CloseSpan(TimeInNanos, SpanId),
    Event(
        TimeInNanos,
        ThreadId,
        HashMap<String, String>,
        DateTime<Utc>,
    ),
}

#[derive(Debug, Clone)]
struct NewSpanData {
    pub trace_id: TraceId,
    pub id: SpanId,
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
    }

    // Move span to "completed" based on ID.
    fn end_span(&mut self, nanos: u64, span_id: SpanId) {
        let pos = self.current_spans.iter().rposition(|i| i.id == span_id);
        if let Some(i) = pos {
            self.current_spans.remove(i).map(|span| {
                self.completed_spans.push(Span {
                    duration: Duration::nanoseconds(nanos as i64 - span.start.timestamp_nanos()),
                    ..span
                })
            });
        }
    }

    // Enter a span (mark it on stack)
    fn enter_span(&mut self, span_id: SpanId) {
        self.entered_spans.push_back(span_id);
    }

    // Exit a span (pop from stack)
    fn exit_span(&mut self, span_id: SpanId) {
        let pos = self.entered_spans.iter().rposition(|i| *i == span_id);
        if let Some(i) = pos {
            self.entered_spans.remove(i);
        }
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

    fn drain(self, end_time: DateTime<Utc>) -> Vec<Span> {
        let parent_span = Span {
            duration: end_time.signed_duration_since(self.parent_span.start.clone()),
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
    traces: HashMap<TraceId, SpanCollection>,
    spans_to_trace_id: HashMap<SpanId, TraceId>,
    current_trace_for_thread: HashMap<ThreadId, TraceId>,
    current_thread_for_trace: HashMap<TraceId, ThreadId>,
}

impl SpanStorage {
    fn new() -> Self {
        SpanStorage {
            traces: HashMap::new(),
            spans_to_trace_id: HashMap::new(),
            current_trace_for_thread: HashMap::new(),
            current_thread_for_trace: HashMap::new(),
        }
    }

    // Either start a new trace with the span's trace ID (if there is no span already
    // pushed for that trace ID), or push the span on the "current" stack of spans for that
    // trace ID.  If "parent" is true, that means we need a parent span pushed for this to
    // represent the entire trace.
    fn start_span(&mut self, span: Span) {
        let trace_id = span.trace_id;
        self.spans_to_trace_id.insert(span.id, span.trace_id);
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

    /// End a span and update the current "top of the stack"
    fn end_span(&mut self, nanos: u64, span_id: SpanId) {
        if let Some(trace_id) = self.spans_to_trace_id.get(&span_id) {
            if let Some(ref mut ss) = self.traces.get_mut(&trace_id) {
                ss.end_span(nanos, span_id);
            }
        }
    }

    /// Enter a span for trace, and keep track so that new spans get the correct parent.
    /// Keep track of which trace the current thread is in (for logging and events)
    fn enter_span(&mut self, thread_id: ThreadId, span_id: SpanId) {
        let t_id = self.spans_to_trace_id.get(&span_id).map(|i| *i);
        if let Some(trace_id) = t_id {
            if let Some(ref mut ss) = self.traces.get_mut(&trace_id) {
                ss.enter_span(span_id);
                self.set_current_trace(thread_id, trace_id);
            }
        }
    }

    /// Exit a span for trace, and keep track so that new spans get the correct parent
    fn exit_span(&mut self, span_id: SpanId) {
        if let Some(trace_id) = self.spans_to_trace_id.get(&span_id) {
            if let Some(ref mut ss) = self.traces.get_mut(&trace_id) {
                ss.exit_span(span_id);
            }
        }
    }

    /// Drain the span collection for this trace so we can send the trace through to Datadog,
    /// This effectively ends the trace.  Any new spans on this trace ID will have the same
    /// trace ID, but have a new parent span (and a new trace line in Datadog).
    fn drain_completed(&mut self, trace_id: TraceId, end: DateTime<Utc>) -> Vec<Span> {
        if let Some(ss) = self.traces.remove(&trace_id) {
            ss.drain(end)
        } else {
            vec![]
        }
    }

    /// Record an error event on a span
    fn span_record_error(&mut self, trace_id: TraceId, error: crate::model::ErrorInfo) {
        if let Some(ref mut ss) = self.traces.get_mut(&trace_id) {
            ss.err_span(error)
        }
    }

    /// Record HTTP info onto a span
    fn span_record_http(&mut self, trace_id: TraceId, http: crate::model::HttpInfo) {
        if let Some(ref mut ss) = self.traces.get_mut(&trace_id) {
            ss.add_http(http)
        }
    }

    /// Record tag info onto a span
    fn span_record_tag(&mut self, trace_id: TraceId, key: String, value: String) {
        if let Some(ref mut ss) = self.traces.get_mut(&trace_id) {
            ss.add_tag(key, value)
        }
    }

    fn get_trace_id_for_thread(&self, thread_id: ThreadId) -> Option<u64> {
        self.current_trace_for_thread.get(&thread_id).map(|i| *i)
    }

    fn set_current_trace(&mut self, thread_id: ThreadId, trace_id: TraceId) {
        self.current_trace_for_thread.insert(thread_id, trace_id);
        self.current_thread_for_trace.insert(trace_id, thread_id);
    }

    fn remove_current_trace(&mut self, trace_id: TraceId) {
        let thread_id = self.current_thread_for_trace.remove(&trace_id);
        if let Some(thr) = thread_id {
            self.current_trace_for_thread.remove(&thr);
        }
    }

    /// Get the id, if present, of the most current span for the given trace
    fn current_span_id(&self, trace_id: TraceId) -> Option<SpanId> {
        self.traces.get(&trace_id).and_then(|s| s.current_span_id())
    }
}

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
                        match storage
                            .read()
                            .unwrap()
                            .get_trace_id_for_thread(record.thread_id)
                            .and_then(|tr_id| {
                                storage
                                    .read()
                                    .unwrap()
                                    .current_span_id(tr_id)
                                    .map(|sp_id| (tr_id, sp_id))
                            }) {
                            Some((tr, sp)) => {
                                // Both trace and span are active on this thread
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
                                // Both trace and span are not active on this thread
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
            Ok(TraceCommand::NewSpan(_nanos, data)) => {
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
            Ok(TraceCommand::Enter(_nanos, thread_id, span_id)) => {
                trace!("ENTER SPAN: {}/{}", thread_id, span_id);
                storage.write().unwrap().enter_span(thread_id, span_id);
            }
            Ok(TraceCommand::Exit(_nanos, span_id)) => {
                trace!("EXIT SPAN: {}", span_id);
                storage.write().unwrap().exit_span(span_id);
            }
            Ok(TraceCommand::Event(_nanos, thread_id, mut event, time)) => {
                trace!("EVENT: {:?}", event);
                // Events are only valid if the trace_id flag is set
                // Send trace specified the trace to send, so use that instead of the thread's
                // current trace.
                if let Some(send_trace_id) = event
                    .remove("send_trace")
                    .and_then(|t| t.parse::<u64>().ok())
                {
                    let send_vec = storage
                        .write()
                        .unwrap()
                        .drain_completed(send_trace_id, time);
                    // Thread has ended this trace.  Until it enters a new span, it
                    // is not in a trace.
                    storage.write().unwrap().remove_current_trace(send_trace_id);
                    trace!("Pulling trace for ID: {}=[{:?}]", send_trace_id, send_vec);
                    if !send_vec.is_empty() {
                        client.send(send_vec);
                    }
                }
                // Tag events only work inside a trace, so get the trace from the thread.
                // No trace means no tagging.
                let trace_id_opt = storage.read().unwrap().get_trace_id_for_thread(thread_id);
                if let Some(trace_id) = trace_id_opt {
                    let http_evt = to_http_info(
                        event.remove("http_url"),
                        event.remove("http_status_code"),
                        event.remove("http_method"),
                    );

                    let err_evt = to_error_info(
                        event.remove("error_msg"),
                        event.remove("error_type"),
                        event.remove("error_stack"),
                    );

                    if let Some(h) = http_evt {
                        storage.write().unwrap().span_record_http(trace_id, h);
                    }

                    if let Some(e) = err_evt {
                        storage.write().unwrap().span_record_error(trace_id, e)
                    }

                    event.into_iter().for_each(|(k, v)| {
                        storage.write().unwrap().span_record_tag(trace_id, k, v)
                    });
                }
            }
            Ok(TraceCommand::CloseSpan(nanos, span_id)) => {
                trace!("CLOSE SPAN: {}", span_id);
                storage.write().unwrap().end_span(nanos, span_id);
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
            apm_config: config.apm_config,
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

    fn send_new_span(&self, nanos: u64, span: NewSpanData) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(TraceCommand::NewSpan(nanos, span))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_enter_span(&self, nanos: u64, thread_id: ThreadId, id: SpanId) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(TraceCommand::Enter(nanos, thread_id, id))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_exit_span(&self, nanos: u64, id: SpanId) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(TraceCommand::Exit(nanos, id))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_close_span(&self, nanos: u64, span_id: SpanId) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(TraceCommand::CloseSpan(nanos, span_id))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_event(
        &self,
        nanos: u64,
        thread_id: ThreadId,
        event: HashMap<String, String>,
        time: DateTime<Utc>,
    ) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(TraceCommand::Event(nanos, thread_id, event, time))
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

lazy_static! {
    static ref UNIQUEID_COUNTER: AtomicU8 = AtomicU8::new(0);
    static ref THREAD_COUNTER: AtomicU32 = AtomicU32::new(0);
}

thread_local! {
    static THREAD_ID: ThreadId = THREAD_COUNTER.fetch_add(1, Ordering::Relaxed)
}

pub fn get_thread_id() -> ThreadId {
    THREAD_ID.with(|id| *id)
}

// Format
// |                       6 bytes                       |      2 bytes    |
// +--------+--------+--------+--------+--------+--------+--------+--------+
// |     number of milliseconds since epoch (1970)       | static counter  |
// +--------+--------+--------+--------+--------+--------+--------+--------+
// 0        8        16       24       32       40       48       56       64
//
// This will hold up to the year 10,000 before it cycles.
pub fn create_unique_id64() -> u64 {
    let now = Utc::now();
    let baseline = Utc.timestamp(0, 0);

    let millis_since_epoch: u64 =
        (now.signed_duration_since(baseline).num_milliseconds() << 16) as u64;
    let rand: u8 = rand::thread_rng().gen_range::<u8>(0, 255u8);
    millis_since_epoch
        + ((rand as u64) << 8)
        + UNIQUEID_COUNTER.fetch_add(1, Ordering::Relaxed) as u64
}

pub struct HashMapVisitor {
    fields: HashMap<String, String>,
}

impl HashMapVisitor {
    fn new() -> Self {
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
        self.add_value(field, format!("{}", value));
    }
    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        self.add_value(field, format!("{}", value));
    }
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        self.add_value(field, format!("{}", value));
    }
    fn record_i64(&mut self, field: &tracing::field::Field, value: i64) {
        self.add_value(field, format!("{}", value));
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
        let nanos = Utc::now().timestamp_nanos() as u64;
        let mut new_span_visitor = HashMapVisitor::new();
        span.record(&mut new_span_visitor);
        let trace_id = new_span_visitor
            .fields
            .remove("trace_id")
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(Utc::now().timestamp_nanos() as u64);
        let span_id = Utc::now().timestamp_nanos() as u64 + 1;
        let new_span = NewSpanData {
            id: span_id,
            trace_id,
            start: Utc::now(),
            resource: span.metadata().target().to_string(),
            name: span.metadata().name().to_string(),
        };
        self.send_new_span(nanos, new_span).unwrap_or(());
        tracing::span::Id::from_u64(span_id)
    }

    fn record(&self, _span: &tracing::span::Id, _values: &tracing::span::Record<'_>) {}

    fn record_follows_from(&self, _span: &tracing::span::Id, _follows: &tracing::span::Id) {}

    fn event(&self, event: &tracing::Event<'_>) {
        let nanos = Utc::now().timestamp_nanos() as u64;
        let thread_id = get_thread_id();
        let mut new_evt_visitor = HashMapVisitor::new();
        event.record(&mut new_evt_visitor);

        self.send_event(nanos, thread_id, new_evt_visitor.fields, Utc::now())
            .unwrap_or(());
    }

    fn enter(&self, span: &tracing::span::Id) {
        let nanos = Utc::now().timestamp_nanos() as u64;
        let thread_id = get_thread_id();
        self.send_enter_span(nanos, thread_id, span.clone().into_u64())
            .unwrap_or(());
    }

    fn exit(&self, span: &tracing::span::Id) {
        let nanos = Utc::now().timestamp_nanos() as u64;
        self.send_exit_span(nanos, span.clone().into_u64())
            .unwrap_or(());
    }

    fn try_close(&self, span: tracing::span::Id) -> bool {
        let nanos = Utc::now().timestamp_nanos() as u64;
        self.send_close_span(nanos, span.into_u64()).unwrap_or(());
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
                let thread_id = get_thread_id();
                let now = chrono::Utc::now();
                let msg_str = format!("{}", record.args());
                let log_rec = LogRecord {
                    thread_id,
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
    apm_config: ApmConfig,
}

impl DdAgentClient {
    fn send(self, stack: Vec<Span>) {
        trace!("Sending spans: {:?}", stack);
        let count = stack.len();
        let spans: Vec<Vec<RawSpan>> = vec![stack
            .into_iter()
            .map(|s| RawSpan::from_span(&s, &self.service, &self.env, &self.apm_config))
            .collect()];
        match to_string(&spans) {
            Err(e) => warn!("Couldn't encode payload for datadog: {:?}", e),
            Ok(payload) => {
                trace!("Sending to localhost agent payload: {:?}", payload);

                let mut headers = Headers::new();
                headers.set(ContentLength(payload.len() as u64));
                headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                headers.set_raw(
                    "X-Datadog-Trace-Count",
                    vec![format!("{}", count).into_bytes().to_vec()],
                );
                let req = self
                    .http_client
                    .request(Method::Post, &self.endpoint)
                    .headers(headers)
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
    use tracing::{event, span};

    async fn long_call(trace_id: u64) {
        let span = span!(tracing::Level::INFO, "long_call", trace_id = trace_id);
        let _e = span.enter();
        debug!("Waiting on I/O {}", trace_id);
        sleep_call(trace_id);
        info!("I/O Finished {}", trace_id);
    }

    fn sleep_call(trace_id: u64) {
        let span = span!(tracing::Level::INFO, "sleep_call", trace_id = trace_id);
        let _e = span.enter();
        debug!("Long call {}", trace_id);
        debug!("Current thread ID: {}", get_thread_id());
        std::thread::sleep(std::time::Duration::from_millis(2000));
    }

    async fn traced_func_no_send(trace_id: u64) {
        let span = span!(
            tracing::Level::INFO,
            "traced_func_no_send",
            trace_id = trace_id
        );
        let _e = span.enter();
        debug!("Performing some function for id={}", trace_id);
        long_call(trace_id).await;
    }

    async fn traced_http_func(trace_id: u64) {
        let span = span!(
            tracing::Level::INFO,
            "traced_http_func",
            trace_id = trace_id
        );
        let _e = span.enter();
        debug!("Performing some function for id={}", trace_id);
        long_call(trace_id).await;
        event!(
            tracing::Level::INFO,
            http_url = "http://test.test/",
            http_status_code = "200",
            http_method = "GET"
        );
        event!(tracing::Level::INFO, send_trace = trace_id);
    }

    async fn traced_error_func(trace_id: u64) {
        let span = span!(
            tracing::Level::INFO,
            "traced_error_func",
            trace_id = trace_id
        );
        let _e = span.enter();
        debug!("Performing some function for id={}", trace_id);
        long_call(trace_id).await;
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
            send_trace = trace_id
        );
    }

    async fn traced_error_func_single_event(trace_id: u64) {
        let span = span!(
            tracing::Level::INFO,
            "traced_error_func_single_event",
            trace_id = trace_id
        );
        let _e = span.enter();

        debug!("Performing some function for trace_id={}", trace_id);
        long_call(trace_id).await;
        event!(
            tracing::Level::ERROR,
            send_trace = trace_id,
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
                level: Level::Trace,
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
        let trace_id = create_unique_id64();
        trace_config();
        let f1 = tokio::spawn(async move {
            traced_func_no_send(trace_id).await;
            event!(tracing::Level::INFO, send_trace = trace_id);
        });

        f1.await.unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_parallel_two_threads_two_traces() {
        let trace_id1 = create_unique_id64();
        let trace_id2 = create_unique_id64();
        trace_config();
        let f1 = tokio::spawn(async move {
            traced_func_no_send(trace_id1).await;
            event!(tracing::Level::INFO, send_trace = trace_id1);
        });
        let f2 = tokio::spawn(async move {
            traced_func_no_send(trace_id2).await;
            event!(tracing::Level::INFO, send_trace = trace_id2);
        });

        let (r1, r2) = tokio::join!(f1, f2);
        r1.unwrap();
        r2.unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[test]
    fn test_parallel_two_threads_ten_traces() {
        let mut rt = tokio::runtime::Builder::new()
            .threaded_scheduler()
            .enable_all()
            .core_threads(2)
            .max_threads(2)
            .build()
            .unwrap();
        let trace_id1 = create_unique_id64();
        let trace_id2 = create_unique_id64() + 1;
        let trace_id3 = create_unique_id64() + 2;
        let trace_id4 = create_unique_id64() + 3;
        let trace_id5 = create_unique_id64() + 4;
        let trace_id6 = create_unique_id64() + 5;
        let trace_id7 = create_unique_id64() + 6;
        let trace_id8 = create_unique_id64() + 7;
        let trace_id9 = create_unique_id64() + 8;
        let trace_id10 = create_unique_id64() + 9;
        trace_config();
        rt.block_on(async move {
            let f1 = tokio::spawn(async move {
                traced_func_no_send(trace_id1).await;
                event!(tracing::Level::INFO, send_trace = trace_id1);
            });
            let f2 = tokio::spawn(async move {
                traced_func_no_send(trace_id2).await;
                event!(tracing::Level::INFO, send_trace = trace_id2);
            });
            let f3 = tokio::spawn(async move {
                traced_func_no_send(trace_id3).await;
                event!(tracing::Level::INFO, send_trace = trace_id3);
            });
            let f4 = tokio::spawn(async move {
                traced_func_no_send(trace_id4).await;
                event!(tracing::Level::INFO, send_trace = trace_id4);
            });
            let f5 = tokio::spawn(async move {
                traced_func_no_send(trace_id5).await;
                event!(tracing::Level::INFO, send_trace = trace_id5);
            });
            let f6 = tokio::spawn(async move {
                traced_func_no_send(trace_id6).await;
                event!(tracing::Level::INFO, send_trace = trace_id6);
            });
            let f7 = tokio::spawn(async move {
                traced_func_no_send(trace_id7).await;
                event!(tracing::Level::INFO, send_trace = trace_id7);
            });
            let f8 = tokio::spawn(async move {
                traced_func_no_send(trace_id8).await;
                event!(tracing::Level::INFO, send_trace = trace_id8);
            });
            let f9 = tokio::spawn(async move {
                traced_func_no_send(trace_id9).await;
                event!(tracing::Level::INFO, send_trace = trace_id9);
            });
            let f10 = tokio::spawn(async move {
                traced_func_no_send(trace_id10).await;
                event!(tracing::Level::INFO, send_trace = trace_id10);
            });
            let (r1, r2, r3, r4, r5, r6, r7, r8, r9, r10) =
                tokio::join!(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10);
            r1.unwrap();
            r2.unwrap();
            r3.unwrap();
            r4.unwrap();
            r5.unwrap();
            r6.unwrap();
            r7.unwrap();
            r8.unwrap();
            r9.unwrap();
            r10.unwrap();
        });

        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_error_span() {
        let trace_id = create_unique_id64();
        trace_config();
        let f3 = tokio::spawn(async move {
            traced_error_func(trace_id).await;
        });
        f3.await.unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_error_span_as_single_event() {
        let trace_id = create_unique_id64();
        trace_config();
        let f4 = tokio::spawn(async move {
            traced_error_func_single_event(trace_id).await;
        });
        f4.await.unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_two_funcs_in_one_span() {
        let trace_id = create_unique_id64();
        trace_config();
        let f5 = tokio::spawn(async move {
            traced_func_no_send(trace_id).await;
            traced_func_no_send(trace_id).await;
            // Send both funcs under one parent span and one trace
            event!(tracing::Level::INFO, send_trace = trace_id);
        });
        f5.await.unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_one_thread_two_funcs_serial_two_traces() {
        let trace_id1 = create_unique_id64();
        let trace_id2 = create_unique_id64();
        trace_config();
        let f7 = tokio::spawn(async move {
            traced_func_no_send(trace_id1).await;
            event!(tracing::Level::INFO, send_trace = trace_id1);

            traced_func_no_send(trace_id2).await;
            event!(tracing::Level::INFO, send_trace = trace_id2);
        });
        f7.await.unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[tokio::test(threaded_scheduler)]
    async fn test_http_span() {
        let trace_id = create_unique_id64();
        trace_config();
        let f3 = tokio::spawn(async move {
            traced_http_func(trace_id).await;
        });
        f3.await.unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }
}
