use crate::{api::RawSpan, model::Span};

use attohttpc;
use chrono::{DateTime, Duration, TimeZone, Utc};
use log::{warn, Level as LogLevel, Log, Record};
use serde_json::to_string;
use std::{
    cell::Cell,
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicU16, AtomicU32, Ordering},
        mpsc,
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
    /// Datadog agent port, defaults to `8126`.
    pub port: String,
    /// Optional Logging Config to also set this tracer as the main logger
    pub logging_config: Option<LoggingConfig>,
    /// APM Config to set up APM Analytics (default is to disable)
    pub apm_config: ApmConfig,
    /// Turn on tracing
    pub enable_tracing: bool,
    /// Number of threads to send the HTTP messages to the Datadog agent
    pub num_client_send_threads: u32,
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
            num_client_send_threads: 4,
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

    fn add_tag(&mut self, k: String, v: String) {
        self.current_spans.back_mut().map(|span| {
            span.tags.insert(k.clone(), v.clone());
        });
        self.parent_span.tags.insert(k, v);
    }

    fn drain_current(mut self) -> Self {
        std::mem::take(&mut self.current_spans)
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
        let mut ret = self.drain_current().completed_spans;
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
        if let Some(trace_id) = self.spans_to_trace_id.remove(&span_id) {
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
                if ss.entered_spans.len() == 1 {
                    self.set_current_trace(thread_id, trace_id);
                }
            }
        }
    }

    /// Exit a span for trace, and keep track so that new spans get the correct parent
    fn exit_span(&mut self, span_id: SpanId) {
        let trace_id = self.spans_to_trace_id.get(&span_id).cloned();
        if let Some(trace_id) = trace_id {
            if let Some(ref mut ss) = self.traces.get_mut(&trace_id) {
                ss.exit_span(span_id);
                if ss.entered_spans.is_empty() {
                    self.remove_current_trace(trace_id);
                }
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

fn trace_server_loop(
    client: DdAgentClient,
    buffer_receiver: mpsc::Receiver<TraceCommand>,
    log_config: Option<LoggingConfig>,
) {
    let mut storage = SpanStorage::new();

    loop {
        match buffer_receiver.recv() {
            Ok(TraceCommand::Log(record)) => {
                if let Some(ref lc) = log_config {
                    let skip = record
                        .module
                        .as_ref()
                        .map(|m: &String| lc.mod_filter.iter().any(|filter| m.contains(*filter)))
                        .unwrap_or(false);
                    let body_skip = lc
                        .body_filter
                        .iter()
                        .filter(|f| record.msg_str.contains(*f))
                        .next()
                        .is_some();
                    if !skip && !body_skip {
                        match storage
                            .get_trace_id_for_thread(record.thread_id)
                            .and_then(|tr_id| {
                                storage.current_span_id(tr_id).map(|sp_id| (tr_id, sp_id))
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
                storage.start_span(Span {
                    id: data.id,
                    trace_id: data.trace_id,
                    tags: HashMap::new(),
                    parent_id: None,
                    start: data.start,
                    name: data.name,
                    resource: data.resource,
                    sql: None,
                    duration: Duration::seconds(0),
                });
            }
            Ok(TraceCommand::Enter(_nanos, thread_id, span_id)) => {
                storage.enter_span(thread_id, span_id);
            }
            Ok(TraceCommand::Exit(_nanos, span_id)) => {
                storage.exit_span(span_id);
            }
            Ok(TraceCommand::Event(_nanos, thread_id, mut event, time)) => {
                // Events are only valid if the trace_id flag is set
                // Send trace specified the trace to send, so use that instead of the thread's
                // current trace.
                if let Some(send_trace_id) = event
                    .remove("send_trace")
                    .and_then(|t| t.parse::<u64>().ok())
                {
                    let send_vec = storage.drain_completed(send_trace_id, time);
                    // Thread has ended this trace.  Until it enters a new span, it
                    // is not in a trace.
                    storage.remove_current_trace(send_trace_id);
                    if !send_vec.is_empty() {
                        client.send(send_vec);
                    }
                }
                // Tag events only work inside a trace, so get the trace from the thread.
                // No trace means no tagging.
                let trace_id_opt = storage.get_trace_id_for_thread(thread_id);
                if let Some(trace_id) = trace_id_opt {
                    if let Some(type_event) = event.remove("error.etype") {
                        storage.span_record_tag(trace_id, "error.type".to_string(), type_event)
                    }
                    event
                        .into_iter()
                        .for_each(|(k, v)| storage.span_record_tag(trace_id, k, v));
                }
            }
            Ok(TraceCommand::CloseSpan(nanos, span_id)) => {
                storage.end_span(nanos, span_id);
            }
            Err(_) => {
                return;
            }
        }
    }
}

#[derive(Debug, Clone)]
pub struct DatadogTracing {
    buffer_sender: mpsc::Sender<TraceCommand>,
    log_config: Option<LoggingConfig>,
}

unsafe impl Sync for DatadogTracing {}

impl DatadogTracing {
    pub fn new(config: Config) -> DatadogTracing {
        let (buffer_sender, buffer_receiver) = mpsc::channel();
        let sample_rate = config.apm_config.sample_rate;
        let client = DdAgentClient::new(&config);

        let log_config = config.logging_config.clone();
        std::thread::spawn(move || trace_server_loop(client, buffer_receiver, log_config));

        let tracer = DatadogTracing {
            buffer_sender,
            log_config: config.logging_config,
        };

        if let Some(ref lc) = tracer.log_config {
            let _ = log::set_boxed_logger(Box::new(tracer.clone()));
            log::set_max_level(lc.level.to_level_filter());
        }
        if config.enable_tracing {
            // Only set the global sample rate once when the tracer is set as the global tracer.
            // This must be marked unsafe because we are overwriting a global, but it only gets done
            // once in a process's lifetime.
            unsafe {
                if SAMPLING_RATE.is_none() {
                    SAMPLING_RATE = Some(sample_rate);
                }
            }

            tracing::subscriber::set_global_default(tracer.clone()).unwrap_or_else(|_| {
                warn!(
                    "Global subscriber has already been set!  \
                           This should only be set once in the executable."
                )
            });
        }
        tracer
    }

    pub fn get_global_sampling_rate() -> f64 {
        unsafe { SAMPLING_RATE.clone().unwrap_or(0f64) }
    }

    fn send_log(&self, record: LogRecord) -> Result<(), ()> {
        self.buffer_sender
            .clone()
            .send(TraceCommand::Log(record))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_new_span(&self, nanos: u64, span: NewSpanData) -> Result<(), ()> {
        self.buffer_sender
            .clone()
            .send(TraceCommand::NewSpan(nanos, span))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_enter_span(&self, nanos: u64, thread_id: ThreadId, id: SpanId) -> Result<(), ()> {
        self.buffer_sender
            .clone()
            .send(TraceCommand::Enter(nanos, thread_id, id))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_exit_span(&self, nanos: u64, id: SpanId) -> Result<(), ()> {
        self.buffer_sender
            .clone()
            .send(TraceCommand::Exit(nanos, id))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_close_span(&self, nanos: u64, span_id: SpanId) -> Result<(), ()> {
        self.buffer_sender
            .clone()
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
            .clone()
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

static UNIQUEID_COUNTER: AtomicU16 = AtomicU16::new(0);
static THREAD_COUNTER: AtomicU32 = AtomicU32::new(0);

static mut SAMPLING_RATE: Option<f64> = None;

thread_local! {
    static THREAD_ID: ThreadId = THREAD_COUNTER.fetch_add(1, Ordering::Relaxed);
    static CURRENT_SPAN_ID: Cell<Option<SpanId>> = Cell::new(None);
}

pub fn get_thread_id() -> ThreadId {
    THREAD_ID.with(|id| *id)
}

pub fn get_current_span_id() -> Option<SpanId> {
    CURRENT_SPAN_ID.with(|id| id.get())
}

pub fn set_current_span_id(new_id: Option<SpanId>) {
    CURRENT_SPAN_ID.with(|id| {
        id.set(new_id);
    })
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
    millis_since_epoch + UNIQUEID_COUNTER.fetch_add(1, Ordering::Relaxed) as u64
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
        set_current_span_id(Some(span.into_u64()));
    }

    fn exit(&self, span: &tracing::span::Id) {
        let nanos = Utc::now().timestamp_nanos() as u64;
        self.send_exit_span(nanos, span.clone().into_u64())
            .unwrap_or(());
        set_current_span_id(None);
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
    client_sender: crossbeam_channel::Sender<Vec<Span>>,
}

impl DdAgentClient {
    fn new(config: &Config) -> Self {
        let (client_sender, client_requests) = crossbeam_channel::unbounded();

        for _ in 0..config.num_client_send_threads {
            let env = config.env.clone();
            let service = config.service.clone();
            let host = config.host.clone();
            let port = config.port.clone();
            let apm_config = config.apm_config.clone();
            let cr_channel = client_requests.clone();
            std::thread::spawn(move || {
                DdAgentClient::thread_loop(
                    cr_channel,
                    env,
                    format!("http://{}:{}/v0.3/traces", host, port),
                    service,
                    apm_config,
                )
            });
        }
        DdAgentClient { client_sender }
    }

    fn send(&self, stack: Vec<Span>) {
        self.client_sender.send(stack).unwrap_or_else(|_| {
            println!("Tracing send error: Channel closed!");
        });
    }

    fn thread_loop(
        client_requests: crossbeam_channel::Receiver<Vec<Span>>,
        env: Option<String>,
        endpoint: String,
        service: String,
        apm_config: ApmConfig,
    ) {
        // Loop as long as the channel is open
        while let Ok(stack) = client_requests.recv() {
            let count = stack.len();
            let spans: Vec<Vec<RawSpan>> = vec![stack
                .into_iter()
                .map(|s| RawSpan::from_span(&s, &service, &env, &apm_config))
                .collect()];
            match to_string(&spans) {
                Err(e) => println!("Couldn't encode payload for datadog: {:?}", e),
                Ok(payload) => {
                    let req = attohttpc::post(&endpoint)
                        .header("Content-Length", payload.len() as u64)
                        .header("Content-Type", "application/json")
                        .header("X-Datadog-Trace-Count", count)
                        .text(&payload);

                    match req.send() {
                        Ok(resp) if !resp.is_success() => {
                            println!("error from datadog agent: {:?}", resp)
                        }
                        Err(err) => println!("error sending traces to datadog: {:?}", err),
                        _ => {}
                    }
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

    fn long_call(trace_id: u64) {
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
        debug!(
            "Current thread ID/span ID: {}/{:?}",
            get_thread_id(),
            get_current_span_id()
        );
        std::thread::sleep(std::time::Duration::from_millis(2000));
    }

    fn traced_func_no_send(trace_id: u64) {
        let span = span!(
            tracing::Level::INFO,
            "traced_func_no_send",
            trace_id = trace_id
        );
        let _e = span.enter();
        debug!(
            "Performing some function for id={}/{:?}",
            trace_id,
            get_current_span_id()
        );
        long_call(trace_id);
    }

    fn traced_http_func(trace_id: u64) {
        let span = span!(
            tracing::Level::INFO,
            "traced_http_func",
            trace_id = trace_id
        );
        let _e = span.enter();
        debug!(
            "Performing some function for id={}/{:?}",
            trace_id,
            get_current_span_id()
        );
        long_call(trace_id);
        event!(
            tracing::Level::INFO,
            http.url = "http://test.test/",
            http.status_code = "200",
            http.method = "GET"
        );
        event!(tracing::Level::INFO, send_trace = trace_id);
    }

    fn traced_error_func(trace_id: u64) {
        let span = span!(
            tracing::Level::INFO,
            "traced_error_func",
            trace_id = trace_id
        );
        let _e = span.enter();
        debug!(
            "Performing some function for id={}/{:?}",
            trace_id,
            get_current_span_id()
        );
        long_call(trace_id);
        event!(
            tracing::Level::ERROR,
            error.etype = "",
            error.message = "Test error"
        );
        event!(
            tracing::Level::ERROR,
            http.url = "http://test.test/",
            http.status_code = "400",
            http.method = "GET"
        );
        event!(
            tracing::Level::ERROR,
            custom_tag = "good",
            custom_tag2 = "test",
            send_trace = trace_id
        );
    }

    fn traced_error_func_single_event(trace_id: u64) {
        let span = span!(
            tracing::Level::INFO,
            "traced_error_func_single_event",
            trace_id = trace_id
        );
        let _e = span.enter();

        debug!(
            "Performing some function for id={}/{:?}",
            trace_id,
            get_current_span_id()
        );
        long_call(trace_id);
        event!(
            tracing::Level::ERROR,
            send_trace = trace_id,
            error.etype = "",
            error.message = "Test error",
            http.url = "http://test.test/",
            http.status_code = "400",
            http.method = "GET",
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

    #[test]
    fn test_exit_child_span() {
        trace_config();
        let trace_id = 1u64;

        let f1 = std::thread::spawn(move || {
            let span = span!(tracing::Level::INFO, "parent_span", trace_id = trace_id);
            let _e = span.enter();
            info!("Inside parent_span, should print trace and span ID");
            {
                let span = span!(tracing::Level::INFO, "child_span", trace_id = trace_id);
                let _e = span.enter();
                info!("Inside child_span, should print trace and span ID");
            }
            info!("Back in parent_span, should print trace and span ID");
        });
        f1.join().unwrap();
        event!(tracing::Level::INFO, send_trace = trace_id);
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[test]
    fn test_trace_one_func_stack() {
        let trace_id = create_unique_id64();
        trace_config();

        debug!(
            "Outside of span, this should be None: {:?}",
            get_current_span_id()
        );
        debug!(
            "Sampling rate is {}",
            DatadogTracing::get_global_sampling_rate()
        );

        let f1 = std::thread::spawn(move || {
            traced_func_no_send(trace_id);
            event!(tracing::Level::INFO, send_trace = trace_id);
        });

        debug!(
            "Same as before span, after span completes, this should be None: {:?}",
            get_current_span_id()
        );
        f1.join().unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[test]
    fn test_parallel_two_threads_two_traces() {
        let trace_id1 = create_unique_id64();
        let trace_id2 = create_unique_id64();
        trace_config();
        let f1 = std::thread::spawn(move || {
            traced_func_no_send(trace_id1);
            event!(tracing::Level::INFO, send_trace = trace_id1);
        });
        let f2 = std::thread::spawn(move || {
            traced_func_no_send(trace_id2);
            event!(tracing::Level::INFO, send_trace = trace_id2);
        });

        f1.join().unwrap();
        f2.join().unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[test]
    fn test_parallel_two_threads_ten_traces() {
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
        let f1 = std::thread::spawn(move || {
            traced_func_no_send(trace_id1);
            event!(tracing::Level::INFO, send_trace = trace_id1);
        });
        let f2 = std::thread::spawn(move || {
            traced_func_no_send(trace_id2);
            event!(tracing::Level::INFO, send_trace = trace_id2);
        });
        let f3 = std::thread::spawn(move || {
            traced_func_no_send(trace_id3);
            event!(tracing::Level::INFO, send_trace = trace_id3);
        });
        let f4 = std::thread::spawn(move || {
            traced_func_no_send(trace_id4);
            event!(tracing::Level::INFO, send_trace = trace_id4);
        });
        let f5 = std::thread::spawn(move || {
            traced_func_no_send(trace_id5);
            event!(tracing::Level::INFO, send_trace = trace_id5);
        });
        let f6 = std::thread::spawn(move || {
            traced_func_no_send(trace_id6);
            event!(tracing::Level::INFO, send_trace = trace_id6);
        });
        let f7 = std::thread::spawn(move || {
            traced_func_no_send(trace_id7);
            event!(tracing::Level::INFO, send_trace = trace_id7);
        });
        let f8 = std::thread::spawn(move || {
            traced_func_no_send(trace_id8);
            event!(tracing::Level::INFO, send_trace = trace_id8);
        });
        let f9 = std::thread::spawn(move || {
            traced_func_no_send(trace_id9);
            event!(tracing::Level::INFO, send_trace = trace_id9);
        });
        let f10 = std::thread::spawn(move || {
            traced_func_no_send(trace_id10);
            event!(tracing::Level::INFO, send_trace = trace_id10);
        });
        f1.join().unwrap();
        f2.join().unwrap();
        f3.join().unwrap();
        f4.join().unwrap();
        f5.join().unwrap();
        f6.join().unwrap();
        f7.join().unwrap();
        f8.join().unwrap();
        f9.join().unwrap();
        f10.join().unwrap();

        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[test]
    fn test_error_span() {
        let trace_id = create_unique_id64();
        trace_config();
        let f3 = std::thread::spawn(move || {
            traced_error_func(trace_id);
        });
        f3.join().unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[test]
    fn test_error_span_as_single_event() {
        let trace_id = create_unique_id64();
        trace_config();
        let f4 = std::thread::spawn(move || {
            traced_error_func_single_event(trace_id);
        });
        f4.join().unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[test]
    fn test_two_funcs_in_one_span() {
        let trace_id = create_unique_id64();
        trace_config();
        let f5 = std::thread::spawn(move || {
            traced_func_no_send(trace_id);
            traced_func_no_send(trace_id);
            // Send both funcs under one parent span and one trace
            event!(tracing::Level::INFO, send_trace = trace_id);
        });
        f5.join().unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[test]
    fn test_one_thread_two_funcs_serial_two_traces() {
        let trace_id1 = create_unique_id64();
        let trace_id2 = create_unique_id64();
        trace_config();
        let f7 = std::thread::spawn(move || {
            traced_func_no_send(trace_id1);
            event!(tracing::Level::INFO, send_trace = trace_id1);

            traced_func_no_send(trace_id2);
            event!(tracing::Level::INFO, send_trace = trace_id2);
        });
        f7.join().unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }

    #[test]
    fn test_http_span() {
        let trace_id = create_unique_id64();
        trace_config();
        let f3 = std::thread::spawn(move || {
            traced_http_func(trace_id);
        });
        f3.join().unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }
}
