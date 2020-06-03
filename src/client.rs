use chrono::{Utc, DateTime, Duration};
use hyper::method::Method;

use hyper::header::{ContentLength, Headers};
use log::{error, trace, warn, Level as LogLevel, Log, Record};
use serde_json::to_string;
use std::{
    sync::{mpsc, Arc, Mutex, RwLock},
    cell::RefCell,
    collections::{HashMap, LinkedList}
};
use crate::{api::RawSpan, model::Span};

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
            enable_tracing: false
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
enum EventType {
    Error(crate::model::ErrorInfo),
    Http(crate::model::HttpInfo),
    Tag(String, String)
}

#[derive(Clone, Debug)]
enum TraceCommand {
    Log(LogRecord),
    NewSpan(NewSpanData),
    Enter(u64, u64),
    Exit(u64, u64),
    CloseSpan(u64),
    Record,
    Event(u64, EventType),
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
        self.current_spans.pop_back()
            .map(|span| self.completed_spans.push(
                Span {
                    duration: Utc::now().signed_duration_since(span.start),
                    ..span
                }
            ));
        self.current_spans.is_empty()
    }

    fn err_span(&mut self, error: crate::model::ErrorInfo) {
        self.current_spans.pop_back().map(
            |span| self.current_spans.push_back(
                Span {
                    error: Some(error),
                    ..span
                }
            )
        );
    }

    fn add_http(&mut self, http: crate::model::HttpInfo) {
        self.current_spans.pop_back().map(
            |span| self.current_spans.push_back(
                Span {
                    http: Some(http),
                    ..span
                }
            )
        );
    }

    fn add_tag(&mut self, k: String, v: String) {
        self.current_spans.pop_back()
            .map(|span| {
                let mut tags = span.tags;
                tags.insert(k, v);
                self.current_spans.push_back(
                    Span {
                        tags,
                        ..span
                    }
                )
            });
    }

    fn drain(&mut self) -> Vec<Span> {
        self.completed_spans.drain(..).collect()
    }
}

struct SpanStorage {
    traces: RwLock<HashMap<u64, SpanCollection>>,
}

impl SpanStorage {
    fn new() -> Self {
        SpanStorage {
            traces: RwLock::new(HashMap::new())
        }
    }

    // Either start a new trace with the span's trace ID (if there is no span already
    // pushed for that trace ID), or push the span on the "current" stack of spans for that
    // trace ID.
    fn start_span(&mut self, span: Span) {
        let trace_id = span.trace_id;
        let mut inner = self.traces.write().unwrap();
        if let Some(ss) = inner.get_mut(&trace_id) {
            ss.start_span(span);
        } else {
            let mut new_ss = SpanCollection::new();
            new_ss.start_span(span);
            inner.insert(trace_id, new_ss);
        }
    }

    /// End a span and possibly return the drained trace data if it was the last span on the stack
    fn end_span(&mut self, trace_id: u64) -> Option<Vec<Span>>{
        let mut inner = self.traces.write().unwrap();
        if let Some(ref mut ss) = inner.get_mut(&trace_id) {
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
        let mut inner = self.traces.write().unwrap();
        if let Some(ref mut ss) = inner.get_mut(&trace_id) {
            ss.err_span(error)
        }
    }

    /// Record HTTP info onto a span
    fn span_record_http(&mut self, trace_id: u64, http: crate::model::HttpInfo) {
        let mut inner = self.traces.write().unwrap();
        if let Some(ref mut ss) = inner.get_mut(&trace_id) {
            ss.add_http(http)
        }
    }

    /// Record tag info onto a span
    fn span_record_tag(&mut self, trace_id: u64, key: String, value: String) {
        let mut inner = self.traces.write().unwrap();
        if let Some(ref mut ss) = inner.get_mut(&trace_id) {
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
                            },
                            _ =>  {
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
            Ok(TraceCommand::Enter(_trace_id, _id)) => {
            }
            Ok(TraceCommand::Exit(_trace_id, _id)) => {
            }
            Ok(TraceCommand::Event(trace_id, event)) => {
                match event {
                    EventType::Error(e) => storage.write().unwrap().span_record_error(trace_id, e),
                    EventType::Http(h) => storage.write().unwrap().span_record_http(trace_id, h),
                    EventType::Tag(k, v) => storage.write().unwrap().span_record_tag(trace_id, k, v),
                }
            }
            Ok(TraceCommand::CloseSpan(trace_id)) => {
                let ret = storage.write().unwrap().end_span(trace_id);
                if let Some(stack) = ret {
                    client.send(stack);
                }
            }
            Ok(TraceCommand::Record) => {
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                warn!("Tracing channel disconnected, exiting");
                return;
            }
            Err(_) => {
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

    fn send_record(&self, _record: LogRecord) -> Result<(), ()> {
        self.buffer_sender
            .lock()
            .unwrap()
            .send(TraceCommand::Record)
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_event(&self, trace_id: u64, event: EventType) -> Result<(), ()> {
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

impl tracing::Subscriber for DatadogTracing {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        match self.log_config {
            Some(ref lc) => log_level_to_trace_level(lc.level) >= *metadata.level(),
            None => false
        }
    }

    fn new_span(&self, span: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        let trace_id = TRACE_ID.with(|tr| {
            match tr.try_borrow_mut() {
                Ok(mut tr_id) => {
                    if let Some(t) = *tr_id {
                        t
                    } else {
                        let new_trace_id = Utc::now().timestamp_nanos() as u64;
                        tr_id.replace(new_trace_id);
                        new_trace_id
                    }
                },
                Err(_) => {
                    Utc::now().timestamp_nanos() as u64
                }
            }
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
    fn record_follows_from(&self, span: &tracing::span::Id, follows: &tracing::span::Id) {
        println!("FOLLOWS FROM {:?} ====> {:?}", follows, span);
    }
    fn event(&self, event: &tracing::Event<'_>) {
        println!("EVENT: {:?}", event);
        // TRACE_ID.with(|tr|
        //     if let Some(ref trace_id) = *tr.borrow() {
        //         self.send_event(*trace_id, event).unwrap_or(());
        //     }
        // );
    }
    fn enter(&self, span: &tracing::span::Id) {
        CURRENT_SPAN_ID.with(|spans|
            match spans.try_borrow_mut() {
                Ok(mut r) => r.push_back(span.into_u64()),
                Err(_) => {}
            }
        );
        TRACE_ID.with(|tr|
            if let Some(ref trace_id) = *tr.borrow() {
                self.send_enter_span(*trace_id, span.clone().into_u64()).unwrap_or(());
            }
        );
    }
    fn clone_span(&self, id: &tracing::span::Id) -> tracing::span::Id {
        println!("CLONE SPAN: {:?}", id);
        id.clone()
    }
    fn exit(&self, span: &tracing::span::Id) {
        CURRENT_SPAN_ID.with(|spans|
            match spans.try_borrow_mut() {
                Ok(mut r) => r.pop_back(),
                Err(_) => None
            }
        );
        TRACE_ID.with(|tr|
            if let Some(ref trace_id) = *tr.borrow() {
                self.send_exit_span(*trace_id, span.clone().into_u64()).unwrap_or(());
            }
        );
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
                let log_rec = TRACE_ID.with(|tr|
                    CURRENT_SPAN_ID.with(|sp|
                        LogRecord {
                            span_id: sp.borrow().back().clone().map(|i| *i),
                            trace_id: tr.borrow().clone(),
                            level: record.level(),
                            time: now,
                            module: record.module_path().map(|s| s.to_string()),
                            msg_str,
                        }
                    )
                );
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

    #[tracing::instrument]
    async fn traced_func(id: u32) {
        long_call(id).await;
    }

    #[tracing::instrument]
    async fn long_call(id: u32) {
        debug!("Waiting on I/O {}", id);
        sleep_call();
        info!("I/O Finished {}", id);
    }

    #[tracing::instrument]
    fn sleep_call() {
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
        tokio::join!(f1, f2);

        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }
}
