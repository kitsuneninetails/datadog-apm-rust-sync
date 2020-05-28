use hyper::method::Method;

use hyper::header::{ContentLength, Headers};
use log::{debug, error, trace, warn};
use serde_json::to_string;
use std::sync::{mpsc, Arc, Mutex, RwLock};

use crate::{api::RawSpan, model::Span};
use std::collections::{HashMap, LinkedList};

/// Configuration settings for the client.
#[derive(Debug)]
pub struct Config {
    /// Datadog apm service name
    pub service: String,
    /// Datadog apm environment
    pub env: Option<String>,
    /// Datadog agent host/ip, defaults to `localhost`.
    pub host: String,
    /// Datadog agent port, defaults to `8196`.
    pub port: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            env: None,
            host: "localhost".to_string(),
            port: "8126".to_string(),
            service: "".to_string(),
        }
    }
}

#[derive(Clone, Debug)]
enum SpanState {
    SpanStart(u64, u64),
    SpanEnd(Span),
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

    // Move span to "completed" and return true if it's time to send, or false to keep
    // buffering the spans.  Unless already set, automatically set the span's parent if there are
    // still "current" spans after popping the current one (indicating that the next on the
    // stack is the current span's "parent").
    fn end_span(&mut self, span: Span) -> bool {
        if let Some(span_id) = self.current_span_stack.pop_back() {
            let add_span = Span {
                parent_id: span
                    .parent_id
                    .or_else(|| self.current_span_stack.back().map(|i| i.clone())),
                ..span
            };
            debug!("Pushing span to completed: {:?}", add_span);
            self.completed_spans.push(add_span);
        }
        return self.current_span_stack.is_empty();
    }
}

struct SpanStorage {
    inner: RwLock<HashMap<u64, SpanStack>>,
}

impl SpanStorage {
    fn new() -> Self {
        SpanStorage {
            inner: RwLock::new(HashMap::new()),
        }
    }

    // Either start a new trace with the span's trace ID (if there is no spans already
    // pushed for that trace ID), or push the span on the "current" stack of spans for that
    // trace ID.
    fn start_span(&mut self, trace_id: u64, span_id: u64) {
        let mut inner = self.inner.write().unwrap();
        if let Some(ref mut ss) = inner.get_mut(&trace_id) {
            debug!("Starting span: {}", span_id);
            ss.start_span(span_id);
        } else {
            let mut new_stack = SpanStack::new();
            new_stack.start_span(span_id);
            debug!("Starting trace for span: {}", span_id);
            inner.insert(trace_id, new_stack);
        }
    }

    // Check if there's a trace for this span's trace ID.  If so, pop the span (which will send
    // the trace if there are no more spans).  If the current span list is empty after the
    // pop, then pop the entire SpanStack and return it (consuming so we can free memory).
    fn end_span(&mut self, span: Span) -> Option<SpanStack> {
        let trace_id = span.trace_id;
        let drop_stack = {
            let mut inner = self.inner.write().unwrap();
            if let Some(ref mut ss) = inner.get_mut(&trace_id) {
                debug!("Ending span: {:?}", span);
                ss.end_span(span)
            } else {
                false
            }
        };
        if drop_stack {
            debug!("Dropping trace stack and returning for: {}", trace_id);
            self.inner.write().unwrap().remove(&trace_id)
        } else {
            None
        }
    }
}

fn trace_server_loop(client: DdAgentClient, buffer_receiver: mpsc::Receiver<SpanState>) {
    let mut storage = SpanStorage::new();

    loop {
        let client = client.clone();

        match buffer_receiver.try_recv() {
            Ok(SpanState::SpanStart(trace_id, span_id)) => {
                storage.start_span(trace_id, span_id);
            }
            Ok(SpanState::SpanEnd(info)) => {
                debug!("End span: {:?}", info);
                if let Some(stack) = storage.end_span(info) {
                    client.send(stack);
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

        std::thread::spawn(move || {
            println!("Starting loop");
            trace_server_loop(client, buffer_receiver);
        });

        DatadogTracing {
            buffer_sender: Arc::new(Mutex::new(buffer_sender)),
        }
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
        debug!("Sending stack to datadog: {:?}", stack);
        let spans: Vec<Vec<RawSpan>> = vec![stack
            .completed_spans
            .into_iter()
            .map(|s| RawSpan::from_span(&s, &self.service, &self.env))
            .collect()];
        debug!("Sending stack to datadog: {:?}", spans);
        match to_string(&spans) {
            Err(e) => warn!("Couldn't encode payload for datadog: {:?}", e),
            Ok(payload) => {
                debug!("Sending to localhost agent payload: {:?}", payload);

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
    use filter_logger::FilterLogger;
    use log::Level;
    use std::collections::HashMap;
    use std::time::{Duration, SystemTime};

    use rand::Rng;

    #[test]
    fn test_send_trace() {
        FilterLogger::init(Level::Debug, vec!["hyper::".into(), "mime".into()], vec![]);
        let config = Config {
            service: String::from("datadog_apm_test"),
            env: Some("staging-01".into()),
            ..Default::default()
        };
        let client = DatadogTracing::new(config);
        let mut rng = rand::thread_rng();
        let trace_id = rng.gen::<u64>();
        let parent_span_id = rng.gen::<u64>();
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

        client.end_span(span).unwrap();

        client.end_span(pspan).unwrap();

        ::std::thread::sleep(::std::time::Duration::from_millis(1000));
    }
}
