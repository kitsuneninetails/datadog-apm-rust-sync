use hyper::{method::Method};

use hyper::header::{ContentLength, Headers};
use log::{trace, debug, warn, error};
use serde_json::to_string;
use std::sync::{Arc, Mutex, mpsc};

use crate::{
    model::Trace,
    api::RawTrace
};

#[derive(Debug, Clone)]
pub struct DatadogTracing {
    buffer_sender: Arc<Mutex<mpsc::Sender<Trace>>>,
}

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

impl DatadogTracing {
    pub fn new(config: Config) -> DatadogTracing {
        let (buffer_sender, buffer_receiver) = mpsc::channel();

        let client = DdAgentClient {
            env: config.env,
            service: config.service,
            endpoint: format!("http://{}:{}/v0.3/traces", config.host, config.port),
            http_client: Arc::new(hyper::Client::new())
        };

        spawn_consume_buffer_task(buffer_receiver, client);

        DatadogTracing {
            buffer_sender: Arc::new(Mutex::new(buffer_sender))
        }
    }

    pub fn send_trace(&self, trace: Trace) {
        match self.buffer_sender.lock().unwrap().send(trace) {
            Ok(_) => trace!("trace enqueued"),
            Err(err) => warn!("could not enqueue trace: {:?}", err),
        };
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
    fn send_traces(self, traces: Vec<Trace>) {
        let traces = traces
            .iter()
            .map(|trace| RawTrace::from_trace(trace, &self.service, &self.env))
            .collect::<Vec<RawTrace>>();

        let trace_count = traces.len();
        match to_string(&traces) {
            Err(e) => warn!("Couldn't encode payload for datadog: {:?}", e),
            Ok(payload) => {
                debug!("Sending to localhost agent payload: {:?}", payload);

                let mut headers = Headers::new();
                headers.set(ContentLength(payload.len() as u64));
                headers.set_raw("Content-Type", vec![b"application/json".to_vec()]);
                headers.set_raw("X-Datadog-Trace-Count", vec![b"1".to_vec()]);
                let req = self.http_client.request(Method::Post, &self.endpoint)
                    .body(payload.as_str());

                match req.send() {
                    Ok(resp) => {
                        debug!("Sent to localhost agent: {:?}", resp);
                        if resp.status.is_success() {
                            trace!("{} traces sent to datadog", trace_count)
                        } else {
                            error!("error sending traces to datadog: {:?}", resp)
                        }
                    }
                    Err(err) => error!("error sending traces to datadog: {:?}", err),
                }
            }
        }
    }
}

fn spawn_consume_buffer_task(buffer_receiver: mpsc::Receiver<Trace>, client: DdAgentClient) {
    debug!("Spawning channel reader");
    std::thread::spawn(move || {
        debug!("Starting loop");
        loop {
            let client = client.clone();

            if let Ok(trace) = buffer_receiver.try_recv() {
                debug!("Pulled trace: {:?}", trace);
                client.send_traces(vec![trace]);
            }
        }
    });
}

#[cfg(test)]
mod tests {
    extern crate rand;

    use super::*;
    use crate::model::{Span, HttpInfo};
    use std::time::{Duration, SystemTime};
    use std::collections::HashMap;

    use rand::Rng;

    fn _test_send_trace() {
        let config = Config {
            service: String::from("service_name"),
            ..Default::default()
        };
        let client = DatadogTracing::new(config);
        let mut rng = rand::thread_rng();
        let trace = Trace {
            id: rng.gen::<u64>(),
            priority: 1,
            spans: vec![Span {
                id: rng.gen::<u64>(),
                name: String::from("request"),
                resource: String::from("/home/v3"),
                r#type: String::from("web"),
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
            }],
        };
        client.send_trace(trace);
    }
}
