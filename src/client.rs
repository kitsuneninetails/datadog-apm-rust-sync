use hyper::method::Method;

use hyper::header::{ContentLength, Headers};
use log::{debug, error, trace, warn};
use serde_json::to_string;
use std::sync::{mpsc, Arc, Mutex};

use crate::{api::RawSpan, model::Span};

#[derive(Debug, Clone)]
pub struct DatadogTracing {
    buffer_sender: Arc<Mutex<mpsc::Sender<Span>>>,
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
            http_client: Arc::new(hyper::Client::new()),
        };

        std::thread::spawn(move || {
            println!("Starting loop");
            loop {
                let client = client.clone();

                match buffer_receiver.try_recv() {
                    Ok(info) => {
                        debug!("Pulled span: {:?}", info);
                        client.send(info);
                    }
                    Err(mpsc::TryRecvError::Disconnected) => {
                        warn!("Tracing channel disconnected, exiting");
                        return;
                    }
                    _ => {}
                }
            }
        });

        DatadogTracing {
            buffer_sender: Arc::new(Mutex::new(buffer_sender)),
        }
    }

    pub fn send(&self, info: Span) -> Result<(), ()> {
        match self.buffer_sender.lock().unwrap().send(info) {
            Ok(_) => {
                debug!("trace enqueued");
                Ok(())
            }
            Err(err) => {
                warn!("could not enqueue trace: {:?}", err);
                Err(())
            }
        }
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
    fn send(self, info: Span) {
        let span = vec![vec![RawSpan::from_span(&info, &self.service, &self.env)]];
        match to_string(&span) {
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
        let span = Span {
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
        client.send(span).unwrap();
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
        client.send(span).unwrap();
        ::std::thread::sleep(::std::time::Duration::from_millis(500));
    }
}
