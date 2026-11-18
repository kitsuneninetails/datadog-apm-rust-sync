use crate::models::{apm_config::ApmConfig, config::Config, datadog_span::DatadogSpan, span::Span};

use attohttpc;
use serde_json::to_string;

#[derive(Debug, Clone)]
pub struct DdAgentClient {
    client_sender: crossbeam_channel::Sender<Vec<Span>>,
}

impl DdAgentClient {
    pub fn new(config: &Config) -> Self {
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

    pub fn send(&self, stack: Vec<Span>) {
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
            let spans: Vec<Vec<DatadogSpan>> = vec![stack
                .into_iter()
                .map(|s| DatadogSpan::from_span(s, &service, &env, &apm_config))
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
