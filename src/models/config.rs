use crate::models::{apm_config::ApmConfig, logging_config::LoggingConfig};

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
