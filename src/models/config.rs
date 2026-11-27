use crate::models::{apm_config::ApmConfig, logging_config::LoggingConfig};

/// Configuration settings for the client.
#[derive(Clone, Debug)]
pub struct DdAgentConfig {
    /// Datadog agent host/ip, defaults to `localhost`.
    pub host: String,
    /// Datadog agent port, defaults to `8126`.
    pub port: u16,
    /// APM Config to set up APM Analytics (default is to disable)
    pub apm_config: ApmConfig,
    /// Number of threads to send the HTTP messages to the Datadog agent
    pub num_client_send_threads: u32,
}

impl Default for DdAgentConfig {
    fn default() -> Self {
        DdAgentConfig {
            host: "localhost".to_string(),
            port: 8126,
            apm_config: ApmConfig::default(),
            num_client_send_threads: 4,
        }
    }
}

/// Configuration settings for the client.
#[derive(Clone, Debug)]
pub struct Config {
    /// If present, send traces over HTTP to the dd-agent running on the machine.
    pub dd_agent: Option<DdAgentConfig>,
    /// Optional Logging Config to also set this tracer as the main logger
    pub logging_config: Option<LoggingConfig>,
    /// Turn on tracing
    pub enable_tracing: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            dd_agent: None,
            logging_config: None,
            enable_tracing: false,
        }
    }
}
