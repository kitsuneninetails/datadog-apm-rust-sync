#[derive(Clone, Debug)]
pub struct ApmConfig {
    /// Turn on APM sampling mechanics,
    pub apm_enabled: bool,
    /// Datadog apm service name
    pub service: String,
    /// Datadog apm environment
    pub env: Option<String>,
    /// Priority for this span compared to others
    pub sample_priority: f64,
    /// Percentage rate of spans to actually process and send
    pub sample_rate: f64,
}

impl Default for ApmConfig {
    fn default() -> Self {
        ApmConfig {
            apm_enabled: false,
            sample_rate: 0f64,
            sample_priority: 0f64,
            env: None,
            service: "".to_string(),
        }
    }
}
