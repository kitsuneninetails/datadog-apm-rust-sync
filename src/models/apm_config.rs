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
