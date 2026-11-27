use log::Level as LogLevel;

#[derive(Clone, Debug)]
pub struct LoggingConfig {
    pub enable_logging: bool,
    pub level: LogLevel,
    pub time_format: String,
    pub mod_filter: Vec<&'static str>,
    pub body_filter: Vec<&'static str>,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        LoggingConfig {
            enable_logging: true,
            level: LogLevel::Info,
            time_format: "%Y-%m-%d %H:%M:%S%z".to_string(),
            mod_filter: Vec::new(),
            body_filter: Vec::new(),
        }
    }
}
