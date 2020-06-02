#![feature(async_closure)]

mod api;
mod client;
mod model;

pub use crate::{
    client::{Config, DatadogTracing},
    model::{ErrorInfo, HttpInfo, Span, SqlInfo},
};

use chrono::{DateTime, Utc};
use log::Level;

#[derive(Clone, Debug)]
pub struct LogRecord {
    pub level: Level,
    pub time: DateTime<Utc>,
    pub msg_str: String,
    pub module: Option<String>,
}
