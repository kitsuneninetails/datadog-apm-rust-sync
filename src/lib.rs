#![feature(async_closure)]

pub mod api;
pub mod client;
pub mod model;

pub use crate::{
    client::{get_thread_trace_id, Config, DatadogTracing, LoggingConfig},
    model::{ErrorInfo, HttpInfo, Span, SqlInfo},
};
