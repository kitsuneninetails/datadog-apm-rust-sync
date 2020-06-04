#![feature(async_closure)]

pub mod api;
pub mod client;
pub mod model;

pub use crate::{
    client::{Config, DatadogTracing, LoggingConfig, get_thread_trace_id},
    model::{ErrorInfo, HttpInfo, Span, SqlInfo},
};
