#![feature(async_closure)]

pub mod api;
pub mod client;
pub mod model;

pub use crate::{
    client::{Config, DatadogTracing, LoggingConfig},
    model::{ErrorInfo, HttpInfo, Span, SqlInfo},
};

