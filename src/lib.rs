#![feature(async_closure)]

mod api;
mod client;
mod model;

pub use crate::{
    client::{Config, DatadogTracing},
    model::{ErrorInfo, HttpInfo, Span, SqlInfo},
};
