#![feature(async_closure)]

mod api;
mod client;
mod model;

pub use crate::{
    client::{DatadogTracing, Config},
    model::{ErrorInfo, HttpInfo, Span, SqlInfo}
};
