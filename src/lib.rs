#![feature(async_closure)]

mod client;
mod model;
mod api;

pub use crate::{
    client::{DatadogTracing, Config},
    model::{ErrorInfo, HttpInfo, Span, SqlInfo}
};
