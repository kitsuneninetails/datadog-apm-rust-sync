pub mod client;
pub mod datadog_tracing;
pub mod models;
pub mod trace_server_loop;
pub mod utils;

pub use crate::{
    datadog_tracing::DatadogTracing,
    models::{
        apm_config::ApmConfig,
        config::Config,
        logging_config::LoggingConfig,
        span::{Span, SqlInfo},
    },
    utils::{create_unique_id64, log_level_to_trace_level, send_trace},
};

use crate::models::trace_command::TraceCommand;
use atomic_float::AtomicF64;
use std::sync::{
    atomic::AtomicU16,
    mpsc::{channel, Receiver, Sender},
    LazyLock, Mutex,
};

const SEND_DEBUG_STATE_COMMAND: &str = "send_debug_state";

type TimeInNanos = u64;
type TraceId = u64;
type SpanId = u64;

static COMMAND_CHANNEL_RECEIVER: LazyLock<(Sender<TraceCommand>, Mutex<Receiver<TraceCommand>>)> =
    LazyLock::new(|| {
        let (sender, receiver) = channel();
        (sender, Mutex::new(receiver))
    });

static UNIQUEID_COUNTER: AtomicU16 = AtomicU16::new(0);
static SAMPLING_RATE: AtomicF64 = AtomicF64::new(0.0);
