use crate::{
    client::DdAgentClient,
    datadog_tracing::DebugData,
    models::{
        log_record::LogRecord, logging_config::LoggingConfig, span::Span,
        trace_command::TraceCommand, trace_span_storage::TraceSpanStorage,
    },
    utils::filter_log,
    TraceId, COMMAND_CHANNEL_RECEIVER,
};
use std::sync::mpsc::Sender;

pub(crate) fn trace_server_loop(
    client: DdAgentClient,
    debug_sender: Option<Sender<DebugData>>,
    log_config: Option<LoggingConfig>,
) {
    let mut storage = TraceSpanStorage::new();
    let Ok(recv) = COMMAND_CHANNEL_RECEIVER.1.try_lock()
    else { 
        println!("Datadog tracing layer server loop already running, abandoning new start loop.");
        return; 
    };
    loop {
        match recv.recv() {
            Ok(TraceCommand::Log(record)) => {
                log_config.as_ref().inspect(|lc| filter_log(lc, record));
            }
            Ok(TraceCommand::CloseSpan(end_time, span_data)) => {
                let trace_id_opt = span_data.trace_id;
                if let Some(trace_id) = trace_id_opt {
                    storage.add_span_for_trace(
                        trace_id,
                        Span {
                            id: span_data.id,
                            trace_id,
                            tags: span_data.tags,
                            parent_id: span_data.parent_id,
                            start: span_data.start,
                            name: span_data.name,
                            target: span_data.target,
                            sql: None,
                            duration: end_time - span_data.start,
                        },
                    );
                }
            }
            Ok(TraceCommand::Event(event)) => {
                // Events are logs, so only log if logging config turned on (otherwise, a
                // different logger may handle it).
                if let Some(ref lc) = log_config {
                    let trace_id_opt: Option<TraceId> = event.trace_id;

                    let record = LogRecord {
                        trace_id: trace_id_opt,
                        span_id: event.span_id,
                        level: event.level,
                        time: event.time,
                        message: event.message,
                        module: event.module,
                        #[cfg(feature = "json")]
                        key_values: event.fields,
                    };
                    filter_log(lc, record);
                }
            }
            Ok(TraceCommand::SendTrace(trace_id)) => {
                let send_vec = storage.drain_spans_for_trace(trace_id);
                // Thread has ended this trace.  Until it enters a new span, it
                // is not in a trace.x
                if !send_vec.is_empty() {
                    client.send(send_vec);
                }
            }
            Ok(TraceCommand::DebugGetSpanState(trace_id)) => {
                if let Some(ref dbg_send) = debug_sender {
                    let completed_spans = storage.get_spans_for_trace(trace_id);
                    dbg_send
                        .send(DebugData::SpanData(completed_spans))
                        .unwrap_or_else(|e| {
                            println!("ERROR! Can't send to debug channel: {:?}", e)
                        });
                }
            }
            Err(_) => {
                return;
            }
        }
    }
}
