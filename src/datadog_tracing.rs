use crate::{
    client::DdAgentClient,
    models::{
        config::Config,
        event_record::EventRecord,
        hashmap_visitor::HashMapVisitor,
        log_record::LogRecord,
        logging_config::LoggingConfig,
        span::Span,
        span_record_data::{SpanExtensionData, SpanExtensionDataRecordable},
        trace_command::TraceCommand,
    },
    trace_server_loop::trace_server_loop,
    utils::log_level_to_trace_level,
    SpanId, TraceId, COMMAND_CHANNEL_RECEIVER, SAMPLING_RATE, SEND_DEBUG_STATE_COMMAND,
};
use chrono::{DateTime, Utc};
use log::{Log, Record};
use std::collections::HashMap;
use std::sync::{atomic::Ordering, mpsc::Sender};
use tracing::{span, Subscriber};
use tracing_subscriber::registry::LookupSpan;

#[derive(Debug, Clone)]
pub struct DatadogTracing {
    log_config: Option<LoggingConfig>,
}

#[derive(Clone, Debug)]
pub enum DebugData {
    SpanData(Option<Vec<Span>>),
}

unsafe impl Sync for DatadogTracing {}

impl DatadogTracing {
    pub fn new(config: Config) -> DatadogTracing {
        DatadogTracing::new_with_debug_channel(config, None)
    }

    pub fn new_with_debug_channel(
        config: Config,
        debug_sender: Option<Sender<DebugData>>,
    ) -> DatadogTracing {
        let sample_rate = config.apm_config.sample_rate;
        let client = DdAgentClient::new(&config);

        let log_config = config.logging_config.clone();
        std::thread::spawn(move || trace_server_loop(client, debug_sender, log_config));

        let tracer = DatadogTracing {
            log_config: config.logging_config,
        };

        if let Some(ref lc) = tracer.log_config {
            let _ = log::set_boxed_logger(Box::new(tracer.clone()));
            log::set_max_level(lc.level.to_level_filter());
        }
        if config.enable_tracing {
            // Only set the global sample rate once when the tracer is set as the global tracer.
            // This must be marked unsafe because we are overwriting a global, but it only gets done
            // once in a process's lifetime.
            SAMPLING_RATE.store(sample_rate, Ordering::Release);
        }
        tracer
    }

    pub fn get_global_sampling_rate() -> f64 {
        SAMPLING_RATE.load(Ordering::Acquire)
    }

    fn send_log(&self, record: LogRecord) -> Result<(), ()> {
        COMMAND_CHANNEL_RECEIVER
            .0
            .send(TraceCommand::Log(record))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_close_span(
        &self,
        end_time: u64,
        _span_id: SpanId,
        data: SpanExtensionData,
    ) -> Result<(), ()> {
        COMMAND_CHANNEL_RECEIVER
            .0
            .send(TraceCommand::CloseSpan(end_time, data))
            .map(|_| ())
            .map_err(|_| ())
    }

    fn send_event(
        &self,
        trace_id: Option<TraceId>,
        span_id: Option<SpanId>,
        mut event: HashMap<String, String>,
        time: DateTime<Utc>,
        level: &tracing::Level,
        module: Option<String>,
    ) -> Result<(), ()> {
        let command = event
            .remove(SEND_DEBUG_STATE_COMMAND)
            .and_then(|trace_id| trace_id.parse::<u64>().ok())
            .map(TraceCommand::DebugGetSpanState)
            .unwrap_or(TraceCommand::Event(EventRecord::new(
                trace_id, span_id, event, time, *level, module,
            )));
        COMMAND_CHANNEL_RECEIVER
            .0
            .send(command)
            .map(|_| ())
            .map_err(|_| ())
    }
}

fn add_values_to_span_tags<S>(
    span: &span::Id,
    values: impl SpanExtensionDataRecordable,
    parent: Option<span::Id>,
    ctx: &tracing_subscriber::layer::Context<'_, S>,
) where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    let span_ref_opt = ctx.span(span);

    if let Some(span_ref) = span_ref_opt {
        let mut ext = span_ref.extensions_mut();

        if let Some(span_data) = ext.get_mut::<SpanExtensionData>() {
            span_data.fill_data(values);
        } else {
            let nanos = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
            let mut new_data = SpanExtensionData::new(
                span.into_u64(),
                span_ref.name().to_string(),
                span_ref.metadata().target().to_string(),
                nanos,
            );
            if let Some(parent_id) = parent {
                new_data.parent_id = Some(parent_id.into_u64());
            }
            new_data.fill_data(values);
            ext.insert(new_data);
        }
    }
}

fn drain_span_extension_data<S>(
    span: &span::Id,
    ctx: &tracing_subscriber::layer::Context<'_, S>,
) -> Option<SpanExtensionData>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    ctx.span(span)
        .and_then(|span_ref| span_ref.extensions_mut().remove::<SpanExtensionData>())
}

fn get_trace_id_from_span_extension_data<S>(
    span: &span::Id,
    ctx: &tracing_subscriber::layer::Context<'_, S>,
) -> Option<TraceId>
where
    S: Subscriber + for<'lookup> LookupSpan<'lookup>,
{
    ctx.span(span).and_then(|span_ref| {
        span_ref
            .extensions()
            .get::<SpanExtensionData>()
            .and_then(|data| data.trace_id)
    })
}

impl<S: Subscriber> tracing_subscriber::Layer<S> for DatadogTracing
where
    S: for<'lookup> LookupSpan<'lookup>,
{
    fn on_new_span(
        &self,
        attrs: &span::Attributes<'_>,
        id: &span::Id,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        add_values_to_span_tags(
            id,
            attrs,
            attrs
                .parent()
                .cloned()
                .or_else(|| tracing::Span::current().id()),
            &ctx,
        );
    }

    fn on_record(
        &self,
        span: &span::Id,
        values: &span::Record<'_>,
        ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        add_values_to_span_tags(span, values, None, &ctx);
    }

    fn event_enabled(
        &self,
        _event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        true
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut new_evt_visitor = HashMapVisitor::new();
        event.record(&mut new_evt_visitor);

        let trace_id = ctx
            .current_span()
            .id()
            .and_then(|id| get_trace_id_from_span_extension_data(id, &ctx));

        self.send_event(
            trace_id,
            ctx.current_span().id().map(|id| id.into_u64()),
            new_evt_visitor.fields,
            Utc::now(),
            event.metadata().level(),
            event.metadata().module_path().map(|s| s.to_string()),
        )
        .unwrap_or(());
    }

    fn on_close(&self, id: span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let nanos = Utc::now().timestamp_nanos_opt().unwrap_or(0) as u64;
        let ext_data = drain_span_extension_data(&id, &ctx);
        if let Some(data) = ext_data {
            self.send_close_span(nanos, id.into_u64(), data)
                .unwrap_or(());
        }
    }

    fn enabled(
        &self,
        metadata: &tracing::Metadata<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        match self.log_config {
            Some(ref lc) => log_level_to_trace_level(lc.level) >= *metadata.level(),
            None => false,
        }
    }
}

impl Log for DatadogTracing {
    fn enabled(&self, metadata: &log::Metadata) -> bool {
        if let Some(ref lc) = self.log_config {
            metadata.level() <= lc.level
        } else {
            false
        }
    }

    fn log(&self, record: &Record) {
        if let Some(ref lc) = self.log_config {
            #[cfg(feature = "json")]
            let key_values = crate::utils::build_logger_key_value_map(record);
            if record.level() <= lc.level {
                let now = chrono::Utc::now();
                let msg_str = format!("{}", record.args());
                let log_rec = LogRecord {
                    trace_id: None,
                    span_id: None,
                    level: log_level_to_trace_level(record.level()),
                    time: now,
                    module: record.module_path().map(|s| s.to_string()),
                    message: Some(msg_str),
                    #[cfg(feature = "json")]
                    key_values,
                };
                self.send_log(log_rec).unwrap_or(());
            }
        }
    }

    fn flush(&self) {}
}

#[cfg(test)]
mod tests {
    use core::panic;
    use std::sync::{
        mpsc::{channel, Receiver, Sender},
        LazyLock, Mutex,
    };

    use crate::{
        add_span_tags, create_unique_id64, http_info_span,
        models::{apm_config::ApmConfig, datadog_span::DatadogSpan},
    };

    use super::*;
    use log::{debug, info};
    use tracing::{event, record_all, span};
    use tracing::{field, level_filters};

    static DEBUG_CHANNEL: LazyLock<(Sender<DebugData>, Mutex<Receiver<DebugData>>)> =
        LazyLock::new(|| {
            let (sender, receiver) = channel();
            (sender, Mutex::new(receiver))
        });

    static DEBUG_CHANNEL_MUTEX: std::sync::LazyLock<std::sync::Mutex<bool>> =
        LazyLock::new(|| std::sync::Mutex::new(true));

    fn long_call(trace_id: u64) {
        let span = span!(tracing::Level::INFO, "long_call", trace_id = trace_id);
        let _e = span.enter();
        debug!("Waiting on I/O {}", trace_id);
        sleep_call(trace_id);
        info!("I/O Finished {}", trace_id);
    }

    fn sleep_call(trace_id: u64) {
        let span = span!(tracing::Level::INFO, "sleep_call", trace_id = trace_id);
        let _e = span.enter();
        debug!("Long call {}", trace_id);
        debug!("Current span ID: {:?}", span.id());
        std::thread::sleep(std::time::Duration::from_millis(2000));
    }

    fn traced_func_no_send(trace_id: u64) {
        let span = http_info_span!(
            "traced_error_func",
            trace_id,
            name = format!("traced_func_no_send_{trace_id}")
        );
        let _e = span.enter();
        debug!(
            "Performing some function for id={}/{:?}",
            trace_id,
            span.id()
        );
    }

    fn traced_http_func(trace_id: u64) {
        let span = http_info_span!("traced_http_func", trace_id);
        let _e = span.enter();
        debug!(
            "Performing some function for id={}/{:?}",
            trace_id,
            span.id()
        );
        long_call(trace_id);
        add_span_tags!(
            http.url = "http://test.test/",
            http.status_code = "200",
            http.method = "GET"
        );
    }

    fn traced_error_func(trace_id: u64) {
        let span = http_info_span!(
            "traced_error_func",
            trace_id,
            custom_tag = field::Empty,
            custom_tag2 = field::Empty
        );
        let _e = span.enter();
        debug!(
            "Performing some function for id={}/{:?}",
            trace_id,
            span.id()
        );
        long_call(trace_id);
        debug!("Setting error on id={}/{:?}", trace_id, span.id());
        add_span_tags!(error.etype = "", error.message = "Test error");
        add_span_tags!(
            http.url = "http://test.test/",
            http.status_code = "400",
            http.method = "GET"
        );
        add_span_tags!(custom_tag = "good", custom_tag2 = "test",);
    }

    fn traced_error_func_single_event(trace_id: u64) {
        let span = http_info_span!(
            "traced_error_func_single_event",
            trace_id,
            custom_tag = field::Empty,
            custom_tag2 = field::Empty
        );
        let _e = span.enter();

        debug!(
            "Performing some function for id={}/{:?}",
            trace_id,
            span.id()
        );
        long_call(trace_id);
        add_span_tags!(
            error.etype = "",
            error.message = "Test error",
            http.url = "http://test.test/",
            http.status_code = "400",
            http.method = "GET",
            custom_tag = "good",
            custom_tag2 = "test"
        );
    }

    fn trace_config_with_debug_channel(log_level: log::Level, trace_level: tracing::Level) {
        let config = Config {
            service: String::from("datadog_apm_test"),
            env: Some("staging-01".into()),
            logging_config: Some(LoggingConfig {
                level: log_level,
                mod_filter: vec!["hyper", "mime", "test_log"],
                ..LoggingConfig::default()
            }),
            enable_tracing: true,
            ..Default::default()
        };
        let _client = tracing_subscriber::registry()
            .with(DatadogTracing::new_with_debug_channel(
                config,
                Some((*DEBUG_CHANNEL).0.clone()),
            ))
            .with(
                filter::targets::Targets::new()
                    .with_target(
                        "datadog_apm_sync::datadog_tracing::tests::test_trace",
                        level_filters::LevelFilter::OFF,
                    )
                    .with_default(trace_level),
            )
            .try_init()
            .or_else(|e| {
                log::warn!("Error initializing logger: {e}");
                Result::<(), ()>::Ok(())
            })
            .unwrap();
    }

    fn get_completed_spans_from_debug_channel(trace_id: u64) -> Vec<DatadogSpan> {
        let span_info = {
            let _g = DEBUG_CHANNEL_MUTEX.lock().unwrap();
            event!(tracing::Level::INFO, send_debug_state = trace_id);
            DEBUG_CHANNEL
                .1
                .lock()
                .unwrap()
                .recv_timeout(std::time::Duration::from_secs(2))
                .unwrap()
        };
        match span_info {
            DebugData::SpanData(Some(complete)) => complete
                .into_iter()
                .map(|span| DatadogSpan::from_span(span, "", &None, &ApmConfig::default()))
                .collect::<Vec<DatadogSpan>>(),
            DebugData::SpanData(_) => panic!("Trace ID not found!"),
        }
    }

    #[test]
    fn test_debug_channel() {
        let trace_id = create_unique_id64();

        trace_config_with_debug_channel(log::Level::Trace, tracing::Level::TRACE);
        let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id);
        let _e = parent.enter();
        {
            let span2 = tracing::span!(tracing::Level::INFO, "test", trace_id = trace_id);
            let _e2 = span2.enter();
        }
        let complete = get_completed_spans_from_debug_channel(trace_id);
        assert_eq!(complete.get(0).unwrap().name, "test");
    }

    #[test]
    fn test_new_span_tags() {
        let trace_id = create_unique_id64();

        trace_config_with_debug_channel(log::Level::Trace, tracing::Level::TRACE);
        let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id);
        let _e = parent.enter();

        {
            let span = tracing::span!(
                tracing::Level::INFO,
                "test",
                trace_id = trace_id,
                newtag = "test"
            );
            let _e = span.enter();
        }
        let complete = get_completed_spans_from_debug_channel(trace_id);
        assert_eq!(complete.get(0).unwrap().name, "test");
        assert_eq!(complete.get(0).unwrap().meta.get("newtag").unwrap(), "test");
    }

    #[test]
    fn test_span_type_from_tags() {
        let trace_id = create_unique_id64();

        trace_config_with_debug_channel(log::Level::Trace, tracing::Level::TRACE);

        //Make an HTTP span to make sure its type is set to "web"
        {
            let span = http_info_span!(
                "web",
                trace_id,
                http.method = "GET",
                http.url = "/my/path/test1"
            );
            let _e = span.enter();
        }
        //Make a normal span and make sure type is set to "custom"
        {
            let span = tracing::span!(
                tracing::Level::INFO,
                "custom",
                trace_id = trace_id,
                resource = "GET /my/path/test2",
            );
            let _e = span.enter();
        }
        //Make a normal span and set its type
        {
            let span = tracing::span!(
                tracing::Level::INFO,
                "normal",
                trace_id = trace_id,
                span.type = "mytype_normal",
            );
            let _e = span.enter();
        }
        //Make an HTTP span and set its type, make sure override is used
        {
            let span = http_info_span!(
                "override",
                trace_id,
                span.type = "mytype_override",
                http.url = "/wrong/path",
                http.method = "NO"
            );
            let _e = span.enter();
        }
        // Can set type on later event
        {
            let span = http_info_span!("later", trace_id);
            let _e = span.enter();
            add_span_tags!(span.type = "mytype_later");
        }
        // Even when set on later event, type tag should take precedence
        {
            let span = tracing::span!(
                tracing::Level::INFO,
                "later_override",
                trace_id = trace_id,
                span.type = "no"
            );
            let _e = span.enter();
            add_span_tags!(span.type = "mytype_later_override");
        }
        // Even when set on HTTP span in later event, type tag should take precedence
        {
            let span = http_info_span!(
                "later_override_http",
                trace_id,
                http.url = "/wrong/path",
                http.method = "NO"
            );
            let _e = span.enter();
            add_span_tags!(span.type = format!("mytype_later_override_http"));
        }

        let complete = get_completed_spans_from_debug_channel(trace_id);
        assert_eq!(complete.len(), 7);
        assert_eq!(complete.get(0).unwrap().r#type, "web");
        assert_eq!(complete.get(1).unwrap().r#type, "custom");
        complete[2..].into_iter().for_each(|span| {
            let check_type = format!("mytype_{}", span.name);
            assert_eq!(*span.r#type, check_type);
        });
    }

    #[test]
    fn test_resource_tag_from_http_uri_method() {
        let trace_id = create_unique_id64();

        trace_config_with_debug_channel(log::Level::Trace, tracing::Level::TRACE);
        {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id);
            let _e = parent.enter();

            //URL/Method set on span creation
            {
                let span = http_info_span!(
                    "test1",
                    trace_id,
                    http.url = "/my/path/test1",
                    http.method = "GET"
                );
                let _e = span.enter();
            }
            //Resource should take precedence, even if a later event sets HTTP URL/Method
            {
                let span = http_info_span!(
                    "test2",
                    trace_id,
                    resource = "GET /my/path/test2",
                    http.url = "/wrong/path1",
                    http.method = "NO"
                );
                let _e = span.enter();
                add_span_tags!(http.method = "NO", http.url = "/wrong/path2");
            }
            // Can set HTTP URL/method on later event
            {
                let span = http_info_span!("test3", trace_id);
                let _e = span.enter();
                add_span_tags!(http.method = "GET", http.url = "/my/path/test3");
            }
            // Even when set on later event, resource should take precedence
            {
                let span = http_info_span!(
                    "test4",
                    trace_id,
                    http.url = "/wrong/path3",
                    http.method = "NO"
                );
                let _e = span.enter();
                add_span_tags!(resource = "GET /my/path/test4");
                add_span_tags!(http.method = "NO", http.url = "/wrong/path4");
            }
        }
        let mut complete = get_completed_spans_from_debug_channel(trace_id);
        assert_eq!(complete.len(), 5);
        complete.pop();
        complete.into_iter().for_each(|span| {
            let check_path = format!("GET /my/path/{}", span.name);
            assert_eq!(*span.resource, check_path);
        });
    }

    #[test]
    fn test_set_parent_id() {
        let trace_id = create_unique_id64();

        trace_config_with_debug_channel(log::Level::Trace, tracing::Level::TRACE);
        let sib_mutex = std::sync::Arc::new(std::sync::Mutex::new(false));
        let sib_wait = sib_mutex.clone();
        let _g = sib_mutex.lock().unwrap();
        {
            let parent = tracing::span!(
                tracing::Level::INFO,
                "parent",
                trace_id = trace_id,
                parent_id = 111
            );
            let _ep = parent.enter();

            let sibling_span = tracing::span!(tracing::Level::INFO, "sibling", trace_id = trace_id);
            let sibling_id = sibling_span.id().unwrap().into_u64();

            let sib_join = std::thread::spawn(move || {
                let _es = sibling_span.enter();
                let _g = sib_wait.lock().unwrap();
            });
            {
                let span = tracing::span!(
                    tracing::Level::INFO,
                    "child",
                    trace_id = trace_id,
                    parent_id = sibling_id
                );
                let _ec = span.enter();
            }
            {
                let span = tracing::span!(
                    parent: Some(span::Id::from_u64(sibling_id)),
                    tracing::Level::INFO,
                    "child_explicit",
                    trace_id = trace_id,
                );
                let _ece = span.enter();
            }
            drop(_g);
            sib_join.join().unwrap();
        }

        let completed = get_completed_spans_from_debug_channel(trace_id);
        let child = completed.get(0).unwrap();
        let child_explicit = completed.get(1).unwrap();
        let sibling = completed.get(2).unwrap();
        let parent = completed.get(3).unwrap();
        assert_eq!(parent.name, "parent");
        assert_eq!(parent.parent_id, Some(111u64));
        assert_eq!(sibling.name, "sibling");
        assert_eq!(sibling.parent_id, Some(parent.span_id));
        assert_eq!(child.name, "child");
        assert_eq!(child.parent_id, Some(sibling.span_id));
        assert_eq!(child_explicit.name, "child_explicit");
        assert_eq!(child_explicit.parent_id, Some(sibling.span_id));
    }

    #[test]
    fn test_exit_child_span() {
        trace_config_with_debug_channel(log::Level::Trace, tracing::Level::TRACE);
        let trace_id = create_unique_id64();
        let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id);
        let _e = parent.enter();

        let f1: std::thread::JoinHandle<()> = std::thread::spawn(move || {
            info!("Inside parent_span, should print trace and span ID");
            {
                let span = span!(tracing::Level::INFO, "child_span", trace_id = trace_id);
                let _e = span.enter();
                info!("Inside child_span, should print trace and span ID");
            }
            info!("Back in parent_span, should print trace and span ID");
        });
        f1.join().unwrap();
    }

    #[test]
    fn test_trace_one_func_stack() {
        let trace_id = create_unique_id64();

        trace_config_with_debug_channel(log::Level::Trace, tracing::Level::TRACE);
        {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id);
            let _e = parent.enter();

            let f1 = std::thread::spawn(move || {
                traced_func_no_send(trace_id);
            });
            f1.join().unwrap();
            let completed = get_completed_spans_from_debug_channel(trace_id);
            assert_eq!(
                completed.get(0).unwrap().name,
                format!("traced_func_no_send_{trace_id}")
            );
        }
    }

    #[test]
    fn test_parallel_two_threads_two_traces() {
        let trace_id1 = create_unique_id64();
        let trace_id2 = create_unique_id64();

        trace_config_with_debug_channel(log::Level::Trace, tracing::Level::TRACE);

        let f1 = std::thread::spawn(move || {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id1);
            let _e = parent.enter();
            traced_func_no_send(trace_id1);
        });
        let f2 = std::thread::spawn(move || {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id2);
            let _e = parent.enter();
            traced_func_no_send(trace_id2);
        });

        f1.join().unwrap();
        f2.join().unwrap();

        let completed = get_completed_spans_from_debug_channel(trace_id1);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id1}")
        );

        let completed = get_completed_spans_from_debug_channel(trace_id2);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id2}")
        );
    }

    #[test]
    fn test_parallel_ten_threads_ten_traces() {
        let trace_id1 = create_unique_id64();
        let trace_id2 = create_unique_id64();
        let trace_id3 = create_unique_id64();
        let trace_id4 = create_unique_id64();
        let trace_id5 = create_unique_id64();
        let trace_id6 = create_unique_id64();
        let trace_id7 = create_unique_id64();
        let trace_id8 = create_unique_id64();
        let trace_id9 = create_unique_id64();
        let trace_id10 = create_unique_id64();

        trace_config_with_debug_channel(log::Level::Trace, tracing::Level::TRACE);

        let f1 = std::thread::spawn(move || {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id1);
            let _e = parent.enter();
            traced_func_no_send(trace_id1);
        });
        let f2 = std::thread::spawn(move || {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id2);
            let _e = parent.enter();
            traced_func_no_send(trace_id2);
        });
        let f3 = std::thread::spawn(move || {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id3);
            let _e = parent.enter();
            traced_func_no_send(trace_id3);
        });
        let f4 = std::thread::spawn(move || {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id4);
            let _e = parent.enter();
            traced_func_no_send(trace_id4);
        });
        let f5 = std::thread::spawn(move || {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id5);
            let _e = parent.enter();
            traced_func_no_send(trace_id5);
        });
        let f6 = std::thread::spawn(move || {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id6);
            let _e = parent.enter();
            traced_func_no_send(trace_id6);
        });
        let f7 = std::thread::spawn(move || {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id7);
            let _e = parent.enter();
            traced_func_no_send(trace_id7);
        });
        let f8 = std::thread::spawn(move || {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id8);
            let _e = parent.enter();
            traced_func_no_send(trace_id8);
        });
        let f9 = std::thread::spawn(move || {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id9);
            let _e = parent.enter();
            traced_func_no_send(trace_id9);
        });
        let f10 = std::thread::spawn(move || {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id10);
            let _e = parent.enter();
            traced_func_no_send(trace_id10);
        });
        f1.join().unwrap();
        f2.join().unwrap();
        f3.join().unwrap();
        f4.join().unwrap();
        f5.join().unwrap();
        f6.join().unwrap();
        f7.join().unwrap();
        f8.join().unwrap();
        f9.join().unwrap();
        f10.join().unwrap();

        let completed = get_completed_spans_from_debug_channel(trace_id1);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id1}")
        );
        let completed = get_completed_spans_from_debug_channel(trace_id2);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id2}")
        );
        let completed = get_completed_spans_from_debug_channel(trace_id3);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id3}")
        );
        let completed = get_completed_spans_from_debug_channel(trace_id4);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id4}")
        );
        let completed = get_completed_spans_from_debug_channel(trace_id5);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id5}")
        );
        let completed = get_completed_spans_from_debug_channel(trace_id6);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id6}")
        );
        let completed = get_completed_spans_from_debug_channel(trace_id7);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id7}")
        );
        let completed = get_completed_spans_from_debug_channel(trace_id8);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id8}")
        );
        let completed = get_completed_spans_from_debug_channel(trace_id9);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id9}")
        );
        let completed = get_completed_spans_from_debug_channel(trace_id10);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id10}")
        );
    }

    #[test]
    fn test_error_span() {
        let trace_id = create_unique_id64();

        trace_config_with_debug_channel(log::Level::Trace, tracing::Level::TRACE);
        let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id);
        let _e = parent.enter();

        let f3 = std::thread::spawn(move || {
            traced_error_func(trace_id);
        });
        f3.join().unwrap();

        let completed = get_completed_spans_from_debug_channel(trace_id);
        assert_eq!(completed.get(0).unwrap().name, "sleep_call");
        assert_eq!(completed.get(1).unwrap().name, "long_call");
        assert_eq!(completed.get(2).unwrap().name, "traced_error_func");
        assert_eq!(completed.get(2).unwrap().error, 1);
        assert_eq!(
            completed.get(2).unwrap().meta.get("custom_tag").unwrap(),
            "good"
        );
    }

    #[test]
    fn test_error_span_as_single_event() {
        let trace_id = create_unique_id64();

        trace_config_with_debug_channel(log::Level::Trace, tracing::Level::TRACE);
        let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id);
        let _e = parent.enter();
        let f4 = std::thread::spawn(move || {
            traced_error_func_single_event(trace_id);
        });
        f4.join().unwrap();

        let completed = get_completed_spans_from_debug_channel(trace_id);
        assert_eq!(completed.get(0).unwrap().name, "sleep_call");
        assert_eq!(completed.get(1).unwrap().name, "long_call");
        assert_eq!(
            completed.get(2).unwrap().name,
            "traced_error_func_single_event"
        );
        assert_eq!(completed.get(2).unwrap().error, 1);
        assert_eq!(
            completed.get(2).unwrap().meta.get("custom_tag").unwrap(),
            "good"
        );
    }

    #[test]
    fn test_two_funcs_in_one_span() {
        let trace_id = create_unique_id64();

        trace_config_with_debug_channel(log::Level::Trace, tracing::Level::TRACE);
        let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id);
        let _e = parent.enter();

        let f5 = std::thread::spawn(move || {
            traced_func_no_send(trace_id);
            traced_func_no_send(trace_id);
        });
        f5.join().unwrap();

        let completed = get_completed_spans_from_debug_channel(trace_id);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id}")
        );
        assert_eq!(
            completed.get(1).unwrap().name,
            format!("traced_func_no_send_{trace_id}")
        );
    }

    #[test]
    fn test_one_thread_two_funcs_serial_two_traces() {
        let trace_id1 = create_unique_id64();
        let trace_id2 = create_unique_id64();
        trace_config_with_debug_channel(log::Level::Trace, tracing::Level::TRACE);

        let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id1);
        let _e = parent.enter();
        let parent2 = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id2);
        let _e2 = parent2.enter();

        let f7 = std::thread::spawn(move || {
            traced_func_no_send(trace_id1);
            traced_func_no_send(trace_id2);
        });
        f7.join().unwrap();
        let completed = get_completed_spans_from_debug_channel(trace_id1);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id1}")
        );
        let completed = get_completed_spans_from_debug_channel(trace_id2);
        assert_eq!(
            completed.get(0).unwrap().name,
            format!("traced_func_no_send_{trace_id2}")
        );
    }

    #[test]
    fn test_http_span() {
        let trace_id = create_unique_id64();
        trace_config_with_debug_channel(log::Level::Trace, tracing::Level::TRACE);
        {
            let parent = tracing::span!(tracing::Level::INFO, "parent", trace_id = trace_id);
            let _e = parent.enter();

            let f3 = std::thread::spawn(move || {
                traced_http_func(trace_id);
            });
            f3.join().unwrap();
        }
        let completed = get_completed_spans_from_debug_channel(trace_id);
        assert_eq!(completed.get(0).unwrap().name, "sleep_call");
        assert_eq!(completed.get(1).unwrap().name, "long_call");
        assert_eq!(completed.get(2).unwrap().name, "traced_http_func");
        assert_eq!(completed.get(2).unwrap().r#type, "web");
        assert_eq!(completed.get(2).unwrap().error, 0);
    }

    pub mod test_log {
        pub fn test_log_fn() {
            tracing::event!(
                tracing::Level::INFO,
                message = "TEST_INFO EVENT in filtered mod - SHOULD ____NOT____ SEE!!"
            );
            log::info!("TEST_INFO LOG in filtered mod - SHOULD ____NOT____ SEE!!");
        }
    }

    pub mod test_trace {
        pub fn test_trace_fn() {
            tracing::event!(
                tracing::Level::INFO,
                message = "TEST_INFO EVENT in filtered trace mod - SHOULD ____NOT____ SEE!!"
            );
        }
    }

    #[test]
    fn test_log() {
        let _trace_id = create_unique_id64();

        trace_config_with_debug_channel(log::Level::Info, tracing::Level::INFO);
        log::info!("TEST_INFO - SHOULD SEE!!");
        log::debug!("TEST_DEBUG - SHOULD NOT SEE!!");

        test_log::test_log_fn();
    }

    use tracing_subscriber::filter;
    use tracing_subscriber::prelude::*;

    #[test]
    fn test_trace_event_log() {
        let _trace_id = create_unique_id64();

        trace_config_with_debug_channel(log::Level::Info, tracing::Level::INFO);

        tracing::info!("TEST_INFO EVENT - SHOULD SEE!!");
        tracing::debug!("TEST_DEBUG - SHOULD ____NOT____ SEE!!");
        tracing::event!(
            tracing::Level::INFO,
            message = "TEST_INFO EVENT - SHOULD SEE!!"
        );
        tracing::event!(
            tracing::Level::DEBUG,
            message = "TEST_DEBUG EVENT - SHOULD ____NOT____ SEE!!"
        );

        test_log::test_log_fn();
        test_trace::test_trace_fn();
    }
}
