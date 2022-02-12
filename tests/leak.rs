use datadog_apm_sync::{Config, DatadogTracing, LoggingConfig};
use log::warn;
use tracing::{event, span};

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_leak() {
    let config = Config {
        service: String::from("datadog_apm_test"),
        env: Some("staging-01".into()),
        logging_config: Some(LoggingConfig {
            level: log::Level::Warn,
            mod_filter: vec!["hyper", "mime", "datadog_apm_sync::client"],
            ..LoggingConfig::default()
        }),
        enable_tracing: true,
        ..Default::default()
    };
    let _client = DatadogTracing::new(config);

    let f1 = tokio::spawn(async {
        for i in 1..100_000_000 {
            let rate_limit = false; //(i % 100_000) > 99_900;
            leak(i, rate_limit).await.unwrap();
            leak(i, rate_limit).await.unwrap();
            event!(tracing::Level::INFO, send_trace = i);
        }
    });
    f1.await.unwrap();
}

async fn leak(trace_id: u64, rate_limit: bool) -> Result<(), ()> {
    let _span = span!(tracing::Level::INFO, "leak_test", trace_id = trace_id);
    warn!("Span1");
    warn!("Span2");
    warn!("Span3");
    warn!("Span4");
    leak_inner1(trace_id).await;
    leak_inner2(trace_id).await;
    if rate_limit {
        std::thread::sleep(std::time::Duration::from_secs(1));
    }
    Ok(())
}

async fn leak_inner1(trace_id: u64) {
    let _span = span!(tracing::Level::INFO, "leak_test_inner", trace_id = trace_id);
    warn!("Span_inner1");
    warn!("Span_inner2");
    warn!("Span_inner3");
    leak_inner_inner(trace_id).await;
}

async fn leak_inner2(trace_id: u64) {
    let _span = span!(tracing::Level::INFO, "leak_test_inner", trace_id = trace_id);
    warn!("Span_inner1b");
    warn!("Span_inner2b");
    warn!("Span_inner3b");
}

async fn leak_inner_inner(trace_id: u64) {
    let _span = span!(tracing::Level::INFO, "leak_test_inner", trace_id = trace_id);
    warn!("Span_inner_inner1");
    warn!("Span_inner_inner2");
    warn!("Span_inner_inner3");
}
