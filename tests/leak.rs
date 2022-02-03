use datadog_apm_sync::{Config, DatadogTracing, LoggingConfig};
use log::debug;
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
        for i in 1..100000000 {
            leak(i).await.unwrap();
            leak(i).await.unwrap();
            event!(tracing::Level::INFO, send_trace = i);
            if i % 10000 == 0 {
                std::thread::sleep(std::time::Duration::from_secs(5));
            }
        }
    });
    f1.await.unwrap();
}

async fn leak(trace_id: u64) -> Result<(), ()> {
    let _span = span!(tracing::Level::INFO, "leak_test", trace_id = trace_id);
    debug!("Span");
    Ok(())
}
