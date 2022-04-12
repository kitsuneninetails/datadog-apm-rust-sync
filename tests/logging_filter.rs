use datadog_apm_sync::{Config, DatadogTracing, LoggingConfig};

mod my_test {
    use log::error;
    pub fn test_log() {
        error!("TEST");
    }
}

mod my_test_start_with {
    use log::error;
    pub fn test_log() {
        error!("TEST");
    }
}

mod in_the_my_test_middle {
    use log::error;
    pub fn test_log() {
        error!("TEST");
    }
}

mod ends_with_my_test {
    use log::error;
    pub fn test_log() {
        error!("TEST");
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore]
async fn test_log_filters() {
    let config = Config {
        service: String::from("datadog_apm_test"),
        env: Some("staging-01".into()),
        logging_config: Some(LoggingConfig {
            level: log::Level::Error,
            mod_filter: vec!["my_test"],
            ..LoggingConfig::default()
        }),
        enable_tracing: true,
        ..Default::default()
    };
    let _client = DatadogTracing::new(config);

    my_test::test_log();
    my_test_start_with::test_log();
    ends_with_my_test::test_log();
    in_the_my_test_middle::test_log();
}
