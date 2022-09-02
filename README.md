# Datadog apm (sync-bsed) for Rust (original fork from datadog-apm)

[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)
![CI](https://github.com/kitsuneninetails/datadog-apm-rust-sync/workflows/CI/badge.svg)

Credits
-------
Originally based on a fork from <https://github.com/pipefy/datadog-apm-rust>.
Original code written by Fernando Gonçalves (github: <https://github.com/fhsgoncalves>).

Credit and my gratitude go to the original author for the original code.  This repo builds on top 
of his hard work and research.


Usage
------

Add to your `Cargo.toml`:
```toml
tracing = "0.1"
tracing-futures = "*"
datadog-apm-sync = {version = "0.5.1", git = "http://github.com/kitsuneninetails/datadog-apm-rust-sync"}
```

In your Rust code, instantiate a DatadogTracer:

```text
# {
    let config = Config {
        service: "service_name".into(),
        logging_config: Some(LoggingConfig {
            level: Level::Debug,
            ..LoggingConfig::default()
        }),
        enable_tracing: true,
        ..Default::default()
    };
    let _client = DatadogTracing::new(config);
#}
```

Then, just use the tracing library normally (with #[instrument] tags, and span! + span.enter() code) and your data
will log with trace-id/span-id where applicable (due to the `logging_config` beign passed in; pass in `None` to disable
the DatadogTracing as a logger) and will prepare datadog traces via the tracing::Subscriber calls (due to `enable_tracing`
being set; set to `false` to disable the DatadogTracing as a tracing subscriber).

Use events (event! macro) to send error information (use the keys: error_msg, error_stack, and error_type) or 
HTTP metdata information (use http_url, http_method, and http_status_code keys).  Also, to actually force through the 
send to Datadog, send an event:

```text
event!(tracing::Level::INFO, send_trace=true);
```

Other Changes from Original
------
* Removed tokio crate.  
* Removed all mention of async/await.
* Changed MPSC to std::sync version rather than tokio::sync version.
* Wrapped MPSC Sender channel in a Arc-Mutex to compensate for not being tokio version.
* Changed the calls to the channels to align to std::sync version API.

The reason behind these changes were twofold.  First, the code wasn't working out-of-the-box due to collisions
with the tokio::sync::mpsc channels, which would panic when used at the same time.  Second, minor alterations of the 
code only led to failures, panics, and code blocks simply not running due to the async/await.  These two points 
rendered this service unable to be used in production for my purposes, and obfuscated a lot of the inner processing 
of the code (specifically, it was hard to understand why the busy-wait loop was never running, or why certain 
parts of the code never got called), meaning it could not be trusted in production in case something went wrong. 

The specific modifications to move to sync was made since the only I/O-bound function in this whole service was 
the call to the Datadog Agent API, and as this is usually a localhost service, it's not even very I/O bound.  
Given the busy-wait loop already in place to handle the sending of the API calls, it didn't seem necessary
to introduce a complicated async/await system (which wasn't working out of the box).  Removing this 
greatly reduced the complexity, and raised the understandability and clarity of the code, which makes me a lot 
more confident to use this in production.

In addition, the following changes were made to the architecture and implementation, as some of the design 
decisions and code structure didn't align with my needs:

* Removed the buffering for the datadog send.  As the busy-wait loop contains a channel (which serves as a buffer),
  and as it is basically sending to a local dogstatsd agent (which itself handles buffering to send to Datadog server),
  buffering in this code just adds complexity with little gain in the way of performance or safety.
* Altered the API to just send single traces.
* Removed the map-packing of the data (which was not working and would error out sending to DataDog Agent).  
  Just use JSON instead (which the DataDog Agent API supports).
* Split the Client into a DatadogTracing struct which just holds the Sender channel (which is all a user of this 
  module needs, as all they do is send a trace to the channel) and a "DdAgentClient" which is only used by the busy-wait 
  loop to read from the channel and send to DataDog Agent API. 
* Moved the Trace and Span structs into a "model" module, which can be used to build the model separately 
  prior to sending via the "DatadogTracing" struct.
* Moved the internal client-specific code to "api" (to hold the DataDog Agent API objects) along with the
  transformers on each respective struct (RawTrace- which is just a wrapper around Vec<RawSpan>, and RawSpan) to
  convert the model struct to the api struct needed for the DataDog Agent API.
* Now, only "DatadogTracing" is exposed publically, as the DdAgentClient is only used internally.
* Moved tests to match the module.

This project is licensed under the [MIT license](LICENSE).
