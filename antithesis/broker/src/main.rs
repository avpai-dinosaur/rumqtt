//! rumqttd, built for Antithesis: the same `Broker` as `rumqttd`'s own `main`, with
//! single-line JSON logs on stderr (for the driver's log parser) and the Antithesis
//! instrumentation shim linked in.
use antithesis_instrumentation as _;
use rumqttd::Broker;
use tracing_subscriber::EnvFilter;

fn main() {
    antithesis_sdk::antithesis_init();

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("rumqttd=info"));
    tracing_subscriber::fmt()
        .json()
        .flatten_event(false)
        .with_current_span(true)
        .with_span_list(true)
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();

    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/rumqttd.toml".to_string());
    let config: rumqttd::Config = config::Config::builder()
        .add_source(config::File::with_name(&path))
        .build()
        .expect("config loads")
        .try_deserialize()
        .expect("config deserializes");

    tracing::info!(?path, "rumqttd-antithesis starting");
    let mut broker = Broker::new(config);
    broker.start().expect("broker runs");
}
