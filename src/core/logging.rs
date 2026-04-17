use tracing_subscriber::{EnvFilter, fmt};

pub fn init_logging() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let subscriber = fmt()
        .with_env_filter(filter)
        .json()
        .with_current_span(false)
        .with_span_list(false)
        .finish();
    let _ = tracing::subscriber::set_global_default(subscriber);
}
