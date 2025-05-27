use tracing::{subscriber::set_global_default};
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

/// Initialize the global tracing subscriber with a custom format layer
pub fn init() -> Result<(), Box<dyn std::error::Error>> {
    let fmt_layer = fmt::layer()
        .pretty()
        .with_target(false)
        .with_level(true)
        .with_ansi(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_file(false)
        .with_line_number(false)
        .with_writer(std::io::stdout);

    let subscriber = tracing_subscriber::registry()
        .with(EnvFilter::from_default_env())
        .with(fmt_layer);

    set_global_default(subscriber)?;
    Ok(())
}
