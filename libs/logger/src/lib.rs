use tracing_log::LogTracer;
use tracing::subscriber;
use tracing_subscriber::{FmtSubscriber, EnvFilter};
use std::error::Error;

pub fn init() -> Result<(), Box<dyn Error + Send + Sync + 'static>> {
    LogTracer::init()?;

    let subscriber = FmtSubscriber::builder()
        .with_env_filter(EnvFilter::from_default_env())
        .finish();

    subscriber::set_global_default(subscriber)?;

    Ok(())
}
