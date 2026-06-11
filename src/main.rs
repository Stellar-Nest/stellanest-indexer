use sqlx::postgres::PgPoolOptions;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

mod dispatcher;
mod watchers;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "stellanest_indexer=debug".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://stellanest:stellanest@localhost:5432/stellanest".into());
    let redis_url = std::env::var("REDIS_URL")
        .unwrap_or_else(|_| "redis://localhost:6379".into());
    let horizon_url = std::env::var("STELLAR_HORIZON")
        .unwrap_or_else(|_| "https://horizon-testnet.stellar.org".into());

    // Connect
    let db = PgPoolOptions::new()
        .max_connections(10)
        .connect(&database_url)
        .await?;
    tracing::info!("connected to PostgreSQL");

    let redis_client = redis::Client::open(redis_url)?;
    let redis = redis_client.get_multiplexed_async_connection().await?;
    tracing::info!("connected to Redis (multiplexed)");

    let dispatcher = dispatcher::Dispatcher::new(db.clone(), redis);

    // Shared HTTP client with timeouts
    let http_client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(30))
        .build()?;
    tracing::info!("HTTP client configured with 10s connect / 30s request timeouts");

    // Spawn watchers
    let dex_handle = {
        let db = db.clone();
        let horizon = horizon_url.clone();
        let disp = dispatcher.clone();
        let client = http_client.clone();
        tokio::spawn(async move {
            watchers::dex::run(db, horizon, disp, client).await;
        })
    };

    let contract_handle = {
        let db = db.clone();
        let horizon = horizon_url.clone();
        let disp = dispatcher.clone();
        let client = http_client.clone();
        tokio::spawn(async move {
            watchers::contracts::run(db, horizon, disp, client).await;
        })
    };

    let health_handle = {
        let db = db.clone();
        let disp = dispatcher.clone();
        tokio::spawn(async move {
            watchers::health::run(db, disp).await;
        })
    };

    tracing::info!("Stellanest indexer started");

    // Wait for shutdown
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down...");

    dex_handle.abort();
    contract_handle.abort();
    health_handle.abort();

    Ok(())
}
