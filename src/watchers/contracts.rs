use sqlx::PgPool;
use tokio::time::{sleep, Duration};
use tracing::{info, error};

use crate::dispatcher::{Dispatcher, IndexerEvent};

/// Poll interval for contract events.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Watch Soroban contract events (index_updated, position_*, liquidation).
pub async fn run(db: PgPool, horizon_url: String, dispatcher: Dispatcher, http_client: reqwest::Client) {
    info!("contract event watcher started");

    let mut last_ledger: u64 = 0;

    loop {
        match poll_events(&http_client, &horizon_url, last_ledger).await {
            Ok((events, max_ledger)) => {
                for event in events {
                    dispatcher.dispatch(event).await;
                }
                if max_ledger > last_ledger {
                    last_ledger = max_ledger;
                }
            }
            Err(e) => {
                error!("contract poll failed: {}", e);
            }
        }

        sleep(POLL_INTERVAL).await;
    }
}

/// Fetch Soroban contract events from Stellar Horizon.
async fn poll_events(
    http_client: &reqwest::Client,
    horizon_url: &str,
    last_ledger: u64,
) -> anyhow::Result<(Vec<IndexerEvent>, u64)> {
    let url = format!(
        "{}/events?order=desc&limit=100",
        horizon_url
    );

    let resp = http_client.get(&url).send().await?;
    let body: serde_json::Value = resp.json().await?;

    let mut events = Vec::new();
    let mut max_ledger = last_ledger;

    if let Some(records) = body["_embedded"]["records"].as_array() {
        for record in records {
            let ledger: u64 = record["ledger"].as_u64().unwrap_or(0);
            if ledger <= last_ledger {
                continue;
            }
            max_ledger = max_ledger.max(ledger);

            let event_type = record["type"].as_str().unwrap_or("");
            let topic = record["topic"].as_str().unwrap_or("");

            match event_type {
                "contract" => {
                    // Parse Soroban contract events
                    // TODO: Decode event data based on topic
                    info!(topic, ledger, "contract event");
                }
                _ => {}
            }
        }
    }

    Ok((events, max_ledger))
}
