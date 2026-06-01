use sqlx::PgPool;
use tokio::time::{sleep, Duration};
use tracing::{info, error};

use crate::dispatcher::{Dispatcher, IndexerEvent};

/// Poll interval for DEX trades.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// Watch Stellar DEX for city index asset trades.
pub async fn run(db: PgPool, horizon_url: String, dispatcher: Dispatcher) {
    info!("DEX watcher started");

    let mut cursor: Option<String> = None;

    loop {
        match poll_trades(&horizon_url, cursor.as_deref()).await {
            Ok((trades, next_cursor)) => {
                for event in trades {
                    dispatcher.dispatch(event).await;
                }
                cursor = Some(next_cursor);
            }
            Err(e) => {
                error!("DEX poll failed: {}", e);
            }
        }

        sleep(POLL_INTERVAL).await;
    }
}

/// Fetch recent trades from Stellar Horizon.
async fn poll_trades(
    horizon_url: &str,
    cursor: Option<&str>,
) -> anyhow::Result<(Vec<IndexerEvent>, String)> {
    let url = format!("{}/trades?order=desc&limit=100", horizon_url);

    let resp = reqwest::get(&url).await?;
    let body: serde_json::Value = resp.json().await?;

    let mut events = Vec::new();
    let mut next_cursor = String::new();

    if let Some(records) = body["_embedded"]["records"].as_array() {
        for record in records {
            // Filter for city index assets (SRE_* pattern)
            let base_asset = record["base_asset_code"].as_str().unwrap_or("");
            if !base_asset.starts_with("SRE_") {
                continue;
            }

            let city_code = base_asset.trim_start_matches("SRE_").to_string();
            let price: f64 = record["price"]["n"]
                .as_f64()
                .unwrap_or(0.0) / record["price"]["d"].as_f64().unwrap_or(1.0);
            let size: f64 = record["base_amount"].as_f64().unwrap_or(0.0);

            events.push(IndexerEvent::TradeExecuted {
                city_code,
                price,
                size,
                side: "buy".into(), // Simplified
                tx_hash: record["paging_token"].as_str().unwrap_or("").into(),
            });

            next_cursor = record["paging_token"].as_str().unwrap_or("").to_string();
        }
    }

    Ok((events, next_cursor))
}
