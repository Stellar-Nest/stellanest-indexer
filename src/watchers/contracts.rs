use sqlx::PgPool;
use tokio::time::{sleep, Duration};
use tracing::{info, error, warn};

use crate::dispatcher::{Dispatcher, IndexerEvent};

/// Poll interval for contract events.
const POLL_INTERVAL: Duration = Duration::from_secs(10);

/// Watch Soroban contract events (index_updated, position_*, liquidation).
pub async fn run(db: PgPool, horizon_url: String, dispatcher: Dispatcher) {
    info!("contract event watcher started");

    let mut last_ledger: u64 = 0;

    loop {
        match poll_events(&horizon_url, last_ledger).await {
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

/// Extract the event name from the Horizon record topic field.
///
/// Horizon returns `topic` as an array of base64-encoded Soroban SCVal values.
/// The first element typically represents the event symbol. We attempt to read
/// it as a plain string first and fall back to heuristic detection based on the
/// event data shape.
fn extract_event_name(record: &serde_json::Value) -> String {
    // Try topic as a plain string (simplified / mocked responses)
    if let Some(s) = record["topic"].as_str() {
        if !s.is_empty() && !s.contains('=') {
            return s.to_string();
        }
    }

    // Try first element of topic array
    if let Some(topics) = record["topic"].as_array() {
        if let Some(first) = topics.first().and_then(|t| t.as_str()) {
            if !first.contains('=') {
                return first.to_string();
            }
        }
    }

    // Fall back to heuristic detection from event data shape
    let value = &record["value"];
    if value.get("position_id").is_some() {
        if value.get("collateral_seized").is_some() {
            return "liquidation".to_string();
        }
        if value.get("pnl").is_some() {
            return "position_closed".to_string();
        }
        return "position_opened".to_string();
    }
    if value.get("new_value").is_some()
        || (value.get("city_code").is_some() && value.get("position_id").is_none())
    {
        return "index_updated".to_string();
    }

    String::new()
}

/// Fetch Soroban contract events from Stellar Horizon.
async fn poll_events(
    horizon_url: &str,
    last_ledger: u64,
) -> anyhow::Result<(Vec<IndexerEvent>, u64)> {
    let url = format!(
        "{}/events?order=desc&limit=100",
        horizon_url
    );

    let resp = reqwest::get(&url).await?;
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

            match event_type {
                "contract" => {
                    let event_name = extract_event_name(record);
                    let value = &record["value"];
                    let tx_hash = record["tx_hash"].as_str().unwrap_or("");

                    info!(event = %event_name, ledger, "contract event");

                    match event_name.as_str() {
                        "position_opened" => {
                            events.push(IndexerEvent::PositionOpened {
                                position_id: value["position_id"]
                                    .as_str()
                                    .unwrap_or("")
                                    .to_string(),
                                user_wallet: value["user"]
                                    .as_str()
                                    .or_else(|| value["user_wallet"].as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                city_code: value["city"]
                                    .as_str()
                                    .or_else(|| value["city_code"].as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                direction: value["direction"]
                                    .as_str()
                                    .unwrap_or("long")
                                    .to_string(),
                                size: value["size"].as_f64().unwrap_or(0.0),
                            });
                        }
                        "position_closed" => {
                            events.push(IndexerEvent::PositionClosed {
                                position_id: value["position_id"]
                                    .as_str()
                                    .unwrap_or("")
                                    .to_string(),
                                pnl: value["pnl"].as_f64().unwrap_or(0.0),
                                collateral_returned: value["collateral_returned"]
                                    .as_f64()
                                    .unwrap_or(0.0),
                            });
                        }
                        "liquidation" => {
                            events.push(IndexerEvent::Liquidation {
                                position_id: value["position_id"]
                                    .as_str()
                                    .unwrap_or("")
                                    .to_string(),
                                user_wallet: value["user"]
                                    .as_str()
                                    .or_else(|| value["user_wallet"].as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                city_code: value["city"]
                                    .as_str()
                                    .or_else(|| value["city_code"].as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                collateral_seized: value["collateral_seized"]
                                    .as_f64()
                                    .unwrap_or(0.0),
                                penalty: value["penalty"].as_f64().unwrap_or(0.0),
                                tx_hash: if !tx_hash.is_empty() {
                                    tx_hash.to_string()
                                } else {
                                    value["tx_hash"]
                                        .as_str()
                                        .unwrap_or("")
                                        .to_string()
                                },
                            });
                        }
                        "index_updated" => {
                            events.push(IndexerEvent::IndexUpdated {
                                city_code: value["city"]
                                    .as_str()
                                    .or_else(|| value["city_code"].as_str())
                                    .unwrap_or("")
                                    .to_string(),
                                new_value: value["new_value"]
                                    .as_f64()
                                    .or_else(|| value["value"].as_f64())
                                    .unwrap_or(0.0),
                                change_24h: value["change_24h"].as_f64().unwrap_or(0.0),
                                source_count: value["source_count"]
                                    .as_i64()
                                    .unwrap_or(0) as i32,
                            });
                        }
                        _ => {
                            warn!(topic = %event_name, "unknown contract event type");
                        }
                    }
                }
                _ => {}
            }
        }
    }

    Ok((events, max_ledger))
}
