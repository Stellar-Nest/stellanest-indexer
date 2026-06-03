use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing::{info, error};

/// Events processed by the dispatcher.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum IndexerEvent {
    IndexUpdated {
        city_code: String,
        new_value: f64,
        change_24h: f64,
        source_count: i32,
    },
    PositionOpened {
        position_id: String,
        user_wallet: String,
        city_code: String,
        direction: String,
        size: f64,
    },
    PositionClosed {
        position_id: String,
        pnl: f64,
        collateral_returned: f64,
    },
    Liquidation {
        position_id: String,
        user_wallet: String,
        city_code: String,
        collateral_seized: f64,
        penalty: f64,
        tx_hash: String,
    },
    TradeExecuted {
        city_code: String,
        price: f64,
        size: f64,
        side: String,
        tx_hash: String,
    },
}

/// Dispatcher processes indexer events and updates the database + Redis.
#[derive(Clone)]
pub struct Dispatcher {
    db: PgPool,
    redis: redis::aio::Connection,
}

impl Dispatcher {
    pub fn new(db: PgPool, redis: redis::aio::Connection) -> Self {
        Self { db, redis }
    }

    /// Dispatch an event to the appropriate handler.
    pub async fn dispatch(&self, event: IndexerEvent) {
        match event {
            IndexerEvent::IndexUpdated { city_code, new_value, change_24h, source_count } => {
                self.handle_index_update(&city_code, new_value, change_24h, source_count).await;
            }
            IndexerEvent::PositionOpened { position_id, user_wallet, city_code, direction, size } => {
                self.handle_position_opened(&position_id, &user_wallet, &city_code, &direction, size).await;
            }
            IndexerEvent::PositionClosed { position_id, pnl, .. } => {
                self.handle_position_closed(&position_id, pnl).await;
            }
            IndexerEvent::Liquidation { position_id, user_wallet, city_code, collateral_seized, penalty, tx_hash } => {
                self.handle_liquidation(&position_id, &user_wallet, &city_code, collateral_seized, penalty, &tx_hash).await;
            }
            IndexerEvent::TradeExecuted { city_code, price, size, side, tx_hash } => {
                self.handle_trade(&city_code, price, size, &side, &tx_hash).await;
            }
        }
    }

    async fn handle_index_update(&self, city: &str, value: f64, change: f64, sources: i32) {
        let res = sqlx::query(
            "UPDATE city_indices SET current_value = $1, change_24h = $2, data_source_count = $3, updated_at = NOW() WHERE city_code = $4"
        )
        .bind(value)
        .bind(change)
        .bind(sources)
        .bind(city)
        .execute(&self.db)
        .await;

        if let Err(e) = res {
            error!("failed to update index for {}: {}", city, e);
            return;
        }

        // Insert history
        match sqlx::query(
            "INSERT INTO index_history (time, city_code, close, source_count) VALUES (NOW(), $1, $2, $3)"
        )
        .bind(city)
        .bind(value)
        .bind(sources)
        .execute(&self.db)
        .await {
            Ok(_) => {},
            Err(e) => {
                error!(error = %e, "failed to insert into index_history");
            }
        }

        info!(city, value, change, "index updated");
    }

    async fn handle_position_opened(&self, id: &str, wallet: &str, city: &str, direction: &str, size: f64) {
        match sqlx::query(
            "INSERT INTO positions (user_wallet, city_code, direction, size, status, soroban_position_id)
             VALUES ($1, $2, $3, $4, 'open', $5)"
        )
        .bind(wallet)
        .bind(city)
        .bind(direction)
        .bind(size)
        .bind(id)
        .execute(&self.db)
        .await {
            Ok(_) => {},
            Err(e) => {
                error!(error = %e, "failed to insert into positions");
            }
        }

        info!(id, wallet, city, direction, size, "position opened");
    }

    async fn handle_position_closed(&self, id: &str, pnl: f64) {
        match sqlx::query(
            "UPDATE positions SET status = 'closed', realized_pnl = $1, closed_at = NOW() WHERE soroban_position_id = $2"
        )
        .bind(pnl)
        .bind(id)
        .execute(&self.db)
        .await {
            Ok(_) => {},
            Err(e) => {
                error!(error = %e, "failed to update positions");
            }
        }

        info!(id, pnl, "position closed");
    }

    async fn handle_liquidation(&self, id: &str, wallet: &str, city: &str, seized: f64, penalty: f64, tx: &str) {
        match sqlx::query(
            "UPDATE positions SET status = 'liquidated', closed_at = NOW() WHERE soroban_position_id = $1"
        )
        .bind(id)
        .execute(&self.db)
        .await {
            Ok(_) => {},
            Err(e) => {
                error!(error = %e, "failed to update positions");
            }
        }

        match sqlx::query(
            "INSERT INTO liquidations (position_id, user_wallet, city_code, collateral_seized, penalty, tx_hash)
             VALUES ($1, $2, $3, $4, $5, $6)"
        )
        .bind(id)
        .bind(wallet)
        .bind(city)
        .bind(seized)
        .bind(penalty)
        .bind(tx)
        .execute(&self.db)
        .await {
            Ok(_) => {},
            Err(e) => {
                error!(error = %e, "failed to insert into liquidations");
            }
        }

        info!(id, seized, penalty, "liquidation processed");
    }

    async fn handle_trade(&self, city: &str, price: f64, size: f64, side: &str, tx: &str) {
        match sqlx::query(
            "INSERT INTO trades (time, city_code, price, size, side, tx_hash) VALUES (NOW(), $1, $2, $3, $4, $5)"
        )
        .bind(city)
        .bind(price)
        .bind(size)
        .bind(side)
        .bind(tx)
        .execute(&self.db)
        .await {
            Ok(_) => {},
            Err(e) => {
                error!(error = %e, "failed to insert into trades");
            }
        }
    }
}
