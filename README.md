# Stellanest — Indexer

Rust service that watches the Stellar ledger and indexes on-chain data.

## Watchers

| Watcher | Polls | Does |
|---|---|---|
| **DEX** | Horizon `/trades` every 5s | Records city index trades, updates volume/VWAP |
| **Contracts** | Horizon `/events` every 10s | Processes Soroban events (index_updated, position_*, liquidation) |
| **Health** | DB query every 30s | Recalculates P&L and health factors, flags at-risk positions |

## Setup

```bash
export DATABASE_URL="postgres://stellanest:stellanest@localhost:5432/stellanest"
export REDIS_URL="redis://localhost:6379"
export STELLAR_HORIZON="https://horizon-testnet.stellar.org"

cargo run
```
