use anyhow::{Context, Result};
use tracing::{info, warn};

use engineering_challenge::gamma::GammaClient;
use engineering_challenge::symbol::SymbolState;
use engineering_challenge::websocket::status::WsStatus;
use engineering_challenge::websocket::subscription_manager::SubscriptionManager;
use engineering_challenge::websocket::types::WsRole;
use engineering_challenge::websocket::ws::Ws;
use engineering_challenge::websocket::ws_manager::{
    DEFAULT_MAX_ACTIVE_PAIRS, DEFAULT_MAX_SYMBOLS_PER_SOCKET, DEFAULT_MIN_ACTIVE_PAIRS,
    MIN_STANDBY_CAPACITY, WsManager,
};
use engineering_challenge::writer::{BOUNDED_CHANNEL_CAPACITY, SbeBoundedWriter};
use rand::seq::SliceRandom;

const DEFAULT_POLYMARKET_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("Starting Polymarket Ingestion Engine");

    let polymarket_ws_url = std::env::var("POLYMARKET_WS_URL")
        .unwrap_or_else(|_| DEFAULT_POLYMARKET_WS_URL.to_string());

    let gamma_client = GammaClient::new();
    info!("Querying Gamma API active markets");
    let mut symbols = gamma_client
        .fetch_all_symbols()
        .await
        .context("Failed to fetch symbols from Gamma API")?;
    if symbols.is_empty() {
        warn!("Zero symbols discovered from Gamma API");
        anyhow::bail!("Gamma API returned 0 tokens");
    }
    info!(
        "Discovered {} symbols from Gamma API. Sample: {:?}",
        symbols.len(),
        symbols.iter().take(5).collect::<Vec<_>>()
    );

    symbols.shuffle(&mut rand::rng());
    info!("Shuffled symbols across sockets");

    let min_pairs = std::env::var("MIN_ACTIVE_PAIRS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MIN_ACTIVE_PAIRS);
    let max_pairs = std::env::var("MAX_ACTIVE_PAIRS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_ACTIVE_PAIRS);
    let max_symbols_per_socket = std::env::var("MAX_SYMBOLS_PER_SOCKET")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_MAX_SYMBOLS_PER_SOCKET);

    // Calculate required socket pairs dynamically based on discovered symbol count and per-socket ceiling
    let calculated_pairs = (symbols.len().div_ceil(max_symbols_per_socket))
        .max(min_pairs)
        .min(max_pairs);
    info!(
        "Topology: {} active pairs (bounds: [{min_pairs}, {max_pairs}], max {max_symbols_per_socket} symbols/socket)",
        calculated_pairs
    );

    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "data".to_string());
    info!("Spawning SbeBoundedWriter thread in directory {data_dir:?}");
    let (writer_tx, writer_rx) = crossbeam::channel::bounded(BOUNDED_CHANNEL_CAPACITY);
    let writer_handle = SbeBoundedWriter::spawn(&data_dir, writer_rx)
        .context("Failed to spawn SbeBoundedWriter background thread")?;

    info!("Initializing WsManager with {calculated_pairs} pairs");
    let subscription_manager = SubscriptionManager::new(std::time::Duration::from_secs(15), 5, 1);
    let mut ws_manager = WsManager::new(writer_tx, subscription_manager);

    let active_count = calculated_pairs;
    let mut socket_symbols: Vec<Vec<String>> = vec![Vec::new(); active_count];
    for (i, sym) in symbols.iter().enumerate() {
        let target_pair = i % active_count;
        socket_symbols[target_pair].push(sym.clone());
        // Primary sockets occupy even indices: 2 * target_pair
        ws_manager
            .symbol_states
            .insert(sym.clone(), SymbolState::new(sym.clone(), 2 * target_pair));
    }

    for (pair_id, assigned) in socket_symbols.iter().enumerate().take(active_count) {
        let status = if pair_id == 0 {
            WsStatus::Master
        } else {
            WsStatus::Healthy
        };

        // Primary socket
        match Ws::connect(&polymarket_ws_url, status, WsRole::Primary, pair_id).await {
            Ok(mut primary_ws) => {
                if !assigned.is_empty() {
                    if let Err(e) = primary_ws.subscribe(assigned).await {
                        warn!("Pair #{pair_id} Primary subscribe error: {e}");
                    } else {
                        info!(
                            "Pair #{pair_id} Primary ({status:?}) subscribed: {} symbols",
                            assigned.len()
                        );
                    }
                }
                ws_manager.pool.push(primary_ws);
            }
            Err(e) => {
                warn!("Pair #{pair_id} Primary connect error: {e}");
            }
        }

        match Ws::connect(
            &polymarket_ws_url,
            WsStatus::Healthy,
            WsRole::Replica,
            pair_id,
        )
        .await
        {
            Ok(mut replica_ws) => {
                if !assigned.is_empty() {
                    if let Err(e) = replica_ws.subscribe(assigned).await {
                        warn!("Pair #{pair_id} Replica subscribe error: {e}");
                    } else {
                        info!(
                            "Pair #{pair_id} Replica subscribed: {} symbols",
                            assigned.len()
                        );
                    }
                }
                ws_manager.pool.push(replica_ws);
            }
            Err(e) => {
                warn!("Pair #{pair_id} Replica connect error: {e}");
            }
        }
    }

    info!("Pre-warming {MIN_STANDBY_CAPACITY} standby sockets");
    for idx in 0..MIN_STANDBY_CAPACITY {
        match Ws::connect(
            &polymarket_ws_url,
            WsStatus::Healthy,
            WsRole::Replica,
            usize::MAX,
        )
        .await
        {
            Ok(ws) => {
                info!("Standby #{idx} pre-warmed");
                ws_manager.standby_pool.push_back(ws);
            }
            Err(e) => {
                warn!("Standby #{idx} pre-warm error: {e}");
            }
        }
    }

    let ping_secs = std::env::var("PING_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10);
    let snapshot_secs = std::env::var("SNAPSHOT_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(60);

    let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(ping_secs));
    ping_interval.tick().await;

    let mut snapshot_interval =
        tokio::time::interval(std::time::Duration::from_secs(snapshot_secs));
    snapshot_interval.tick().await;

    let mut subscription_check_interval = tokio::time::interval(std::time::Duration::from_secs(2));
    subscription_check_interval.tick().await;

    let mut stats_interval = tokio::time::interval(std::time::Duration::from_secs(5));
    stats_interval.tick().await;

    info!("Engine running. Polling sockets");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("SIGINT received. Initiating graceful shutdown");
                break;
            }
            _ = stats_interval.tick() => {
                let total_deltas = ws_manager.total_deltas_received();
                let symbols_count = ws_manager.symbol_states.len();
                let active_pairs = ws_manager.pool.len() / 2;
                info!(
                    target: "feedback",
                    "Symbols: {symbols_count} | Active Pairs: {active_pairs} | Total Deltas: {total_deltas}"
                );
            }
            _ = ping_interval.tick() => {
                ws_manager.send_pings().await;
                for idx in 0..ws_manager.pool.len() {
                    if ws_manager.pool[idx].status == WsStatus::Unhealthy {
                        warn!("Ws died: socket #{idx}. Triggering hot swap");
                        ws_manager.handle_socket_disconnect(idx, &polymarket_ws_url).await;
                    }
                }
                ws_manager.prune_and_ping_standby(&polymarket_ws_url).await;
            }
            _ = subscription_check_interval.tick() => {
                ws_manager.check_and_process_subscriptions().await;
            }
            _ = snapshot_interval.tick() => {
                info!("Executing 60s orderbook snapshots");
                ws_manager.snapshot_resting_books();
            }
            res = ws_manager.poll_next_message(&polymarket_ws_url) => {
                if let Err(e) = res {
                    warn!("Poll error: {e}");
                }
            }
        }
    }

    // Graceful teardown
    info!("Closing websockets");
    ws_manager.close_all().await;
    info!("Flushing batch buffers");
    ws_manager.flush_all();
    info!("Snapshotting resting books");
    ws_manager.snapshot_resting_books();
    drop(ws_manager);

    info!("Flushing SbeBoundedWriter direct-IO chunks");
    writer_handle.join()?;

    info!("Engine shutdown completed");
    Ok(())
}
