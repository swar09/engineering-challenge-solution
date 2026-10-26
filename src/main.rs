use anyhow::{Context, Result};
use tracing::{info, warn};

use engineering_challenge::gamma::GammaClient;
use engineering_challenge::symbol::SymbolState;
use engineering_challenge::websocket::status::WsStatus;
use engineering_challenge::websocket::ws::Ws;
use engineering_challenge::websocket::ws_manager::WsManager;
use engineering_challenge::writer::{SbeBoundedWriter, BOUNDED_CHANNEL_CAPACITY};

const NUM_ACTIVE_SOCKETS: usize = 5;
const NUM_STANDBY_SOCKETS: usize = 2;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    // 1. Initialize structured logging
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    info!("Starting Polymarket Clean Ingestion Engine...");

    let polymarket_ws_url = std::env::var("POLYMARKET_WS_URL")
        .unwrap_or_else(|_| "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string());

    // 2. Query GammaClient for top 50 symbols
    let gamma_client = GammaClient::new();
    info!("Querying Gamma API for top active markets...");
    let symbols = match gamma_client.fetch_top_50_clob_tokens().await {
        Ok(tokens) => {
            info!("Fetched {} tokens from Gamma API", tokens.len());
            tokens
        }
        Err(e) => {
            warn!("Failed to fetch tokens from Gamma API ({e}), using default fallback tokens");
            vec![
                "32338220190071351435772801779725302244575775216413325951443816017994629993401"
                    .to_string(),
                "25659310674993675562345759665114759892400026242514633218387667107987341231962"
                    .to_string(),
            ]
        }
    };

    // 3. Spawn SbeBoundedWriter background thread
    let data_dir = std::env::var("DATA_DIR").unwrap_or_else(|_| "data".to_string());
    info!("Spawning SbeBoundedWriter OS thread in directory {data_dir:?}...");
    let (writer_tx, writer_rx) = crossbeam::channel::bounded(BOUNDED_CHANNEL_CAPACITY);
    let writer_handle = SbeBoundedWriter::spawn(&data_dir, writer_rx)
        .context("Failed to spawn SbeBoundedWriter background thread")?;

    // 4. Initialize WsManager: connect active socket pool and seed symbols equally
    info!("Initializing WsManager and connecting {NUM_ACTIVE_SOCKETS} active sockets...");
    let mut ws_manager = WsManager::new(writer_tx);

    let active_count = NUM_ACTIVE_SOCKETS.max(1);
    let mut socket_symbols: Vec<Vec<String>> = vec![Vec::new(); active_count];
    for (i, sym) in symbols.iter().enumerate() {
        let target_sock = i % active_count;
        socket_symbols[target_sock].push(sym.clone());
        ws_manager
            .symbol_states
            .insert(sym.clone(), SymbolState::new(sym.clone(), target_sock));
    }

    for (idx, assigned) in socket_symbols.iter().enumerate().take(active_count) {
        let status = if idx == 0 {
            WsStatus::Master
        } else {
            WsStatus::Healthy
        };

        match Ws::connect(&polymarket_ws_url, status).await {
            Ok(mut ws) => {
                if !assigned.is_empty() {
                    if let Err(e) = ws.subscribe(assigned).await {
                        warn!("Socket #{idx} failed to subscribe to initial symbols: {e}");
                    } else {
                        info!("Socket #{idx} ({status:?}) subscribed to {} symbols", assigned.len());
                    }
                }
                ws_manager.pool.push(ws);
            }
            Err(e) => {
                warn!("Failed to connect initial active socket #{idx}: {e}");
            }
        }
    }

    // 5. Pre-warm standby sockets in standby_pool
    info!("Pre-warming {NUM_STANDBY_SOCKETS} standby sockets in standby_pool...");
    for idx in 0..NUM_STANDBY_SOCKETS {
        match Ws::connect(&polymarket_ws_url, WsStatus::Healthy).await {
            Ok(ws) => {
                info!("Standby socket #{idx} pre-warmed successfully");
                ws_manager.standby_pool.push_back(ws);
            }
            Err(e) => {
                warn!("Failed to pre-warm standby socket #{idx}: {e}");
            }
        }
    }

    // 6. Run event loop: poll sockets, maintain delta_rate_sum, route discovery events on Master
    // 7. Execute periodic 60-second full orderbook snapshots
    // 8. Intercept SIGINT (Ctrl+C): invoke flush_all(), snapshot resting books, and close file handles cleanly
    let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(10));
    // First tick fires immediately, consume it
    ping_interval.tick().await;

    let mut snapshot_interval = tokio::time::interval(std::time::Duration::from_secs(60));
    snapshot_interval.tick().await;

    let mut flush_interval = tokio::time::interval(std::time::Duration::from_millis(250));
    flush_interval.tick().await;

    info!("Engine running. Polling active sockets...");

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("Received SIGINT (Ctrl+C). Initiating graceful shutdown...");
                break;
            }
            _ = ping_interval.tick() => {
                ws_manager.send_pings().await;
            }
            _ = snapshot_interval.tick() => {
                info!("Executing periodic 60-second full orderbook snapshots...");
                ws_manager.snapshot_resting_books();
                ws_manager.flush_all();
            }
            _ = flush_interval.tick() => {
                ws_manager.flush_all();
            }
            res = ws_manager.poll_next_message() => {
                if let Err(e) = res {
                    warn!("Error during message poll: {e}");
                }
            }
        }
    }

    // Graceful teardown
    info!("Shutdown sequence: snapshotting resting books...");
    ws_manager.snapshot_resting_books();
    info!("Shutdown sequence: flushing all batch buffers...");
    ws_manager.flush_all();
    info!("Shutdown sequence: closing all active and standby websockets cleanly...");
    ws_manager.close_all().await;
    drop(ws_manager);

    info!("Waiting for SbeBoundedWriter to flush and sync 64 MiB direct-IO chunks...");
    writer_handle.join()?;
    info!("Ingestion engine closed cleanly.");

    Ok(())
}
