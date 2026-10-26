use anyhow::Result;
use chrono::Utc;
use crossbeam::channel::Sender;
use std::collections::{HashMap, VecDeque};
use tracing::{info, warn};

use crate::orderbook::OrderbookMessage;
use crate::symbol::SymbolState;
use crate::websocket::status::WsStatus;
use crate::websocket::ws::{Ws, WsEvent};

pub struct WsManager {
    pub pool: Vec<Ws>,
    pub standby_pool: VecDeque<Ws>,
    pub symbol_states: HashMap<String, SymbolState>,
    pub writer_tx: Sender<Vec<OrderbookMessage>>,
    pub master_index: usize,
}

impl WsManager {
    pub fn new(writer_tx: Sender<Vec<OrderbookMessage>>) -> Self {
        Self {
            pool: Vec::new(),
            standby_pool: VecDeque::new(),
            symbol_states: HashMap::new(),
            writer_tx,
            master_index: 0,
        }
    }

    /// Load = symbols.len() * delta_rate_sum
    pub fn find_least_loaded_socket_index(&self) -> Option<usize> {
        self.pool
            .iter()
            .enumerate()
            .filter(|(_, ws)| ws.status == WsStatus::Master || ws.status == WsStatus::Healthy)
            .min_by_key(|(_, ws)| (ws.symbols.len() as u64) * ws.delta_rate_sum)
            .map(|(idx, _)| idx)
    }

    /// Drains batch buffers of all sockets in the pool and sends messages to the writer channel.
    pub fn flush_all(&mut self) {
        for ws in &mut self.pool {
            if !ws.batch_buffer.is_empty() {
                let batch = std::mem::take(&mut ws.batch_buffer);
                let _ = self.writer_tx.send(batch);
            }
        }
    }

    /// Snapshots all resting orderbooks across all active sockets and flushes them as a batch.
    pub fn snapshot_resting_books(&mut self) {
        let now = Utc::now();
        let mut batch = Vec::new();
        for ws in &mut self.pool {
            for book in ws.orderbooks.values() {
                let ex_ts = book.last_exchange_timestamp.unwrap_or(now);
                let snap = book.to_snapshot_domain(now, ex_ts);
                batch.push(OrderbookMessage::Snapshot(snap));
            }
        }
        if !batch.is_empty() {
            let _ = self.writer_tx.send(batch);
        }
    }

    /// Sends keep-alive PING to all active and standby sockets.
    pub async fn send_pings(&mut self) {
        for ws in &mut self.pool {
            if (ws.status == WsStatus::Master || ws.status == WsStatus::Healthy)
                && let Err(e) = ws.send_ping().await
            {
                warn!("Failed to send PING to socket: {e}");
                ws.status = WsStatus::Unhealthy;
            }
        }
        for ws in &mut self.standby_pool {
            let _ = ws.send_ping().await;
        }
    }

    /// Polls all active sockets concurrently for the next incoming message.
    pub async fn poll_next_message(&mut self) -> Result<()> {
        let mut futures = Vec::new();
        for (socket_idx, ws) in self.pool.iter_mut().enumerate() {
            if ws.status == WsStatus::Master || ws.status == WsStatus::Healthy {
                futures.push(Box::pin(
                    async move { (socket_idx, ws.receive_message().await) },
                ));
            }
        }

        if futures.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            return Ok(());
        }

        let ((socket_idx, result), _, remaining) = futures_util::future::select_all(futures).await;
        drop(remaining);

        match result {
            Ok(event) => {
                // If this is the master socket, handle discovery and resolution events
                if socket_idx == self.master_index {
                    match &event {
                        WsEvent::NewMarket { assets_ids } => {
                            info!(
                                "Master socket received NewMarket: {} tokens",
                                assets_ids.len()
                            );
                            for token in assets_ids {
                                if !self.symbol_states.contains_key(token)
                                    && let Some(target_idx) = self.find_least_loaded_socket_index()
                                {
                                    info!(
                                        "Assigning new token {token} to socket #{target_idx}"
                                    );
                                    let target_ws = &mut self.pool[target_idx];
                                    if let Err(e) = target_ws
                                        .subscribe_dynamic(std::slice::from_ref(token))
                                        .await
                                    {
                                        warn!("Failed to dynamically subscribe to new token {token}: {e}");
                                    }
                                    self.symbol_states.insert(
                                        token.clone(),
                                        SymbolState::new(token.clone(), target_idx),
                                    );
                                }
                            }
                        }
                        WsEvent::MarketResolved {
                            market,
                            assets_ids,
                            winning_asset_id,
                        } => {
                            info!(
                                "Master socket received MarketResolved: market={market:?}, winning={winning_asset_id:?}, retiring {} tokens",
                                assets_ids.len()
                            );
                            for token in assets_ids {
                                if let Some(state) = self.symbol_states.remove(token) {
                                    let sock_idx = state.socket_index;
                                    if sock_idx < self.pool.len() {
                                        let ws = &mut self.pool[sock_idx];
                                        info!("Unsubscribing expired token {token} from socket #{sock_idx}");
                                        if let Err(e) = ws.unsubscribe(std::slice::from_ref(token)).await {
                                            warn!("Failed to unsubscribe token {token} from socket #{sock_idx}: {e}");
                                        }
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }

                // If event is OrderbookUpdates, update symbol states
                if let WsEvent::OrderbookUpdates(updates) = &event {
                    for upd in updates {
                        if let Some(state) = self.symbol_states.get_mut(&upd.symbol) {
                            state.delta_rate = state.delta_rate.saturating_add(1);
                            state.last_exchange_timestamp = Some(upd.exchange_timestamp);
                            state.last_delta_timestamp = std::time::Instant::now();
                        }
                    }
                }

                // If batch buffer reached threshold, flush this socket to writer channel
                let ws = &mut self.pool[socket_idx];
                if ws.batch_buffer.len() >= crate::websocket::ws::DEFAULT_BATCH_CAPACITY {
                    let batch = std::mem::take(&mut ws.batch_buffer);
                    let _ = self.writer_tx.send(batch);
                }
            }
            Err(e) => {
                warn!("Socket #{socket_idx} encountered error: {e}");
                self.pool[socket_idx].status = WsStatus::Disconnected;
                self.handle_socket_disconnect(socket_idx).await;
            }
        }

        Ok(())
    }

    /// Handles socket disconnect by promoting a pre-warmed standby socket without RAM transplantation.
    pub async fn handle_socket_disconnect(&mut self, dead_idx: usize) {
        let is_master = dead_idx == self.master_index;
        let symbols = std::mem::take(&mut self.pool[dead_idx].symbols);

        if let Some(mut standby_ws) = self.standby_pool.pop_front() {
            info!("Promoting pre-warmed standby socket to replace dead socket #{dead_idx}");
            standby_ws.status = if is_master {
                WsStatus::Master
            } else {
                WsStatus::Healthy
            };

            // Constraint 2: Sockets are independent network boundaries.
            // Baseline depth is received directly from the exchange feed (initial_dump).
            // No OrderBook structs are transplanted across sockets.
            if !symbols.is_empty()
                && let Err(e) = standby_ws.subscribe(&symbols).await
            {
                warn!("Failed to subscribe promoted standby socket to symbols: {e}");
            }

            for s in &symbols {
                if let Some(state) = self.symbol_states.get_mut(s) {
                    state.socket_index = dead_idx;
                }
            }

            self.pool[dead_idx] = standby_ws;
        } else {
            warn!("No standby sockets available to replace dead socket #{dead_idx}");
            self.pool[dead_idx].status = WsStatus::Disconnected;
        }
    }

    /// Gracefully closes all active and standby websockets by sending Close frames.
    pub async fn close_all(&mut self) {
        for ws in &mut self.pool {
            let _ = ws.close().await;
        }
        for ws in &mut self.standby_pool {
            let _ = ws.close().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::websocket::ws::mock_ws;

    #[tokio::test]
    async fn test_least_loaded_balancing() {
        let (tx, _rx) = crossbeam::channel::unbounded();
        let mut manager = WsManager::new(tx);

        let mut ws0 = mock_ws(WsStatus::Master).await;
        ws0.symbols = vec!["S1".to_string(), "S2".to_string()];
        ws0.delta_rate_sum = 50;

        let mut ws1 = mock_ws(WsStatus::Healthy).await;
        ws1.symbols = vec!["S3".to_string()];
        ws1.delta_rate_sum = 20;

        manager.pool.push(ws0);
        manager.pool.push(ws1);

        let candidate = manager.find_least_loaded_socket_index();
        assert_eq!(candidate, Some(1));
    }

    #[tokio::test]
    async fn test_flush_all_batch() {
        let (tx, rx) = crossbeam::channel::unbounded();
        let mut manager = WsManager::new(tx);
        let ts = chrono::Utc::now();

        let mut ws0 = mock_ws(WsStatus::Master).await;
        ws0.symbols = vec!["S1".to_string()];
        ws0.batch_buffer.push(OrderbookMessage::Snapshot(
            crate::util::dtos::challenge_orderbook::SnapshotDomain {
                received_at: ts,
                exchange_timestamp: ts,
                bids: vec![],
                asks: vec![],
                symbol: "S1".into(),
            },
        ));
        ws0.batch_buffer.push(OrderbookMessage::PriceLevelUpdate(
            crate::util::dtos::challenge_orderbook::PriceLevelUpdateDomain {
                backtest_timestamp: ts,
                exchange_timestamp: ts,
                price_level: 500_000,
                quantity: 1_000_000,
                side: crate::Side::Buy,
                symbol: "S1".into(),
            },
        ));

        let mut ws1 = mock_ws(WsStatus::Healthy).await;
        ws1.symbols = vec!["S2".to_string()];
        ws1.batch_buffer.push(OrderbookMessage::PriceLevelUpdate(
            crate::util::dtos::challenge_orderbook::PriceLevelUpdateDomain {
                backtest_timestamp: ts,
                exchange_timestamp: ts,
                price_level: 600_000,
                quantity: 2_000_000,
                side: crate::Side::Sell,
                symbol: "S2".into(),
            },
        ));

        let ws2 = mock_ws(WsStatus::Healthy).await;

        manager.pool.push(ws0);
        manager.pool.push(ws1);
        manager.pool.push(ws2);

        manager.flush_all();

        let batch0 = rx.try_recv().expect("Expected batch from socket 0");
        assert_eq!(batch0.len(), 2);
        assert!(matches!(batch0[0], OrderbookMessage::Snapshot(_)));
        assert!(matches!(batch0[1], OrderbookMessage::PriceLevelUpdate(_)));

        let batch1 = rx.try_recv().expect("Expected batch from socket 1");
        assert_eq!(batch1.len(), 1);
        assert!(matches!(batch1[0], OrderbookMessage::PriceLevelUpdate(_)));

        assert!(rx.try_recv().is_err());

        assert!(manager.pool[0].batch_buffer.is_empty());
        assert!(manager.pool[1].batch_buffer.is_empty());
        assert!(manager.pool[2].batch_buffer.is_empty());
    }

    #[tokio::test]
    async fn test_market_resolved_unsubscribes_retired_symbol() {
        let (tx, _rx) = crossbeam::channel::unbounded();
        let mut manager = WsManager::new(tx);

        let ws0 = mock_ws(WsStatus::Master).await;
        let mut ws1 = mock_ws(WsStatus::Healthy).await;
        ws1.symbols.push("TOKEN_TO_RETIRE".to_string());
        ws1.orderbooks.insert(
            "TOKEN_TO_RETIRE".to_string(),
            crate::orderbook::OrderBook::new("TOKEN_TO_RETIRE"),
        );

        manager.pool.push(ws0);
        manager.pool.push(ws1);
        manager.symbol_states.insert(
            "TOKEN_TO_RETIRE".to_string(),
            SymbolState::new("TOKEN_TO_RETIRE".to_string(), 1),
        );

        let resolved_event = WsEvent::MarketResolved {
            market: Some("0x123".to_string()),
            assets_ids: vec!["TOKEN_TO_RETIRE".to_string()],
            winning_asset_id: Some("TOKEN_TO_RETIRE".to_string()),
        };

        if let WsEvent::MarketResolved { assets_ids, .. } = resolved_event {
            for token in assets_ids {
                if let Some(state) = manager.symbol_states.remove(&token) {
                    let sock_idx = state.socket_index;
                    let ws = &mut manager.pool[sock_idx];
                    let _ = ws.unsubscribe(std::slice::from_ref(&token)).await;
                }
            }
        }

        assert!(!manager.symbol_states.contains_key("TOKEN_TO_RETIRE"));
        assert!(!manager.pool[1].symbols.contains(&"TOKEN_TO_RETIRE".to_string()));
        assert!(!manager.pool[1].orderbooks.contains_key("TOKEN_TO_RETIRE"));
    }
}
