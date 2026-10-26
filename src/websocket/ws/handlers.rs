//! Message framing and event ingestion handlers for [`Ws`].
//!
//! Converts raw Polymarket frames into domain orderbook deltas and snapshots,
//! updating partitioned orderbooks in-place and returning messages directly for global min-heap ingestion.

use tracing::info;

use super::super::types::{
    DEFAULT_SCALE, DiscoveryEvent, parse_exchange_timestamp, parse_fixed_point_scale,
};
use super::Ws;
use crate::Side;
use crate::orderbook::{OrderbookMessage, TimedDelta};
use crate::polymarket::dtos::{Book, PriceChange, PriceChangeEntry, TickSizeChange};
use crate::polymarket::events::{MarketEvent, parse_frame};
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};
use crate::websocket::status::WsStatus;

impl Ws {
    /// Applies a full `Book` snapshot to the in-memory orderbook for the given asset:
    /// parses bids and asks into fixed-point integers, replaces the resting book state,
    /// replays any buffered deltas that arrived during the synchronization phase from the min-heap,
    /// and returns the `SnapshotDomain` alongside all replayed post-snapshot updates.
    pub fn handle_book(
        &mut self,
        book: &Book,
    ) -> anyhow::Result<(SnapshotDomain, Vec<PriceLevelUpdateDomain>)> {
        let exchange_timestamp = parse_exchange_timestamp(&book.timestamp);
        let received_at = crate::util::local_now();

        let book_state = self
            .orderbooks
            .entry(book.asset_id.clone())
            .or_insert_with(|| crate::orderbook::OrderBook::new(book.asset_id.clone()));

        let mut bids = Vec::with_capacity(book.bids.len());
        for bid in &book.bids {
            let price = parse_fixed_point_scale(&bid.price, DEFAULT_SCALE)?;
            let qty = parse_fixed_point_scale(&bid.size, DEFAULT_SCALE)?;
            bids.push((price, qty));
        }

        let mut asks = Vec::with_capacity(book.asks.len());
        for ask in &book.asks {
            let price = parse_fixed_point_scale(&ask.price, DEFAULT_SCALE)?;
            let qty = parse_fixed_point_scale(&ask.size, DEFAULT_SCALE)?;
            asks.push((price, qty));
        }

        let (snapshot, replayed) =
            book_state.apply_initial_dump(bids, asks, exchange_timestamp, received_at);

        Ok((snapshot, replayed))
    }

    /// Ingests a single `PriceChangeEntry` delta into the per-symbol min-heap:
    /// increments `delta_rate_sum`, converts price and quantity to fixed-point integers,
    /// checks if initial dump arrived; if not, delta remains queued in heap.
    /// If initial dump arrived, pops the lowest-timestamp delta from heap, applies to book,
    /// and returns the resulting `PriceLevelUpdateDomain`.
    pub fn handle_price_change_entry(
        &mut self,
        entry: &PriceChangeEntry,
        timestamp_str: &str,
    ) -> anyhow::Result<Option<PriceLevelUpdateDomain>> {
        self.delta_rate_sum = self.delta_rate_sum.saturating_add(1);

        let exchange_timestamp = parse_exchange_timestamp(timestamp_str);
        let price_level = parse_fixed_point_scale(&entry.price, DEFAULT_SCALE)?;
        let quantity = parse_fixed_point_scale(&entry.size, DEFAULT_SCALE)?;
        let side = match entry.side {
            crate::polymarket::Side::Buy => Side::Buy,
            crate::polymarket::Side::Sell => Side::Sell,
        };

        let book = self
            .orderbooks
            .entry(entry.asset_id.clone())
            .or_insert_with(|| crate::orderbook::OrderBook::new(entry.asset_id.clone()));

        let timed_delta = TimedDelta {
            exchange_ts_micros: exchange_timestamp.timestamp_micros(),
            sequence_counter: self.delta_rate_sum,
            side,
            price_level,
            quantity,
            exchange_timestamp,
            backtest_timestamp: crate::util::local_now(),
            hash: entry.hash.clone(),
        };

        Ok(book.ingest_delta(timed_delta))
    }

    /// Iterates all entries in a `PriceChange` batch, delegates each to `handle_price_change_entry`, and returns the collected `PriceLevelUpdateDomain` updates.
    pub fn handle_price_change(
        &mut self,
        pc: &PriceChange,
    ) -> anyhow::Result<Vec<PriceLevelUpdateDomain>> {
        let mut updates = Vec::with_capacity(pc.price_changes.len());
        for entry in &pc.price_changes {
            if let Some(u) = self.handle_price_change_entry(entry, &pc.timestamp)? {
                updates.push(u);
            }
        }
        Ok(updates)
    }

    /// Converts the tick size values in a `TickSizeChange` event to fixed-point integers and emits a structured log entry recording the old and new tick sizes for this symbol.
    pub fn handle_tick_size_change(&mut self, change: &TickSizeChange) -> anyhow::Result<()> {
        let old_tick_fixed = parse_fixed_point_scale(&change.old_tick_size, DEFAULT_SCALE)?;
        let new_tick_fixed = parse_fixed_point_scale(&change.new_tick_size, DEFAULT_SCALE)?;

        info!(
            symbol = %change.asset_id,
            market = %change.market,
            old_tick = old_tick_fixed,
            new_tick = new_tick_fixed,
            "Tick size updated from {} to {}",
            change.old_tick_size,
            change.new_tick_size
        );

        Ok(())
    }

    /// Deserialises a raw WebSocket text frame into typed `MarketEvent`s, routes each event through the appropriate handler,
    /// surfaces discovery events when Master, and returns applied messages directly for the caller to ingest into the global heap.
    pub fn handle_raw_frame_events(
        &mut self,
        text: &str,
    ) -> anyhow::Result<(usize, Vec<OrderbookMessage>, Vec<DiscoveryEvent>)> {
        let events = parse_frame(text)?;
        let count = events.len();
        let mut discovery = Vec::new();
        let mut messages = Vec::new();

        let should_emit = self.should_write_to_buffer();

        for event in events {
            match event {
                MarketEvent::Book(book) => {
                    let (snapshot, replayed) = self.handle_book(&book)?;
                    if should_emit {
                        messages.push(OrderbookMessage::Snapshot(snapshot));
                        for upd in replayed {
                            messages.push(OrderbookMessage::PriceLevelUpdate(upd));
                        }
                    }
                }
                MarketEvent::PriceChange(pc) => {
                    let updates = self.handle_price_change(&pc)?;
                    if should_emit {
                        for upd in updates {
                            messages.push(OrderbookMessage::PriceLevelUpdate(upd));
                        }
                    }
                }
                MarketEvent::TickSizeChange(t) => {
                    self.handle_tick_size_change(&t)?;
                }
                MarketEvent::NewMarket(m) if self.status == WsStatus::Master => {
                    discovery.push(DiscoveryEvent::NewMarket(m));
                }
                MarketEvent::MarketResolved(r) if self.status == WsStatus::Master => {
                    discovery.push(DiscoveryEvent::MarketResolved(r));
                }
                _ => {}
            }
        }

        Ok((count, messages, discovery))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::polymarket::dtos::{
        Book, MarketResolved, NewMarket, OrderSummary, PriceChangeEntry,
    };
    use crate::polymarket::types::Side as WireSide;
    use crate::websocket::types::WsRole;
    use std::collections::HashMap;
    use std::time::Instant;
    use tokio_tungstenite::WebSocketStream;
    use tokio_tungstenite::tungstenite::protocol::Role;

    async fn create_test_ws(status: WsStatus, role: WsRole) -> Ws {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server_stream, _) = listener.accept().await.unwrap();

        tokio::spawn(async move {
            use tokio::io::AsyncReadExt;
            let mut buf = [0u8; 1024];
            let mut s = server_stream;
            while let Ok(n) = s.read(&mut buf).await {
                if n == 0 {
                    break;
                }
            }
        });

        let stream = tokio_tungstenite::MaybeTlsStream::Plain(client_stream);
        let socket = WebSocketStream::from_raw_socket(stream, Role::Client, None).await;
        Ws {
            socket,
            status,
            role,
            pair_id: 0,
            symbols: Vec::new(),
            orderbooks: HashMap::new(),
            delta_rate_sum: 0,
            last_message_at: Instant::now(),
        }
    }

    #[tokio::test]
    async fn test_handle_book_populates_orderbook() {
        let mut ws = create_test_ws(WsStatus::Healthy, WsRole::Primary).await;

        let book = Book {
            asset_id: "TOKEN_A".to_string(),
            market: "0xmarket_a".to_string(),
            timestamp: "1700000000000".to_string(),
            hash: "0xhash".to_string(),
            bids: vec![OrderSummary {
                price: "0.45".to_string(),
                size: "100.0".to_string(),
            }],
            asks: vec![OrderSummary {
                price: "0.55".to_string(),
                size: "200.0".to_string(),
            }],
        };

        let (snapshot, _) = ws.handle_book(&book).unwrap();
        assert_eq!(snapshot.symbol, "TOKEN_A");
        assert_eq!(snapshot.bids[0].price_level, 450_000);
        assert_eq!(snapshot.bids[0].quantity, 100_000_000);
        assert_eq!(snapshot.asks[0].price_level, 550_000);
        assert_eq!(snapshot.asks[0].quantity, 200_000_000);

        let ob = ws.orderbooks.get("TOKEN_A").unwrap();
        assert_eq!(*ob.bids.get(&450_000).unwrap(), 100_000_000);
        assert_eq!(*ob.asks.get(&550_000).unwrap(), 200_000_000);
        assert!(ob.initial_dump_applied);
    }

    #[tokio::test]
    async fn test_handle_price_change_entry_applies_insert_and_removal_after_snapshot() {
        let mut ws = create_test_ws(WsStatus::Healthy, WsRole::Primary).await;

        let book = Book {
            asset_id: "TOKEN_DELTA".to_string(),
            market: "0xmarket".to_string(),
            timestamp: "1700000000000".to_string(),
            hash: "0xhash".to_string(),
            bids: vec![],
            asks: vec![],
        };
        ws.handle_book(&book).unwrap();

        let buy_entry = PriceChangeEntry {
            asset_id: "TOKEN_DELTA".to_string(),
            price: "0.55".to_string(),
            size: "120.0".to_string(),
            side: WireSide::Buy,
            hash: "0x1".to_string(),
            best_bid: None,
            best_ask: None,
        };

        let update = ws
            .handle_price_change_entry(&buy_entry, "1700000000500")
            .unwrap()
            .unwrap();

        assert_eq!(update.symbol, "TOKEN_DELTA");
        assert_eq!(update.price_level, 550_000);
        assert_eq!(update.quantity, 120_000_000);
        assert_eq!(ws.delta_rate_sum, 1);

        let ob = ws.orderbooks.get("TOKEN_DELTA").unwrap();
        assert_eq!(*ob.bids.get(&550_000).unwrap(), 120_000_000);
        assert!(ob.last_delta_received_at.is_some());

        // Zero-quantity removal
        let remove_entry = PriceChangeEntry {
            asset_id: "TOKEN_DELTA".to_string(),
            price: "0.55".to_string(),
            size: "0".to_string(),
            side: WireSide::Buy,
            hash: "0x2".to_string(),
            best_bid: None,
            best_ask: None,
        };

        let update2 = ws
            .handle_price_change_entry(&remove_entry, "1700000001000")
            .unwrap()
            .unwrap();

        assert_eq!(update2.quantity, 0);
        assert_eq!(ws.delta_rate_sum, 2);

        let ob2 = ws.orderbooks.get("TOKEN_DELTA").unwrap();
        assert!(!ob2.bids.contains_key(&550_000));
    }

    #[tokio::test]
    async fn test_handle_tick_size_change_logs_without_error() {
        let mut ws = create_test_ws(WsStatus::Healthy, WsRole::Primary).await;

        let tick_change = TickSizeChange {
            asset_id: "TOKEN_TICK".to_string(),
            market: "0xmarket_tick".to_string(),
            old_tick_size: "0.01".to_string(),
            new_tick_size: "0.001".to_string(),
            timestamp: "1700000000000".to_string(),
        };

        let res = ws.handle_tick_size_change(&tick_change);
        assert!(res.is_ok());
    }

    #[tokio::test]
    async fn test_handle_raw_frame_events_master_discovery_routing() {
        let mut master = create_test_ws(WsStatus::Master, WsRole::Primary).await;

        let new_market = MarketEvent::NewMarket(Box::new(NewMarket {
            market: "0xmarket_discovered".to_string(),
            assets_ids: vec!["DISC_A".to_string(), "DISC_B".to_string()],
            ..Default::default()
        }));
        let frame = serde_json::to_string(&new_market).unwrap();

        let (count, msgs, discovery) = master.handle_raw_frame_events(&frame).unwrap();
        assert_eq!(count, 1);
        assert!(msgs.is_empty());
        assert_eq!(discovery.len(), 1);
        match &discovery[0] {
            DiscoveryEvent::NewMarket(m) => {
                assert_eq!(m.assets_ids, vec!["DISC_A", "DISC_B"]);
            }
            _ => panic!("Expected DiscoveryEvent::NewMarket"),
        }

        let resolved = MarketEvent::MarketResolved(Box::new(MarketResolved {
            market: "0xmarket_resolved".to_string(),
            winning_asset_id: "WINNER".to_string(),
            ..Default::default()
        }));
        let frame2 = serde_json::to_string(&resolved).unwrap();

        let (count2, msgs2, discovery2) = master.handle_raw_frame_events(&frame2).unwrap();
        assert_eq!(count2, 1);
        assert!(msgs2.is_empty());
        assert_eq!(discovery2.len(), 1);
        match &discovery2[0] {
            DiscoveryEvent::MarketResolved(r) => {
                assert_eq!(r.winning_asset_id, "WINNER");
            }
            _ => panic!("Expected DiscoveryEvent::MarketResolved"),
        }

        // Non-master socket should ignore discovery
        let mut worker = create_test_ws(WsStatus::Healthy, WsRole::Primary).await;
        let (count3, _, discovery3) = worker.handle_raw_frame_events(&frame).unwrap();
        assert_eq!(count3, 1);
        assert!(discovery3.is_empty());
    }
}
