use std::collections::HashMap;
use std::time::Instant;
use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::orderbook::{parse_fixed_point, OrderBook, OrderbookMessage};
use crate::websocket::status::WsStatus;
use crate::Side;

const WS_HOST: &str = "ws-subscriptions-clob.polymarket.com";
const WS_PORT: u16 = 443;

async fn connect_tcp_stream(host: &str, port: u16) -> Result<TcpStream> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    if let Ok(proxy_str) = std::env::var("https_proxy") {
        if !proxy_str.is_empty() {
            let clean = proxy_str
                .trim_start_matches("http://")
                .trim_start_matches("https://");
            let proxy_addr = clean.split('/').next().unwrap_or(clean);
            if let Ok(mut stream) = TcpStream::connect(proxy_addr).await {
                stream.set_nodelay(true)?;
                let connect_req =
                    format!("CONNECT {host}:{port} HTTP/1.1\r\nHost: {host}:{port}\r\n\r\n");
                stream.write_all(connect_req.as_bytes()).await?;

                let mut buf = [0u8; 1024];
                let mut total_read = 0;
                while total_read < buf.len() {
                    let n = stream.read(&mut buf[total_read..]).await?;
                    if n == 0 {
                        break;
                    }
                    total_read += n;
                    if buf[..total_read].windows(4).any(|w| w == b"\r\n\r\n") {
                        break;
                    }
                }

                let resp = String::from_utf8_lossy(&buf[..total_read]);
                if resp.starts_with("HTTP/1.1 200") || resp.starts_with("HTTP/1.0 200") {
                    return Ok(stream);
                }
            }
        }
    }

    let stream = TcpStream::connect(format!("{host}:{port}"))
        .await
        .with_context(|| format!("Failed to connect directly to {host}:{port}"))?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

pub const DEFAULT_BATCH_CAPACITY: usize = 64;

#[derive(Debug, Clone, PartialEq)]
pub struct SymbolDeltaUpdate {
    pub symbol: String,
    pub exchange_timestamp: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub enum WsEvent {
    OrderbookUpdates(Vec<SymbolDeltaUpdate>),
    NewMarket {
        assets_ids: Vec<String>,
    },
    MarketResolved {
        market: Option<String>,
        assets_ids: Vec<String>,
        winning_asset_id: Option<String>,
    },
    Ping,
    Pong,
    Disconnected,
    Empty,
}

pub struct Ws {
    pub socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pub status: WsStatus,
    pub symbols: Vec<String>,
    pub orderbooks: HashMap<String, OrderBook>,
    pub batch_buffer: Vec<OrderbookMessage>,
    pub delta_rate_sum: u64,
    pub last_message_at: Instant,
}

impl Ws {
    pub fn new(socket: WebSocketStream<MaybeTlsStream<TcpStream>>, status: WsStatus) -> Self {
        Self {
            socket,
            status,
            symbols: Vec::new(),
            orderbooks: HashMap::new(),
            batch_buffer: Vec::with_capacity(DEFAULT_BATCH_CAPACITY),
            delta_rate_sum: 0,
            last_message_at: Instant::now(),
        }
    }

    /// Connects to the Polymarket WebSocket endpoint via TLS
    pub async fn connect(url: &str, status: WsStatus) -> Result<Self> {
        let tcp_stream = connect_tcp_stream(WS_HOST, WS_PORT).await?;
        let (ws_stream, _) =
            tokio_tungstenite::client_async_tls_with_config(url, tcp_stream, None, None)
                .await
                .map_err(|e| anyhow::anyhow!("Failed WebSocket TLS handshake: {url}: {e:?}"))?;
        Ok(Self::new(ws_stream, status))
    }

    /// Initial subscription when opening connection for the given symbols.
    pub async fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        for s in symbols {
            if !self.symbols.contains(s) {
                self.symbols.push(s.clone());
            }
            self.orderbooks
                .entry(s.clone())
                .or_insert_with(|| OrderBook::new(s.clone()));
        }

        let sub_payload = serde_json::json!({
            "type": "market",
            "assets_ids": symbols,
            "initial_dump": true,
            "custom_feature_enabled": true
        });

        self.socket
            .send(Message::Text(sub_payload.to_string().into()))
            .await
            .context("Failed to send subscription frame")?;

        Ok(())
    }

    /// Dynamically sends an update operation frame (subscribe or unsubscribe) on an active socket.
    pub async fn update_subscription(&mut self, operation: &str, symbols: &[String]) -> Result<()> {
        let payload = serde_json::json!({
            "operation": operation,
            "assets_ids": symbols
        });

        self.socket
            .send(Message::Text(payload.to_string().into()))
            .await
            .with_context(|| format!("Failed to send {operation} frame"))?;

        Ok(())
    }

    /// Dynamically subscribes to new symbols on an active socket without reconnecting.
    pub async fn subscribe_dynamic(&mut self, symbols: &[String]) -> Result<()> {
        for s in symbols {
            if !self.symbols.contains(s) {
                self.symbols.push(s.clone());
            }
            self.orderbooks
                .entry(s.clone())
                .or_insert_with(|| OrderBook::new(s.clone()));
        }

        self.update_subscription("subscribe", symbols).await
    }

    /// Dynamically unsubscribes from symbols and cleans up resting orderbooks.
    pub async fn unsubscribe(&mut self, symbols: &[String]) -> Result<()> {
        for s in symbols {
            self.symbols.retain(|sym| sym != s);
            self.orderbooks.remove(s);
        }
        self.update_subscription("unsubscribe", symbols).await
    }

    /// Gracefully closes this WebSocket stream by sending a Close frame.
    pub async fn close(&mut self) -> Result<()> {
        self.status = WsStatus::Disconnected;
        let _ = self.socket.close(None).await;
        Ok(())
    }

    /// Sends a heartbeat PING text frame to keep the connection alive.
    pub async fn send_ping(&mut self) -> Result<()> {
        self.socket
            .send(Message::Text("PING".into()))
            .await
            .context("Failed to send PING heartbeat")?;
        Ok(())
    }

    /// Receives and processes the next message from the WebSocket stream.
    pub async fn receive_message(&mut self) -> Result<WsEvent> {
        let next_msg = self.socket.next().await;
        match next_msg {
            Some(Ok(Message::Text(text))) => {
                self.last_message_at = Instant::now();
                Ok(self.process_text_payload(&text))
            }
            Some(Ok(Message::Ping(payload))) => {
                self.last_message_at = Instant::now();
                let _ = self.socket.send(Message::Pong(payload)).await;
                Ok(WsEvent::Ping)
            }
            Some(Ok(Message::Pong(_))) => {
                self.last_message_at = Instant::now();
                Ok(WsEvent::Pong)
            }
            Some(Ok(Message::Close(_))) => {
                self.status = WsStatus::Disconnected;
                Ok(WsEvent::Disconnected)
            }
            Some(Err(e)) => {
                self.status = WsStatus::Disconnected;
                Err(anyhow::anyhow!("WebSocket read error: {e}"))
            }
            None => {
                self.status = WsStatus::Disconnected;
                Ok(WsEvent::Disconnected)
            }
            _ => Ok(WsEvent::Empty),
        }
    }

    /// Processes text payload without network IO
    pub fn process_text_payload(&mut self, text: &str) -> WsEvent {
        let trimmed = text.trim();
        if trimmed.eq_ignore_ascii_case("pong") {
            return WsEvent::Pong;
        }

        let parsed: Value = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => return WsEvent::Empty,
        };

        if let Some(arr) = parsed.as_array() {
            let mut total_updates = Vec::new();
            let mut last_event = WsEvent::Empty;
            for item in arr {
                match self.process_json_value(item) {
                    WsEvent::OrderbookUpdates(updates) => total_updates.extend(updates),
                    ev @ (WsEvent::NewMarket { .. } | WsEvent::MarketResolved { .. }) => {
                        last_event = ev;
                    }
                    _ => {}
                }
            }
            if !total_updates.is_empty() {
                WsEvent::OrderbookUpdates(total_updates)
            } else {
                last_event
            }
        } else {
            self.process_json_value(&parsed)
        }
    }

    fn process_json_value(&mut self, val: &Value) -> WsEvent {
        let event_type = val
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or_default();

        match event_type {
            "book" => {
                let asset_id = val
                    .get("asset_id")
                    .and_then(|v| v.as_str())
                    .or_else(|| val.get("market").and_then(|v| v.as_str()))
                    .unwrap_or_default()
                    .to_string();

                if asset_id.is_empty() {
                    return WsEvent::Empty;
                }

                let exchange_ts = val
                    .get("timestamp")
                    .map(parse_exchange_timestamp)
                    .unwrap_or_else(Utc::now);

                let mut bids = Vec::new();
                if let Some(bids_val) = val.get("bids").and_then(|v| v.as_array()) {
                    for b in bids_val {
                        let p_str = b.get("price").and_then(|v| v.as_str()).unwrap_or("0");
                        let s_str = b.get("size").and_then(|v| v.as_str()).unwrap_or("0");
                        if let (Some(p), Some(s)) =
                            (parse_fixed_point(p_str), parse_fixed_point(s_str))
                        {
                            bids.push((p, s));
                        }
                    }
                }

                let mut asks = Vec::new();
                if let Some(asks_val) = val.get("asks").and_then(|v| v.as_array()) {
                    for a in asks_val {
                        let p_str = a.get("price").and_then(|v| v.as_str()).unwrap_or("0");
                        let s_str = a.get("size").and_then(|v| v.as_str()).unwrap_or("0");
                        if let (Some(p), Some(s)) =
                            (parse_fixed_point(p_str), parse_fixed_point(s_str))
                        {
                            asks.push((p, s));
                        }
                    }
                }

                let book = self
                    .orderbooks
                    .entry(asset_id.clone())
                    .or_insert_with(|| OrderBook::new(asset_id.clone()));
                let snap = book.apply_snapshot(bids, asks, exchange_ts, Utc::now());
                self.batch_buffer.push(OrderbookMessage::Snapshot(snap));

                WsEvent::OrderbookUpdates(vec![SymbolDeltaUpdate {
                    symbol: asset_id,
                    exchange_timestamp: exchange_ts,
                }])
            }
            "price_change" => {
                let exchange_ts = val
                    .get("timestamp")
                    .map(parse_exchange_timestamp)
                    .unwrap_or_else(Utc::now);

                let mut updates = Vec::new();
                if let Some(changes) = val.get("price_changes").and_then(|v| v.as_array()) {
                    for change in changes {
                        // Pre-aggregated load tracking: update in-line during message parsing
                        self.delta_rate_sum = self.delta_rate_sum.saturating_add(1);

                        let asset_id = change
                            .get("asset_id")
                            .and_then(|v| v.as_str())
                            .unwrap_or_default()
                            .to_string();

                        if asset_id.is_empty() {
                            continue;
                        }

                        let side_str =
                            change.get("side").and_then(|v| v.as_str()).unwrap_or("BUY");
                        let side = if side_str.eq_ignore_ascii_case("buy") {
                            Side::Buy
                        } else {
                            Side::Sell
                        };

                        let p_str = change.get("price").and_then(|v| v.as_str()).unwrap_or("0");
                        let s_str = change.get("size").and_then(|v| v.as_str()).unwrap_or("0");
                        let p = parse_fixed_point(p_str).unwrap_or(0);
                        let s = parse_fixed_point(s_str).unwrap_or(0);

                        let book = self
                            .orderbooks
                            .entry(asset_id.clone())
                            .or_insert_with(|| OrderBook::new(asset_id.clone()));
                        let delta = book.apply_delta(side, p, s, exchange_ts, Utc::now());
                        self.batch_buffer
                            .push(OrderbookMessage::PriceLevelUpdate(delta));

                        updates.push(SymbolDeltaUpdate {
                            symbol: asset_id,
                            exchange_timestamp: exchange_ts,
                        });
                    }
                }

                WsEvent::OrderbookUpdates(updates)
            }
            "new_market" => {
                let mut assets_ids = Vec::new();
                if let Some(ids) = val.get("assets_ids").and_then(|v| v.as_array()) {
                    for id in ids {
                        if let Some(s) = id.as_str() {
                            assets_ids.push(s.to_string());
                        }
                    }
                }
                WsEvent::NewMarket { assets_ids }
            }
            "market_resolved" => {
                let market = val.get("market").and_then(|v| v.as_str()).map(String::from);
                let winning_asset_id = val
                    .get("winning_asset_id")
                    .and_then(|v| v.as_str())
                    .map(String::from);
                let mut assets_ids = Vec::new();
                if let Some(ids) = val.get("assets_ids").and_then(|v| v.as_array()) {
                    for id in ids {
                        if let Some(s) = id.as_str() {
                            assets_ids.push(s.to_string());
                        }
                    }
                }
                WsEvent::MarketResolved {
                    market,
                    assets_ids,
                    winning_asset_id,
                }
            }
            _ => WsEvent::Empty,
        }
    }
}

fn parse_exchange_timestamp(val: &Value) -> DateTime<Utc> {
    let millis = match val {
        Value::Number(n) => n.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    };
    millis
        .and_then(DateTime::from_timestamp_millis)
        .unwrap_or_else(Utc::now)
}

#[cfg(test)]
pub async fn mock_ws(status: WsStatus) -> Ws {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client_stream = tokio::net::TcpStream::connect(addr).await.unwrap();
    client_stream.set_nodelay(true).unwrap();
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
    let stream = MaybeTlsStream::Plain(client_stream);
    let socket = WebSocketStream::from_raw_socket(
        stream,
        tokio_tungstenite::tungstenite::protocol::Role::Client,
        None,
    )
    .await;
    Ws::new(socket, status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_process_book_and_price_change() {
        let mut ws = mock_ws(WsStatus::Healthy).await;
        ws.symbols.push("T1".to_string());

        let book_json = r#"{
            "event_type": "book",
            "asset_id": "T1",
            "timestamp": "1700000000000",
            "bids": [{"price": "0.45", "size": "100.0"}],
            "asks": [{"price": "0.55", "size": "150.0"}]
        }"#;

        let ev = ws.process_text_payload(book_json);
        assert!(matches!(ev, WsEvent::OrderbookUpdates(ref u) if u.len() == 1));
        assert_eq!(ws.batch_buffer.len(), 1);
        assert_eq!(ws.delta_rate_sum, 0);

        let ob = ws.orderbooks.get("T1").unwrap();
        assert_eq!(*ob.bids.get(&450_000).unwrap(), 100_000_000);
        assert_eq!(*ob.asks.get(&550_000).unwrap(), 150_000_000);

        let delta_json = r#"{
            "event_type": "price_change",
            "timestamp": "1700000001000",
            "price_changes": [
                {"asset_id": "T1", "price": "0.46", "size": "50.0", "side": "BUY"},
                {"asset_id": "T1", "price": "0.45", "size": "0", "side": "BUY"}
            ]
        }"#;

        let ev2 = ws.process_text_payload(delta_json);
        assert!(matches!(ev2, WsEvent::OrderbookUpdates(ref u) if u.len() == 2));
        assert_eq!(ws.batch_buffer.len(), 3);
        assert_eq!(ws.delta_rate_sum, 2);

        let ob2 = ws.orderbooks.get("T1").unwrap();
        assert_eq!(*ob2.bids.get(&460_000).unwrap(), 50_000_000);
        assert!(!ob2.bids.contains_key(&450_000));
    }

    #[tokio::test]
    async fn test_live_ws_connect() {
        dotenvy::dotenv().ok();
        let ws_url = std::env::var("POLYMARKET_WS_URL")
            .unwrap_or_else(|_| "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string());
        match Ws::connect(&ws_url, WsStatus::Master).await {
            Ok(mut ws) => {
                assert_eq!(ws.status, WsStatus::Master);
                let _ = ws.send_ping().await;
            }
            Err(e) => {
                eprintln!("External Polymarket WS unreachable in current environment: {e}");
            }
        }
    }
}
