//! WebSocket connection abstraction for streaming Polymarket CLOB market data.

pub mod handlers;

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::time::Instant;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};

use crate::orderbook::{OrderBook, OrderbookMessage};
use crate::polymarket::dtos::{SubscriptionRequest, SubscriptionUpdate};
use crate::polymarket::types::Operation;
use crate::websocket::status::WsStatus;
use crate::websocket::types::{DiscoveryEvent, WsRole};

pub const DEFAULT_BATCH_CAPACITY: usize = 64;

#[derive(Debug, Clone)]
pub enum WsEvent {
    Processed {
        count: usize,
        messages: Vec<OrderbookMessage>,
    },
    Discovery(Vec<DiscoveryEvent>),
    Ping,
    Pong,
    Disconnected,
    Empty,
}

pub struct Ws {
    pub socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pub status: WsStatus,
    pub role: WsRole,
    pub pair_id: usize,
    pub symbols: Vec<String>,
    pub orderbooks: HashMap<String, OrderBook>,
    pub delta_rate_sum: u64,
    pub last_message_at: Instant,
}

impl Ws {
    pub fn new(
        socket: WebSocketStream<MaybeTlsStream<TcpStream>>,
        status: WsStatus,
        role: WsRole,
        pair_id: usize,
    ) -> Self {
        Self {
            socket,
            status,
            role,
            pair_id,
            symbols: Vec::new(),
            orderbooks: HashMap::new(),
            delta_rate_sum: 0,
            last_message_at: Instant::now(),
        }
    }

    /// Returns `true` when this socket holds the Primary role, enabling direct emissions into the global min-heap.
    #[inline]
    pub fn should_write_to_buffer(&self) -> bool {
        self.role == WsRole::Primary
    }

    /// Opens a TLS WebSocket connection to the given URL and returns a fully initialised `Ws` instance.
    pub async fn connect(
        url: &str,
        status: WsStatus,
        role: WsRole,
        pair_id: usize,
    ) -> Result<Self> {
        let (ws_stream, _) = connect_async(url)
            .await
            .map_err(|e| anyhow::anyhow!("Failed WebSocket connection to {url}: {e:?}"))?;
        Ok(Self::new(ws_stream, status, role, pair_id))
    }

    /// Sends an initial subscription frame for the given symbols, registers each symbol in the local orderbook map, and requests a full orderbook dump from the exchange.
    pub async fn subscribe(&mut self, symbols: &[String]) -> Result<()> {
        for s in symbols {
            if !self.symbols.contains(s) {
                self.symbols.push(s.clone());
            }
            self.orderbooks
                .entry(s.clone())
                .or_insert_with(|| OrderBook::new(s.clone()));
        }

        let request = if self.status == WsStatus::Master {
            SubscriptionRequest::with_discovery(symbols.to_vec())
        } else {
            SubscriptionRequest::new(symbols.to_vec())
        };

        let sub_payload =
            serde_json::to_string(&request).context("Failed to serialize SubscriptionRequest")?;

        self.socket
            .send(Message::Text(sub_payload.into()))
            .await
            .context("Failed to send subscription frame")?;

        Ok(())
    }

    /// Transmits a subscription mutation frame (`subscribe` or `unsubscribe`) on the active connection to add or remove symbol coverage.
    pub async fn update_subscription(
        &mut self,
        operation: Operation,
        symbols: &[String],
    ) -> Result<()> {
        let update = SubscriptionUpdate {
            operation,
            assets_ids: symbols.to_vec(),
        };

        let payload =
            serde_json::to_string(&update).context("Failed to serialize SubscriptionUpdate")?;

        self.socket
            .send(Message::Text(payload.into()))
            .await
            .with_context(|| format!("Failed to send {operation:?} frame"))?;

        Ok(())
    }

    /// Registers new symbols on this live socket, initialises their in-memory orderbook entries, and dispatches a dynamic subscribe frame to the exchange.
    pub async fn subscribe_dynamic(&mut self, symbols: &[String]) -> Result<()> {
        for s in symbols {
            if !self.symbols.contains(s) {
                self.symbols.push(s.clone());
            }
            self.orderbooks
                .entry(s.clone())
                .or_insert_with(|| OrderBook::new(s.clone()));
        }

        self.update_subscription(Operation::Subscribe, symbols)
            .await
    }

    /// Removes symbols from the local tracking list, tears down their in-memory orderbook entries, and dispatches an unsubscribe frame to the exchange.
    pub async fn unsubscribe(&mut self, symbols: &[String]) -> Result<()> {
        for s in symbols {
            self.symbols.retain(|sym| sym != s);
            self.orderbooks.remove(s);
        }
        self.update_subscription(Operation::Unsubscribe, symbols)
            .await
    }

    /// Marks this socket as `Disconnected` and sends a WebSocket Close frame to the remote endpoint.
    pub async fn close(&mut self) -> Result<()> {
        self.status = WsStatus::Disconnected;
        let _ = self.socket.close(None).await;
        Ok(())
    }

    /// Transmits a `PING` text frame to the remote endpoint to keep the connection alive.
    pub async fn send_ping(&mut self) -> Result<()> {
        self.socket
            .send(Message::Text("PING".into()))
            .await
            .context("Failed to send PING heartbeat")?;
        Ok(())
    }

    /// Awaits the next frame from the WebSocket stream, dispatches it through the appropriate handler,
    /// updates `last_message_at`, and returns a typed `WsEvent` describing the outcome.
    pub async fn receive_message(&mut self) -> Result<WsEvent> {
        let next_msg = self.socket.next().await;
        match next_msg {
            Some(Ok(Message::Text(text))) => {
                self.last_message_at = Instant::now();
                match self.handle_raw_frame_events(&text) {
                    Ok((count, messages, discovery)) => {
                        if !discovery.is_empty() {
                            Ok(WsEvent::Discovery(discovery))
                        } else {
                            Ok(WsEvent::Processed { count, messages })
                        }
                    }
                    Err(e) => {
                        tracing::warn!("Failed to parse frame: {e} | Raw text: {text:?}");
                        Ok(WsEvent::Empty)
                    }
                }
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
}
