pub mod kalshi;
pub mod polymarket;

pub use kalshi::KalshiCodec;
pub use polymarket::PolymarketCodec;

use chrono::{DateTime, Utc};

/// Representation of order book side in the normalized engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalizedSide {
    Buy,
    Sell,
}

/// Normalized snapshot for seeding local orderbook ladder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedSnapshot {
    pub symbol: String,
    pub exchange_timestamp: DateTime<Utc>,
    pub bids: Vec<(i64, i64)>, // (fixed-point price, fixed-point quantity)
    pub asks: Vec<(i64, i64)>,
}

/// Delta semantics: whether the quantity replaces or adds to the resting ladder level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaOp {
    /// Overwrites existing level with absolute quantity (0 removes level). Used by Polymarket.
    SetAbsolute,
    /// Adds/subtracts quantity at level (resulting <= 0 removes level). Used by Kalshi.
    AddRelative,
}

/// Normalized price level delta.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NormalizedDelta {
    pub symbol: String,
    pub exchange_timestamp: DateTime<Utc>,
    pub side: NormalizedSide,
    pub price: i64,
    pub quantity: i64,
    pub op: DeltaOp,
}

/// Normalized discovery / market lifecycle event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizedDiscovery {
    MarketCreated {
        market_id: String,
        symbols: Vec<String>,
        metadata: Option<String>,
    },
    MarketResolved {
        market_id: String,
        symbols: Vec<String>,
        winning_symbol: Option<String>,
    },
}

/// Normalized high-level event yielded by the codec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NormalizedEvent {
    Snapshot(NormalizedSnapshot),
    Delta(NormalizedDelta),
    Discovery(NormalizedDiscovery),
    HeartbeatPing,
    HeartbeatPong,
    Ignored,
}

/// Contract that all prediction market exchange codecs must implement.
pub trait ExchangeCodec: Send + Sync + 'static {
    /// Human-readable exchange identifier (e.g. "polymarket", "kalshi").
    const NAME: &'static str;

    /// Primary production WebSocket URL.
    fn default_ws_url() -> &'static str;

    /// Format payload to subscribe to a list of symbols.
    /// `is_master` denotes whether discovery channels should be requested.
    fn format_subscription_frame(symbols: &[String], is_master: bool) -> anyhow::Result<String>;

    /// Format payload to unsubscribe from symbols.
    fn format_unsubscription_frame(symbols: &[String]) -> anyhow::Result<String>;

    /// Format heartbeat ping frame if required by the wire protocol.
    fn format_ping_frame() -> Option<String> {
        None
    }

    /// Parse a raw incoming text message into normalized events.
    /// A single websocket frame may contain multiple deltas (micro-batching).
    fn parse_frame(text: &str, out_events: &mut Vec<NormalizedEvent>) -> anyhow::Result<()>;
}
