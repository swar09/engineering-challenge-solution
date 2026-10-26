//! Protocol types and constants for the Polymarket WebSocket and REST APIs.

use serde::{Deserialize, Serialize};

/// Literal string constant sent by the exchange as a heartbeat reply to a `PING` frame.
pub const PONG: &str = "PONG";

/// Bid or ask side of an order, serialised in uppercase on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    /// Swaps Buy for Sell and Sell for Buy, returning the complementary book side.
    #[inline]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

/// Channel category used in the subscription frame to select the market data feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChannelType {
    Market,
    User,
}

/// Operation applied to symbol subscriptions on an already-connected WebSocket without requiring reconnection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Subscribe,
    Unsubscribe,
}

/// Tag enum mapping each `event_type` string value to its corresponding `MarketEvent` variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketEventType {
    Book,
    PriceChange,
    LastTradePrice,
    TickSizeChange,
    BestBidAsk,
    NewMarket,
    MarketResolved,
}
