//! Protocol types and constants for the Polymarket WebSocket and REST APIs.

use serde::{Deserialize, Serialize};

/// Fixed string constant for the Polymarket WebSocket heartbeat reply.
pub const PONG: &str = "PONG";

/// Order book side on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    /// Returns the opposite side of the book.
    #[inline]
    pub const fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

/// WebSocket channel type requested during initial subscription.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ChannelType {
    Market,
    User,
}

/// Dynamic subscription mutation operation on an open connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Subscribe,
    Unsubscribe,
}

/// Discriminator for all incoming Polymarket WebSocket market channel frames.
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
