//! Polymarket incoming WebSocket event model and JSON frame parser.

use super::dtos::{
    BestBidAsk, Book, LastTradePrice, MarketResolved, NewMarket, PriceChange, TickSizeChange,
};
use super::types::{MarketEventType, PONG};
use serde::{Deserialize, Serialize};

/// Every JSON message from the Polymarket public market WebSocket, tagged by `event_type`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event_type", rename_all = "snake_case")]
pub enum MarketEvent {
    Book(Book),
    PriceChange(PriceChange),
    LastTradePrice(LastTradePrice),
    TickSizeChange(TickSizeChange),
    BestBidAsk(BestBidAsk),
    NewMarket(Box<NewMarket>),
    MarketResolved(Box<MarketResolved>),
}

impl MarketEvent {
    /// Returns the discriminator event type.
    pub const fn event_type(&self) -> MarketEventType {
        match self {
            Self::Book(_) => MarketEventType::Book,
            Self::PriceChange(_) => MarketEventType::PriceChange,
            Self::LastTradePrice(_) => MarketEventType::LastTradePrice,
            Self::TickSizeChange(_) => MarketEventType::TickSizeChange,
            Self::BestBidAsk(_) => MarketEventType::BestBidAsk,
            Self::NewMarket(_) => MarketEventType::NewMarket,
            Self::MarketResolved(_) => MarketEventType::MarketResolved,
        }
    }

    /// Returns the primary asset/token ID if applicable.
    pub fn asset_id(&self) -> Option<&str> {
        match self {
            Self::Book(b) => Some(&b.asset_id),
            Self::LastTradePrice(t) => Some(&t.asset_id),
            Self::TickSizeChange(t) => Some(&t.asset_id),
            Self::BestBidAsk(b) => Some(&b.asset_id),
            Self::PriceChange(p) => p.price_changes.first().map(|e| e.asset_id.as_str()),
            Self::NewMarket(m) => m.assets_ids.first().map(String::as_str),
            Self::MarketResolved(r) => Some(&r.winning_asset_id),
        }
    }

    /// Returns the market condition ID if applicable.
    pub fn market_id(&self) -> Option<&str> {
        match self {
            Self::Book(b) => Some(&b.market),
            Self::PriceChange(p) => Some(&p.market),
            Self::LastTradePrice(t) => Some(&t.market),
            Self::TickSizeChange(t) => Some(&t.market),
            Self::BestBidAsk(b) => Some(&b.market),
            Self::NewMarket(m) => Some(&m.market),
            Self::MarketResolved(r) => Some(&r.market),
        }
    }

    /// Returns the exchange timestamp string.
    pub fn timestamp(&self) -> &str {
        match self {
            Self::Book(b) => &b.timestamp,
            Self::PriceChange(p) => &p.timestamp,
            Self::LastTradePrice(t) => &t.timestamp,
            Self::TickSizeChange(t) => &t.timestamp,
            Self::BestBidAsk(b) => &b.timestamp,
            Self::NewMarket(m) => &m.timestamp,
            Self::MarketResolved(r) => &r.timestamp,
        }
    }
}

/// Parses a raw text frame from the Polymarket WebSocket into a list of `MarketEvent`s.
pub fn parse_frame(text: &str) -> Result<Vec<MarketEvent>, serde_json::Error> {
    let trimmed = text.trim();
    if trimmed.eq_ignore_ascii_case(PONG) {
        return Ok(Vec::new());
    }
    if trimmed.starts_with('[') {
        serde_json::from_str::<Vec<MarketEvent>>(trimmed)
    } else {
        serde_json::from_str::<MarketEvent>(trimmed).map(|e| vec![e])
    }
}
