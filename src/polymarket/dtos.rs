//! Wire data transfer objects (DTOs) for Polymarket public market channels.

use super::types::{ChannelType, Operation, Side};
use serde::{Deserialize, Serialize};

/// Aggregated price level entry (price + size as strings) within a full orderbook snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderSummary {
    pub price: String,
    pub size: String,
}

/// Descriptive metadata fields for the parent event grouping a set of related prediction markets.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventMessage {
    pub id: Option<String>,
    pub ticker: Option<String>,
    pub slug: Option<String>,
    pub title: Option<String>,
    pub description: Option<String>,
}

/// Serialisable frame sent to the exchange immediately after opening a WebSocket connection to establish market channel subscriptions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionRequest {
    pub assets_ids: Vec<String>,
    #[serde(rename = "type")]
    pub kind: ChannelType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub initial_dump: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_feature_enabled: Option<bool>,
}

impl SubscriptionRequest {
    /// Builds a subscription frame with `initial_dump: true` and `level: 2`, requesting the full L2 orderbook snapshot from the exchange on connection.
    pub fn new(assets_ids: Vec<String>) -> Self {
        Self {
            assets_ids,
            kind: ChannelType::Market,
            initial_dump: Some(true),
            level: Some(2),
            custom_feature_enabled: Some(false),
        }
    }

    /// Builds a subscription frame with `custom_feature_enabled: true`, activating the discovery feed that emits `new_market` and `market_resolved` events on the anchor socket.
    pub fn with_discovery(assets_ids: Vec<String>) -> Self {
        Self {
            assets_ids,
            kind: ChannelType::Market,
            initial_dump: Some(true),
            level: Some(2),
            custom_feature_enabled: Some(true),
        }
    }
}

/// Frame for adding or removing symbols on an open WebSocket connection without tearing down the connection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionUpdate {
    pub operation: Operation,
    pub assets_ids: Vec<String>,
}

/// Full L2 orderbook snapshot frame received when `event_type = "book"`, containing all resting bid and ask levels.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Book {
    pub asset_id: String,
    pub market: String,
    pub timestamp: String,
    pub hash: String,
    pub bids: Vec<OrderSummary>,
    pub asks: Vec<OrderSummary>,
}

/// One price level mutation within an incremental update batch, carrying asset ID, price, size, and side.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceChangeEntry {
    pub asset_id: String,
    pub price: String,
    pub size: String,
    pub side: Side,
    pub hash: String,
    pub best_bid: Option<String>,
    pub best_ask: Option<String>,
}

/// Batch of incremental price level mutations received when `event_type = "price_change"`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceChange {
    pub market: String,
    pub timestamp: String,
    pub price_changes: Vec<PriceChangeEntry>,
}

/// Executed trade notification received when `event_type = "last_trade_price"`, carrying the matched price, size, and direction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LastTradePrice {
    pub asset_id: String,
    pub market: String,
    pub price: String,
    pub size: String,
    pub side: Side,
    pub timestamp: String,
    pub fee_rate_bps: Option<String>,
    pub transaction_hash: Option<String>,
}

/// Tick size update notification received when `event_type = "tick_size_change"`, carrying the old and new minimum price increment for an asset.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TickSizeChange {
    pub asset_id: String,
    pub market: String,
    pub old_tick_size: String,
    pub new_tick_size: String,
    pub timestamp: String,
}

/// Best bid and ask quote received when `event_type = "best_bid_ask"`, reflecting the current top-of-book spread.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BestBidAsk {
    pub asset_id: String,
    pub market: String,
    pub best_bid: String,
    pub best_ask: String,
    pub spread: String,
    pub timestamp: String,
}

/// New prediction market creation event received when `event_type = "new_market"`, carrying full market metadata and associated CLOB token IDs.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct NewMarket {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub question: String,
    #[serde(default)]
    pub market: String,
    #[serde(default)]
    pub slug: String,
    #[serde(default)]
    pub assets_ids: Vec<String>,
    #[serde(default)]
    pub outcomes: Vec<String>,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub event_message: Option<EventMessage>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub condition_id: Option<String>,
    #[serde(default)]
    pub active: Option<bool>,
    #[serde(default)]
    pub clob_token_ids: Option<Vec<String>>,
    #[serde(default)]
    pub sports_market_type: Option<String>,
    #[serde(default)]
    pub line: Option<String>,
    #[serde(default)]
    pub game_start_time: Option<String>,
    #[serde(default)]
    pub order_price_min_tick_size: Option<String>,
    #[serde(default)]
    pub group_item_title: Option<String>,
}

/// Market resolution event received when `event_type = "market_resolved"`, identifying the winning asset and outcome.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct MarketResolved {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub market: String,
    #[serde(default)]
    pub assets_ids: Vec<String>,
    #[serde(default)]
    pub winning_asset_id: String,
    #[serde(default)]
    pub winning_outcome: String,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub event_message: Option<EventMessage>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
}
