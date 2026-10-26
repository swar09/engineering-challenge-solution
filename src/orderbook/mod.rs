use std::collections::BTreeMap;
use chrono::{DateTime, Utc};
use crate::Side;
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use crate::util::dtos::SbeEncode;

pub type Price = i64;
pub type Quantity = i64;

pub const FIXED_POINT_SCALE: f64 = 1_000_000.0;

#[inline]
pub fn to_fixed_point(val: f64) -> i64 {
    (val * FIXED_POINT_SCALE).round() as i64
}

#[inline]
pub fn parse_fixed_point(s: &str) -> Option<i64> {
    s.parse::<f64>().ok().map(to_fixed_point)
}

#[derive(Debug, Clone, PartialEq)]
pub enum OrderbookMessage {
    Snapshot(SnapshotDomain),
    PriceLevelUpdate(PriceLevelUpdateDomain),
}

impl SbeEncode for OrderbookMessage {
    #[inline]
    fn encoded_length_with_header(&self) -> anyhow::Result<usize> {
        match self {
            Self::Snapshot(s) => Ok(s.encoded_length_with_header()?),
            Self::PriceLevelUpdate(u) => Ok(u.encoded_length_with_header()?),
        }
    }

    #[inline]
    fn encode(&self, buf: &mut [u8]) -> anyhow::Result<usize> {
        match self {
            Self::Snapshot(s) => Ok(s.encode(buf)?),
            Self::PriceLevelUpdate(u) => Ok(u.encode(buf)?),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub symbol: String,
    pub bids: BTreeMap<Price, Quantity>,
    pub asks: BTreeMap<Price, Quantity>,
    pub last_exchange_timestamp: Option<DateTime<Utc>>,
}

impl OrderBook {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_exchange_timestamp: None,
        }
    }

    /// Clears resting book and applies a full snapshot of bids and asks.
    pub fn apply_snapshot(
        &mut self,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
        exchange_timestamp: DateTime<Utc>,
        received_at: DateTime<Utc>,
    ) -> SnapshotDomain {
        self.bids.clear();
        self.asks.clear();

        for (price, qty) in bids {
            if qty > 0 {
                self.bids.insert(price, qty);
            }
        }
        for (price, qty) in asks {
            if qty > 0 {
                self.asks.insert(price, qty);
            }
        }
        self.last_exchange_timestamp = Some(exchange_timestamp);

        self.to_snapshot_domain(received_at, exchange_timestamp)
    }

    /// Applies an incremental L2 delta update. If quantity is 0, the price level is removed.
    pub fn apply_delta(
        &mut self,
        side: Side,
        price_level: Price,
        quantity: Quantity,
        exchange_timestamp: DateTime<Utc>,
        backtest_timestamp: DateTime<Utc>,
    ) -> PriceLevelUpdateDomain {
        match side {
            Side::Buy => {
                if quantity <= 0 {
                    self.bids.remove(&price_level);
                } else {
                    self.bids.insert(price_level, quantity);
                }
            }
            Side::Sell => {
                if quantity <= 0 {
                    self.asks.remove(&price_level);
                } else {
                    self.asks.insert(price_level, quantity);
                }
            }
        }
        self.last_exchange_timestamp = Some(exchange_timestamp);

        PriceLevelUpdateDomain {
            backtest_timestamp,
            exchange_timestamp,
            price_level,
            quantity,
            side,
            symbol: self.symbol.clone(),
        }
    }

    /// Produces a SnapshotDomain representing the current book state.
    pub fn to_snapshot_domain(
        &self,
        received_at: DateTime<Utc>,
        exchange_timestamp: DateTime<Utc>,
    ) -> SnapshotDomain {
        let bids = self
            .bids
            .iter()
            .rev()
            .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                price_level,
                quantity,
            })
            .collect();

        let asks = self
            .asks
            .iter()
            .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                price_level,
                quantity,
            })
            .collect();

        SnapshotDomain {
            received_at,
            exchange_timestamp,
            symbol: self.symbol.clone(),
            bids,
            asks,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_orderbook_snapshot_and_delta() {
        let mut ob = OrderBook::new("TEST");
        let ts = DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap();

        let snap = ob.apply_snapshot(
            vec![(400_000, 1_000_000), (450_000, 2_000_000)],
            vec![(550_000, 1_500_000), (500_000, 3_000_000)],
            ts,
            ts,
        );

        assert_eq!(snap.bids[0].price_level, 450_000);
        assert_eq!(snap.bids[1].price_level, 400_000);

        assert_eq!(snap.asks[0].price_level, 500_000);
        assert_eq!(snap.asks[1].price_level, 550_000);

        let delta = ob.apply_delta(Side::Buy, 400_000, 0, ts, ts);
        assert_eq!(delta.quantity, 0);
        assert_eq!(ob.bids.len(), 1);
        assert!(!ob.bids.contains_key(&400_000));

        ob.apply_delta(Side::Sell, 490_000, 500_000, ts, ts);
        let snap2 = ob.to_snapshot_domain(ts, ts);
        assert_eq!(snap2.asks[0].price_level, 490_000);
        assert_eq!(snap2.asks[1].price_level, 500_000);
    }
}
