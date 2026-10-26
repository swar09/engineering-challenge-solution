use crate::Side;
use crate::util::dtos::SbeEncode;
use crate::util::dtos::challenge_orderbook::{
    PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};
use chrono::{DateTime, Utc};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::time::Instant;

pub type Price = i64;
pub type Quantity = i64;

/// Multiplier for converting floating-point prices and quantities into integer fixed-point representation (1_000_000 units = 1 USDC or 1 share).
pub const DEFAULT_SCALE: i64 = 1_000_000;
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

impl OrderbookMessage {
    #[inline]
    pub fn exchange_timestamp(&self) -> DateTime<Utc> {
        match self {
            Self::Snapshot(s) => s.exchange_timestamp,
            Self::PriceLevelUpdate(u) => u.exchange_timestamp,
        }
    }

    #[inline]
    pub fn symbol(&self) -> &str {
        match self {
            Self::Snapshot(s) => &s.symbol,
            Self::PriceLevelUpdate(u) => &u.symbol,
        }
    }
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

/// Delta element queued in the per-symbol min-heap, ordered strictly by exchange timestamp.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct TimedDelta {
    pub exchange_ts_micros: i64,
    pub sequence_counter: u64,
    pub side: Side,
    pub price_level: Price,
    pub quantity: Quantity,
    pub exchange_timestamp: DateTime<Utc>,
    pub backtest_timestamp: DateTime<Utc>,
    pub hash: String,
}

impl Ord for TimedDelta {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.exchange_ts_micros
            .cmp(&other.exchange_ts_micros)
            .then_with(|| self.sequence_counter.cmp(&other.sequence_counter))
    }
}

impl PartialOrd for TimedDelta {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone)]
pub struct OrderBook {
    pub symbol: String,
    pub bids: BTreeMap<Price, Quantity>,
    pub asks: BTreeMap<Price, Quantity>,
    pub initial_dump_applied: bool,
    pub delta_heap: BinaryHeap<Reverse<TimedDelta>>,
    pub last_exchange_timestamp: Option<DateTime<Utc>>,
    pub last_delta_received_at: Option<Instant>,
}

impl OrderBook {
    pub fn new(symbol: impl Into<String>) -> Self {
        Self {
            symbol: symbol.into(),
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            initial_dump_applied: false,
            delta_heap: BinaryHeap::new(),
            last_exchange_timestamp: None,
            last_delta_received_at: None,
        }
    }

    /// Clears the existing bids and asks, populates the B-Tree with the supplied price levels,
    /// marks `initial_dump_applied = true`, and pops all buffered deltas in strict timestamp order,
    /// applying only those occurring strictly after the snapshot timestamp.
    pub fn apply_initial_dump(
        &mut self,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
        exchange_timestamp: DateTime<Utc>,
        received_at: DateTime<Utc>,
    ) -> (SnapshotDomain, Vec<PriceLevelUpdateDomain>) {
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
        self.initial_dump_applied = true;
        self.last_exchange_timestamp = Some(exchange_timestamp);

        let snapshot = self.to_snapshot_domain(received_at, exchange_timestamp);

        let mut ready_updates = Vec::new();
        while let Some(Reverse(d)) = self.delta_heap.pop() {
            // Discard deltas older than or equal to snapshot exchange timestamp
            if d.exchange_ts_micros > exchange_timestamp.timestamp_micros() {
                let update = self.apply_delta(
                    d.side,
                    d.price_level,
                    d.quantity,
                    d.exchange_timestamp,
                    d.backtest_timestamp,
                );
                ready_updates.push(update);
            }
        }

        (snapshot, ready_updates)
    }

    /// Clears existing bids and asks and populates with new levels.
    /// Also marks `initial_dump_applied = true`.
    pub fn apply_snapshot(
        &mut self,
        bids: Vec<(Price, Quantity)>,
        asks: Vec<(Price, Quantity)>,
        exchange_timestamp: DateTime<Utc>,
        received_at: DateTime<Utc>,
    ) -> SnapshotDomain {
        let (snap, _) = self.apply_initial_dump(bids, asks, exchange_timestamp, received_at);
        snap
    }

    /// Ingests a delta into the per-symbol min-heap.
    /// If `initial_dump_applied` is false, keeps delta buffered in heap and returns None.
    /// If `initial_dump_applied` is true, pops the lowest-timestamp delta from the heap,
    /// applies it to the order book, and returns the resulting `PriceLevelUpdateDomain`.
    pub fn ingest_delta(&mut self, delta: TimedDelta) -> Option<PriceLevelUpdateDomain> {
        self.delta_heap.push(Reverse(delta));

        if !self.initial_dump_applied {
            return None;
        }

        let Reverse(d) = self.delta_heap.pop()?;
        Some(self.apply_delta(
            d.side,
            d.price_level,
            d.quantity,
            d.exchange_timestamp,
            d.backtest_timestamp,
        ))
    }

    /// Applies a single price level delta to the resting B-Tree: inserts the price
    /// level when quantity is positive, removes it when quantity is zero or negative,
    /// records the exchange timestamp, and returns a `PriceLevelUpdateDomain` describing the change.
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
        self.last_delta_received_at = Some(Instant::now());

        PriceLevelUpdateDomain {
            backtest_timestamp,
            exchange_timestamp,
            price_level,
            quantity,
            side,
            symbol: self.symbol.clone(),
        }
    }

    /// Serialises the current resting B-Tree into a `SnapshotDomain`, ordering bids descending by
    /// price and asks ascending by price, and stamping both the received-at and exchange timestamps.
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
        assert!(ob.initial_dump_applied);

        let delta = ob.apply_delta(Side::Buy, 400_000, 0, ts, ts);
        assert_eq!(delta.quantity, 0);
        assert_eq!(ob.bids.len(), 1);
        assert!(!ob.bids.contains_key(&400_000));

        ob.apply_delta(Side::Sell, 490_000, 500_000, ts, ts);
        let snap2 = ob.to_snapshot_domain(ts, ts);
        assert_eq!(snap2.asks[0].price_level, 490_000);
        assert_eq!(snap2.asks[1].price_level, 500_000);
    }

    #[test]
    fn test_orderbook_heap_buffering_and_initial_dump() {
        let mut ob = OrderBook::new("TEST_HEAP");
        assert!(!ob.initial_dump_applied);

        let ts_before = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
        let ts_snap = DateTime::from_timestamp_millis(1_700_000_001_000).unwrap();
        let ts_after1 = DateTime::from_timestamp_millis(1_700_000_003_000).unwrap();
        let ts_after2 = DateTime::from_timestamp_millis(1_700_000_002_000).unwrap(); // Arrived out of order!

        // Deltas arrive before initial snapshot
        let res1 = ob.ingest_delta(TimedDelta {
            exchange_ts_micros: ts_before.timestamp_micros(),
            sequence_counter: 1,
            side: Side::Buy,
            price_level: 400_000,
            quantity: 500_000,
            exchange_timestamp: ts_before,
            backtest_timestamp: ts_before,
            hash: "h1".into(),
        });
        assert!(res1.is_none());

        // Ingest ts_after1 (timestamp 3000)
        let res2 = ob.ingest_delta(TimedDelta {
            exchange_ts_micros: ts_after1.timestamp_micros(),
            sequence_counter: 2,
            side: Side::Buy,
            price_level: 420_000,
            quantity: 700_000,
            exchange_timestamp: ts_after1,
            backtest_timestamp: ts_after1,
            hash: "h2".into(),
        });
        assert!(res2.is_none());

        // Ingest ts_after2 (timestamp 2000, out of order before ts_after1)
        let res3 = ob.ingest_delta(TimedDelta {
            exchange_ts_micros: ts_after2.timestamp_micros(),
            sequence_counter: 3,
            side: Side::Buy,
            price_level: 410_000,
            quantity: 600_000,
            exchange_timestamp: ts_after2,
            backtest_timestamp: ts_after2,
            hash: "h3".into(),
        });
        assert!(res3.is_none());
        assert_eq!(ob.delta_heap.len(), 3);

        // Now initial dump arrives
        let (snap, ready) = ob.apply_initial_dump(
            vec![(400_000, 1_000_000)],
            vec![(500_000, 1_000_000)],
            ts_snap,
            ts_snap,
        );

        assert!(ob.initial_dump_applied);
        assert_eq!(snap.bids.len(), 1);
        // ts_before was discarded (older than snapshot)
        // ts_after2 (2000) and ts_after1 (3000) were popped in strict sorted order!
        assert_eq!(ready.len(), 2);
        assert_eq!(ready[0].price_level, 410_000); // 2000 ms popped first
        assert_eq!(ready[1].price_level, 420_000); // 3000 ms popped second

        // Delta heap is now empty
        assert_eq!(ob.delta_heap.len(), 0);

        // Subsequent deltas now immediately pop from the heap
        let ts_live = DateTime::from_timestamp_millis(1_700_000_004_000).unwrap();
        let res_live = ob.ingest_delta(TimedDelta {
            exchange_ts_micros: ts_live.timestamp_micros(),
            sequence_counter: 4,
            side: Side::Buy,
            price_level: 430_000,
            quantity: 800_000,
            exchange_timestamp: ts_live,
            backtest_timestamp: ts_live,
            hash: "h4".into(),
        });
        assert!(res_live.is_some());
        assert_eq!(res_live.unwrap().price_level, 430_000);
        assert_eq!(*ob.bids.get(&430_000).unwrap(), 800_000);
    }
}
