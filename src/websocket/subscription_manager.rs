//! Subscription monitoring, differential stall detection, and token-bucket resubscription.

use std::collections::{HashSet, VecDeque};
use std::time::{Duration, Instant};
use tracing::{info, warn};

use crate::polymarket::types::Operation;
use crate::websocket::ws::Ws;

pub const TOKEN_SCALE: u64 = 1_000_000;

#[derive(Debug, Clone)]
pub struct TokenBucket {
    pub capacity_micros: u64,
    pub available_micros: u64,
    pub refill_micros_per_ms: u64,
    pub last_refill: Instant,
}

impl TokenBucket {
    pub fn new(capacity_tokens: u64, refill_tokens_per_sec: u64) -> Self {
        let capacity_micros = capacity_tokens.saturating_mul(TOKEN_SCALE);
        let refill_micros_per_ms = refill_tokens_per_sec.saturating_mul(1_000);
        Self {
            capacity_micros,
            available_micros: capacity_micros,
            refill_micros_per_ms,
            last_refill: Instant::now(),
        }
    }

    pub fn refill(&mut self) {
        let now = Instant::now();
        let elapsed_ms = now.duration_since(self.last_refill).as_millis() as u64;
        if elapsed_ms > 0 {
            let added = elapsed_ms.saturating_mul(self.refill_micros_per_ms);
            self.available_micros =
                (self.available_micros.saturating_add(added)).min(self.capacity_micros);
            self.last_refill = now;
        }
    }

    pub fn try_consume(&mut self) -> bool {
        self.refill();
        if self.available_micros >= TOKEN_SCALE {
            self.available_micros -= TOKEN_SCALE;
            true
        } else {
            false
        }
    }
}

/// Coordinates subscription health checks across Primary and Replica socket pairs.
#[derive(Debug)]
pub struct SubscriptionManager {
    pub staleness_threshold: Duration,
    pub queue: VecDeque<String>,
    pub in_flight: HashSet<String>,
    pub rate_limiter: TokenBucket,
}

impl SubscriptionManager {
    /// Creates a subscription manager with differential staleness threshold and token bucket rate limits.
    pub fn new(
        staleness_threshold: Duration,
        burst_capacity: u64,
        refill_tokens_per_sec: u64,
    ) -> Self {
        Self {
            staleness_threshold,
            queue: VecDeque::new(),
            in_flight: HashSet::new(),
            rate_limiter: TokenBucket::new(burst_capacity, refill_tokens_per_sec),
        }
    }

    /// Enqueues a symbol for resubscription if not already queued or in-flight.
    pub fn enqueue(&mut self, symbol: &str) -> bool {
        if self.in_flight.contains(symbol) {
            return false;
        }
        self.in_flight.insert(symbol.to_string());
        self.queue.push_back(symbol.to_string());
        true
    }

    /// Scans assigned symbols between Primary and Replica sockets and enqueues divergent symbols.
    pub fn monitor_pairs(&mut self, primary_ws: &Ws, replica_ws: &Ws) {
        for symbol in &primary_ws.symbols {
            let pri_time = primary_ws
                .orderbooks
                .get(symbol)
                .and_then(|ob| ob.last_delta_received_at);
            let rep_time = replica_ws
                .orderbooks
                .get(symbol)
                .and_then(|ob| ob.last_delta_received_at);

            match (pri_time, rep_time) {
                (Some(p), Some(r)) => {
                    let diff = if p > r {
                        p.duration_since(r)
                    } else {
                        r.duration_since(p)
                    };
                    if diff >= self.staleness_threshold && self.enqueue(symbol) {
                        warn!("Divergence {diff:?} on {symbol}; enqueued resubscription");
                    }
                }
                (Some(p), None) => {
                    if p.elapsed() >= self.staleness_threshold && self.enqueue(symbol) {
                        warn!("Replica zero deltas on {symbol}; enqueued resubscription");
                    }
                }
                (None, Some(r)) => {
                    if r.elapsed() >= self.staleness_threshold && self.enqueue(symbol) {
                        warn!("Primary zero deltas on {symbol}; enqueued resubscription");
                    }
                }
                (None, None) => {}
            }
        }
    }

    /// Processes queued resubscriptions subject to token bucket rate limiting, dispatching to Replica first then Primary.
    pub async fn process_queue(&mut self, pool: &mut [Ws]) {
        while !self.queue.is_empty() {
            if !self.rate_limiter.try_consume() {
                break;
            }

            let symbol = match self.queue.pop_front() {
                Some(s) => s,
                None => break,
            };

            info!("Resubscription: {symbol}");

            // Locate primary and replica for this symbol
            let mut target_pair: Option<usize> = None;
            for ws in pool.iter() {
                if ws.symbols.contains(&symbol) {
                    target_pair = Some(ws.pair_id);
                    break;
                }
            }

            if let Some(pair_id) = target_pair {
                // Find replica and primary in pool
                let mut replica_idx = None;
                let mut primary_idx = None;
                for (idx, ws) in pool.iter().enumerate() {
                    if ws.pair_id == pair_id {
                        if ws.role == crate::websocket::types::WsRole::Replica {
                            replica_idx = Some(idx);
                        } else if ws.role == crate::websocket::types::WsRole::Primary {
                            primary_idx = Some(idx);
                        }
                    }
                }

                // Resubscribe on Replica first: explicit Unsubscribe then Subscribe cycle to reset server subscription state
                if let Some(r_idx) = replica_idx {
                    let r_ws = &mut pool[r_idx];
                    let _ = r_ws
                        .update_subscription(Operation::Unsubscribe, std::slice::from_ref(&symbol))
                        .await;
                    let _ = r_ws
                        .update_subscription(Operation::Subscribe, std::slice::from_ref(&symbol))
                        .await;
                    if let Some(ob) = r_ws.orderbooks.get_mut(&symbol) {
                        ob.last_delta_received_at = Some(Instant::now());
                    }
                }

                // Resubscribe on Primary second: explicit Unsubscribe then Subscribe cycle
                if let Some(p_idx) = primary_idx {
                    let p_ws = &mut pool[p_idx];
                    let _ = p_ws
                        .update_subscription(Operation::Unsubscribe, std::slice::from_ref(&symbol))
                        .await;
                    let _ = p_ws
                        .update_subscription(Operation::Subscribe, std::slice::from_ref(&symbol))
                        .await;
                    if let Some(ob) = p_ws.orderbooks.get_mut(&symbol) {
                        ob.last_delta_received_at = Some(Instant::now());
                    }
                }
            }

            self.in_flight.remove(&symbol);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_token_bucket_integer_consumption() {
        let mut bucket = TokenBucket::new(2, 1);
        assert!(bucket.try_consume());
        assert!(bucket.try_consume());
        assert!(!bucket.try_consume());
    }

    #[test]
    fn test_token_bucket_refill_math() {
        let mut bucket = TokenBucket::new(2, 2); // 2 tokens/sec = 2_000 micro-tokens/ms
        assert!(bucket.try_consume());
        assert!(bucket.try_consume());
        assert!(!bucket.try_consume());

        // Simulate 500ms elapsed
        bucket.last_refill = Instant::now() - Duration::from_millis(500);
        bucket.refill();
        // 500ms * 2_000 micros/ms = 1_000_000 micros = 1 whole token
        assert!(bucket.try_consume());
        assert!(!bucket.try_consume());
    }

    #[test]
    fn test_subscription_manager_deduplication() {
        let mut sm = SubscriptionManager::new(Duration::from_secs(15), 5, 1);
        assert!(sm.enqueue("TOKEN_A"));
        assert!(!sm.enqueue("TOKEN_A"));
        assert_eq!(sm.queue.len(), 1);
        assert_eq!(sm.in_flight.len(), 1);

        assert!(sm.enqueue("TOKEN_B"));
        assert_eq!(sm.queue.len(), 2);
        assert_eq!(sm.in_flight.len(), 2);
    }
}
