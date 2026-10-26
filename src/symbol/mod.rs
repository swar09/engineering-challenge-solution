use std::time::Instant;
use chrono::{DateTime, Utc};

#[derive(Debug, Clone)]
pub struct SymbolState {
    pub symbol: String,
    pub socket_index: usize,
    pub delta_rate: u64,
    pub last_exchange_timestamp: Option<DateTime<Utc>>,
    pub last_delta_timestamp: Instant,
}

impl SymbolState {
    pub fn new(symbol: String, socket_index: usize) -> Self {
        Self {
            symbol,
            socket_index,
            delta_rate: 0,
            last_exchange_timestamp: None,
            last_delta_timestamp: Instant::now(),
        }
    }
}

