//! Kalshi v2 WebSocket codec conforming to official Kalshi documentation.
//!
//! Official Reference: <https://docs.kalshi.com/websockets/orderbook-updates>
//! - Channels: `orderbook_delta`
//! - Flow: Emits `orderbook_snapshot` first, followed by incremental `orderbook_delta` frames.
//! - Price: Given in dollars fixed point (e.g. "0.0800") or cents (1-99).
//! - Delta: Given in `delta_fp` (e.g. "-54.00") or `delta` integer contracts.

use super::{
    DeltaOp, ExchangeCodec, NormalizedDelta, NormalizedEvent, NormalizedSide, NormalizedSnapshot,
};
use crate::websocket::types::DEFAULT_SCALE;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub struct KalshiCodec;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiSubscribeCmd {
    pub id: u64,
    pub cmd: String, // "subscribe" or "unsubscribe"
    pub params: KalshiSubscribeParams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiSubscribeParams {
    pub channels: Vec<String>,
    pub market_tickers: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiUpdateSubscriptionCmd {
    pub id: u64,
    pub cmd: String, // "update_subscription"
    pub params: KalshiUpdateParams,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KalshiUpdateParams {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sids: Option<Vec<u64>>,
    pub market_tickers: Vec<String>,
    pub action: String, // "add_markets", "delete_markets", "get_snapshot"
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum KalshiInboundFrame {
    #[serde(rename = "orderbook_snapshot")]
    Snapshot {
        #[serde(default)]
        sending_ts_ms: Option<i64>,
        msg: KalshiSnapshotMsg,
    },
    #[serde(rename = "orderbook_delta")]
    Delta {
        #[serde(default)]
        sending_ts_ms: Option<i64>,
        msg: KalshiDeltaMsg,
    },
    #[serde(other)]
    Other,
}

#[derive(Debug, Deserialize)]
struct KalshiSnapshotMsg {
    market_ticker: String,
    #[serde(default)]
    yes_dollars_fp: Vec<[String; 2]>, // [ ["0.0800", "300.00"], ... ]
    #[serde(default)]
    no_dollars_fp: Vec<[String; 2]>,
    #[serde(default)]
    yes: Vec<Vec<i64>>, // Legacy fallback: [ [8, 300], ... ]
    #[serde(default)]
    no: Vec<Vec<i64>>,
}

#[derive(Debug, Deserialize)]
struct KalshiDeltaMsg {
    market_ticker: String,
    #[serde(default)]
    price_dollars: Option<String>,
    #[serde(default)]
    price: Option<i64>,
    #[serde(default)]
    delta_fp: Option<String>,
    #[serde(default)]
    delta: Option<i64>,
    side: String, // "yes" or "no"
    #[serde(default)]
    ts_ms: Option<i64>,
}

fn parse_fp_dollars(s: &str) -> Option<i64> {
    let f = s.parse::<f64>().ok()?;
    Some((f * (DEFAULT_SCALE as f64)).round() as i64)
}

fn parse_fp_qty(s: &str) -> Option<i64> {
    let f = s.parse::<f64>().ok()?;
    Some(f.round() as i64)
}

impl ExchangeCodec for KalshiCodec {
    const NAME: &'static str = "kalshi";

    fn default_ws_url() -> &'static str {
        "wss://api.elections.kalshi.com/trade-api/ws/v2"
    }

    fn format_subscription_frame(symbols: &[String], _is_master: bool) -> anyhow::Result<String> {
        let cmd = KalshiSubscribeCmd {
            id: 1,
            cmd: "subscribe".to_string(),
            params: KalshiSubscribeParams {
                channels: vec!["orderbook_delta".to_string()],
                market_tickers: symbols.to_vec(),
            },
        };
        Ok(serde_json::to_string(&cmd)?)
    }

    fn format_unsubscription_frame(symbols: &[String]) -> anyhow::Result<String> {
        let cmd = KalshiUpdateSubscriptionCmd {
            id: 2,
            cmd: "update_subscription".to_string(),
            params: KalshiUpdateParams {
                sids: None,
                market_tickers: symbols.to_vec(),
                action: "delete_markets".to_string(),
            },
        };
        Ok(serde_json::to_string(&cmd)?)
    }

    fn parse_frame(text: &str, out_events: &mut Vec<NormalizedEvent>) -> anyhow::Result<()> {
        let frame: KalshiInboundFrame = match serde_json::from_str(text) {
            Ok(f) => f,
            Err(_) => return Ok(()),
        };

        match frame {
            KalshiInboundFrame::Snapshot { sending_ts_ms, msg } => {
                let ts = sending_ts_ms
                    .and_then(DateTime::from_timestamp_millis)
                    .unwrap_or_else(Utc::now);

                let mut bids = Vec::new();
                if !msg.yes_dollars_fp.is_empty() {
                    for entry in &msg.yes_dollars_fp {
                        if let (Some(p), Some(q)) =
                            (parse_fp_dollars(&entry[0]), parse_fp_qty(&entry[1]))
                        {
                            bids.push((p, q));
                        }
                    }
                } else {
                    for pair in &msg.yes {
                        if pair.len() >= 2 {
                            bids.push((pair[0] * 10_000, pair[1]));
                        }
                    }
                }

                let mut asks = Vec::new();
                if !msg.no_dollars_fp.is_empty() {
                    for entry in &msg.no_dollars_fp {
                        if let (Some(p_no), Some(q)) =
                            (parse_fp_dollars(&entry[0]), parse_fp_qty(&entry[1]))
                        {
                            let p_yes_ask = (DEFAULT_SCALE - p_no).max(0);
                            asks.push((p_yes_ask, q));
                        }
                    }
                } else {
                    for pair in &msg.no {
                        if pair.len() >= 2 {
                            asks.push(((100 - pair[0]) * 10_000, pair[1]));
                        }
                    }
                }

                out_events.push(NormalizedEvent::Snapshot(NormalizedSnapshot {
                    symbol: msg.market_ticker,
                    exchange_timestamp: ts,
                    bids,
                    asks,
                }));
            }
            KalshiInboundFrame::Delta { sending_ts_ms, msg } => {
                let ts = msg
                    .ts_ms
                    .or(sending_ts_ms)
                    .and_then(DateTime::from_timestamp_millis)
                    .unwrap_or_else(Utc::now);

                let price = if let Some(ref p_str) = msg.price_dollars {
                    parse_fp_dollars(p_str).unwrap_or(0)
                } else {
                    msg.price.map(|p| p * 10_000).unwrap_or(0)
                };

                let quantity = if let Some(ref q_str) = msg.delta_fp {
                    parse_fp_qty(q_str).unwrap_or(0)
                } else {
                    msg.delta.unwrap_or(0)
                };

                let is_yes = msg.side.eq_ignore_ascii_case("yes");
                let (side, final_price) = if is_yes {
                    (NormalizedSide::Buy, price)
                } else {
                    (NormalizedSide::Sell, (DEFAULT_SCALE - price).max(0))
                };

                out_events.push(NormalizedEvent::Delta(NormalizedDelta {
                    symbol: msg.market_ticker,
                    exchange_timestamp: ts,
                    side,
                    price: final_price,
                    quantity,
                    op: DeltaOp::AddRelative,
                }));
            }
            KalshiInboundFrame::Other => {
                out_events.push(NormalizedEvent::Ignored);
            }
        }

        Ok(())
    }
}
