use super::{
    DeltaOp, ExchangeCodec, NormalizedDelta, NormalizedDiscovery, NormalizedEvent, NormalizedSide,
    NormalizedSnapshot,
};
use crate::polymarket::dtos::{SubscriptionRequest, SubscriptionUpdate};
use crate::polymarket::events::MarketEvent;
use crate::polymarket::types::{Operation, Side as PolymarketSide};
use crate::websocket::types::{DEFAULT_SCALE, parse_exchange_timestamp, parse_fixed_point_scale};

pub struct PolymarketCodec;

impl ExchangeCodec for PolymarketCodec {
    const NAME: &'static str = "polymarket";

    fn default_ws_url() -> &'static str {
        "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    }

    fn format_subscription_frame(symbols: &[String], is_master: bool) -> anyhow::Result<String> {
        let req = if is_master {
            SubscriptionRequest::with_discovery(symbols.to_vec())
        } else {
            SubscriptionRequest::new(symbols.to_vec())
        };
        Ok(serde_json::to_string(&req)?)
    }

    fn format_unsubscription_frame(symbols: &[String]) -> anyhow::Result<String> {
        let update = SubscriptionUpdate {
            assets_ids: symbols.to_vec(),
            operation: Operation::Unsubscribe,
        };
        Ok(serde_json::to_string(&update)?)
    }

    fn parse_frame(text: &str, out_events: &mut Vec<NormalizedEvent>) -> anyhow::Result<()> {
        let events = crate::polymarket::events::parse_frame(text)?;
        for ev in events {
            match ev {
                MarketEvent::Book(book) => {
                    let ts = parse_exchange_timestamp(&book.timestamp);
                    let mut bids = Vec::with_capacity(book.bids.len());
                    for b in book.bids {
                        bids.push((
                            parse_fixed_point_scale(&b.price, DEFAULT_SCALE)?,
                            parse_fixed_point_scale(&b.size, DEFAULT_SCALE)?,
                        ));
                    }
                    let mut asks = Vec::with_capacity(book.asks.len());
                    for a in book.asks {
                        asks.push((
                            parse_fixed_point_scale(&a.price, DEFAULT_SCALE)?,
                            parse_fixed_point_scale(&a.size, DEFAULT_SCALE)?,
                        ));
                    }
                    out_events.push(NormalizedEvent::Snapshot(NormalizedSnapshot {
                        symbol: book.asset_id,
                        exchange_timestamp: ts,
                        bids,
                        asks,
                    }));
                }
                MarketEvent::PriceChange(pc) => {
                    let ts = parse_exchange_timestamp(&pc.timestamp);
                    for item in pc.price_changes {
                        let side = match item.side {
                            PolymarketSide::Buy => NormalizedSide::Buy,
                            PolymarketSide::Sell => NormalizedSide::Sell,
                        };
                        out_events.push(NormalizedEvent::Delta(NormalizedDelta {
                            symbol: item.asset_id,
                            exchange_timestamp: ts,
                            side,
                            price: parse_fixed_point_scale(&item.price, DEFAULT_SCALE)?,
                            quantity: parse_fixed_point_scale(&item.size, DEFAULT_SCALE)?,
                            op: DeltaOp::SetAbsolute,
                        }));
                    }
                }
                MarketEvent::NewMarket(m) => {
                    out_events.push(NormalizedEvent::Discovery(
                        NormalizedDiscovery::MarketCreated {
                            market_id: m.market,
                            symbols: m.assets_ids,
                            metadata: Some(m.question),
                        },
                    ));
                }
                MarketEvent::MarketResolved(r) => {
                    out_events.push(NormalizedEvent::Discovery(
                        NormalizedDiscovery::MarketResolved {
                            market_id: r.market,
                            symbols: r.assets_ids,
                            winning_symbol: Some(r.winning_asset_id),
                        },
                    ));
                }
                MarketEvent::LastTradePrice(_)
                | MarketEvent::TickSizeChange(_)
                | MarketEvent::BestBidAsk(_) => {
                    out_events.push(NormalizedEvent::Ignored);
                }
            }
        }
        Ok(())
    }
}
