mod common;

use common::mock_ws;
use engineering_challenge::polymarket::dtos::{Book, OrderSummary, PriceChange, PriceChangeEntry};
use engineering_challenge::polymarket::events::MarketEvent;
use engineering_challenge::polymarket::types::Side as WireSide;
use engineering_challenge::websocket::status::WsStatus;
use engineering_challenge::websocket::types::WsRole;

#[tokio::test]
async fn test_process_book_and_price_change() {
    let mut ws = mock_ws(WsStatus::Healthy, WsRole::Primary).await;
    ws.symbols.push("T1".to_string());

    let book = Book {
        asset_id: "T1".to_string(),
        market: "0x123".to_string(),
        timestamp: "1700000000000".to_string(),
        hash: "0xabc".to_string(),
        bids: vec![OrderSummary {
            price: "0.45".to_string(),
            size: "100.0".to_string(),
        }],
        asks: vec![OrderSummary {
            price: "0.55".to_string(),
            size: "150.0".to_string(),
        }],
    };
    let book_json = serde_json::to_string(&MarketEvent::Book(book)).unwrap();

    let (count, messages, disc) = ws.handle_raw_frame_events(&book_json).unwrap();
    assert_eq!(count, 1);
    assert!(disc.is_empty());
    assert_eq!(messages.len(), 1);
    assert_eq!(ws.delta_rate_sum, 0);

    let ob = ws.orderbooks.get("T1").unwrap();
    assert_eq!(*ob.bids.get(&450_000).unwrap(), 100_000_000);
    assert_eq!(*ob.asks.get(&550_000).unwrap(), 150_000_000);

    let delta = PriceChange {
        market: "0x123".to_string(),
        timestamp: "1700000001000".to_string(),
        price_changes: vec![
            PriceChangeEntry {
                asset_id: "T1".to_string(),
                price: "0.46".to_string(),
                size: "50.0".to_string(),
                side: WireSide::Buy,
                hash: "0x1".to_string(),
                best_bid: None,
                best_ask: None,
            },
            PriceChangeEntry {
                asset_id: "T1".to_string(),
                price: "0.45".to_string(),
                size: "0".to_string(),
                side: WireSide::Buy,
                hash: "0x2".to_string(),
                best_bid: None,
                best_ask: None,
            },
        ],
    };
    let delta_json = serde_json::to_string(&MarketEvent::PriceChange(delta)).unwrap();

    let (count2, messages2, disc2) = ws.handle_raw_frame_events(&delta_json).unwrap();
    assert_eq!(count2, 1);
    assert!(disc2.is_empty());
    assert_eq!(messages2.len(), 2);
    assert_eq!(ws.delta_rate_sum, 2);

    let ob2 = ws.orderbooks.get("T1").unwrap();
    assert_eq!(*ob2.bids.get(&460_000).unwrap(), 50_000_000);
    assert!(!ob2.bids.contains_key(&450_000));
}

#[tokio::test]
async fn test_replica_does_not_emit_messages() {
    let mut ws = mock_ws(WsStatus::Healthy, WsRole::Replica).await;
    ws.symbols.push("T1".to_string());

    let book = Book {
        asset_id: "T1".to_string(),
        market: "0x123".to_string(),
        timestamp: "1700000000000".to_string(),
        hash: "0xabc".to_string(),
        bids: vec![OrderSummary {
            price: "0.45".to_string(),
            size: "100.0".to_string(),
        }],
        asks: vec![OrderSummary {
            price: "0.55".to_string(),
            size: "150.0".to_string(),
        }],
    };
    let book_json = serde_json::to_string(&MarketEvent::Book(book)).unwrap();

    let (count, messages, _) = ws.handle_raw_frame_events(&book_json).unwrap();
    assert_eq!(count, 1);
    let ob = ws.orderbooks.get("T1").unwrap();
    assert_eq!(*ob.bids.get(&450_000).unwrap(), 100_000_000);
    assert!(messages.is_empty());
}
