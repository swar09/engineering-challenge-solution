mod common;

use common::mock_ws;
use engineering_challenge::orderbook::OrderBook;
use engineering_challenge::websocket::status::WsStatus;
use engineering_challenge::websocket::subscription_manager::SubscriptionManager;
use engineering_challenge::websocket::types::WsRole;
use std::time::{Duration, Instant};

#[tokio::test]
async fn test_monitor_pairs_detects_staleness_and_ignores_quiet() {
    let mut sm = SubscriptionManager::new(Duration::from_secs(10), 5, 1);

    let mut primary = mock_ws(WsStatus::Healthy, WsRole::Primary).await;
    let mut replica = mock_ws(WsStatus::Healthy, WsRole::Replica).await;

    primary.symbols.push("QUIET_TOKEN".to_string());
    replica.symbols.push("QUIET_TOKEN".to_string());
    primary
        .orderbooks
        .insert("QUIET_TOKEN".to_string(), OrderBook::new("QUIET_TOKEN"));
    replica
        .orderbooks
        .insert("QUIET_TOKEN".to_string(), OrderBook::new("QUIET_TOKEN"));

    primary.symbols.push("STALE_TOKEN".to_string());
    replica.symbols.push("STALE_TOKEN".to_string());
    let mut p_book = OrderBook::new("STALE_TOKEN");
    let mut r_book = OrderBook::new("STALE_TOKEN");

    p_book.last_delta_received_at = Some(Instant::now() - Duration::from_secs(25));
    r_book.last_delta_received_at = Some(Instant::now());

    primary.orderbooks.insert("STALE_TOKEN".to_string(), p_book);
    replica.orderbooks.insert("STALE_TOKEN".to_string(), r_book);

    sm.monitor_pairs(&primary, &replica);

    assert!(!sm.in_flight.contains("QUIET_TOKEN"));
    assert!(sm.in_flight.contains("STALE_TOKEN"));
    assert_eq!(sm.queue.len(), 1);
    assert_eq!(sm.queue.front().unwrap(), "STALE_TOKEN");
}

#[tokio::test]
async fn test_process_queue_dispatches_with_rate_limiting() {
    let mut sm = SubscriptionManager::new(Duration::from_secs(10), 1, 1);

    let mut primary = mock_ws(WsStatus::Healthy, WsRole::Primary).await;
    let mut replica = mock_ws(WsStatus::Healthy, WsRole::Replica).await;
    primary.pair_id = 42;
    replica.pair_id = 42;

    primary.symbols.push("TOKEN_1".to_string());
    primary.symbols.push("TOKEN_2".to_string());
    replica.symbols.push("TOKEN_1".to_string());
    replica.symbols.push("TOKEN_2".to_string());

    primary
        .orderbooks
        .insert("TOKEN_1".to_string(), OrderBook::new("TOKEN_1"));
    replica
        .orderbooks
        .insert("TOKEN_1".to_string(), OrderBook::new("TOKEN_1"));
    primary
        .orderbooks
        .insert("TOKEN_2".to_string(), OrderBook::new("TOKEN_2"));
    replica
        .orderbooks
        .insert("TOKEN_2".to_string(), OrderBook::new("TOKEN_2"));

    assert!(sm.enqueue("TOKEN_1"));
    assert!(sm.enqueue("TOKEN_2"));
    assert_eq!(sm.queue.len(), 2);

    let mut pool = vec![primary, replica];

    sm.process_queue(&mut pool).await;

    assert_eq!(sm.queue.len(), 1);
    assert_eq!(sm.queue.front().unwrap(), "TOKEN_2");
    assert!(!sm.in_flight.contains("TOKEN_1"));
    assert!(sm.in_flight.contains("TOKEN_2"));

    sm.process_queue(&mut pool).await;
    assert_eq!(sm.queue.len(), 1);

    sm.rate_limiter.available_micros =
        engineering_challenge::websocket::subscription_manager::TOKEN_SCALE;
    sm.process_queue(&mut pool).await;

    assert!(sm.queue.is_empty());
    assert!(sm.in_flight.is_empty());
}
