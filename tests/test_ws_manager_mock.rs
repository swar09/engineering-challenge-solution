mod common;

use engineering_challenge::orderbook::OrderbookMessage;
use engineering_challenge::polymarket::dtos::{Book, NewMarket, OrderSummary};
use engineering_challenge::polymarket::events::MarketEvent;
use engineering_challenge::websocket::status::WsStatus;
use engineering_challenge::websocket::subscription_manager::SubscriptionManager;
use engineering_challenge::websocket::types::WsRole;
use engineering_challenge::websocket::ws_manager::WsManager;
use futures_util::SinkExt;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

use common::{drain_mock_server_background, mock_ws, mock_ws_with_server};

fn test_sub_mgr() -> SubscriptionManager {
    SubscriptionManager::new(Duration::from_secs(15), 5, 1)
}

#[tokio::test]
async fn test_least_loaded_balancing() {
    let (tx, _rx) = crossbeam::channel::unbounded();
    let mut manager = WsManager::new(tx, test_sub_mgr());

    let mut ws0 = mock_ws(WsStatus::Master, WsRole::Primary).await;
    ws0.symbols = vec!["S1".to_string(), "S2".to_string()];
    ws0.delta_rate_sum = 50;

    let mut ws1 = mock_ws(WsStatus::Healthy, WsRole::Primary).await;
    ws1.symbols = vec!["S3".to_string()];
    ws1.delta_rate_sum = 20;

    manager.pool.push(ws0);
    manager.pool.push(ws1);

    let candidate = manager.find_least_loaded_socket_index();
    assert_eq!(candidate, Some(1));
}

#[tokio::test]
async fn test_flush_all_batch() {
    let (tx, rx) = crossbeam::channel::unbounded();
    let mut manager = WsManager::new(tx, test_sub_mgr());
    let ts = chrono::Utc::now();

    let mut ws0 = mock_ws(WsStatus::Master, WsRole::Primary).await;
    ws0.symbols = vec!["S1".to_string()];
    manager.push_to_global_heap(OrderbookMessage::Snapshot(
        engineering_challenge::util::dtos::challenge_orderbook::SnapshotDomain {
            received_at: ts,
            exchange_timestamp: ts,
            bids: vec![],
            asks: vec![],
            symbol: "S1".into(),
        },
    ));

    manager.pool.push(ws0);
    manager.flush_all();

    let batch0 = rx.try_recv().expect("Expected batch from socket 0");
    assert_eq!(batch0.len(), 1);
    assert!(matches!(batch0[0], OrderbookMessage::Snapshot(_)));
}

#[tokio::test]
async fn test_standby_pool_replenish() {
    let (tx, _rx) = crossbeam::channel::unbounded();
    let mut manager = WsManager::new(tx, test_sub_mgr());

    for _ in 0..10 {
        manager
            .standby_pool
            .push_back(mock_ws(WsStatus::Healthy, WsRole::Replica).await);
    }
    assert_eq!(manager.standby_pool.len(), 10);

    let _ = manager.standby_pool.pop_front();
    assert_eq!(manager.standby_pool.len(), 9);
}

#[tokio::test]
async fn test_failover_promotes_replica_and_emits_snapshot() {
    let (tx, rx) = crossbeam::channel::unbounded();
    let mut manager = WsManager::new(tx, test_sub_mgr());

    let mut primary = mock_ws(WsStatus::Master, WsRole::Primary).await;
    primary.pair_id = 0;
    primary.symbols = vec!["T1".to_string()];

    let mut replica = mock_ws(WsStatus::Healthy, WsRole::Replica).await;
    replica.pair_id = 0;
    replica.symbols = vec!["T1".to_string()];
    let mut ob = engineering_challenge::orderbook::OrderBook::new("T1");
    ob.initial_dump_applied = true;
    ob.bids.insert(500_000, 100_000_000);
    replica.orderbooks.insert("T1".to_string(), ob);

    manager.pool.push(primary);
    manager.pool.push(replica);

    let standby = mock_ws(WsStatus::Healthy, WsRole::Replica).await;
    manager.standby_pool.push_back(standby);

    manager.handle_socket_disconnect(0, "mock://unused").await;

    assert_eq!(manager.pool[1].role, WsRole::Primary);
    assert_eq!(manager.pool[1].status, WsStatus::Master);

    let batch = rx.try_recv().expect("Expected failover snapshot batch");
    assert_eq!(batch.len(), 1);
    assert!(matches!(batch[0], OrderbookMessage::Snapshot(_)));

    assert_eq!(manager.pool[0].role, WsRole::Replica);
    assert_eq!(manager.pool[0].pair_id, 0);
}

#[tokio::test]
async fn test_failover_branch_b_cold_standby_when_replica_unhealthy() {
    let (tx, _rx) = crossbeam::channel::unbounded();
    let mut manager = WsManager::new(tx, test_sub_mgr());

    let mut primary = mock_ws(WsStatus::Master, WsRole::Primary).await;
    primary.pair_id = 0;
    primary.symbols = vec!["T1".to_string()];

    let mut replica = mock_ws(WsStatus::Unhealthy, WsRole::Replica).await;
    replica.pair_id = 0;
    replica.symbols = vec!["T1".to_string()];

    manager.pool.push(primary);
    manager.pool.push(replica);

    let standby1 = mock_ws(WsStatus::Healthy, WsRole::Replica).await;
    let standby2 = mock_ws(WsStatus::Healthy, WsRole::Replica).await;
    manager.standby_pool.push_back(standby1);
    manager.standby_pool.push_back(standby2);

    manager.handle_socket_disconnect(0, "mock://unused").await;

    assert_eq!(manager.pool[0].role, WsRole::Primary);
    assert_eq!(manager.pool[0].status, WsStatus::Master);

    assert_eq!(manager.pool[1].role, WsRole::Replica);
    assert_eq!(manager.pool[1].status, WsStatus::Healthy);
}

#[tokio::test]
async fn test_poll_next_message_discovery_and_batch_flush() {
    let (tx, rx) = crossbeam::channel::unbounded();
    let mut manager = WsManager::new(tx, test_sub_mgr());

    let (mut master_ws, mut master_server) =
        mock_ws_with_server(WsStatus::Master, WsRole::Primary).await;
    master_ws.pair_id = 0;

    let (mut replica_ws, replica_server) =
        mock_ws_with_server(WsStatus::Healthy, WsRole::Replica).await;
    replica_ws.pair_id = 0;
    drain_mock_server_background(replica_server);

    let new_market = MarketEvent::NewMarket(Box::new(NewMarket {
        market: "0xmarket_test".to_string(),
        assets_ids: vec!["DISCOVERED_TOKEN_1".to_string()],
        ..Default::default()
    }));
    let frame_json = serde_json::to_string(&new_market).unwrap();
    master_server
        .send(Message::Text(frame_json.into()))
        .await
        .unwrap();
    drain_mock_server_background(master_server);

    manager.pool.push(master_ws);
    manager.pool.push(replica_ws);
    manager.master_index = 0;

    manager.poll_next_message("mock://unused").await.unwrap();

    assert!(manager.symbol_states.contains_key("DISCOVERED_TOKEN_1"));
    assert_eq!(
        manager
            .symbol_states
            .get("DISCOVERED_TOKEN_1")
            .unwrap()
            .socket_index,
        0
    );

    let (mut worker_ws, mut worker_server) =
        mock_ws_with_server(WsStatus::Healthy, WsRole::Primary).await;
    worker_ws.pair_id = 1;
    worker_ws.symbols.push("TOKEN_X".to_string());

    let book = Book {
        asset_id: "TOKEN_X".to_string(),
        market: "0x123".to_string(),
        timestamp: "1700000000000".to_string(),
        hash: "0xabc".to_string(),
        bids: vec![OrderSummary {
            price: "0.50".to_string(),
            size: "10.0".to_string(),
        }],
        asks: vec![],
    };
    let book_frame = serde_json::to_string(&MarketEvent::Book(book)).unwrap();

    for _ in 0..(engineering_challenge::websocket::ws_manager::GLOBAL_HEAP_WATERMARK_CAPACITY - 1) {
        manager.push_to_global_heap(OrderbookMessage::Snapshot(
            engineering_challenge::util::dtos::challenge_orderbook::SnapshotDomain {
                received_at: chrono::Utc::now(),
                exchange_timestamp: chrono::Utc::now(),
                bids: vec![],
                asks: vec![],
                symbol: "TOKEN_X".into(),
            },
        ));
    }

    worker_server
        .send(Message::Text(book_frame.into()))
        .await
        .unwrap();
    drain_mock_server_background(worker_server);

    manager.pool.push(worker_ws);

    manager.poll_next_message("mock://unused").await.unwrap();

    let flushed = rx
        .try_recv()
        .expect("Expected batch flush when capacity reached");
    assert_eq!(
        flushed.len(),
        engineering_challenge::websocket::ws::DEFAULT_BATCH_CAPACITY
    );
}
