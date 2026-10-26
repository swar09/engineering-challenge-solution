use chrono::DateTime;
use crossbeam::channel::unbounded;
use engineering_challenge::Side;
use engineering_challenge::orderbook::{OrderBook, OrderbookMessage, TimedDelta};
use engineering_challenge::websocket::subscription_manager::SubscriptionManager;
use engineering_challenge::websocket::ws_manager::WsManager;
use std::time::Duration;

#[test]
fn test_per_symbol_heap_ordering_and_phantom_prevention() {
    let mut ob = OrderBook::new("TEST_TOKEN");
    assert!(!ob.initial_dump_applied);

    let ts_old = DateTime::from_timestamp_millis(1_700_000_000_000).unwrap();
    let ts_snap = DateTime::from_timestamp_millis(1_700_000_001_000).unwrap();
    let ts_del = DateTime::from_timestamp_millis(1_700_000_003_000).unwrap();
    let ts_ins = DateTime::from_timestamp_millis(1_700_000_002_000).unwrap();

    // 1. Delta arrives before initial snapshot: deletion of 500_000
    // If applied directly, this would fail as a phantom deletion!
    let r1 = ob.ingest_delta(TimedDelta {
        exchange_ts_micros: ts_del.timestamp_micros(),
        sequence_counter: 1,
        side: Side::Buy,
        price_level: 500_000,
        quantity: 0,
        exchange_timestamp: ts_del,
        backtest_timestamp: ts_del,
        hash: "h_del".into(),
    });
    assert!(r1.is_none(), "Delta must be buffered before initial dump");

    // 2. An older delta arrives (older than snapshot)
    let r2 = ob.ingest_delta(TimedDelta {
        exchange_ts_micros: ts_old.timestamp_micros(),
        sequence_counter: 2,
        side: Side::Buy,
        price_level: 490_000,
        quantity: 100,
        exchange_timestamp: ts_old,
        backtest_timestamp: ts_old,
        hash: "h_old".into(),
    });
    assert!(r2.is_none());

    // 3. An insertion arrives that sets price 500_000 (timestamp 2000, before deletion timestamp 3000)
    let r3 = ob.ingest_delta(TimedDelta {
        exchange_ts_micros: ts_ins.timestamp_micros(),
        sequence_counter: 3,
        side: Side::Buy,
        price_level: 500_000,
        quantity: 1_000_000,
        exchange_timestamp: ts_ins,
        backtest_timestamp: ts_ins,
        hash: "h_ins".into(),
    });
    assert!(r3.is_none());
    assert_eq!(ob.delta_heap.len(), 3);

    // 4. Initial dump arrives with timestamp 1000
    let (snap, replayed) = ob.apply_initial_dump(
        vec![(450_000, 2_000_000)],
        vec![(550_000, 2_000_000)],
        ts_snap,
        ts_snap,
    );

    assert!(ob.initial_dump_applied);
    assert_eq!(snap.bids.len(), 1);
    assert_eq!(snap.asks.len(), 1);

    // Replay sequence:
    // ts_old (1000) was discarded (<= ts_snap).
    // ts_ins (2000) inserted 500_000 with qty 1_000_000.
    // ts_del (3000) deleted 500_000.
    assert_eq!(replayed.len(), 2);
    assert_eq!(replayed[0].price_level, 500_000);
    assert_eq!(replayed[0].quantity, 1_000_000); // Insertion first
    assert_eq!(replayed[1].price_level, 500_000);
    assert_eq!(replayed[1].quantity, 0); // Deletion second

    // Orderbook state has zero phantom errors!
    assert!(!ob.bids.contains_key(&500_000));
}

#[test]
fn test_global_min_heap_sorting_across_sockets() {
    let (tx, rx) = unbounded();
    let sub_mgr = SubscriptionManager::new(Duration::from_secs(15), 5, 1);
    let mut ws_manager = WsManager::new(tx, sub_mgr);

    let ts1 = DateTime::from_timestamp_millis(1_700_000_001_000).unwrap();
    let ts2 = DateTime::from_timestamp_millis(1_700_000_002_000).unwrap();
    let ts3 = DateTime::from_timestamp_millis(1_700_000_003_000).unwrap();

    // Sockets push messages in scrambled/jittered arrival order: ts3, ts1, ts2
    ws_manager.push_to_global_heap(OrderbookMessage::Snapshot(
        engineering_challenge::util::dtos::challenge_orderbook::SnapshotDomain {
            received_at: ts3,
            exchange_timestamp: ts3,
            bids: vec![],
            asks: vec![],
            symbol: "SYM_3".into(),
        },
    ));

    ws_manager.push_to_global_heap(OrderbookMessage::Snapshot(
        engineering_challenge::util::dtos::challenge_orderbook::SnapshotDomain {
            received_at: ts1,
            exchange_timestamp: ts1,
            bids: vec![],
            asks: vec![],
            symbol: "SYM_1".into(),
        },
    ));

    ws_manager.push_to_global_heap(OrderbookMessage::Snapshot(
        engineering_challenge::util::dtos::challenge_orderbook::SnapshotDomain {
            received_at: ts2,
            exchange_timestamp: ts2,
            bids: vec![],
            asks: vec![],
            symbol: "SYM_2".into(),
        },
    ));

    assert_eq!(ws_manager.global_heap.len(), 3);

    // Drain all from global heap
    let drained = ws_manager.drain_all_global_heap();
    assert_eq!(drained.len(), 3);

    // Must be strictly sorted: ts1, ts2, ts3
    assert_eq!(drained[0].symbol(), "SYM_1");
    assert_eq!(drained[1].symbol(), "SYM_2");
    assert_eq!(drained[2].symbol(), "SYM_3");

    // Also verify flush_all writes to the crossbeam channel in sorted order
    ws_manager.push_to_global_heap(drained[2].clone());
    ws_manager.push_to_global_heap(drained[0].clone());
    ws_manager.flush_all();

    let channel_batch = rx.recv().expect("Channel should receive batch");
    assert_eq!(channel_batch.len(), 2);
    assert_eq!(channel_batch[0].symbol(), "SYM_1");
    assert_eq!(channel_batch[1].symbol(), "SYM_3");
}
