use engineering_challenge::exchange::{
    DeltaOp, ExchangeCodec, KalshiCodec, NormalizedEvent, NormalizedSide, PolymarketCodec,
};

#[test]
fn test_polymarket_codec_subscription_and_unsubscription() {
    let symbols = vec!["TOKEN_YES".to_string(), "TOKEN_NO".to_string()];

    let sub_worker = PolymarketCodec::format_subscription_frame(&symbols, false).unwrap();
    assert!(sub_worker.contains("TOKEN_YES"));
    assert!(sub_worker.contains("market"));
    assert!(sub_worker.contains("\"custom_feature_enabled\":false"));

    let sub_master = PolymarketCodec::format_subscription_frame(&symbols, true).unwrap();
    assert!(sub_master.contains("\"custom_feature_enabled\":true"));

    let unsub = PolymarketCodec::format_unsubscription_frame(&symbols).unwrap();
    assert!(unsub.contains("unsubscribe"));
    assert!(unsub.contains("TOKEN_NO"));
}

#[test]
fn test_polymarket_codec_parse_delta_and_snapshot() {
    let book_frame = r#"[{"event_type":"book","asset_id":"123","market":"0xabc","timestamp":"1700000000000","hash":"h1","bids":[{"price":"0.55","size":"100"}],"asks":[{"price":"0.60","size":"200"}]}]"#;
    let mut events = Vec::new();
    PolymarketCodec::parse_frame(book_frame, &mut events).unwrap();
    assert_eq!(events.len(), 1);

    if let NormalizedEvent::Snapshot(snap) = &events[0] {
        assert_eq!(snap.symbol, "123");
        assert_eq!(snap.bids, vec![(550_000, 100_000_000)]);
        assert_eq!(snap.asks, vec![(600_000, 200_000_000)]);
    } else {
        panic!("Expected snapshot event");
    }

    let delta_frame = r#"{"event_type":"price_change","market":"0xabc","timestamp":"1700000000100","price_changes":[{"asset_id":"123","price":"0.56","size":"150","side":"BUY","hash":"h2"}]}"#;
    events.clear();
    PolymarketCodec::parse_frame(delta_frame, &mut events).unwrap();
    assert_eq!(events.len(), 1);

    if let NormalizedEvent::Delta(delta) = &events[0] {
        assert_eq!(delta.symbol, "123");
        assert_eq!(delta.side, NormalizedSide::Buy);
        assert_eq!(delta.price, 560_000);
        assert_eq!(delta.quantity, 150_000_000);
        assert_eq!(delta.op, DeltaOp::SetAbsolute);
    } else {
        panic!("Expected delta event");
    }
}

#[test]
fn test_kalshi_codec_subscription_and_delta() {
    let symbols = vec!["FED-24DEC-T5.0".to_string()];
    let sub = KalshiCodec::format_subscription_frame(&symbols, false).unwrap();
    assert!(sub.contains("orderbook_delta"));
    assert!(sub.contains("FED-24DEC-T5.0"));

    let delta_frame = r#"{"type":"orderbook_delta","msg":{"market_ticker":"FED-24DEC-T5.0","price":48,"delta":25,"side":"yes"}}"#;
    let mut events = Vec::new();
    KalshiCodec::parse_frame(delta_frame, &mut events).unwrap();
    assert_eq!(events.len(), 1);

    if let NormalizedEvent::Delta(delta) = &events[0] {
        assert_eq!(delta.symbol, "FED-24DEC-T5.0");
        assert_eq!(delta.side, NormalizedSide::Buy);
        assert_eq!(delta.price, 480_000);
        assert_eq!(delta.quantity, 25);
        assert_eq!(delta.op, DeltaOp::AddRelative);
    } else {
        panic!("Expected delta event");
    }
}
