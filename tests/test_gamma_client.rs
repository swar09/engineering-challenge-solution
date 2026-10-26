use engineering_challenge::gamma::GammaClient;

#[tokio::test]
async fn test_fetch_all_symbols_with_mock() {
    let client = GammaClient::new();
    let res = client
        .fetch_all_symbols_with_timeout(std::time::Duration::from_millis(500))
        .await;
    assert!(res.is_ok());
}
