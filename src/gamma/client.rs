use crate::gamma::types::{GammaMarket, KeysetResponse};
use anyhow::{Context, Result};

pub struct GammaClient {
    client: reqwest::Client,
    base_url: String,
}

impl Default for GammaClient {
    fn default() -> Self {
        Self::new()
    }
}

impl GammaClient {
    pub fn new() -> Self {
        dotenvy::dotenv().ok();
        let base_url = std::env::var("GAMMA_API_BASE_URL")
            .unwrap_or_else(|_| "https://gamma-api.polymarket.com".to_string());
        Self::with_base_url(base_url)
    }

    pub fn with_base_url(base_url: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .local_address(std::net::IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED))
                .build()
                .unwrap_or_default(),
            base_url: base_url.into(),
        }
    }

    /// Queries the Gamma REST API for active, open markets ordered by 24-hour volume descending,
    /// filters for markets with positive 24-hour volume and non-empty resting liquidity,
    /// extracts and deduplicates the CLOB token IDs, and returns up to 50 active tradeable tokens.
    pub async fn fetch_top_50_clob_tokens(&self) -> Result<Vec<String>> {
        // Query markets sorted by 24h trading volume descending to guarantee active trading
        let url = format!(
            "{}/markets?limit=100&active=true&closed=false&order=volume24hr&ascending=false",
            self.base_url
        );
        let resp = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to query Gamma API")?;

        let markets: Vec<GammaMarket> = resp
            .json()
            .await
            .context("Failed to parse markets JSON from Gamma API")?;

        let mut token_ids = Vec::new();
        for market in markets {
            let vol = market.volume_24hr.unwrap_or(0.0);
            let liq = market.parse_liquidity();
            if vol > 0.0 && liq > 0.0 {
                let ids = market.parse_clob_token_ids();
                for id in ids {
                    if !token_ids.contains(&id) {
                        token_ids.push(id);
                        if token_ids.len() >= 50 {
                            return Ok(token_ids);
                        }
                    }
                }
            }
        }

        Ok(token_ids)
    }

    /// Paginates through all active markets on the Gamma REST API using a 30-second wall-clock timeout, deduplicates CLOB token IDs across pages, and returns the complete flat list.
    pub async fn fetch_all_symbols(&self) -> Result<Vec<String>> {
        self.fetch_all_symbols_with_timeout(std::time::Duration::from_secs(30))
            .await
    }

    /// Paginates through all active markets on the Gamma REST API using `/markets/keyset` cursor pagination
    /// until all pages are retrieved or the given timeout elapses, deduplicating CLOB token IDs.
    pub async fn fetch_all_symbols_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Vec<String>> {
        let start_time = std::time::Instant::now();
        let mut token_ids = Vec::new();
        let mut cursor: Option<String> = None;
        const PAGE_LIMIT: usize = 100;

        while start_time.elapsed() < timeout {
            let remaining = timeout.saturating_sub(start_time.elapsed());
            if remaining.is_zero() {
                break;
            }

            let mut url = format!(
                "{}/markets/keyset?limit={PAGE_LIMIT}&active=true&closed=false",
                self.base_url
            );
            if let Some(c) = &cursor {
                url.push_str("&cursor=");
                url.push_str(c);
            }

            let resp = match self.client.get(&url).timeout(remaining).send().await {
                Ok(r) if r.status().is_success() => r,
                Ok(_) => {
                    // Fall back to standard offset /markets if /markets/keyset is not supported on mock
                    let fallback_url = format!(
                        "{}/markets?limit={PAGE_LIMIT}&active=true&closed=false",
                        self.base_url
                    );
                    if let Ok(r) = self
                        .client
                        .get(&fallback_url)
                        .timeout(remaining)
                        .send()
                        .await
                        && r.status().is_success()
                        && let Ok(markets) = r.json::<Vec<GammaMarket>>().await
                    {
                        for m in markets {
                            for id in m.parse_clob_token_ids() {
                                if !token_ids.contains(&id) {
                                    token_ids.push(id);
                                }
                            }
                        }
                    }
                    break;
                }
                Err(e) => {
                    tracing::warn!("Pagination finished or timed out: {e}");
                    break;
                }
            };

            // Parse response as KeysetResponse or fallback Vec<GammaMarket>
            let body_text = match resp.text().await {
                Ok(t) => t,
                Err(_) => break,
            };

            if let Ok(keyset) = serde_json::from_str::<KeysetResponse>(&body_text) {
                if keyset.markets.is_empty() {
                    break;
                }
                for market in keyset.markets {
                    for id in market.parse_clob_token_ids() {
                        if !token_ids.contains(&id) {
                            token_ids.push(id);
                        }
                    }
                }
                match keyset.next_cursor {
                    Some(next) if !next.is_empty() && next != "LTE=" => {
                        cursor = Some(next);
                    }
                    _ => break,
                }
            } else if let Ok(markets) = serde_json::from_str::<Vec<GammaMarket>>(&body_text) {
                if markets.is_empty() {
                    break;
                }
                for market in markets {
                    for id in market.parse_clob_token_ids() {
                        if !token_ids.contains(&id) {
                            token_ids.push(id);
                        }
                    }
                }
                break;
            } else {
                break;
            }
        }

        Ok(token_ids)
    }
}
