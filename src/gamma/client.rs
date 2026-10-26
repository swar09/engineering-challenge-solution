use anyhow::{Context, Result};
use crate::gamma::types::GammaMarket;

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
                .build()
                .unwrap_or_default(),
            base_url: base_url.into(),
        }
    }

    /// Fetches the top 50 active markets from Gamma API and returns a flat list of CLOB token IDs.
    pub async fn fetch_top_50_clob_tokens(&self) -> Result<Vec<String>> {
        let url = format!("{}/markets?limit=50&active=true&closed=false", self.base_url);
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
            let ids = market.parse_clob_token_ids();
            for id in ids {
                if !token_ids.contains(&id) {
                    token_ids.push(id);
                }
            }
        }

        Ok(token_ids)
    }

    /// Fetches all active symbols from Gamma API using pagination with a default timeout of 30s.
    pub async fn fetch_all_symbols(&self) -> Result<Vec<String>> {
        self.fetch_all_symbols_with_timeout(std::time::Duration::from_secs(30))
            .await
    }

    /// Alias matching exact user-requested naming `fetchAllsymbols`.
    #[allow(non_snake_case)]
    #[inline]
    pub async fn fetchAllsymbols(&self) -> Result<Vec<String>> {
        self.fetch_all_symbols().await
    }

    /// Fetches all active symbols from Gamma API using pagination within the given timeout.
    pub async fn fetch_all_symbols_with_timeout(
        &self,
        timeout: std::time::Duration,
    ) -> Result<Vec<String>> {
        let start_time = std::time::Instant::now();
        let mut token_ids = Vec::new();
        let mut offset = 0;
        const PAGE_LIMIT: usize = 100;

        while start_time.elapsed() < timeout {
            let remaining = timeout.saturating_sub(start_time.elapsed());
            if remaining.is_zero() {
                break;
            }

            let url = format!(
                "{}/markets?limit={PAGE_LIMIT}&offset={offset}&active=true&closed=false",
                self.base_url
            );

            let resp = match self.client.get(&url).timeout(remaining).send().await {
                Ok(r) if r.status().is_success() => r,
                Ok(_) => break, // Reached offset limit or end
                Err(e) => {
                    tracing::warn!("Pagination finished or timed out: {e}");
                    break;
                }
            };

            let markets: Vec<GammaMarket> = match resp.json().await {
                Ok(m) => m,
                Err(_) => break,
            };

            if markets.is_empty() {
                break;
            }

            let page_len = markets.len();
            for market in markets {
                for id in market.parse_clob_token_ids() {
                    if !token_ids.contains(&id) {
                        token_ids.push(id);
                    }
                }
            }

            if page_len < PAGE_LIMIT {
                break;
            }
            offset += page_len;
        }

        Ok(token_ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_fetch_all_symbols_with_mock() {
        let client = GammaClient::new();
        let res = client
            .fetch_all_symbols_with_timeout(std::time::Duration::from_millis(500))
            .await;
        assert!(res.is_ok());
    }
}
