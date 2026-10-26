use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GammaMarket {
    pub id: Option<String>,
    pub question: Option<String>,
    #[serde(rename = "conditionId")]
    pub condition_id: Option<String>,
    #[serde(rename = "clobTokenIds")]
    pub clob_token_ids: Option<serde_json::Value>,
    pub active: Option<bool>,
    pub closed: Option<bool>,
    #[serde(rename = "volume24hr")]
    pub volume_24hr: Option<f64>,
    #[serde(default)]
    pub liquidity: Option<serde_json::Value>,
}

impl GammaMarket {
    /// Extracts liquidity as a floating-point number, whether returned as a string, float, or int.
    pub fn parse_liquidity(&self) -> f64 {
        match &self.liquidity {
            Some(serde_json::Value::String(s)) => s.parse::<f64>().unwrap_or(0.0),
            Some(serde_json::Value::Number(n)) => n.as_f64().unwrap_or(0.0),
            _ => 0.0,
        }
    }

    pub fn parse_clob_token_ids(&self) -> Vec<String> {
        let Some(val) = &self.clob_token_ids else {
            return Vec::new();
        };
        match val {
            serde_json::Value::String(s) => {
                serde_json::from_str::<Vec<String>>(s).unwrap_or_default()
            }
            serde_json::Value::Array(arr) => arr
                .iter()
                .filter_map(|v| v.as_str().map(|s| s.to_string()))
                .collect(),
            _ => Vec::new(),
        }
    }
}

/// Response payload from `/markets/keyset` endpoint.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeysetResponse {
    #[serde(default)]
    pub markets: Vec<GammaMarket>,
    #[serde(default)]
    pub next_cursor: Option<String>,
}
