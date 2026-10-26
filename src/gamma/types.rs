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
}

impl GammaMarket {
    pub fn parse_clob_token_ids(&self) -> Vec<String> {
        let Some(val) = &self.clob_token_ids else {
            return Vec::new();
        };
        match val {
            serde_json::Value::String(s) => {
                serde_json::from_str::<Vec<String>>(s).unwrap_or_default()
            }
            serde_json::Value::Array(arr) => {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            }
            _ => Vec::new(),
        }
    }
}
