//! WebSocket management, connection pooling, and Polymarket streaming protocols.

pub mod status;
pub mod subscription_manager;
pub mod types;
pub mod ws;
pub mod ws_manager;

pub use status::WsStatus;
pub use subscription_manager::{SubscriptionManager, TokenBucket};
pub use types::{DEFAULT_SCALE, DiscoveryEvent, WsRole};
pub use ws::{Ws, WsEvent};
pub use ws_manager::WsManager;
