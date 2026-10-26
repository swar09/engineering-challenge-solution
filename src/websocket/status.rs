#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WsStatus {
    Master,
    Healthy,
    Unhealthy,
    Disconnected,
}
