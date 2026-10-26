use anyhow::Result;
use crossbeam::channel::Sender;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, VecDeque};
use tracing::{info, warn};

use crate::orderbook::OrderbookMessage;
use crate::symbol::SymbolState;
use crate::websocket::status::WsStatus;
use crate::websocket::subscription_manager::SubscriptionManager;
use crate::websocket::types::{DiscoveryEvent, WsRole};
use crate::websocket::ws::{Ws, WsEvent};

pub const MIN_STANDBY_CAPACITY: usize = 10;
pub const DEFAULT_MAX_SYMBOLS_PER_SOCKET: usize = 50;
pub const DEFAULT_MAX_DELTA_RATE_PER_SOCKET: u64 = 1500;
pub const DEFAULT_MAX_ACTIVE_PAIRS: usize = 128;
pub const DEFAULT_MIN_ACTIVE_PAIRS: usize = 4;
pub const GLOBAL_HEAP_WATERMARK_CAPACITY: usize = 128;

/// Timestamped message for lock-free global min-heap sorting across all sockets.
#[derive(Debug, Clone, PartialEq)]
pub struct SequencedMessage {
    pub exchange_ts_micros: i64,
    pub arrival_counter: u64,
    pub message: OrderbookMessage,
}

impl Eq for SequencedMessage {}

impl Ord for SequencedMessage {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.exchange_ts_micros
            .cmp(&other.exchange_ts_micros)
            .then_with(|| self.arrival_counter.cmp(&other.arrival_counter))
    }
}

impl PartialOrd for SequencedMessage {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

pub struct WsManager {
    pub pool: Vec<Ws>,
    pub standby_pool: VecDeque<Ws>,
    pub symbol_states: HashMap<String, SymbolState>,
    pub writer_tx: Sender<Vec<OrderbookMessage>>,
    pub master_index: usize,
    pub subscription_manager: SubscriptionManager,
    pub max_symbols_per_socket: usize,
    pub max_delta_rate_per_socket: u64,
    pub max_active_pairs: usize,
    pub global_heap: BinaryHeap<Reverse<SequencedMessage>>,
    pub global_seq_counter: u64,
}

impl WsManager {
    pub fn new(
        writer_tx: Sender<Vec<OrderbookMessage>>,
        subscription_manager: SubscriptionManager,
    ) -> Self {
        dotenvy::dotenv().ok();
        let max_symbols = std::env::var("MAX_SYMBOLS_PER_SOCKET")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_SYMBOLS_PER_SOCKET);
        let max_delta_rate = std::env::var("MAX_DELTA_RATE_PER_SOCKET")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_DELTA_RATE_PER_SOCKET);
        let max_pairs = std::env::var("MAX_ACTIVE_PAIRS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_MAX_ACTIVE_PAIRS);

        Self {
            pool: Vec::new(),
            standby_pool: VecDeque::new(),
            symbol_states: HashMap::new(),
            writer_tx,
            master_index: 0,
            subscription_manager,
            max_symbols_per_socket: max_symbols,
            max_delta_rate_per_socket: max_delta_rate,
            max_active_pairs: max_pairs,
            global_heap: BinaryHeap::new(),
            global_seq_counter: 0,
        }
    }

    /// Pushes an `OrderbookMessage` into the global min-heap sorted by exchange timestamp.
    pub fn push_to_global_heap(&mut self, msg: OrderbookMessage) {
        self.global_seq_counter = self.global_seq_counter.saturating_add(1);
        let ts_micros = msg.exchange_timestamp().timestamp_micros();
        self.global_heap.push(Reverse(SequencedMessage {
            exchange_ts_micros: ts_micros,
            arrival_counter: self.global_seq_counter,
            message: msg,
        }));
    }

    /// Drains up to `count` items from the global min-heap in strictly sorted order.
    pub fn drain_global_heap_batch(&mut self, count: usize) -> Vec<OrderbookMessage> {
        let mut batch = Vec::with_capacity(count);
        while batch.len() < count
            && let Some(Reverse(seq_msg)) = self.global_heap.pop()
        {
            batch.push(seq_msg.message);
        }
        batch
    }

    /// Drains all messages currently buffered in the global min-heap in strictly sorted order.
    pub fn drain_all_global_heap(&mut self) -> Vec<OrderbookMessage> {
        let mut all = Vec::with_capacity(self.global_heap.len());
        while let Some(Reverse(seq_msg)) = self.global_heap.pop() {
            all.push(seq_msg.message);
        }
        all
    }

    /// Returns the sum of all deltas processed across all sockets in the pool.
    pub fn total_deltas_received(&self) -> u64 {
        self.pool.iter().map(|ws| ws.delta_rate_sum).sum()
    }

    /// Scans all active Primary sockets and returns the pool index of the one with the smallest product of `symbols.len() * delta_rate_sum`, indicating the lightest current load.
    pub fn find_least_loaded_socket_index(&self) -> Option<usize> {
        self.pool
            .iter()
            .enumerate()
            .filter(|(_, ws)| {
                (ws.status == WsStatus::Master || ws.status == WsStatus::Healthy)
                    && ws.role == WsRole::Primary
            })
            .min_by_key(|(_, ws)| (ws.symbols.len() as u64) * ws.delta_rate_sum)
            .map(|(idx, _)| idx)
    }

    /// Scans active Primary sockets below the 80% capacity threshold (`symbols < max_symbols` and `delta_rate < max_delta_rate`)
    /// and returns the least-loaded candidate socket index.
    pub fn find_available_candidate_socket_index(&self) -> Option<usize> {
        self.pool
            .iter()
            .enumerate()
            .filter(|(_, ws)| {
                (ws.status == WsStatus::Master || ws.status == WsStatus::Healthy)
                    && ws.role == WsRole::Primary
                    && ws.symbols.len() < self.max_symbols_per_socket
                    && ws.delta_rate_sum < self.max_delta_rate_per_socket
            })
            .min_by_key(|(_, ws)| (ws.symbols.len() as u64) * ws.delta_rate_sum)
            .map(|(idx, _)| idx)
    }

    /// Dynamically expands the connection pool by popping a primary and replica pair from the standby pool,
    /// assigning a new pair ID, and returning the primary socket index in `self.pool`.
    pub async fn expand_pool_from_standby(&mut self, ws_url: &str) -> Option<usize> {
        let current_pairs = self.pool.len() / 2;
        if current_pairs >= self.max_active_pairs {
            warn!(
                "Active socket pool reached hard ceiling of {} pairs; cannot expand further",
                self.max_active_pairs
            );
            return None;
        }

        if self.standby_pool.len() < 2 {
            let _ = self.maintain_standby_pool(ws_url).await;
        }

        let mut primary = self.standby_pool.pop_front()?;
        let mut replica = self.standby_pool.pop_front()?;

        let new_pair_id = self
            .pool
            .iter()
            .map(|ws| ws.pair_id)
            .max()
            .map(|id| id + 1)
            .unwrap_or(0);
        primary.pair_id = new_pair_id;
        primary.role = WsRole::Primary;
        primary.status = WsStatus::Healthy;

        replica.pair_id = new_pair_id;
        replica.role = WsRole::Replica;
        replica.status = WsStatus::Healthy;

        let primary_idx = self.pool.len();
        self.pool.push(primary);
        self.pool.push(replica);

        info!("Dynamic expansion: added Pair #{new_pair_id} from standby (Primary #{primary_idx})");
        let _ = self.maintain_standby_pool(ws_url).await;

        Some(primary_idx)
    }

    /// Scans the pool and returns the index of the Replica socket assigned to the given `pair_id`.
    pub fn find_paired_replica_index(&self, pair_id: usize) -> Option<usize> {
        self.pool
            .iter()
            .position(|ws| ws.pair_id == pair_id && ws.role == WsRole::Replica)
    }

    /// Drains all messages currently buffered in the global min-heap in strictly sorted order
    /// and dispatches them to the writer channel.
    pub fn flush_all(&mut self) {
        let sorted = self.drain_all_global_heap();
        if !sorted.is_empty() {
            let _ = self.writer_tx.send(sorted);
        }
    }

    /// Iterates all active Primary sockets, converts each resting orderbook into a `SnapshotDomain`,
    /// pushes snapshots into the global min-heap, and flushes to writer channel.
    pub fn snapshot_resting_books(&mut self) {
        let now = crate::util::local_now();
        let mut snaps = Vec::new();
        for ws in &mut self.pool {
            if ws.should_write_to_buffer() {
                for book in ws.orderbooks.values() {
                    if book.initial_dump_applied {
                        let ex_ts = book.last_exchange_timestamp.unwrap_or(now);
                        let snap = book.to_snapshot_domain(now, ex_ts);
                        snaps.push(OrderbookMessage::Snapshot(snap));
                    }
                }
            }
        }
        for s in snaps {
            self.push_to_global_heap(s);
        }
        let sorted = self.drain_all_global_heap();
        if !sorted.is_empty() {
            let _ = self.writer_tx.send(sorted);
        }
    }

    /// Sends a PING frame to every active pool socket and every standby socket, marking any socket that fails as `WsStatus::Unhealthy`.
    pub async fn send_pings(&mut self) {
        for ws in &mut self.pool {
            if (ws.status == WsStatus::Master || ws.status == WsStatus::Healthy)
                && let Err(e) = ws.send_ping().await
            {
                warn!("Failed to send PING to socket: {e}");
                ws.status = WsStatus::Unhealthy;
            }
        }
        for ws in &mut self.standby_pool {
            if ws.status == WsStatus::Healthy
                && let Err(e) = ws.send_ping().await
            {
                warn!("Failed to send PING to standby socket: {e}");
                ws.status = WsStatus::Unhealthy;
            }
        }
    }

    /// Ensures the standby pool maintains at least `MIN_STANDBY_CAPACITY` pre-warmed sockets.
    pub async fn maintain_standby_pool(&mut self, ws_url: &str) -> Result<()> {
        let current_len = self.standby_pool.len();
        if current_len < MIN_STANDBY_CAPACITY {
            let deficit = MIN_STANDBY_CAPACITY - current_len;
            for _ in 0..deficit {
                match Ws::connect(ws_url, WsStatus::Healthy, WsRole::Replica, usize::MAX).await {
                    Ok(ws) => {
                        self.standby_pool.push_back(ws);
                    }
                    Err(e) => {
                        warn!("Failed to pre-warm standby socket: {e}");
                        break;
                    }
                }
            }
        }
        Ok(())
    }

    /// Pings all standby sockets, filters out disconnected or unhealthy sockets, and replenishes the standby pool.
    pub async fn prune_and_ping_standby(&mut self, ws_url: &str) {
        let mut healthy_standbys = VecDeque::new();
        while let Some(mut ws) = self.standby_pool.pop_front() {
            if ws.status == WsStatus::Healthy && ws.send_ping().await.is_ok() {
                healthy_standbys.push_back(ws);
            }
        }
        self.standby_pool = healthy_standbys;
        let _ = self.maintain_standby_pool(ws_url).await;
    }

    /// Checks delta timing differential across all paired Primary and Replica sockets and dispatches rate-limited resubscriptions.
    pub async fn check_and_process_subscriptions(&mut self) {
        // Collect primary-replica index pairs
        let mut pairs: Vec<(usize, usize)> = Vec::new();
        for (p_idx, ws) in self.pool.iter().enumerate() {
            if ws.role == WsRole::Primary
                && (ws.status == WsStatus::Master || ws.status == WsStatus::Healthy)
                && let Some(r_idx) = self.find_paired_replica_index(ws.pair_id)
            {
                pairs.push((p_idx, r_idx));
            }
        }

        for (pri_idx, rep_idx) in pairs {
            let pri_ws = &self.pool[pri_idx];
            let rep_ws = &self.pool[rep_idx];
            self.subscription_manager.monitor_pairs(pri_ws, rep_ws);
        }

        self.subscription_manager
            .process_queue(&mut self.pool)
            .await;
    }

    /// Races all active pool sockets concurrently for the next incoming frame, dispatches the winning event,
    /// feeds Primary messages into the global min-heap, and flushes strictly sorted batches to writer channel.
    pub async fn poll_next_message(&mut self, ws_url: &str) -> Result<()> {
        let mut futures = Vec::new();
        for (socket_idx, ws) in self.pool.iter_mut().enumerate() {
            if ws.status == WsStatus::Master || ws.status == WsStatus::Healthy {
                futures.push(Box::pin(
                    async move { (socket_idx, ws.receive_message().await) },
                ));
            }
        }

        if futures.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            return Ok(());
        }

        let ((socket_idx, result), _, remaining) = futures_util::future::select_all(futures).await;
        drop(remaining);

        match result {
            Ok(event) => {
                match event {
                    WsEvent::Discovery(discovery_events) => {
                        for ev in discovery_events {
                            match ev {
                                DiscoveryEvent::NewMarket(m) => {
                                    info!("NewMarket discovered: {} tokens", m.assets_ids.len());
                                    for token in &m.assets_ids {
                                        if !self.symbol_states.contains_key(token) {
                                            // 1. Try finding an existing socket below 80% threshold
                                            // 2. If all sockets are saturated, dynamically claim a new pair from standby
                                            // 3. Fallback to least loaded socket if hard cap reached
                                            let target_primary_idx = match self
                                                .find_available_candidate_socket_index()
                                            {
                                                Some(idx) => Some(idx),
                                                None => {
                                                    info!(
                                                        "All active sockets saturated; dynamic expansion for token {token}..."
                                                    );
                                                    self.expand_pool_from_standby(ws_url)
                                                        .await
                                                        .or_else(|| {
                                                            self.find_least_loaded_socket_index()
                                                        })
                                                }
                                            };

                                            if let Some(primary_idx) = target_primary_idx {
                                                let pair_id = self.pool[primary_idx].pair_id;
                                                info!(
                                                    "Assigning token {token} to pair #{pair_id} (Primary #{primary_idx})"
                                                );

                                                if let Some(replica_idx) =
                                                    self.find_paired_replica_index(pair_id)
                                                {
                                                    let replica_ws = &mut self.pool[replica_idx];
                                                    if let Err(e) = replica_ws
                                                        .subscribe_dynamic(std::slice::from_ref(
                                                            token,
                                                        ))
                                                        .await
                                                    {
                                                        warn!(
                                                            "Failed subscribing Replica #{replica_idx}: {e}"
                                                        );
                                                    }
                                                }

                                                let primary_ws = &mut self.pool[primary_idx];
                                                if let Err(e) = primary_ws
                                                    .subscribe_dynamic(std::slice::from_ref(token))
                                                    .await
                                                {
                                                    warn!(
                                                        "Failed subscribing Primary #{primary_idx}: {e}"
                                                    );
                                                }

                                                self.symbol_states.insert(
                                                    token.clone(),
                                                    SymbolState::new(token.clone(), primary_idx),
                                                );
                                            }
                                        }
                                    }
                                }
                                DiscoveryEvent::MarketResolved(r) => {
                                    info!(
                                        "MarketResolved: market={}, retiring {} tokens",
                                        r.market,
                                        r.assets_ids.len()
                                    );
                                    for token in &r.assets_ids {
                                        if let Some(state) = self.symbol_states.remove(token) {
                                            let primary_idx = state.socket_index;
                                            if primary_idx < self.pool.len() {
                                                let pair_id = self.pool[primary_idx].pair_id;

                                                if let Some(replica_idx) =
                                                    self.find_paired_replica_index(pair_id)
                                                {
                                                    let replica_ws = &mut self.pool[replica_idx];
                                                    let _ = replica_ws
                                                        .unsubscribe(std::slice::from_ref(token))
                                                        .await;
                                                }

                                                let primary_ws = &mut self.pool[primary_idx];
                                                let _ = primary_ws
                                                    .unsubscribe(std::slice::from_ref(token))
                                                    .await;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    WsEvent::Processed { count: _, messages } => {
                        let ws = &self.pool[socket_idx];
                        if ws.should_write_to_buffer() {
                            for msg in messages {
                                self.push_to_global_heap(msg);
                            }
                        }
                    }
                    _ => {}
                }

                // If global heap reached watermark capacity, flush 64-message sorted batch to writer channel
                if self.global_heap.len() >= GLOBAL_HEAP_WATERMARK_CAPACITY {
                    let batch =
                        self.drain_global_heap_batch(crate::websocket::ws::DEFAULT_BATCH_CAPACITY);
                    if !batch.is_empty() {
                        let _ = self.writer_tx.send(batch);
                    }
                }
            }
            Err(e) => {
                warn!("Ws died: socket #{socket_idx} error: {e}");
                self.pool[socket_idx].status = WsStatus::Disconnected;
                self.handle_socket_disconnect(socket_idx, ws_url).await;
            }
        }

        Ok(())
    }

    /// Executes hot-swap failover protocol for a disconnected socket:
    /// evaluates paired replica health to choose Branch A (direct promotion) or Branch B (cold standby),
    /// and replenishes the standby pool to the minimum capacity.
    pub async fn handle_socket_disconnect(&mut self, dead_idx: usize, ws_url: &str) {
        let dead_role = self.pool[dead_idx].role;
        let dead_pair_id = self.pool[dead_idx].pair_id;
        let is_master = dead_idx == self.master_index;
        let symbols = std::mem::take(&mut self.pool[dead_idx].symbols);

        if dead_role == WsRole::Primary {
            info!("Ws died: Primary #{dead_idx} (pair #{dead_pair_id}). Evaluating failover...");
            let replica_opt = self.find_paired_replica_index(dead_pair_id);
            let replica_is_healthy = replica_opt
                .map(|idx| {
                    self.pool[idx].status == WsStatus::Healthy
                        || self.pool[idx].status == WsStatus::Master
                })
                .unwrap_or(false);

            if replica_is_healthy {
                let replica_idx = replica_opt.unwrap();
                let now = crate::util::local_now();
                let mut snaps = Vec::new();
                let rep_symbols = self.pool[replica_idx].symbols.clone();

                {
                    let replica = &mut self.pool[replica_idx];
                    replica.role = WsRole::Primary;
                    if is_master {
                        replica.status = WsStatus::Master;
                        self.master_index = replica_idx;
                    }
                    info!(
                        "Hot swap Branch A: promoted Replica #{replica_idx} to Primary (pair #{dead_pair_id})"
                    );

                    for book in replica.orderbooks.values() {
                        if book.initial_dump_applied {
                            let ex_ts = book.last_exchange_timestamp.unwrap_or(now);
                            let snap = book.to_snapshot_domain(now, ex_ts);
                            snaps.push(OrderbookMessage::Snapshot(snap));
                        }
                    }
                }

                for s in snaps {
                    self.push_to_global_heap(s);
                }
                let sorted = self.drain_all_global_heap();
                if !sorted.is_empty() {
                    let _ = self.writer_tx.send(sorted);
                }

                for s in &rep_symbols {
                    if let Some(state) = self.symbol_states.get_mut(s) {
                        state.socket_index = replica_idx;
                    }
                }

                // Restore replica from standby pool
                if let Some(mut standby_ws) = self.standby_pool.pop_front() {
                    info!("Hot swap: deploying standby as Replica (pair #{dead_pair_id})");
                    standby_ws.status = WsStatus::Healthy;
                    standby_ws.role = WsRole::Replica;
                    standby_ws.pair_id = dead_pair_id;

                    if !symbols.is_empty()
                        && let Err(e) = standby_ws.subscribe(&symbols).await
                    {
                        warn!("Failed subscribing new replica: {e}");
                    }
                    self.pool[dead_idx] = standby_ws;
                } else {
                    warn!("Standby pool depleted replacing socket #{dead_idx}!");
                    self.pool[dead_idx].status = WsStatus::Disconnected;
                }
            } else {
                // Replica is not healthy -> Promote cold standby with initial_dump: true
                warn!(
                    "Hot swap Branch B: replica dead; deploying cold standby as Primary (pair #{dead_pair_id})"
                );
                if let Some(mut new_primary) = self.standby_pool.pop_front() {
                    new_primary.role = WsRole::Primary;
                    new_primary.pair_id = dead_pair_id;
                    if is_master {
                        new_primary.status = WsStatus::Master;
                        self.master_index = dead_idx;
                    }

                    // Subscribe with initial_dump: true so full book arrives and snapshots to disk
                    if !symbols.is_empty()
                        && let Err(e) = new_primary.subscribe(&symbols).await
                    {
                        warn!("Failed subscribing new primary with initial dump: {e}");
                    }

                    for s in &symbols {
                        if let Some(state) = self.symbol_states.get_mut(s) {
                            state.socket_index = dead_idx;
                        }
                    }

                    self.pool[dead_idx] = new_primary;
                } else {
                    warn!("Standby pool depleted while deploying Primary #{dead_idx}!");
                    self.pool[dead_idx].status = WsStatus::Disconnected;
                }

                // Deploy second standby to replace the dead replica
                if let Some(replica_idx) = replica_opt
                    && let Some(mut new_replica) = self.standby_pool.pop_front()
                {
                    new_replica.role = WsRole::Replica;
                    new_replica.pair_id = dead_pair_id;
                    new_replica.status = WsStatus::Healthy;
                    if !symbols.is_empty()
                        && let Err(e) = new_replica.subscribe(&symbols).await
                    {
                        warn!("Failed subscribing 2nd standby replica: {e}");
                    }
                    self.pool[replica_idx] = new_replica;
                }
            }
        } else {
            // Dead socket was a Replica: replace from standby
            info!(
                "Ws died: Replica #{dead_idx} (pair #{dead_pair_id}). Hot swapping from standby..."
            );
            if let Some(mut standby_ws) = self.standby_pool.pop_front() {
                standby_ws.status = WsStatus::Healthy;
                standby_ws.role = WsRole::Replica;
                standby_ws.pair_id = dead_pair_id;

                if !symbols.is_empty()
                    && let Err(e) = standby_ws.subscribe(&symbols).await
                {
                    warn!("Failed subscribing new replica: {e}");
                }
                self.pool[dead_idx] = standby_ws;
            } else {
                warn!("Standby pool depleted replacing replica #{dead_idx}!");
                self.pool[dead_idx].status = WsStatus::Disconnected;
            }
        }

        // Maintain standby pool (len >= 10)
        let _ = self.maintain_standby_pool(ws_url).await;
    }

    /// Sends a Close frame to every socket in the active pool and every socket in the standby pool, marking each as `Disconnected`.
    pub async fn close_all(&mut self) {
        for ws in &mut self.pool {
            let _ = ws.close().await;
        }
        for ws in &mut self.standby_pool {
            let _ = ws.close().await;
        }
    }
}
