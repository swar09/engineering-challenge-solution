# latency optimizations experiments


## experiment 1: tcp source port rotation and edge path diversity

both polymarket and kalshi host their public websockets behind edge cloud networks (cloudflare and aws cloudfront respectively). these anycast ip addresses resolve to multiple physical edge datacenters and ingress routers.

when establishing a standard tcp connection, the linux kernel assigns an ephemeral source port from the local range. edge load balancers compute an ecmp (equal-cost multi-path) hash over the 5-tuple:

$$\text{hash} = \text{hash}(\text{src\_ip}, \text{src\_port}, \text{dst\_ip}, \text{dst\_port}, \text{protocol})$$

because `src_ip`, `dst_ip`, `dst_port` (443), and protocol (tcp) are fixed, the `src_port` alone dictates which edge server, router queue, and core network transit path handles the websocket stream.

### method
we bound outgoing tcp client sockets across rotating source port ranges before issuing the tls handshake:

```rust
use socket2::{Domain, Protocol, Socket, Type};
use std::net::SocketAddr;

fn bind_ephemeral_source_socket(local_port: u16) -> std::io::Result<std::net::TcpStream> {
    let socket = Socket::new(Domain::IPV4, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_reuse_address(true)?;
    socket.set_reuse_port(true)?;
    socket.set_nodelay(true)?;
    
    let bind_addr: SocketAddr = format!("0.0.0.0:{local_port}").parse().unwrap();
    socket.bind(&bind_addr.into())?;
    Ok(socket.into())
}
```

### findings
across 100 connection samples to polymarket's clob websocket endpoint (`wss://ws-subscriptions-clob.polymarket.com`):

1. **rtt distribution**: ping round-trip times varied from 14.2 ms to 38.6 ms on the exact same server and internet connection.
2. **edge router clustering**: certain source port hashes consistently landed on edge nodes with 8 to 12 ms lower ping variance during peak us trading hours.
3. **takeaway**: pre-measuring rtt during socket pool warming and binding the top quartile of low-latency paths to primary sockets reduced median delta arrival delay by 11.4 ms.

## experiment 2: connection racing and happy eyeballs

dns resolution for `ws-subscriptions-clob.polymarket.com` returns multiple anycast ip addresses. standard clients call `connect` on the first resolved address sequentially.

### method
we tested a connection racer that issues simultaneous asynchronous tcp/tls handshakes to all resolved a/aaaa records. the first socket to complete the tls handshake and receive the websocket 101 switching protocols frame is kept as primary. the runner-up sockets are not discarded; they are transitioned into the standby pool.

```rust
async fn race_connections(target_addrs: &[SocketAddr], url: &str) -> Option<tokio_tungstenite::WebSocketStream<...>> {
    let mut tasks = Vec::new();
    for &addr in target_addrs {
        tasks.push(tokio::spawn(async move {
            let start = quanta::Instant::now();
            let res = connect_direct_socket(addr, url).await;
            (res, start.elapsed())
        }));
    }
    // select fastest winner
}
```

### findings
1. dns round-robin often includes edge nodes under temporary peering congestion.
2. connection racing shaved 45 to 110 ms off cold-start startup time compared to sequential retries.
3. runner-up connections pre-fill the standby pool with known low-latency sockets, eliminating connection lag when a failover occurs.

## experiment 3: post-maintenance connection sniping

exchanges periodically schedule gateway maintenance, database migrations, or morning system restarts (kalshi typically between 03:00 and 05:00 est, polymarket during off-peak contracts rollouts).

### observation
immediately after an exchange gateway reopens, market makers and retail participants begin reconnecting. within 20 to 60 seconds, thousands of websocket connections flood the ingress servers, causing edge queues to build up and buffering delays to spike.

### strategy
1. when an exchange gateway returns 502/503 during maintenance windows, our standby manager issues exponential backoff probes starting at 50 ms.
2. the moment the first 101 switching protocols response returns, the master socket sends its full subscription batch before general market traffic arrives.
3. by securing ingress buffer slots early, delta packets arrive with minimal buffer wait time on the exchange edge proxies.

## experiment 4: kernel socket tuning and syscall elimination

under high delta volume (exceeding 2,000 updates a second across hundreds of symbols), small kernel overheads accumulate into packet delays. we benchmarked three tuning parameters on linux:

### 1. TCP_NODELAY
disabling nagle's algorithm is standard but critical. nagle buffers small outgoing frames up to 200 ms to combine packets. with `socket.set_nodelay(true)`, subscription frames and pings transmit immediately.

### 2. TCP_QUICKACK
by default, linux uses delayed acknowledgments (up to 40 ms) to combine acks with outgoing data. enabling `TCP_QUICKACK` via `setsockopt` forces the kernel to ack inbound packet windows immediately, prompting the exchange proxy to stream subsequent tcp segments without pause.

### 3. replacing clock_gettime with tsc quanta
calling `chrono::Utc::now()` on every orderbook update issues a `clock_gettime(CLOCK_REALTIME)` system call. while linux optimizes this through vdso, issuing 10,000 vdso reads a second still creates cache misses and register spills.

benchmarks measuring 100,000 iterations:

| clock method | latency per call | throughput |
| :--- | :--- | :--- |
| `chrono::Utc::now()` (vdso) | 54.2 ns | 18.4m calls/sec |
| `quanta::Clock::now()` (tsc `rdtsc`) | 7.8 ns | 128.2m calls/sec |

using the hardware tsc counter cut local timestamp overhead by 85 percent, leaving cpu cycles available for book sorting and sbe decoding.

## summary of practical recommendations

1. bind outgoing sockets across varied ephemeral ports to avoid congested ecmp edge paths.
2. race connection handshakes across resolved gateway ips to automatically latch onto the lowest-latency edge node.
3. set `TCP_NODELAY` and `TCP_QUICKACK` on all active websocket sockets.
4. use hardware tsc reads for local timestamping so the event loop never blocks on kernel time queries.
