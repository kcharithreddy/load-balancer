# High-Performance Dynamic Load Balancer (Go)

A fast, resilient, dynamic HTTP/WebSocket load balancer written in **Go** designed for distributed group chat systems.

## Key Features

- **Dynamic Performance Load Balancing**: Calculates real-time backend load scores combining active in-flight request counts and Exponential Moving Average (EWMA) response latency.
  $$\text{Score} = (\text{InFlight} \times 2.0) + \text{EWMA Latency (ms)}$$
- **Parallel Concurrent Feed Aggregation**: Serves unified feed queries (`GET /feed`) by querying all active backends concurrently in parallel using `sync.WaitGroup`, merging records in RAM, and deduplicating by `msg_id` in $<3\text{ ms}$.
- **Zero-Downtime Health Probing**: Periodically probes `/health` on all registered backend nodes to detect node recovery and failures automatically.
- **Sub-Second Response Caching**: In-memory thread-safe `FeedCache` prevents CPU spikes and socket queue contention during heavy concurrent load spikes.
- **WebSocket Streaming Proxy**: Upgrades and pipes bidirectional WebSocket streams (`/ws`).
- **Resilient Socket Binding**: Features automatic port-retry binding loops to handle kernel `TIME_WAIT` socket states gracefully.

## Architecture & Routes

| Route | Protocol | Description |
| :--- | :---: | :--- |
| `/message` | HTTP POST/GET | Dynamically routes POST requests to the optimal backend node. |
| `/feed` | HTTP GET | Merges chat history across all backends concurrently. |
| `/ws` | WebSocket | Proxies real-time WebSocket communication. |
| `/health` | HTTP GET | Exposes active connection metrics and live backend health status. |

## Build & Run

### Prerequisites
- Go 1.20+

### Local Execution
```bash
# Clone repository
git clone git@github.com:kcharithreddy/load-balancer.git
cd load-balancer

# Build binary
go build -o ws_load_balancer .

# Run Load Balancer on port 3000
PORT=3000 ./ws_load_balancer
```
