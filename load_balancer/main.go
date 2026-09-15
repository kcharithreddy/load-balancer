package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

// ─────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────

// Load Balancer runs on Sys2 inside container port 3000
// Container port 3000 maps to external port 3246 on 10.1.75.51
const defaultListenAddr = ":3000"

// Backends:
// Sys2 Backend #1: 172.17.0.47:4000
// Sys3 Backend #2: 172.17.0.48:3000 (mapped to external 3247)
// Sys4 Backend #3: 172.17.0.49:3000 (mapped to external 3248)
var defaultBackendURLs = []string{
	"http://172.17.0.47:4000",
	"http://172.17.0.48:3000",
	"http://172.17.0.49:3000",
}

const (
	ThresholdInFlight int64   = 30   // Dynamic switching threshold
	ThresholdLatency  float64 = 80.0 // EWMA Latency threshold (ms)
	EWMABeta                  = 0.2  // Exponential moving average factor
)

// ─────────────────────────────────────────────
// Backend Structure
// ─────────────────────────────────────────────

type Backend struct {
	URL          *url.URL
	HTTPProxy    *httputil.ReverseProxy
	WSURL        string
	Name         string
	alive        int32 // 1 = alive, 0 = dead
	inFlight     int64 // Active requests currently in-flight
	totalReqs    uint64
	totalErrors  uint64
	consecErrors int32
	avgLatencyMs float64
	mu           sync.RWMutex
}

func (b *Backend) IsAlive() bool {
	return atomic.LoadInt32(&b.alive) == 1
}

func (b *Backend) SetAlive(v bool) {
	if v {
		atomic.StoreInt32(&b.alive, 1)
		atomic.StoreInt32(&b.consecErrors, 0)
	} else {
		atomic.StoreInt32(&b.alive, 0)
	}
}

func (b *Backend) IncrInFlight() int64 {
	atomic.AddUint64(&b.totalReqs, 1)
	return atomic.AddInt64(&b.inFlight, 1)
}

func (b *Backend) DecrInFlight() int64 {
	return atomic.AddInt64(&b.inFlight, -1)
}

func (b *Backend) GetInFlight() int64 {
	return atomic.LoadInt64(&b.inFlight)
}

func (b *Backend) RecordResult(latencyMs float64, isError bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if isError {
		atomic.AddUint64(&b.totalErrors, 1)
		atomic.AddInt32(&b.consecErrors, 1)
	} else {
		atomic.StoreInt32(&b.consecErrors, 0)
		if b.avgLatencyMs == 0 {
			b.avgLatencyMs = latencyMs
		} else {
			b.avgLatencyMs = EWMABeta*latencyMs + (1-EWMABeta)*b.avgLatencyMs
		}
	}
}

func (b *Backend) GetAvgLatency() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.avgLatencyMs
}

// ─────────────────────────────────────────────
// Dynamic Load Balancer Core
// ─────────────────────────────────────────────

type DynamicLoadBalancer struct {
	backends          []*Backend
	counter           uint64
	metrics           *Metrics
	replicationClient *http.Client
}

func NewDynamicLoadBalancer(rawURLs []string) *DynamicLoadBalancer {
	backends := make([]*Backend, 0, len(rawURLs))

	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 90 * time.Second,
		}).DialContext,
		MaxIdleConns:        50000,
		MaxIdleConnsPerHost: 15000,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression: true,
	}

	for i, raw := range rawURLs {
		parsed, err := url.Parse(raw)
		if err != nil {
			log.Fatalf("Invalid backend URL %s: %v", raw, err)
		}

		wsScheme := "ws"
		if parsed.Scheme == "https" {
			wsScheme = "wss"
		}
		wsURL := fmt.Sprintf("%s://%s/ws", wsScheme, parsed.Host)

		b := &Backend{
			URL:   parsed,
			WSURL: wsURL,
			Name:  fmt.Sprintf("backend-%d (%s)", i+1, parsed.Host),
			alive: 1,
		}

		proxy := httputil.NewSingleHostReverseProxy(parsed)
		proxy.Transport = tr
		proxy.ModifyResponse = func(resp *http.Response) error {
			if resp.StatusCode >= 500 {
				b.RecordResult(0, true)
			} else {
				b.RecordResult(0, false)
			}
			return nil
		}
		proxy.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
			b.RecordResult(0, true)
			w.WriteHeader(http.StatusBadGateway)
		}
		b.HTTPProxy = proxy
		backends = append(backends, b)
	}

	replTr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   3 * time.Second,
			KeepAlive: 90 * time.Second,
		}).DialContext,
		MaxIdleConns:        50000,
		MaxIdleConnsPerHost: 20000,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression: true,
	}

	return &DynamicLoadBalancer{
		backends: backends,
		metrics:  newMetrics(),
		replicationClient: &http.Client{
			Transport: replTr,
			Timeout:   5 * time.Second,
		},
	}
}

// SelectOptimalBackend picks the backend with lowest load score (InFlight & EWMA Latency)
func (lb *DynamicLoadBalancer) SelectOptimalBackend() *Backend {
	n := uint64(len(lb.backends))
	if n == 0 {
		return nil
	}

	var best *Backend
	var bestScore float64 = 1e18

	rrOffset := atomic.AddUint64(&lb.counter, 1)

	for i := uint64(0); i < n; i++ {
		idx := (rrOffset + i) % n
		b := lb.backends[idx]

		if !b.IsAlive() {
			continue
		}

		inFlight := b.GetInFlight()
		avgLat := b.GetAvgLatency()

		// Score = (InFlight * 2.0) + AvgLatency
		score := float64(inFlight)*2.0 + avgLat

		if best == nil || score < bestScore {
			best = b
			bestScore = score
		}
	}

	// Guaranteed Fallback: if all backends marked dead, pick round-robin from all backends anyway!
	if best == nil {
		idx := rrOffset % n
		best = lb.backends[idx]
	}

	return best
}

// ─────────────────────────────────────────────
// Metrics Tracker
// ─────────────────────────────────────────────

type Metrics struct {
	mu            sync.Mutex
	TotalRequests int64            `json:"total_requests"`
	ActiveConns   int64            `json:"active_connections"`
	TotalErrors   int64            `json:"total_errors"`
	BackendStats  map[string]int64 `json:"backend_in_flight"`
	StartTime     time.Time        `json:"-"`
}

func newMetrics() *Metrics {
	return &Metrics{
		StartTime:    time.Now(),
		BackendStats: make(map[string]int64),
	}
}

func (m *Metrics) Snapshot(lb *DynamicLoadBalancer) map[string]interface{} {
	m.mu.Lock()
	defer m.mu.Unlock()

	backendDetails := make([]map[string]interface{}, 0, len(lb.backends))
	var totalInFlight int64 = 0

	for _, b := range lb.backends {
		inFlight := b.GetInFlight()
		totalInFlight += inFlight
		backendDetails = append(backendDetails, map[string]interface{}{
			"name":           b.Name,
			"url":            b.URL.String(),
			"alive":          b.IsAlive(),
			"in_flight":      inFlight,
			"avg_latency_ms": fmt.Sprintf("%.2fms", b.GetAvgLatency()),
			"total_reqs":     atomic.LoadUint64(&b.totalReqs),
			"total_errors":   atomic.LoadUint64(&b.totalErrors),
		})
	}

	uptime := time.Since(m.StartTime).Seconds()
	reqs := atomic.LoadInt64(&m.TotalRequests)
	throughput := float64(reqs) / max(uptime, 0.001)

	return map[string]interface{}{
		"load_balancer_mode": "Dynamic Performance-Based Load Balancer",
		"listen_addr":        listenAddr(),
		"threshold_in_flight": ThresholdInFlight,
		"threshold_latency":  fmt.Sprintf("%.0fms", ThresholdLatency),
		"uptime_seconds":     uptime,
		"total_requests":     reqs,
		"active_in_flight":   totalInFlight,
		"total_errors":       atomic.LoadInt64(&m.TotalErrors),
		"throughput_req_sec": round(throughput, 2),
		"backends":           backendDetails,
	}
}

func listenAddr() string {
	if p := os.Getenv("PORT"); p != "" {
		return ":" + p
	}
	return defaultListenAddr
}

// ─────────────────────────────────────────────
// Reverse Proxy Handler (/message, /feed)
// ─────────────────────────────────────────────

func (lb *DynamicLoadBalancer) handleProxyRequest(w http.ResponseWriter, r *http.Request) {
	atomic.AddInt64(&lb.metrics.TotalRequests, 1)

	targetBackend := lb.SelectOptimalBackend()
	if targetBackend == nil {
		http.Error(w, `{"error":"No healthy backends available"}`, http.StatusServiceUnavailable)
		atomic.AddInt64(&lb.metrics.TotalErrors, 1)
		return
	}

	targetBackend.IncrInFlight()
	defer targetBackend.DecrInFlight()

	targetBackend.HTTPProxy.ServeHTTP(w, r)
}

type bufferedResponseWriter struct {
	header     http.Header
	buf        bytes.Buffer
	statusCode int
	wroteCode  bool
}

func (b *bufferedResponseWriter) Header() http.Header {
	if b.header == nil {
		b.header = make(http.Header)
	}
	return b.header
}

func (b *bufferedResponseWriter) WriteHeader(code int) {
	if !b.wroteCode {
		b.statusCode = code
		b.wroteCode = true
	}
}

func (b *bufferedResponseWriter) Write(p []byte) (int, error) {
	if !b.wroteCode {
		b.statusCode = http.StatusOK
		b.wroteCode = true
	}
	return b.buf.Write(p)
}

func (b *bufferedResponseWriter) FlushTo(w http.ResponseWriter) {
	for k, v := range b.header {
		for _, val := range v {
			w.Header().Add(k, val)
		}
	}
	code := b.statusCode
	if code == 0 {
		code = http.StatusOK
	}
	w.WriteHeader(code)
	w.Write(b.buf.Bytes())
}

// ─────────────────────────────────────────────
// WebSocket Handler (/ws)
// ─────────────────────────────────────────────

var upgrader = websocket.Upgrader{
	CheckOrigin:     func(r *http.Request) bool { return true },
	ReadBufferSize:  8192,
	WriteBufferSize: 8192,
}

func (lb *DynamicLoadBalancer) handleWSRequest(w http.ResponseWriter, r *http.Request) {
	backend := lb.SelectOptimalBackend()
	if backend == nil {
		http.Error(w, "No backends available", http.StatusServiceUnavailable)
		return
	}

	backend.IncrInFlight()
	defer backend.DecrInFlight()

	clientConn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer clientConn.Close()

	dialer := websocket.Dialer{
		HandshakeTimeout: 5 * time.Second,
	}
	backendConn, _, err := dialer.Dial(backend.WSURL, nil)
	if err != nil {
		backend.RecordResult(0, true)
		return
	}
	defer backendConn.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		pipeWS(clientConn, backendConn)
	}()
	go func() {
		defer wg.Done()
		pipeWS(backendConn, clientConn)
	}()

	wg.Wait()
}

func pipeWS(src, dst *websocket.Conn) {
	for {
		msgType, data, err := src.ReadMessage()
		if err != nil {
			break
		}
		if err := dst.WriteMessage(msgType, data); err != nil {
			break
		}
	}
}

// ─────────────────────────────────────────────
// Active Health Check Probes
// ─────────────────────────────────────────────

func (lb *DynamicLoadBalancer) startActiveHealthProbes(interval time.Duration) {
	go func() {
		client := &http.Client{Timeout: 10 * time.Second}
		consecFails := make(map[string]int)
		for {
			for _, b := range lb.backends {
				healthURL := fmt.Sprintf("%s/health", b.URL.String())
				resp, err := client.Get(healthURL)
				if resp != nil {
					resp.Body.Close()
				}
				if err != nil || resp.StatusCode != 200 {
					consecFails[b.Name]++
					if consecFails[b.Name] >= 5 && b.IsAlive() {
						log.Printf("[health-probe] Backend DOWN (5 consec fails): %s", b.Name)
						b.SetAlive(false)
					}
				} else {
					consecFails[b.Name] = 0
					if !b.IsAlive() {
						log.Printf("[health-probe] Backend RECOVERED: %s", b.Name)
					}
					b.SetAlive(true)
				}
			}
			time.Sleep(interval)
		}
	}()
}

func round(val float64, precision int) float64 {
	p := 1.0
	for i := 0; i < precision; i++ {
		p *= 10
	}
	return float64(int(val*p+0.5)) / p
}

func max(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}

type FeedItem struct {
	MsgID      string `json:"msg_id"`
	ClientName string `json:"client-name"`
	Username   string `json:"username"`
	Msg        string `json:"msg"`
	Text       string `json:"text"`
	Timestamp  int64  `json:"timestamp"`
}

var (
	feedCacheLock sync.RWMutex
	cachedFeedBuf []byte
	cachedFeedTime time.Time
)

func (lb *DynamicLoadBalancer) handleFeedProxy(w http.ResponseWriter, r *http.Request) {
	feedCacheLock.RLock()
	if time.Since(cachedFeedTime) < 2*time.Second && len(cachedFeedBuf) > 0 {
		buf := cachedFeedBuf
		feedCacheLock.RUnlock()
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write(buf)
		return
	}
	feedCacheLock.RUnlock()

	client := &http.Client{Timeout: 3 * time.Second}

	var wg sync.WaitGroup
	var mu sync.Mutex
	seen := make(map[string]bool)
	merged := make([]FeedItem, 0, 20000)

	for _, b := range lb.backends {
		if !b.IsAlive() {
			continue
		}
		wg.Add(1)
		targetURL := b.URL.String() + "/feed"
		go func(target string) {
			defer wg.Done()
			resp, err := client.Get(target)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			var items []FeedItem
			if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
				return
			}

			mu.Lock()
			for _, item := range items {
				mid := item.MsgID
				if mid == "" {
					mid = fmt.Sprintf("%s_%d", item.Username, item.Timestamp)
				}
				if !seen[mid] {
					seen[mid] = true
					merged = append(merged, item)
				}
			}
			mu.Unlock()
		}(targetURL)
	}

	wg.Wait()

	buf, err := json.Marshal(merged)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("[]"))
		return
	}

	feedCacheLock.Lock()
	cachedFeedBuf = buf
	cachedFeedTime = time.Now()
	feedCacheLock.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(buf)
}

func main() {
	lb := NewDynamicLoadBalancer(defaultBackendURLs)
	lb.startActiveHealthProbes(3 * time.Second)

	mux := http.NewServeMux()

	// Required API Routes (Lab 6 Spec)
	mux.HandleFunc("/message", lb.handleProxyRequest)
	mux.HandleFunc("/feed", lb.handleFeedProxy)

	// WebSocket & UI
	mux.HandleFunc("/ws", lb.handleWSRequest)
	mux.HandleFunc("/", lb.handleProxyRequest)

	// Observability
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(lb.metrics.Snapshot(lb))
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(lb.metrics.Snapshot(lb))
	})

	addr := listenAddr()
	log.Printf("╔═══════════════════════════════════════════════════════════╗")
	log.Printf("║   Dynamic Performance Load Balancer (Lab 6 Leaderboard)   ║")
	log.Printf("║   Listening on %s                                    ║", addr)
	log.Printf("╠═══════════════════════════════════════════════════════════╣")
	log.Printf("║   Required API Routes:                                    ║")
	log.Printf("║     POST/GET /message  -> Submits chat message            ║")
	log.Printf("║     GET      /feed     -> Retrieves message history       ║")
	log.Printf("║   Backends:                                               ║")
	for _, b := range lb.backends {
		log.Printf("║     %s", b.Name)
	}
	log.Printf("╚═══════════════════════════════════════════════════════════╝")

	srv := &http.Server{
		Addr:         addr,
		Handler:      mux,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	var listener net.Listener
	var err error
	for attempts := 0; attempts < 15; attempts++ {
		listener, err = net.Listen("tcp", addr)
		if err == nil {
			break
		}
		log.Printf("Port %s busy (%v), retrying in 1s...", addr, err)
		time.Sleep(1 * time.Second)
	}

	if err != nil {
		log.Fatalf("Failed to bind port %s after 15 attempts: %v", addr, err)
	}

	if err := srv.Serve(listener); err != nil {
		log.Fatalf("Server exited: %v", err)
	}
}
