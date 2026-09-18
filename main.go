package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/redis/go-redis/v9"
	"golang.org/x/time/rate"
)

// 0. Prometheus Metrics
var (
	promRequestsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lumina_requests_total",
		Help: "The total number of processed requests",
	})
	promCacheHits = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lumina_cache_hits_total",
		Help: "The total number of cache hits",
	})
	promCacheMisses = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lumina_cache_misses_total",
		Help: "The total number of cache misses",
	})
	promStaleRefreshes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lumina_stale_refreshes_total",
		Help: "The total number of background stale refreshes",
	})
	promCircuitTrips = promauto.NewCounter(prometheus.CounterOpts{
		Name: "lumina_circuit_trips_total",
		Help: "The total number of circuit breaker trips",
	})
)

// 1. Struktur Data dengan Stale & Expires
type CacheItem struct {
	Body        []byte      `json:"body"`
	ContentType string      `json:"content_type"`
	Header      http.Header `json:"header"`
	StatusCode  int         `json:"status_code"`
	CachedAt    time.Time   `json:"cached_at"`
	StaleAt     time.Time   `json:"stale_at"`
	ExpiresAt   time.Time   `json:"expires_at"`
}

// 2. Cache Interface
type CacheBackend interface {
	GetStatus(key string) (CacheItem, string)
	Add(key string, item CacheItem)
	IncHits()
	IncMisses()
	IncStaleRefresh()
	GetMetrics() map[string]interface{}
}

// 3. Cache guna LRU (Local Memory)
type LuminaCache struct {
	lru          *lru.Cache[string, CacheItem]
	Hits         uint64
	Misses       uint64
	StaleRefresh uint64
}

func NewLuminaCache(maxItems int) *LuminaCache {
	cache, _ := lru.New[string, CacheItem](maxItems)
	return &LuminaCache{lru: cache}
}

func (c *LuminaCache) GetStatus(key string) (CacheItem, string) {
	item, found := c.lru.Get(key)
	if !found {
		return CacheItem{}, "MISS"
	}
	if time.Now().After(item.ExpiresAt) {
		c.lru.Remove(key)
		return CacheItem{}, "MISS"
	}
	if time.Now().After(item.StaleAt) {
		return item, "STALE"
	}
	return item, "HIT"
}

func (c *LuminaCache) Add(key string, item CacheItem) { c.lru.Add(key, item) }
func (c *LuminaCache) IncHits()                       { atomic.AddUint64(&c.Hits, 1) }
func (c *LuminaCache) IncMisses()                     { atomic.AddUint64(&c.Misses, 1) }
func (c *LuminaCache) IncStaleRefresh()               { atomic.AddUint64(&c.StaleRefresh, 1) }

func (c *LuminaCache) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"type":              "LRU (Memory)",
		"total_cache_items": c.lru.Len(),
		"total_hits":        atomic.LoadUint64(&c.Hits),
		"total_misses":      atomic.LoadUint64(&c.Misses),
		"stale_refreshes":   atomic.LoadUint64(&c.StaleRefresh),
	}
}

// 4. Redis Cache Implementation
type RedisCache struct {
	client       *redis.Client
	Hits         uint64
	Misses       uint64
	StaleRefresh uint64
}

func NewRedisCache(redisURL string) (*RedisCache, error) {
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	client := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}
	return &RedisCache{client: client}, nil
}

func (c *RedisCache) GetStatus(key string) (CacheItem, string) {
	ctx := context.Background()
	val, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		return CacheItem{}, "MISS"
	}
	var item CacheItem
	if err := json.Unmarshal(val, &item); err != nil {
		return CacheItem{}, "MISS"
	}
	if time.Now().After(item.ExpiresAt) {
		c.client.Del(ctx, key)
		return CacheItem{}, "MISS"
	}
	if time.Now().After(item.StaleAt) {
		return item, "STALE"
	}
	return item, "HIT"
}

func (c *RedisCache) Add(key string, item CacheItem) {
	ctx := context.Background()
	val, _ := json.Marshal(item)
	ttl := time.Until(item.ExpiresAt)
	if ttl > 0 {
		c.client.Set(ctx, key, val, ttl)
	}
}

func (c *RedisCache) IncHits()         { atomic.AddUint64(&c.Hits, 1) }
func (c *RedisCache) IncMisses()       { atomic.AddUint64(&c.Misses, 1) }
func (c *RedisCache) IncStaleRefresh() { atomic.AddUint64(&c.StaleRefresh, 1) }

func (c *RedisCache) GetMetrics() map[string]interface{} {
	return map[string]interface{}{
		"type":            "Redis",
		"total_hits":      atomic.LoadUint64(&c.Hits),
		"total_misses":    atomic.LoadUint64(&c.Misses),
		"stale_refreshes": atomic.LoadUint64(&c.StaleRefresh),
	}
}

// 3. Rate Limiter: Bouncer Pintu (Elak DDoS dari 1 IP)
type IPRateLimiter struct {
	ips  map[string]*rate.Limiter
	seen map[string]time.Time
	mu   sync.RWMutex
	r    rate.Limit
	b    int
}

func NewIPRateLimiter(r rate.Limit, b int) *IPRateLimiter {
	return &IPRateLimiter{
		ips:  make(map[string]*rate.Limiter),
		seen: make(map[string]time.Time),
		r:    r,
		b:    b,
	}
}

func (i *IPRateLimiter) GetLimiter(ip string) *rate.Limiter {
	i.mu.Lock()
	defer i.mu.Unlock()
	now := time.Now()
	if len(i.ips) >= 10000 {
		for key, last := range i.seen {
			if now.Sub(last) > 5*time.Minute {
				delete(i.ips, key)
				delete(i.seen, key)
			}
		}
		if _, exists := i.ips[ip]; len(i.ips) >= 10000 && !exists {
			return rate.NewLimiter(0, 0)
		}
	}
	limiter, exists := i.ips[ip]
	if !exists {
		limiter = rate.NewLimiter(i.r, i.b)
		i.ips[ip] = limiter
	}
	i.seen[ip] = now
	return limiter
}

// 4. Load Balancer & Health Check
type Upstream struct {
	URL   *url.URL
	Alive bool
	mu    sync.RWMutex
}

func (u *Upstream) IsAlive() bool {
	u.mu.RLock()
	defer u.mu.RUnlock()
	return u.Alive
}

func (u *Upstream) SetAlive(alive bool) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.Alive = alive
}

type LoadBalancer struct {
	upstreams []*Upstream
	current   uint64
}

func NewLoadBalancer(urls []string) *LoadBalancer {
	var upstreams []*Upstream
	for _, u := range urls {
		parsed, err := url.Parse(strings.TrimSpace(u))
		if err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") && parsed.Host != "" {
			upstreams = append(upstreams, &Upstream{URL: parsed, Alive: true})
		}
	}
	return &LoadBalancer{upstreams: upstreams}
}

func (lb *LoadBalancer) Next() *Upstream {
	// Round Robin
	for i := 0; i < len(lb.upstreams); i++ {
		idx := atomic.AddUint64(&lb.current, 1) % uint64(len(lb.upstreams))
		if lb.upstreams[idx].IsAlive() {
			return lb.upstreams[idx]
		}
	}
	return nil // Semua mati
}

func (lb *LoadBalancer) StartHealthCheck() {
	go func() {
		for {
			for _, u := range lb.upstreams {
				go func(upstream *Upstream) {
					client := http.Client{Timeout: 2 * time.Second}
					resp, err := client.Get(upstream.URL.String())
					if err != nil || resp.StatusCode >= 500 {
						if upstream.IsAlive() {
							fmt.Printf("[HEALTH] ❌ Upstream DEAD: %s\n", upstream.URL.String())
							upstream.SetAlive(false)
						}
					} else {
						if !upstream.IsAlive() {
							fmt.Printf("[HEALTH] ✅ Upstream ALIVE: %s\n", upstream.URL.String())
							upstream.SetAlive(true)
						}
					}
					if resp != nil {
						resp.Body.Close()
					}
				}(u)
			}
			time.Sleep(5 * time.Second)
		}
	}()
}

// 5. Circuit Breaker: Pelindung Downstream (Netflix Hystrix style)
type CBState int

const (
	StateClosed CBState = iota
	StateOpen
	StateHalfOpen
)

type CircuitBreaker struct {
	failureThreshold int
	failureCount     int
	state            CBState
	lastFailure      time.Time
	resetTimeout     time.Duration
	mu               sync.RWMutex
}

func NewCircuitBreaker(threshold int, timeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		failureThreshold: threshold,
		state:            StateClosed,
		resetTimeout:     timeout,
	}
}

func (cb *CircuitBreaker) AllowRequest() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == StateClosed {
		return true
	}
	if cb.state == StateOpen && time.Since(cb.lastFailure) > cb.resetTimeout {
		cb.state = StateHalfOpen
		return true
	}
	return false
}

func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount = 0
	cb.state = StateClosed
}

func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failureCount++
	cb.lastFailure = time.Now()
	if cb.failureCount >= cb.failureThreshold {
		if cb.state != StateOpen {
			fmt.Println("[CIRCUIT] 🚨 Trip! Circuit is now OPEN.")
			promCircuitTrips.Inc()
		}
		cb.state = StateOpen
	}
}

var serverStartTime = time.Now()

func main() {
	defaultUpstreams := os.Getenv("LUMINA_UPSTREAMS")
	if defaultUpstreams == "" {
		defaultUpstreams = os.Getenv("LUMINA_UPSTREAM") // fallback untuk backward compatibility
	}
	if defaultUpstreams == "" {
		defaultUpstreams = "https://jsonplaceholder.typicode.com"
	}

	defaultPort := os.Getenv("LUMINA_PORT")
	if defaultPort == "" {
		defaultPort = "8080"
	}

	upstreamsFlag := flag.String("upstreams", defaultUpstreams, "Comma-separated upstream URLs")
	portFlag := flag.String("port", defaultPort, "Proxy listening port")
	defaultTTL := 60
	if value := os.Getenv("LUMINA_CACHE_TTL_SECONDS"); value != "" {
		var err error
		defaultTTL, err = strconv.Atoi(value)
		if err != nil || defaultTTL <= 0 {
			log.Fatal("Invalid LUMINA_CACHE_TTL_SECONDS")
		}
	}
	ttlFlag := flag.Int("ttl", defaultTTL, "Cache TTL (Expires) in seconds")
	staleFlag := flag.Int("stale", 30, "Cache Stale in seconds")
	flag.Parse()
	if *ttlFlag <= 0 || *staleFlag < 0 || *staleFlag > *ttlFlag {
		log.Fatal("Require 0 <= stale <= ttl and ttl > 0")
	}

	urls := strings.Split(*upstreamsFlag, ",")
	lb := NewLoadBalancer(urls)
	if len(lb.upstreams) == 0 {
		log.Fatalf("Tiada upstream URL yang valid!")
	}
	lb.StartHealthCheck()

	ttl := time.Duration(*ttlFlag) * time.Second
	staleTTL := time.Duration(*staleFlag) * time.Second

	// Setup Cache (Redis or LRU Memory)
	var cache CacheBackend
	redisURL := os.Getenv("LUMINA_REDIS_URL")
	if redisURL != "" {
		fmt.Println("Connecting to Redis")
		rCache, err := NewRedisCache(redisURL)
		if err != nil {
			log.Fatalf("Gagal sambung ke Redis: %v", err)
		}
		cache = rCache
	} else {
		fmt.Println("🚀 Menggunakan LRU Cache (Local Memory)")
		cache = NewLuminaCache(2000) // LRU Max 2000 items
	}
	// Setup Rate Limiter (Max 10000 request sesaat per IP, Burst 15000)
	limiter := NewIPRateLimiter(10000, 15000)

	// Setup Circuit Breaker (Threshold 5 kegagalan, Reset selepas 10 saat)
	cb := NewCircuitBreaker(5, 10*time.Second)

	handler := newGateway(lb, cache, cb, ttl, staleTTL, limiter)

	server := &http.Server{
		Addr:              ":" + *portFlag,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// 5. Graceful Shutdown
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go func() {
		fmt.Printf("\nGo-Lumina Enterprise running on http://localhost:%s\n", *portFlag)
		fmt.Printf("Keupayaan: LRU Cache (Mem Berhad), Stale-While-Revalidate, Rate Limiter, Graceful Shutdown\n\n")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()

	// Tunggu isyarat Ctrl+C
	<-ctx.Done()
	fmt.Println("\n[SIGINT] Diterima. Memulakan \"Graceful Shutdown\"...")
	fmt.Println("Menunggu request yang sedang berjalan selesai (Max 5 saat)...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Masalah semasa shutdown: %v", err)
	}

	fmt.Println("LuminaProxy ditutup dengan selamat. Bye!")
}
