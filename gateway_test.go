package main

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRedisCacheIntegration(t *testing.T) {
	address := os.Getenv("LUMINA_TEST_REDIS_URL")
	if address == "" {
		t.Skip("set LUMINA_TEST_REDIS_URL to run Redis integration")
	}
	cache, err := NewRedisCache(address)
	if err != nil {
		t.Fatal(err)
	}
	defer cache.client.Close()
	key := fmt.Sprintf("lumina:test:%d", time.Now().UnixNano())
	defer cache.client.Del(context.Background(), key)
	now := time.Now()
	item := CacheItem{Body: []byte("redis payload"), Header: http.Header{"Content-Type": {"text/plain"}, "X-Example": {"one", "two"}}, StatusCode: 200, StaleAt: now.Add(time.Minute), ExpiresAt: now.Add(2 * time.Minute)}
	cache.Add(key, item)
	got, status := cache.GetStatus(key)
	if status != "HIT" || string(got.Body) != "redis payload" || len(got.Header.Values("X-Example")) != 2 {
		t.Fatalf("Redis roundtrip: %s %+v", status, got)
	}
	if ttl := cache.client.TTL(context.Background(), key).Val(); ttl <= 0 || ttl > 2*time.Minute {
		t.Fatalf("Redis TTL %s", ttl)
	}
	item.StaleAt = now.Add(-time.Second)
	cache.Add(key, item)
	if _, status := cache.GetStatus(key); status != "STALE" {
		t.Fatal("Redis stale state missing")
	}
	if err := cache.client.PExpire(context.Background(), key, time.Millisecond).Err(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, status := cache.GetStatus(key); status != "MISS" {
		t.Fatal("Redis expiration failed")
	}
}

func TestGatewayAllUpstreamsDown(t *testing.T) {
	var calls atomic.Int32
	unexpected := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1) }))
	defer unexpected.Close()
	lb := NewLoadBalancer([]string{unexpected.URL})
	lb.upstreams[0].SetAlive(false)
	handler := newGateway(lb, NewLuminaCache(10), NewCircuitBreaker(1, time.Second), time.Minute, 30*time.Second, NewIPRateLimiter(100, 100))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest("GET", unexpected.URL+"/must-not-forward", nil))
	if rec.Code != 503 || calls.Load() != 0 {
		t.Fatalf("status=%d, unexpected requests=%d", rec.Code, calls.Load())
	}
}

func TestGatewayCompressedResponse(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Content-Encoding", "gzip")
		zip := gzip.NewWriter(w)
		io.WriteString(zip, "compressed payload")
		zip.Close()
	}))
	defer upstream.Close()
	handler := newGateway(NewLoadBalancer([]string{upstream.URL}), NewLuminaCache(10), NewCircuitBreaker(5, time.Second), time.Minute, 30*time.Second, NewIPRateLimiter(100, 100))
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("GET", "http://gateway/zip", nil)
		req.Header.Set("Accept-Encoding", "gzip")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Header().Get("Content-Encoding") != "gzip" {
			t.Fatal("lost encoding")
		}
		zip, err := gzip.NewReader(rec.Body)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(zip)
		zip.Close()
		if err != nil || string(body) != "compressed payload" {
			t.Fatalf("body=%q error=%v", body, err)
		}
	}
	if calls.Load() != 2 {
		t.Fatal("encoded response cached")
	}
}

func TestCacheRequestBypass(t *testing.T) {
	for _, header := range []string{"Authorization", "Cookie", "Range", "Cache-Control", "Pragma", "If-None-Match", "If-Modified-Since", "If-Match", "If-Unmodified-Since", "If-Range"} {
		req := httptest.NewRequest("GET", "http://gateway/", nil)
		req.Header.Set(header, "value")
		if cacheRequest(req) {
			t.Errorf("did not bypass %s", header)
		}
	}
	if cacheRequest(httptest.NewRequest("GET", "http://gateway/", strings.NewReader("body"))) {
		t.Fatal("GET body eligible for cache")
	}
}

func TestRateLimiterCapacityAndExpiry(t *testing.T) {
	limiter := NewIPRateLimiter(100, 100)
	for i := 0; i < 10000; i++ {
		limiter.GetLimiter(fmt.Sprint(i))
	}
	if limiter.GetLimiter("new").Allow() {
		t.Fatal("admitted new IP beyond capacity")
	}
	if !limiter.GetLimiter("0").Allow() {
		t.Fatal("blocked existing IP")
	}
	limiter.seen["1"] = time.Now().Add(-6 * time.Minute)
	if !limiter.GetLimiter("new").Allow() || len(limiter.ips) != 10000 {
		t.Fatal("idle eviction failed")
	}
}

func TestGatewayIsolationAndHeaders(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("Access-Control-Allow-Origin", "https://example.test")
		if r.URL.Path == "/login" {
			w.Header().Set("Set-Cookie", "session=abc")
			w.Header().Set("Location", "/home")
			w.WriteHeader(302)
		}
		if r.URL.Path == "/private" {
			w.Header().Set("Cache-Control", "private")
		}
		if r.URL.Path == "/vary" {
			w.Header().Set("Vary", "Accept-Language")
		}
		fmt.Fprint(w, "hello "+r.Header.Get("Authorization")+r.Header.Get("Cookie"))
	}))
	defer upstream.Close()
	handler := newGateway(NewLoadBalancer([]string{upstream.URL}), NewLuminaCache(100), NewCircuitBreaker(5, time.Second), time.Minute, 30*time.Second, NewIPRateLimiter(10000, 15000))
	request := func(path, auth, cookie string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", "http://gateway"+path, nil)
		req.Header.Set("Authorization", auth)
		req.Header.Set("Cookie", cookie)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		return rec
	}
	for _, path := range []string{"/login", "/private", "/vary"} {
		before := calls.Load()
		first := request(path, "", "")
		request(path, "", "")
		if calls.Load()-before != 2 {
			t.Errorf("%s cached", path)
		}
		if first.Header().Get("Access-Control-Allow-Origin") == "" {
			t.Error("lost CORS")
		}
		if path == "/login" && (first.Code != 302 || first.Header().Get("Set-Cookie") == "" || first.Header().Get("Location") != "/home") {
			t.Error("lost redirect/session headers")
		}
	}
	for _, header := range []string{"auth", "cookie"} {
		for _, user := range []string{"alice", "bob"} {
			auth, cookie := "", ""
			if header == "auth" {
				auth = user
			} else {
				cookie = user
			}
			if body := request("/user", auth, cookie).Body.String(); body != "hello "+user {
				t.Fatal(body)
			}
		}
	}
	before := calls.Load()
	request("/public", "", "")
	second := request("/public", "", "")
	if calls.Load()-before != 1 || second.Header().Get("X-Lumina-Cache") != "HIT" {
		t.Error("public cache failed")
	}
}

func TestCacheConcurrencyExpiryAndSize(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		if r.URL.Path == "/large" {
			io.WriteString(w, strings.Repeat("x", maxCacheBody+100))
			return
		}
		fmt.Fprint(w, "ok")
	}))
	defer upstream.Close()
	cache := NewLuminaCache(100)
	transport := &cachingTransport{base: http.DefaultTransport, cache: cache, cb: NewCircuitBreaker(5, time.Second), ttl: time.Minute, stale: 30 * time.Second}
	fetch := func(path string) {
		req, _ := http.NewRequest("GET", upstream.URL+path, nil)
		res, err := transport.RoundTrip(req)
		if err != nil {
			t.Error(err)
			return
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if path == "/large" && len(body) != maxCacheBody+100 {
			t.Error("truncated body")
		}
	}
	var wg sync.WaitGroup
	for n := 0; n < 20; n++ {
		wg.Add(1)
		go func() { defer wg.Done(); fetch("/public") }()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("got %d upstream calls", calls.Load())
	}
	key := "lumina:v2:" + upstream.URL + "/public:{}"
	item, _ := cache.GetStatus(key)
	item.StaleAt = time.Now().Add(-time.Second)
	cache.Add(key, item)
	fetch("/public")
	deadline := time.Now().Add(time.Second)
	for {
		_, status := cache.GetStatus(key)
		if status == "HIT" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("refresh did not complete")
		}
		time.Sleep(time.Millisecond)
	}
	item, _ = cache.GetStatus(key)
	item.ExpiresAt = time.Now().Add(-time.Second)
	cache.Add(key, item)
	fetch("/public")
	if calls.Load() != 3 {
		t.Errorf("expiry/refresh calls %d", calls.Load())
	}
	fetch("/large")
	fetch("/large")
	if calls.Load() != 5 {
		t.Error("oversize cached")
	}
}

func TestBreakerSingleProbe(t *testing.T) {
	cb := NewCircuitBreaker(1, time.Millisecond)
	cb.RecordFailure()
	time.Sleep(2 * time.Millisecond)
	var admitted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if cb.AllowRequest() {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 1 {
		t.Fatalf("probes %d", admitted.Load())
	}
	cb.RecordSuccess()
	if !cb.AllowRequest() {
		t.Fatal("did not recover")
	}
}

func TestGatewayUpstreamFailure(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); http.Error(w, "failed", 500) }))
	defer upstream.Close()
	handler := newGateway(NewLoadBalancer([]string{upstream.URL}), NewLuminaCache(10), NewCircuitBreaker(1, time.Minute), time.Minute, 30*time.Second, NewIPRateLimiter(100, 100))
	for _, want := range []int{500, 503} {
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest("GET", "http://gateway/fail", nil))
		if rec.Code != want {
			t.Fatalf("status %d, want %d", rec.Code, want)
		}
	}
	if calls.Load() != 1 {
		t.Fatal("open breaker contacted upstream")
	}
}

func BenchmarkGatewayEngine(b *testing.B) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"status":"ok"}`)
	}))
	defer upstream.Close()

	lb := NewLoadBalancer([]string{upstream.URL})
	cache := NewLuminaCache(10000)
	cb := NewCircuitBreaker(5, time.Second)
	limiter := NewIPRateLimiter(1000000, 1000000)
	gateway := newGateway(lb, cache, cb, time.Minute, 30*time.Second, limiter)

	// Pre-seed cache
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "http://gateway/benchmark", nil)
	gateway.ServeHTTP(rec, req)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		req := httptest.NewRequest("GET", "http://gateway/benchmark", nil)
		for pb.Next() {
			rec := httptest.NewRecorder()
			gateway.ServeHTTP(rec, req)
		}
	})
}
