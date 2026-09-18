package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/singleflight"
)

const maxCacheBody = 1 << 20

type cachingTransport struct {
	base       http.RoundTripper
	cache      CacheBackend
	cb         *CircuitBreaker
	ttl, stale time.Duration
	group      singleflight.Group
}

func cacheRequest(r *http.Request) bool {
	return r.Method == http.MethodGet && (r.Body == nil || r.Body == http.NoBody) && r.Header.Get("Authorization") == "" && r.Header.Get("Cookie") == "" && r.Header.Get("Range") == "" && r.Header.Get("Cache-Control") == "" && r.Header.Get("Pragma") == "" && r.Header.Get("If-None-Match") == "" && r.Header.Get("If-Modified-Since") == "" && r.Header.Get("If-Match") == "" && r.Header.Get("If-Unmodified-Since") == "" && r.Header.Get("If-Range") == ""
}

func cachedResponse(r *http.Request, item CacheItem, status string) *http.Response {
	h := item.Header.Clone()
	h.Set("X-Lumina-Cache", status)
	return &http.Response{StatusCode: item.StatusCode, Header: h, Body: io.NopCloser(bytes.NewReader(item.Body)), ContentLength: int64(len(item.Body)), Request: r}
}

func (t *cachingTransport) fetch(r *http.Request) (*http.Response, error) {
	if r.URL.Host == "" || !t.cb.AllowRequest() {
		return &http.Response{StatusCode: 503, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("Upstream unavailable")), Request: r}, nil
	}
	res, err := t.base.RoundTrip(r)
	if err != nil || res.StatusCode >= 500 {
		t.cb.RecordFailure()
	} else {
		t.cb.RecordSuccess()
	}
	return res, err
}

func (t *cachingTransport) fill(r *http.Request, key string) (*http.Response, error) {
	var own *http.Response
	_, err, _ := t.group.Do(key, func() (interface{}, error) {
		if _, status := t.cache.GetStatus(key); status == "HIT" {
			return nil, nil
		}
		t.cache.IncMisses()
		promCacheMisses.Inc()
		res, err := t.fetch(r)
		if err != nil {
			return nil, err
		}
		own = res
		// Conservatively bypass responses with cache directives or varying representations.
		if res.StatusCode != 200 || res.Header.Get("Set-Cookie") != "" || res.Header.Get("Cache-Control") != "" || res.Header.Get("Vary") != "" || res.Header.Get("Content-Encoding") != "" || res.Header.Get("Expires") != "" {
			return nil, nil
		}
		body, err := io.ReadAll(io.LimitReader(res.Body, maxCacheBody+1))
		if err != nil {
			res.Body.Close()
			return nil, err
		}
		if len(body) > maxCacheBody {
			res.Body = &prefixBody{Reader: io.MultiReader(bytes.NewReader(body), res.Body), Closer: res.Body}
			return nil, nil
		}
		res.Body.Close()
		res.Body = io.NopCloser(bytes.NewReader(body))
		now := time.Now()
		t.cache.Add(key, CacheItem{Body: body, Header: res.Header.Clone(), StatusCode: res.StatusCode, CachedAt: now, StaleAt: now.Add(t.stale), ExpiresAt: now.Add(t.ttl)})
		return nil, nil
	})
	if err != nil {
		return nil, err
	}
	if own != nil {
		own.Header.Set("X-Lumina-Cache", "MISS")
		return own, nil
	}
	if item, status := t.cache.GetStatus(key); status != "MISS" {
		t.cache.IncHits()
		promCacheHits.Inc()
		return cachedResponse(r, item, "HIT-SHARED"), nil
	}
	// Non-cacheable responses must never be shared between callers.
	return t.fetch(r)
}

type prefixBody struct {
	io.Reader
	io.Closer
}

func (t *cachingTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if !cacheRequest(r) {
		return t.fetch(r)
	}
	// Include authority and request headers to isolate representations conservatively.
	headers, _ := json.Marshal(r.Header)
	key := "lumina:v2:" + r.URL.String() + ":" + string(headers)
	if item, status := t.cache.GetStatus(key); status != "MISS" {
		t.cache.IncHits()
		promCacheHits.Inc()
		if status == "STALE" {
			// DoChan registers the work synchronously; only its leader starts a goroutine.
			t.group.DoChan("refresh:"+key, func() (interface{}, error) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				req := r.Clone(ctx)
				t.cache.IncStaleRefresh()
				promStaleRefreshes.Inc()
				res, err := t.fill(req, key)
				if res != nil {
					res.Body.Close()
				}
				return nil, err
			})
		}
		return cachedResponse(r, item, status), nil
	}
	return t.fill(r, key)
}

func newGateway(lb *LoadBalancer, cache CacheBackend, cb *CircuitBreaker, ttl, stale time.Duration, limiter *IPRateLimiter) http.Handler {
	base := http.DefaultTransport.(*http.Transport).Clone()
	base.ResponseHeaderTimeout = 10 * time.Second
	proxy := &httputil.ReverseProxy{
		Rewrite: func(p *httputil.ProxyRequest) {
			target := lb.Next()
			if target != nil {
				p.SetURL(target.URL)
			} else {
				// Never forward an absolute client URL when every configured upstream is down.
				p.Out.URL = &url.URL{}
			}
			p.SetXForwarded()
		},
		Transport: &cachingTransport{base: base, cache: cache, cb: cb, ttl: ttl, stale: stale},
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		promRequestsTotal.Inc()
		if r.URL.Path == "/metrics" {
			promhttp.Handler().ServeHTTP(w, r)
			return
		}
		if r.URL.Path == "/lumina-metrics" {
			w.Header().Set("Content-Type", "application/json")
			metrics := cache.GetMetrics()
			metrics["uptime_seconds"] = time.Since(serverStartTime).Seconds()
			json.NewEncoder(w).Encode(metrics)
			return
		}
		ip, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			ip = r.RemoteAddr
		}
		if !limiter.GetLimiter(ip).Allow() {
			http.Error(w, "Too many requests", 429)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
		defer cancel()
		proxy.ServeHTTP(w, r.WithContext(ctx))
	})
}
