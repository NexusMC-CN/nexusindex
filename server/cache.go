package server

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	edgecache "github.com/blockbridge/avmcbbs/apps/nexusindex/packages/edgecache-go-client"
)

type Cache interface {
	Get(ctx context.Context, key string) (json.RawMessage, bool)
	Set(ctx context.Context, key string, value any, ttlSeconds int64)
	Clear(ctx context.Context, pattern string)
}

// Caches without a remaining-lifetime contract may serve a hit, but cannot
// safely promote it to a fresh L1 entry.
type cacheWithTTL interface {
	GetWithTTL(context.Context, string) (json.RawMessage, bool, time.Duration)
}

type namespaceVersions interface {
	GetVersion(context.Context, string) (int64, error)
	BumpVersion(context.Context, string) (int64, error)
}

type CacheEntry struct {
	Key        string
	Value      any
	TTLSeconds int64
}

const highlightCacheBudget = 100 * time.Millisecond
const cacheWriteConcurrency = 4

type TieredCacheOptions struct {
	L1MaxEntries        int
	L2                  Cache
	DefaultTTLSeconds   int64
	QueryTTLSeconds     int64
	FilterTTLSeconds    int64
	HighlightTTLSeconds int64
}

type TieredCache struct {
	mu                  sync.Mutex
	l1                  map[string]memoryEntry
	l1MaxEntries        int
	l2                  Cache
	defaultTTLSeconds   int64
	queryTTLSeconds     int64
	filterTTLSeconds    int64
	highlightTTLSeconds int64
	stats               CacheStats
	versions            map[string]int64
	l2Writes            chan struct{}
}

type memoryEntry struct {
	value     json.RawMessage
	expiresAt time.Time
}

type CacheStats struct {
	L1Hits   int64 `json:"l1Hits"`
	L1Misses int64 `json:"l1Misses"`
	L2Hits   int64 `json:"l2Hits"`
	L2Misses int64 `json:"l2Misses"`
	Sets     int64 `json:"sets"`
	Entries  int   `json:"entries"`
}

func NewTieredCache(options TieredCacheOptions) *TieredCache {
	maxEntries := options.L1MaxEntries
	if maxEntries <= 0 {
		maxEntries = 2048
	}
	defaultTTL := options.DefaultTTLSeconds
	if defaultTTL <= 0 {
		defaultTTL = 60
	}
	return &TieredCache{
		versions:            map[string]int64{},
		l2Writes:            make(chan struct{}, cacheWriteConcurrency),
		l1:                  map[string]memoryEntry{},
		l1MaxEntries:        maxEntries,
		l2:                  options.L2,
		defaultTTLSeconds:   defaultTTL,
		queryTTLSeconds:     positiveTTL(options.QueryTTLSeconds, defaultTTL),
		filterTTLSeconds:    positiveTTL(options.FilterTTLSeconds, defaultTTL),
		highlightTTLSeconds: positiveTTL(options.HighlightTTLSeconds, defaultTTL),
	}
}

func (c *TieredCache) Get(ctx context.Context, key string) (json.RawMessage, bool) {
	now := time.Now()
	c.mu.Lock()
	if entry, ok := c.l1[key]; ok {
		if entry.expiresAt.After(now) {
			c.stats.L1Hits++
			value := cloneRawMessage(entry.value)
			c.mu.Unlock()
			return value, true
		}
		delete(c.l1, key)
	}
	c.stats.L1Misses++
	c.mu.Unlock()

	if c.l2 == nil {
		c.mu.Lock()
		c.stats.L2Misses++
		c.mu.Unlock()
		return nil, false
	}
	started := time.Now()
	var value json.RawMessage
	var ok bool
	var ttl time.Duration
	if timed, supported := c.l2.(cacheWithTTL); supported {
		value, ok, ttl = timed.GetWithTTL(ctx, key)
	} else {
		value, ok = c.l2.Get(ctx, key)
	}
	c.mu.Lock()
	if ok {
		c.stats.L2Hits++
		if limit := time.Duration(c.ttlForKey(key)) * time.Second; ttl > limit {
			ttl = limit
		}
		// Subtract the whole round trip, including lock contention. EdgeCache
		// floors remaining TTL to seconds, so this is a conservative expiry.
		if expiry := started.Add(ttl); ttl > 0 && expiry.After(time.Now()) {
			c.setL1UntilLocked(key, value, expiry)
		}
	} else {
		c.stats.L2Misses++
	}
	c.mu.Unlock()
	return value, ok
}

func positiveTTL(value, fallback int64) int64 {
	if value > 0 {
		return value
	}
	return fallback
}

func (c *TieredCache) ttlForKey(key string) int64 {
	parts := strings.Split(key, ":")
	if len(parts) >= 3 {
		switch parts[len(parts)-2] {
		case "query":
			return c.queryTTLSeconds
		case "filter":
			return c.filterTTLSeconds
		case "highlight":
			return c.highlightTTLSeconds
		}
	}
	return c.defaultTTLSeconds
}

func (c *TieredCache) Set(ctx context.Context, key string, value any, ttlSeconds int64) {
	raw, err := json.Marshal(value)
	if err == nil {
		c.mu.Lock()
		c.setL1Locked(key, raw, ttlSeconds)
		c.stats.Sets++
		c.mu.Unlock()
	}
	c.setL2(ctx, CacheEntry{Key: key, Value: value, TTLSeconds: ttlSeconds})
}

func (c *TieredCache) setL2(ctx context.Context, entry CacheEntry) {
	if c.l2 == nil || ctx.Err() != nil {
		return
	}
	select {
	case c.l2Writes <- struct{}{}:
		defer func() { <-c.l2Writes }()
	case <-ctx.Done():
		return
	}
	if ctx.Err() == nil {
		c.l2.Set(ctx, entry.Key, entry.Value, entry.TTLSeconds)
	}
}

// SetBatch warms L1 immediately and bounds all optional L2 writes by one
// shared deadline and a per-cache concurrency limit.
func (c *TieredCache) SetBatch(ctx context.Context, entries []CacheEntry) {
	for _, entry := range entries {
		if raw, err := json.Marshal(entry.Value); err == nil {
			c.mu.Lock()
			c.setL1Locked(entry.Key, raw, entry.TTLSeconds)
			c.stats.Sets++
			c.mu.Unlock()
		}
	}
	ctx, cancel := context.WithTimeout(ctx, highlightCacheBudget)
	defer cancel()
	var next atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < cacheWriteConcurrency && i < len(entries); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				i := int(next.Add(1) - 1)
				if i >= len(entries) {
					return
				}
				c.setL2(ctx, entries[i])
			}
		}()
	}
	wg.Wait()
}

func (c *TieredCache) GetVersion(ctx context.Context, namespace string) (int64, error) {
	if shared, ok := c.l2.(namespaceVersions); ok {
		return shared.GetVersion(ctx, namespace)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.versions[namespace] == 0 {
		c.versions[namespace] = 1
	}
	return c.versions[namespace], nil
}

func (c *TieredCache) BumpVersion(ctx context.Context, namespace string) (int64, error) {
	if shared, ok := c.l2.(namespaceVersions); ok {
		return shared.BumpVersion(ctx, namespace)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.versions[namespace] == 0 {
		c.versions[namespace] = 1
	}
	c.versions[namespace]++
	return c.versions[namespace], nil
}

func (c *TieredCache) Clear(ctx context.Context, pattern string) {
	c.mu.Lock()
	for key := range c.l1 {
		if matchCachePattern(pattern, key) {
			delete(c.l1, key)
		}
	}
	c.mu.Unlock()
	if c.l2 != nil {
		c.l2.Clear(ctx, pattern)
	}
}

func (c *TieredCache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	stats := c.stats
	stats.Entries = len(c.l1)
	return stats
}

func (c *TieredCache) setL1Locked(key string, value json.RawMessage, ttlSeconds int64) {
	if ttlSeconds <= 0 {
		ttlSeconds = 60
	}
	c.setL1UntilLocked(key, value, time.Now().Add(time.Duration(ttlSeconds)*time.Second))
}

func (c *TieredCache) setL1UntilLocked(key string, value json.RawMessage, expiresAt time.Time) {
	if _, exists := c.l1[key]; !exists && len(c.l1) >= c.l1MaxEntries {
		c.evictOneLocked()
	}
	c.l1[key] = memoryEntry{value: cloneRawMessage(value), expiresAt: expiresAt}
}

func (c *TieredCache) evictOneLocked() {
	var oldestKey string
	var oldest time.Time
	for key, entry := range c.l1 {
		if oldestKey == "" || entry.expiresAt.Before(oldest) {
			oldestKey = key
			oldest = entry.expiresAt
		}
	}
	if oldestKey != "" {
		delete(c.l1, oldestKey)
	}
}

func cloneRawMessage(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}

func matchCachePattern(pattern, key string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" || pattern == "*" {
		return true
	}
	if !strings.ContainsAny(pattern, "*?[") {
		return pattern == key
	}
	ok, err := filepath.Match(pattern, key)
	return err == nil && ok
}

type EdgeCacheAdapter struct {
	client *edgecache.Client
}

func NewEdgeCacheAdapter(client *edgecache.Client) Cache {
	if client == nil {
		return nil
	}
	return &EdgeCacheAdapter{client: client}
}

func (c *EdgeCacheAdapter) Get(ctx context.Context, key string) (json.RawMessage, bool) {
	value, hit, _ := c.GetWithTTL(ctx, key)
	return value, hit
}

func (c *EdgeCacheAdapter) GetWithTTL(ctx context.Context, key string) (json.RawMessage, bool, time.Duration) {
	result, err := c.client.Get(ctx, key)
	if err != nil || !result.Hit || string(result.Value) == "null" {
		return nil, false, 0
	}
	return result.Value, true, time.Duration(result.TTLSeconds) * time.Second
}

func (c *EdgeCacheAdapter) GetVersion(ctx context.Context, namespace string) (int64, error) {
	result, err := c.client.GetVersion(ctx, namespace)
	if err == nil && result.Version <= 0 {
		err = errors.New("shared cache version unavailable")
	}
	return result.Version, err
}

func (c *EdgeCacheAdapter) BumpVersion(ctx context.Context, namespace string) (int64, error) {
	result, err := c.client.BumpVersion(ctx, namespace)
	if err == nil && result.Version <= 0 {
		err = errors.New("shared cache version unavailable")
	}
	return result.Version, err
}

func (c *EdgeCacheAdapter) Set(ctx context.Context, key string, value any, ttlSeconds int64) {
	_ = c.client.Set(ctx, key, value, ttlSeconds)
}

func (c *EdgeCacheAdapter) Clear(ctx context.Context, pattern string) {
	_ = c.client.Clear(ctx, pattern)
}
