package server

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"sync"
	"time"

	edgecache "github.com/blockbridge/avmcbbs/apps/nexusindex/packages/edgecache-go-client"
)

type Cache interface {
	Get(ctx context.Context, key string) (json.RawMessage, bool)
	Set(ctx context.Context, key string, value any, ttlSeconds int64)
	Clear(ctx context.Context, pattern string)
}

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
	value, ok := c.l2.Get(ctx, key)
	c.mu.Lock()
	if ok {
		c.stats.L2Hits++
		c.setL1Locked(key, value, c.ttlForKey(key))
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
	if c.l2 != nil {
		c.l2.Set(ctx, key, value, ttlSeconds)
	}
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
	if len(c.l1) >= c.l1MaxEntries {
		c.evictOneLocked()
	}
	c.l1[key] = memoryEntry{value: cloneRawMessage(value), expiresAt: time.Now().Add(time.Duration(ttlSeconds) * time.Second)}
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
	if ok, err := filepath.Match(pattern, key); err == nil && ok {
		return true
	}
	return strings.HasPrefix(key, strings.TrimSuffix(pattern, "*"))
}

type EdgeCacheAdapter struct {
	client *edgecache.Client
}

func NewEdgeCacheAdapter(client *edgecache.Client) *EdgeCacheAdapter {
	if client == nil {
		return nil
	}
	return &EdgeCacheAdapter{client: client}
}

func (c *EdgeCacheAdapter) Get(ctx context.Context, key string) (json.RawMessage, bool) {
	result, err := c.client.Get(ctx, key)
	if err != nil || !result.Hit || string(result.Value) == "null" {
		return nil, false
	}
	return result.Value, true
}

func (c *EdgeCacheAdapter) Set(ctx context.Context, key string, value any, ttlSeconds int64) {
	_ = c.client.Set(ctx, key, value, ttlSeconds)
}

func (c *EdgeCacheAdapter) Clear(ctx context.Context, pattern string) {
	_ = c.client.Clear(ctx, pattern)
}
