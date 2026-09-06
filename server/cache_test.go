package server

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"
	"time"
)

type fakeCache struct {
	value json.RawMessage
}

func (f *fakeCache) Get(context.Context, string) (json.RawMessage, bool) {
	return f.value, f.value != nil
}

func (f *fakeCache) Set(context.Context, string, any, int64) {}

func (f *fakeCache) Clear(context.Context, string) {}

func TestTieredCacheL2BackfillUsesResourceTTL(t *testing.T) {
	l2 := &fakeCache{value: json.RawMessage(`{"ok":true}`)}
	cache := NewTieredCache(TieredCacheOptions{
		L1MaxEntries:        8,
		L2:                  l2,
		DefaultTTLSeconds:   60,
		QueryTTLSeconds:     5,
		FilterTTLSeconds:    20,
		HighlightTTLSeconds: 30,
	})

	key := "nexusindex:v2:query:abc"
	if _, ok := cache.Get(context.Background(), key); !ok {
		t.Fatal("expected L2 hit")
	}
	cache.mu.Lock()
	entry := cache.l1[key]
	ttl := time.Until(entry.expiresAt)
	cache.mu.Unlock()
	if ttl > 6*time.Second || ttl < 3*time.Second {
		t.Fatalf("expected query ttl around 5s, got %s", ttl)
	}
}

func TestCacheKeyVersionBump(t *testing.T) {
	cache := NewTieredCache(TieredCacheOptions{L1MaxEntries: 8})
	server := NewHTTPServer(nil, Options{Cache: CacheOptions{Cache: cache, Namespace: "nexusindex:test"}})
	values := url.Values{"q": []string{"fabric"}}
	before := server.cacheKey("query", values)
	server.clearCachedSearch(context.Background())
	after := server.cacheKey("query", values)
	if before == after {
		t.Fatal("expected cache key to change after version bump")
	}
}
