package server

import (
	"context"
	"encoding/json"
	"net/url"
	"testing"

	"github.com/blockbridge/avmcbbs/apps/nexusindex/internal/indexer"
)

type fakeCache struct {
	value json.RawMessage
}

func (f *fakeCache) Get(context.Context, string) (json.RawMessage, bool) {
	return f.value, f.value != nil
}

func (f *fakeCache) Set(context.Context, string, any, int64) {}

func (f *fakeCache) Clear(context.Context, string) {}

func TestTieredCacheDoesNotBackfillWithoutRemainingTTL(t *testing.T) {
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
	l2.value = nil
	if _, ok := cache.Get(context.Background(), key); ok {
		t.Fatal("unknown L2 lifetime must not be extended by L1 backfill")
	}
}

func TestDisabledEdgeCacheMissDoesNotPanic(t *testing.T) {
	defer func() {
		if recovered := recover(); recovered != nil {
			t.Errorf("disabled L2 panicked: %v", recovered)
		}
	}()
	cache := NewTieredCache(TieredCacheOptions{L2: NewEdgeCacheAdapter(nil)})
	if _, hit := cache.Get(context.Background(), "missing"); hit {
		t.Fatal("unexpected hit")
	}
}

func TestTieredCacheUpdateAtCapacityPreservesOtherKeys(t *testing.T) {
	cache := NewTieredCache(TieredCacheOptions{L1MaxEntries: 2})
	ctx := context.Background()
	cache.Set(ctx, "a", "first", 60)
	cache.Set(ctx, "b", "keep", 20)
	cache.Set(ctx, "a", "updated", 60)
	if value, ok := cache.Get(ctx, "b"); !ok || string(value) != `"keep"` {
		t.Fatal("updating a evicted b")
	}
	if value, ok := cache.Get(ctx, "a"); !ok || string(value) != `"updated"` {
		t.Fatal("a was not updated")
	}
}

func TestTieredCacheExactClearPreservesPrefixNeighbors(t *testing.T) {
	cache := NewTieredCache(TieredCacheOptions{})
	ctx := context.Background()
	cache.Set(ctx, "abc", true, 60)
	cache.Set(ctx, "abcdef", true, 60)
	cache.Clear(ctx, "abc")
	if _, ok := cache.Get(ctx, "abc"); ok {
		t.Fatal("exact key survived clear")
	}
	if _, ok := cache.Get(ctx, "abcdef"); !ok {
		t.Fatal("exact clear removed prefix neighbor")
	}
	cache.Clear(ctx, "abc*")
	if _, ok := cache.Get(ctx, "abcdef"); ok {
		t.Fatal("glob clear did not remove neighbor")
	}
}

func TestCacheKeyVersionBump(t *testing.T) {
	cache := NewTieredCache(TieredCacheOptions{L1MaxEntries: 8})
	server := NewHTTPServer(nil, Options{Cache: CacheOptions{Cache: cache, Namespace: "nexusindex:test"}})
	values := url.Values{"q": []string{"fabric"}}
	before, _ := server.scopedCacheKey(server.withCacheScope(context.Background()), "query", values)
	server.clearCachedSearch(context.Background())
	after, _ := server.scopedCacheKey(server.withCacheScope(context.Background()), "query", values)
	if before == after {
		t.Fatal("expected cache key to change after version bump")
	}
}

func TestSearchCacheValuesIncludeStructuredQuery(t *testing.T) {
	base := searchRequest{Q: "fabric", Sort: "relevance", Limit: 20}
	structured := base
	structured.Must = []indexer.QueryClause{{Field: "title", Operator: "phrase", Value: "fabric api"}}
	structured.Filters = indexer.SearchFilters{Platforms: []string{"java"}}
	if base.cacheValues().Encode() == structured.cacheValues().Encode() {
		t.Fatal("structured query must change the search cache key")
	}
}
