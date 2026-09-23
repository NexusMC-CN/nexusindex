package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blockbridge/avmcbbs/apps/nexusindex/internal/indexer"
	edgecache "github.com/blockbridge/avmcbbs/apps/nexusindex/packages/edgecache-go-client"
)

// Keep the real runtime store; replace only the database search boundary.
type searchStub struct {
	*indexer.Service
	search func(context.Context, indexer.SearchQuery) (indexer.SearchPage, error)
}

// Model only the search/database boundary, exposing the runtime identity that
// HTTP actually passes rather than consulting the store a second time.
type snapshotSearchStub struct {
	*indexer.Service
	afterCapture func()
}

func (s *snapshotSearchStub) RuntimeSnapshot() indexer.RuntimeSnapshot {
	snapshot := s.Service.RuntimeSnapshot()
	if s.afterCapture != nil {
		callback := s.afterCapture
		s.afterCapture = nil
		callback()
	}
	return snapshot
}

func (s *snapshotSearchStub) Search(ctx context.Context, q indexer.SearchQuery) (indexer.SearchPage, error) {
	return s.SearchWithSnapshot(ctx, q, s.Service.RuntimeSnapshot())
}

func (s *snapshotSearchStub) SearchWithSnapshot(_ context.Context, _ indexer.SearchQuery, snapshot indexer.RuntimeSnapshot) (indexer.SearchPage, error) {
	return indexer.SearchPage{Items: []indexer.SearchResult{{ID: "result", Score: snapshot.Config.Scoring.TitleWeight}}, NextCursor: snapshot.Hash}, nil
}

func TestSearchUsesCapturedRuntimeAcrossPatchAndBump(t *testing.T) {
	service := &snapshotSearchStub{Service: indexer.NewService(nil, nil)}
	cache := NewTieredCache(TieredCacheOptions{})
	s := NewHTTPServer(service, Options{Cache: CacheOptions{Cache: cache}})
	old := service.Service.RuntimeSnapshot()
	service.afterCapture = func() {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(`{"scoring":{"title_weight":77}}`)))
		if w.Code != 200 {
			t.Fatalf("patch failed: %s", w.Body.String())
		}
	}
	w := searchHTTP(t, s)
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var body struct {
		Items      []indexer.SearchResult
		NextCursor string `json:"next_cursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if len(body.Items) != 1 || body.Items[0].Score != 8 || body.NextCursor != old.Hash {
		t.Fatalf("captured old identity used new config/result: %s", w.Body.String())
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	queryEntries := 0
	for key, entry := range cache.l1 {
		if !strings.Contains(key, ":query:") {
			continue
		}
		queryEntries++
		if strings.Contains(string(entry.value), `"score":77`) {
			t.Fatal("new configuration result stored under captured old identity")
		}
	}
	if queryEntries != 1 {
		t.Fatalf("expected one cache write for the captured snapshot, got %d", queryEntries)
	}
}

func (s *searchStub) Search(ctx context.Context, q indexer.SearchQuery) (indexer.SearchPage, error) {
	return s.search(ctx, q)
}

func (s *searchStub) SearchWithSnapshot(ctx context.Context, q indexer.SearchQuery, _ indexer.RuntimeSnapshot) (indexer.SearchPage, error) {
	return s.search(ctx, q)
}

func searchHTTP(t *testing.T, server *HTTPServer) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	server.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/api/search?q=fabric&explain=true", nil))
	return w
}

func TestSearchCacheHitUpdatesDebug(t *testing.T) {
	service := &searchStub{Service: indexer.NewService(nil, nil), search: func(context.Context, indexer.SearchQuery) (indexer.SearchPage, error) {
		return indexer.SearchPage{Items: []indexer.SearchResult{}, Total: 9007199254740993}, nil
	}}
	s := NewHTTPServer(service, Options{Cache: CacheOptions{Cache: NewTieredCache(TieredCacheOptions{})}})
	first := searchHTTP(t, s)
	if first.Code != http.StatusOK {
		t.Fatal(first.Body.String())
	}
	second := searchHTTP(t, s)
	var body struct {
		Debug struct {
			CacheHit bool `json:"cache_hit"`
		}
		Total json.Number
	}
	if err := json.Unmarshal(second.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.Debug.CacheHit || second.Header().Get("X-NexusIndex-Cache-Hit") != "true" {
		t.Fatalf("hit debug is stale: %s", second.Body.String())
	}
	if body.Total.String() != "9007199254740993" {
		t.Fatal("cached payload precision changed")
	}
}

func TestSearchCursorMismatchIsBadRequest(t *testing.T) {
	service := &searchStub{Service: indexer.NewService(nil, nil), search: func(context.Context, indexer.SearchQuery) (indexer.SearchPage, error) {
		return indexer.SearchPage{}, fmt.Errorf("search: %w", indexer.ErrCursorMismatch)
	}}
	w := searchHTTP(t, NewHTTPServer(service, Options{}))
	if w.Code != http.StatusBadRequest {
		t.Fatalf("got %d: %s", w.Code, w.Body.String())
	}
}

func TestRuntimePatchesDoNotLoseConcurrentFields(t *testing.T) {
	service := indexer.NewService(nil, nil)
	s := NewHTTPServer(service, Options{})
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			w := httptest.NewRecorder()
			body := fmt.Sprintf(`{"query":{"synonyms":{"term%d":["value"]}}}`, i)
			s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/config", strings.NewReader(body)))
			if w.Code != http.StatusOK {
				t.Errorf("patch failed: %d", w.Code)
			}
		}(i)
	}
	close(start)
	wg.Wait()
	cfg := service.RuntimeConfig()
	for i := 0; i < 80; i++ {
		if _, ok := cfg.Query.Synonyms[fmt.Sprintf("term%d", i)]; !ok {
			t.Errorf("patch %d lost", i)
		}
	}
}

func TestSearchMissCannotPopulateNewCacheVersion(t *testing.T) {
	calls := 0
	var s *HTTPServer
	service := &searchStub{Service: indexer.NewService(nil, nil), search: func(context.Context, indexer.SearchQuery) (indexer.SearchPage, error) {
		calls++
		if calls == 1 {
			s.InvalidateSearchCache(context.Background())
		}
		return indexer.SearchPage{Total: int64(calls)}, nil
	}}
	s = NewHTTPServer(service, Options{Cache: CacheOptions{Cache: NewTieredCache(TieredCacheOptions{})}})
	searchHTTP(t, s)
	second := searchHTTP(t, s)
	if calls != 2 || second.Header().Get("X-NexusIndex-Cache-Hit") != "false" {
		t.Fatalf("old miss populated new version: %s", second.Body.String())
	}
}

// This fixture serves the documented EdgeCache wire protocol to the real Go client.
type edgeWireFixture struct {
	mu                   sync.Mutex
	values               map[string]json.RawMessage
	getTTL               int64
	setDelay             time.Duration
	active, maxActive    int
	failVersion          bool
	versions             map[string]int64
	failBump             bool
	failCompleted        bool
	failVersionNamespace string
	afterVersionGet      func(string)
}

func edgeFixture(t *testing.T) (*edgeWireFixture, Cache) {
	t.Helper()
	f := &edgeWireFixture{versions: map[string]int64{}, values: map[string]json.RawMessage{}, getTTL: 1}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	return f, NewEdgeCacheAdapter(edgecache.New(edgecache.Options{BaseURL: srv.URL}))
}
func (f *edgeWireFixture) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	switch r.URL.Path {
	case "/v1/forum/version", "/v1/forum/version/bump":
		if f.failVersion {
			w.WriteHeader(503)
			return
		}
		namespace := r.URL.Query().Get("namespace")
		bump := r.URL.Path == "/v1/forum/version/bump"
		if !bump && f.failVersionNamespace == namespace {
			w.WriteHeader(503)
			return
		}
		if bump {
			var body struct{ Namespace string }
			if json.NewDecoder(r.Body).Decode(&body) != nil {
				w.WriteHeader(400)
				return
			}
			namespace = body.Namespace
			if f.failBump || (f.failCompleted && strings.HasSuffix(namespace, ":mutation:completed")) {
				w.WriteHeader(503)
				return
			}
		}
		version := f.versions[namespace]
		if !bump && version == 0 {
			version = 1
		}
		if bump {
			version++
		}
		f.versions[namespace] = version
		if !bump && f.afterVersionGet != nil {
			hook := f.afterVersionGet
			f.mu.Unlock()
			hook(namespace)
			f.mu.Lock()
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"namespace": namespace, "version": version})
	case "/v1/cache/get":
		key := r.URL.Query().Get("key")
		value, hit := f.values[key]
		_ = json.NewEncoder(w).Encode(map[string]any{"key": key, "value": value, "hit": hit, "ttlSeconds": f.getTTL})
	case "/v1/cache/set":
		var body struct {
			Key   string
			Value json.RawMessage
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			w.WriteHeader(400)
			return
		}
		highlight := strings.Contains(body.Key, ":highlight:")
		if highlight {
			f.active++
			if f.active > f.maxActive {
				f.maxActive = f.active
			}
		}
		delay := f.setDelay
		f.mu.Unlock()
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
		}
		f.mu.Lock()
		if highlight {
			f.active--
		}
		f.values[body.Key] = body.Value
		w.WriteHeader(204)
	case "/v1/cache/clear":
		var body struct{ Pattern string }
		_ = json.NewDecoder(r.Body).Decode(&body)
		for key := range f.values {
			if matchCachePattern(body.Pattern, key) {
				delete(f.values, key)
			}
		}
		w.WriteHeader(204)
	default:
		w.WriteHeader(404)
	}
}

type mutationServiceStub struct {
	*searchStub
	revision     atomic.Int64
	mutations    atomic.Int64
	upsert       func() error
	lastUpsertMu sync.Mutex
	lastUpsert   indexer.SearchDocument
}

func newMutationService() *mutationServiceStub {
	s := &mutationServiceStub{searchStub: &searchStub{Service: indexer.NewService(nil, nil)}}
	s.revision.Store(1)
	s.search = func(context.Context, indexer.SearchQuery) (indexer.SearchPage, error) {
		return indexer.SearchPage{Total: s.revision.Load()}, nil
	}
	return s
}
func (s *mutationServiceStub) Upsert(_ context.Context, doc indexer.SearchDocument) (indexer.WriteResult, error) {
	s.mutations.Add(1)
	s.lastUpsertMu.Lock()
	s.lastUpsert = doc
	s.lastUpsertMu.Unlock()
	if s.upsert != nil {
		return indexer.WriteResult{Applied: true}, s.upsert()
	}
	s.revision.Add(1)
	return indexer.WriteResult{Applied: true}, nil
}
func (s *mutationServiceStub) MarkDeleted(context.Context, string, string) (indexer.WriteResult, error) {
	s.mutations.Add(1)
	s.revision.Add(1)
	return indexer.WriteResult{Applied: true}, nil
}
func (s *mutationServiceStub) StartFullRebuild(context.Context) (indexer.RebuildResult, error) {
	s.mutations.Add(1)
	return indexer.RebuildResult{JobID: 1}, nil
}
func (s *mutationServiceStub) StartEntityRebuild(context.Context, string, string) (indexer.RebuildResult, error) {
	s.mutations.Add(1)
	return indexer.RebuildResult{JobID: 2}, nil
}

func mutationHTTP(s *HTTPServer, method, path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(body)))
	return w
}

func (s *mutationServiceStub) capturedUpsert() indexer.SearchDocument {
	s.lastUpsertMu.Lock()
	defer s.lastUpsertMu.Unlock()
	return s.lastUpsert
}

func TestIndexDocumentPathIsDecodedExactlyOnce(t *testing.T) {
	service := newMutationService()
	w := mutationHTTP(NewHTTPServer(service, Options{}), http.MethodPut, "/index/a%2562", `{}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if got := service.capturedUpsert().EntityID; got != "a%62" {
		t.Fatalf("entity ID was decoded more than once: got %q, want %q", got, "a%62")
	}
}

func TestIndexDocumentRejectsEncodedSlashWithinSingleIDSegment(t *testing.T) {
	for _, path := range []string{"/index/%2Fvictim", "/index/victim%2F"} {
		t.Run(path, func(t *testing.T) {
			service := newMutationService()
			w := mutationHTTP(NewHTTPServer(service, Options{}), http.MethodPut, path, `{}`)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if got := service.mutations.Load(); got != 0 {
				t.Fatalf("encoded slash executed %d mutations", got)
			}
		})
	}
}

func TestIndexDocumentRejectsUnknownEntityTypeBeforeMutation(t *testing.T) {
	for name, request := range map[string]struct {
		path string
		body string
	}{
		"query": {path: "/index/doc?entityType=document:resource", body: `{}`},
		"body":  {path: "/index/doc", body: `{"entityType":"document:resource"}`},
	} {
		t.Run(name, func(t *testing.T) {
			service := newMutationService()
			w := mutationHTTP(NewHTTPServer(service, Options{}), http.MethodPut, request.path, request.body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if got := service.mutations.Load(); got != 0 {
				t.Fatalf("unknown entity type executed %d mutations", got)
			}
		})
	}
}

func TestIndexDocumentJSONIsStrictAndDoesNotMutateOnInvalidInput(t *testing.T) {
	tests := map[string]string{
		"null":             `null`,
		"concatenated":     `{}{"title":"second"}`,
		"ndjson":           "{}\n{}",
		"trailing garbage": `{} trailing`,
		"body too large":   `{"title":"` + strings.Repeat("x", 64<<10) + `"}`,
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			service := newMutationService()
			w := mutationHTTP(NewHTTPServer(service, Options{}), http.MethodPut, "/index/doc", body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if got := service.mutations.Load(); got != 0 {
				t.Fatalf("invalid JSON executed %d mutations", got)
			}
		})
	}
}

func TestIndexDocumentJSONPreservesLargePayloadNumbers(t *testing.T) {
	service := newMutationService()
	w := mutationHTTP(NewHTTPServer(service, Options{}), http.MethodPut, "/index/doc", `{"payload":{"id":9007199254740993}}`)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	value, ok := service.capturedUpsert().Payload["id"].(json.Number)
	if !ok || value.String() != "9007199254740993" {
		t.Fatalf("payload number lost its JSON token: %#v", service.capturedUpsert().Payload["id"])
	}
}

func totalFromResponse(t *testing.T, w *httptest.ResponseRecorder) int64 {
	t.Helper()
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var body struct{ Total int64 }
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	return body.Total
}

func mutationServers(t *testing.T) (*edgeWireFixture, *mutationServiceStub, *HTTPServer, *HTTPServer) {
	t.Helper()
	f, adapter := edgeFixture(t)
	service := newMutationService()
	newServer := func() *HTTPServer {
		return NewHTTPServer(service, Options{RuntimeConfigFile: "fixture.json", RuntimeConfigLoader: func() (indexer.RuntimeConfig, error) {
			cfg := indexer.DefaultRuntimeConfig()
			cfg.Scoring.TitleWeight = 77
			return cfg, nil
		}, Cache: CacheOptions{Namespace: "mutations", Cache: NewTieredCache(TieredCacheOptions{L2: adapter})}})
	}
	return f, service, newServer(), newServer()
}

func TestHTTPMutationRejectsFailedSharedPrecommit(t *testing.T) {
	for _, tc := range []struct{ method, path, body string }{
		{"PUT", "/index/doc", `{"title":"changed"}`},
		{"POST", "/api/index/doc", `{"title":"changed"}`},
		{"DELETE", "/index/doc", ``},
		{"POST", "/api/config", `{"scoring":{"title_weight":77}}`},
		{"POST", "/api/config/reload", `{}`},
		{"POST", "/api/index/rebuild", `{}`},
		{"POST", "/api/index/rebuild/document/doc", `{}`},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			f, service, a, b := mutationServers(t)
			searchHTTP(t, b)
			old := service.RuntimeSnapshot()
			f.mu.Lock()
			f.failBump = true
			f.mu.Unlock()
			w := mutationHTTP(a, tc.method, tc.path, tc.body)
			if w.Code != 503 {
				t.Errorf("failed precommit returned %d: %s", w.Code, w.Body.String())
			}
			if service.mutations.Load() != 0 || service.RuntimeSnapshot().Hash != old.Hash {
				t.Error("failed precommit executed the mutation")
			}
			f.mu.Lock()
			f.failBump = false
			f.mu.Unlock()
			// A receives no further requests: B must reflect the unchanged source,
			// without relying on A to retry its failed invalidation.
			if got := totalFromResponse(t, searchHTTP(t, b)); got != service.revision.Load() {
				t.Errorf("B served stale revision %d, source=%d", got, service.revision.Load())
			}
		})
	}
}

func TestHTTPMutationCompletionFailureKeepsOtherInstanceBypassing(t *testing.T) {
	f, service, a, b := mutationServers(t)
	searchHTTP(t, b)
	f.mu.Lock()
	f.failCompleted = true
	f.mu.Unlock()
	w := mutationHTTP(a, "PUT", "/index/doc", `{"title":"changed"}`)
	if w.Code != 503 {
		t.Errorf("failed completion returned %d", w.Code)
	}
	if service.revision.Load() != 2 {
		t.Fatal("fixture mutation did not commit")
	}
	f.mu.Lock()
	f.failCompleted = false
	f.mu.Unlock()
	// A can exit here. Recovery only touches B and must never reuse its old L1.
	for i := 0; i < 2; i++ {
		response := searchHTTP(t, b)
		if totalFromResponse(t, response) != 2 || response.Header().Get("X-NexusIndex-Cache") != "BYPASS" {
			t.Fatalf("B reused cache after incomplete mutation: %s state=%s", response.Body.String(), response.Header().Get("X-NexusIndex-Cache"))
		}
	}
}

func TestHTTPMutationWindowBlocksConcurrentCacheRefill(t *testing.T) {
	_, service, a, b := mutationServers(t)
	searchHTTP(t, b)
	entered, release := make(chan struct{}), make(chan struct{})
	service.upsert = func() error { close(entered); <-release; service.revision.Add(1); return nil }
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- mutationHTTP(a, "PUT", "/index/doc", `{}`) }()
	<-entered
	during := searchHTTP(t, b)
	if during.Header().Get("X-NexusIndex-Cache") != "BYPASS" {
		t.Errorf("cache allowed inside mutation window: %s", during.Header().Get("X-NexusIndex-Cache"))
	}
	close(release)
	if w := <-done; w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	after := searchHTTP(t, b)
	if totalFromResponse(t, after) != 2 || after.Header().Get("X-NexusIndex-Cache-Hit") == "true" {
		t.Fatal("concurrent old search refilled the post-mutation cache")
	}
}

func TestFirstMutationInitializesBothFencesBeforeStartedBump(t *testing.T) {
	_, service, a, b := mutationServers(t)
	entered, release := make(chan struct{}), make(chan struct{})
	service.upsert = func() error { close(entered); <-release; service.revision.Add(1); return nil }
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- mutationHTTP(a, "PUT", "/index/doc", `{}`) }()
	<-entered
	// B is the only reader after A's precommit. With Redis cold-start INCR
	// semantics, an uninitialized completed key must not make this a HIT.
	response := searchHTTP(t, b)
	close(release)
	if response.Header().Get("X-NexusIndex-Cache") != "BYPASS" {
		t.Fatalf("cold-start mutation was treated as complete: %s", response.Header().Get("X-NexusIndex-Cache"))
	}
	if w := <-done; w.Code != 200 {
		t.Fatal(w.Body.String())
	}
}

func TestMutationRejectsEitherFenceInitializationFailure(t *testing.T) {
	for _, suffix := range []string{":mutation:started", ":mutation:completed"} {
		t.Run(suffix, func(t *testing.T) {
			f, service, a, _ := mutationServers(t)
			f.mu.Lock()
			f.failVersionNamespace = "mutations" + suffix
			f.mu.Unlock()
			w := mutationHTTP(a, "PUT", "/index/doc", `{}`)
			if w.Code != 503 || service.mutations.Load() != 0 {
				t.Fatalf("failed fence initialization allowed mutation: status=%d mutations=%d", w.Code, service.mutations.Load())
			}
		})
	}
}

func TestConcurrentHTTPMutationsCanCompleteOutOfOrder(t *testing.T) {
	_, service, a, b := mutationServers(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	service.upsert = func() error {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		service.revision.Add(1)
		return nil
	}
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- mutationHTTP(a, "PUT", "/index/first", `{}`) }()
	<-entered
	w := mutationHTTP(b, "PUT", "/index/second", `{}`)
	if w.Code != 200 {
		t.Error(w.Body.String())
	}
	if response := searchHTTP(t, b); response.Header().Get("X-NexusIndex-Cache") != "BYPASS" {
		t.Error("completion of second mutation reopened cache before first completed")
	}
	close(release)
	if w := <-done; w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if got := totalFromResponse(t, searchHTTP(t, b)); got != 3 {
		t.Fatalf("lost completed source updates: %d", got)
	}
	if response := searchHTTP(t, b); response.Header().Get("X-NexusIndex-Cache-Hit") != "true" {
		t.Fatal("counters did not converge after out-of-order completion")
	}
}

func TestFailedHTTPMutationClosesCacheWindow(t *testing.T) {
	_, service, a, b := mutationServers(t)
	service.upsert = func() error { return errors.New("database write rejected") }
	if w := mutationHTTP(a, "PUT", "/index/doc", `{}`); w.Code != 502 {
		t.Fatalf("expected database error, got %d", w.Code)
	}
	searchHTTP(t, b)
	if response := searchHTTP(t, b); response.Header().Get("X-NexusIndex-Cache-Hit") != "true" {
		t.Fatal("failed mutation left the cache window open")
	}
}

func TestMutationStartingBetweenVersionReadsBypassesCache(t *testing.T) {
	f, service, a, b := mutationServers(t)
	searchHTTP(t, b)
	entered, release := make(chan struct{}), make(chan struct{})
	service.upsert = func() error { close(entered); <-release; service.revision.Add(1); return nil }
	done := make(chan *httptest.ResponseRecorder, 1)
	var injected atomic.Bool
	f.mu.Lock()
	f.afterVersionGet = func(namespace string) {
		if strings.HasSuffix(namespace, ":mutation:completed") && injected.CompareAndSwap(false, true) {
			go func() { done <- mutationHTTP(a, "PUT", "/index/doc", `{}`) }()
			<-entered
		}
	}
	f.mu.Unlock()
	response := searchHTTP(t, b)
	close(release)
	if w := <-done; w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	if response.Header().Get("X-NexusIndex-Cache") != "BYPASS" {
		t.Fatalf("mutation between version reads reused old L1: %s", response.Header().Get("X-NexusIndex-Cache"))
	}
}

func TestSharedVersionInvalidatesOtherInstanceL1(t *testing.T) {
	_, adapter := edgeFixture(t)
	calls := 0
	service := &searchStub{Service: indexer.NewService(nil, nil), search: func(context.Context, indexer.SearchQuery) (indexer.SearchPage, error) {
		calls++
		return indexer.SearchPage{Total: int64(calls)}, nil
	}}
	newServer := func() *HTTPServer {
		return NewHTTPServer(service, Options{Cache: CacheOptions{Namespace: "test", Cache: NewTieredCache(TieredCacheOptions{L2: adapter})}})
	}
	a, b := newServer(), newServer()
	searchHTTP(t, b)
	if searchHTTP(t, b).Header().Get("X-NexusIndex-Cache-Hit") != "true" {
		t.Fatal("fixture did not warm B L1")
	}
	a.InvalidateSearchCache(context.Background())
	if response := searchHTTP(t, b); response.Header().Get("X-NexusIndex-Cache-Hit") != "false" {
		t.Fatalf("B retained stale L1: %s", response.Body.String())
	}
}

func TestEdgeCacheBackfillUsesRemainingTTL(t *testing.T) {
	f, adapter := edgeFixture(t)
	f.values["query"] = json.RawMessage(`{"ok":true}`)
	cache := NewTieredCache(TieredCacheOptions{L2: adapter, QueryTTLSeconds: 60})
	if _, ok := cache.Get(context.Background(), "query"); !ok {
		t.Fatal("missing L2 fixture")
	}
	f.mu.Lock()
	delete(f.values, "query")
	f.mu.Unlock()
	time.Sleep(1100 * time.Millisecond)
	if _, ok := cache.Get(context.Background(), "query"); ok {
		t.Fatal("expired L2 value outlived its remaining TTL in L1")
	}
}

func TestEdgeCacheBackfillRetainsConfiguredTTLBound(t *testing.T) {
	f, adapter := edgeFixture(t)
	f.getTTL = 60
	const key = "nexusindex:query:example"
	f.values[key] = json.RawMessage(`{"ok":true}`)
	cache := NewTieredCache(TieredCacheOptions{L2: adapter, QueryTTLSeconds: 1})
	if _, ok := cache.Get(context.Background(), key); !ok {
		t.Fatal("missing L2 fixture")
	}
	f.mu.Lock()
	delete(f.values, key)
	f.mu.Unlock()
	time.Sleep(1100 * time.Millisecond)
	if _, ok := cache.Get(context.Background(), key); ok {
		t.Fatal("backfill exceeded the configured query TTL")
	}
}

func TestSharedVersionFailureBypassesWarmL1(t *testing.T) {
	f, adapter := edgeFixture(t)
	calls := 0
	service := &searchStub{Service: indexer.NewService(nil, nil), search: func(context.Context, indexer.SearchQuery) (indexer.SearchPage, error) {
		calls++
		return indexer.SearchPage{Total: int64(calls)}, nil
	}}
	s := NewHTTPServer(service, Options{Cache: CacheOptions{Cache: NewTieredCache(TieredCacheOptions{L2: adapter})}})
	searchHTTP(t, s)
	f.mu.Lock()
	f.failVersion = true
	f.mu.Unlock()
	w := searchHTTP(t, s)
	if calls != 2 || w.Header().Get("X-NexusIndex-Cache") != "BYPASS" {
		t.Fatalf("unavailable version should bypass warmed cache: calls=%d state=%s", calls, w.Header().Get("X-NexusIndex-Cache"))
	}
}

func TestHighlightWritesHaveBoundedRequestBudget(t *testing.T) {
	f, adapter := edgeFixture(t)
	f.setDelay = 40 * time.Millisecond
	service := &searchStub{Service: indexer.NewService(nil, nil), search: func(context.Context, indexer.SearchQuery) (indexer.SearchPage, error) {
		items := make([]indexer.SearchResult, 100)
		for i := range items {
			items[i].ID = fmt.Sprintf("doc%d", i)
		}
		return indexer.SearchPage{Items: items}, nil
	}}
	s := NewHTTPServer(service, Options{Cache: CacheOptions{Cache: NewTieredCache(TieredCacheOptions{L2: adapter})}})
	started := time.Now()
	response := searchHTTP(t, s)
	elapsed := time.Since(started)
	if response.Code != 200 {
		t.Fatal(response.Body.String())
	}
	if elapsed > 400*time.Millisecond {
		t.Errorf("100 highlights accumulated %s of request delay", elapsed)
	}
	f.mu.Lock()
	max := f.maxActive
	f.mu.Unlock()
	if max > 4 {
		t.Errorf("L2 writes exceeded concurrency bound: %d", max)
	}
}
