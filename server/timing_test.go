package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blockbridge/avmcbbs/apps/nexusindex/internal/indexer"
)

// Both cold and cached explain responses must preserve zero versus unavailable
// even when the search has no candidates and all timing values happen to be 0.
func TestSearchTimingHTTPLagAvailability(t *testing.T) {
	for _, tc := range []struct {
		name, timing, header string
		exists               bool
	}{
		{"unavailable", `{}`, "", false},
		{"empty", `{"index_lag_ms":0}`, "0", true},
		{"backlog", `{"index_lag_ms":125}`, "125", true},
	} {
		for _, explain := range []bool{false, true} {
			t.Run(tc.name+map[bool]string{false: "/normal", true: "/explain"}[explain], func(t *testing.T) {
				var timing indexer.SearchTiming
				if err := json.Unmarshal([]byte(tc.timing), &timing); err != nil {
					t.Fatal(err)
				}
				service := &searchStub{Service: indexer.NewService(nil, nil), search: func(context.Context, indexer.SearchQuery) (indexer.SearchPage, error) {
					return indexer.SearchPage{Timing: timing, Items: []indexer.SearchResult{}}, nil
				}}
				s := NewHTTPServer(service, Options{Cache: CacheOptions{Cache: NewTieredCache(TieredCacheOptions{})}})
				path := "/api/search"
				if explain {
					path += "?explain=true"
				}
				for attempt := 0; attempt < 2; attempt++ {
					w := httptest.NewRecorder()
					s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
					if w.Code != 200 {
						t.Fatal(w.Body.String())
					}
					if got := w.Header().Get("X-NexusIndex-Index-Lag-Ms"); got != tc.header {
						t.Errorf("attempt %d header=%q want=%q body=%s", attempt, got, tc.header, w.Body.String())
					}
					var body map[string]any
					if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
						t.Fatal(err)
					}
					if explain {
						body = body["debug"].(map[string]any)
					}
					timingBody := body["timing"].(map[string]any)
					if value, exists := timingBody["index_lag_ms"]; (tc.exists && !exists) || (!tc.exists && exists && value != nil) {
						t.Errorf("wrong lag availability: %s", w.Body.String())
					}
					if explain && !tc.exists {
						if value, exists := body["index_lag_ms"]; exists && value != nil {
							t.Errorf("debug lag claims available: %s", w.Body.String())
						}
					}
					if w.Header().Get("X-NexusIndex-PG-Ms") != "0" {
						t.Errorf("missing timing for zero-result response: %v", w.Header())
					}
				}
			})
		}
	}
}

func TestSearchTimingHTTPPreservesNonzeroPhases(t *testing.T) {
	for _, explain := range []bool{false, true} {
		t.Run(map[bool]string{false: "normal", true: "explain"}[explain], func(t *testing.T) {
			calls := 0
			service := &searchStub{Service: indexer.NewService(nil, nil), search: func(context.Context, indexer.SearchQuery) (indexer.SearchPage, error) {
				calls++
				return indexer.SearchPage{Timing: indexer.SearchTiming{PGMS: 123, ScoringMS: 17, HighlightMS: 9}, Items: []indexer.SearchResult{}}, nil
			}}
			s := NewHTTPServer(service, Options{Cache: CacheOptions{Cache: NewTieredCache(TieredCacheOptions{})}})
			path := "/api/search"
			if explain {
				path += "?explain=true"
			}
			for attempt := 0; attempt < 2; attempt++ {
				w := httptest.NewRecorder()
				s.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
				if w.Code != 200 {
					t.Fatal(w.Body.String())
				}
				for name, want := range map[string]string{"X-NexusIndex-PG-Ms": "123", "X-NexusIndex-Scoring-Ms": "17", "X-NexusIndex-Highlight-Ms": "9"} {
					if got := w.Header().Get(name); got != want {
						t.Errorf("attempt=%d %s=%q want=%q", attempt, name, got, want)
					}
				}
				var body map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
					t.Fatal(err)
				}
				if explain {
					body = body["debug"].(map[string]any)
				}
				timing := body["timing"].(map[string]any)
				for name, want := range map[string]float64{"pg_ms": 123, "scoring_ms": 17, "highlight_ms": 9} {
					if timing[name] != want {
						t.Errorf("attempt=%d %s=%v want=%v", attempt, name, timing[name], want)
					}
				}
			}
			if calls != 1 {
				t.Fatalf("expected second request to use cached timing; searches=%d", calls)
			}
		})
	}
}
