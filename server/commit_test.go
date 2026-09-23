package server

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/blockbridge/avmcbbs/apps/nexusindex/internal/indexer"
)

type staleWriter struct{ *indexer.Service }

func (*staleWriter) Upsert(context.Context, indexer.SearchDocument) (indexer.WriteResult, error) {
	return indexer.WriteResult{}, nil
}
func (*staleWriter) MarkDeleted(context.Context, string, string) (indexer.WriteResult, error) {
	return indexer.WriteResult{}, nil
}

func TestCommitHTTPStaleWritesReportConflict(t *testing.T) {
	s := NewHTTPServer(&staleWriter{indexer.NewService(nil, nil)}, Options{})
	for _, method := range []string{"PUT", "DELETE"} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest(method, "/index/doc", strings.NewReader(`{"title":"stale"}`)))
		if w.Code != 409 || !strings.Contains(w.Body.String(), "stale_source_version") {
			t.Errorf("%s stale response: %d %s", method, w.Code, w.Body.String())
		}
	}
}

func TestCommitBackgroundInvalidationUsesSharedFence(t *testing.T) {
	f, service, a, b := mutationServers(t)
	if got := totalFromResponse(t, searchHTTP(t, b)); got != 1 {
		t.Fatal(got)
	}
	service.revision.Store(2)
	if err := a.InvalidateSearchCache(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := totalFromResponse(t, searchHTTP(t, b)); got != 2 {
		t.Fatalf("background commit retained old cache: %d", got)
	}
	f.mu.Lock()
	f.failCompleted = true
	f.mu.Unlock()
	if err := a.InvalidateSearchCache(context.Background()); err == nil {
		t.Fatal("completion failure was swallowed")
	}
	if w := searchHTTP(t, b); w.Header().Get("X-NexusIndex-Cache") != "BYPASS" {
		t.Fatalf("failed background fence allowed cache: %v", w.Header())
	}
}

func TestCommitOmittedCreatedAtStaysUnspecified(t *testing.T) {
	doc := (indexDocumentRequest{Title: "updated"}).toSearchDocument("doc")
	if !doc.CreatedAt.IsZero() {
		t.Fatalf("omitted createdAt replaced by %v", doc.CreatedAt)
	}
}
