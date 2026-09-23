package main

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blockbridge/avmcbbs/apps/nexusindex/internal/indexer"
	"github.com/blockbridge/avmcbbs/apps/nexusindex/internal/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRunQueryReturnsErrorWhenAnySampleFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	err := runQuery(context.Background(), queryOptions{
		BaseURL: server.URL, PerClass: 1, Concurrency: 1, TimeoutMS: 1000, Limit: 10, CheckCursor: true,
	})
	if err == nil {
		t.Fatal("query stability accepted failed samples")
	}
}

// A healthy service rejects punctuation-only queries. Treating every sample
// as a successful search makes the CLI fail against its production contract.
func TestRunQueryAcceptsExpectedInvalidQueryResponse(t *testing.T) {
	var negativeCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Query  string `json:"q"`
			Cursor string `json:"cursor"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.Query == "???" {
			negativeCalls.Add(1)
			if request.Cursor != "" {
				t.Error("negative query requested a second page")
			}
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"code":"invalid_query_text","message":"invalid text"},"next_cursor":"must-not-follow"}`))
			return
		}
		_, _ = w.Write([]byte(`{"items":[{"id":"stable"}]}`))
	}))
	defer server.Close()
	opt := queryOptions{BaseURL: server.URL, PerClass: 4, Concurrency: 2, TimeoutMS: 1000, Limit: 10, CheckCursor: true}
	if sample := executeQuery(context.Background(), server.Client(), opt, "garbage", queryClasses["garbage"][1], ""); sample.Failed {
		t.Errorf("expected invalid-query response failed: %s", sample.Error)
	}
	if err := runQuery(context.Background(), opt); err != nil {
		t.Errorf("healthy query fixture failed: %v", err)
	}
	if negativeCalls.Load() < 11 {
		t.Errorf("negative contract was not repeatedly checked: %d calls", negativeCalls.Load())
	}
}

func TestInvalidQuerySamplesRejectWrongStatusOrErrorCode(t *testing.T) {
	for name, response := range map[string]struct {
		status int
		body   string
	}{
		"unexpected success": {200, `{"items":[]}`},
		"wrong status":       {422, `{"error":{"code":"invalid_query_text"}}`},
		"wrong code":         {400, `{"error":{"code":"invalid_request"}}`},
		"unstructured error": {400, `{"error":"invalid_query_text"}`},
		"trailing value":     {400, `{"error":{"code":"invalid_query_text"}} {}`},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(response.status)
				_, _ = w.Write([]byte(response.body))
			}))
			defer server.Close()
			opt := queryOptions{BaseURL: server.URL, TimeoutMS: 1000, Limit: 10}
			if sample := executeQuery(context.Background(), server.Client(), opt, "garbage", queryClasses["garbage"][1], ""); !sample.Failed {
				t.Fatalf("accepted wrong negative-query contract: %#v", sample)
			}
		})
	}
}

func TestInvalidQueryStabilityDetectsChangedErrorCode(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		code := "invalid_query_text"
		if calls.Add(1) > 1 {
			code = "invalid_request"
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]string{"code": code}})
	}))
	defer server.Close()
	check, _ := checkQueryStability(context.Background(), server.Client(), queryOptions{BaseURL: server.URL, CheckCursor: true}, "garbage", queryClasses["garbage"][1])
	if check.Stable || calls.Load() != 2 {
		t.Fatalf("expected second response to fail contract: check=%#v calls=%d", check, calls.Load())
	}
}

func TestRunQueryReturnsErrorWhenTopResultsAreUnstable(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[{"id":"result-` + string(rune('a'+calls.Add(1)%26)) + `"}]}`))
	}))
	defer server.Close()

	err := runQuery(context.Background(), queryOptions{
		BaseURL: server.URL, PerClass: 1, Concurrency: 1, TimeoutMS: 1000, Limit: 10,
	})
	if err == nil {
		t.Fatal("query stability accepted unstable top results")
	}
}

func TestExecuteQueryRejectsMalformedSuccessResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"items":`))
	}))
	defer server.Close()

	sample := executeQuery(context.Background(), server.Client(), queryOptions{
		BaseURL: server.URL, TimeoutMS: 1000, Limit: 10,
	}, "fixture", queryCase{Query: "query"}, "")
	if !sample.Failed {
		t.Fatalf("malformed success response was accepted: %#v", sample)
	}
}

func TestExecuteQueryRequires2xxObjectWithItemsArray(t *testing.T) {
	for name, response := range map[string]struct {
		status int
		body   string
	}{
		"redirect":      {status: http.StatusFound, body: `{"items":[]}`},
		"null":          {status: http.StatusOK, body: `null`},
		"missing items": {status: http.StatusOK, body: `{}`},
		"null items":    {status: http.StatusOK, body: `{"items":null}`},
		"object items":  {status: http.StatusOK, body: `{"items":{}}`},
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(response.status)
				_, _ = w.Write([]byte(response.body))
			}))
			defer server.Close()

			sample := executeQuery(context.Background(), server.Client(), queryOptions{
				BaseURL: server.URL, TimeoutMS: 1000, Limit: 10,
			}, "fixture", queryCase{Query: "query"}, "")
			if !sample.Failed {
				t.Fatalf("invalid search response was accepted: %#v", sample)
			}
		})
	}
}

func TestExecuteQueryRejectsFollowedRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/search" {
			http.Redirect(w, r, "/redirected", http.StatusFound)
			return
		}
		_, _ = w.Write([]byte(`{"items":[]}`))
	}))
	defer server.Close()

	sample := executeQuery(context.Background(), server.Client(), queryOptions{
		BaseURL: server.URL, TimeoutMS: 1000, Limit: 10,
	}, "fixture", queryCase{Query: "query"}, "")
	if !sample.Failed {
		t.Fatalf("followed redirect was accepted: %#v", sample)
	}
}

func TestCheckQueryStabilityDetectsCursorDisappearance(t *testing.T) {
	var firstPages atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := requestCursor(t, r)
		response := map[string]any{"items": []map[string]string{{"id": "first"}}}
		if cursor != "" {
			response["items"] = []map[string]string{}
		} else if firstPages.Add(1) == 1 {
			response["next_cursor"] = "page-2"
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	_, cursor := checkQueryStability(context.Background(), server.Client(), queryOptions{
		BaseURL: server.URL, TimeoutMS: 1000, Limit: 10, CheckCursor: true,
	}, "fixture", queryCase{Query: "query"})
	if cursor.Stable {
		t.Fatalf("cursor disappearance was accepted: %#v", cursor)
	}
}

func TestCheckQueryStabilityDetectsEmptySecondPageChanging(t *testing.T) {
	var secondPages atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cursor := requestCursor(t, r)
		response := map[string]any{"items": []map[string]string{{"id": "first"}}, "next_cursor": "page-2"}
		if cursor != "" {
			response = map[string]any{"items": []map[string]string{}}
			if secondPages.Add(1) > 1 {
				response["items"] = []map[string]string{{"id": "second"}}
			}
		}
		_ = json.NewEncoder(w).Encode(response)
	}))
	defer server.Close()

	_, cursor := checkQueryStability(context.Background(), server.Client(), queryOptions{
		BaseURL: server.URL, TimeoutMS: 1000, Limit: 10, CheckCursor: true,
	}, "fixture", queryCase{Query: "query"})
	if cursor.Stable {
		t.Fatalf("empty/non-empty second page change was accepted: %#v", cursor)
	}
}

func TestCheckQueryStabilitySkipsSecondPageWhenDisabled(t *testing.T) {
	var secondPages atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requestCursor(t, r) != "" {
			secondPages.Add(1)
		}
		_, _ = w.Write([]byte(`{"items":[{"id":"first"}],"next_cursor":"page-2"}`))
	}))
	defer server.Close()

	checkQueryStability(context.Background(), server.Client(), queryOptions{
		BaseURL: server.URL, TimeoutMS: 1000, Limit: 10, CheckCursor: false,
	}, "fixture", queryCase{Query: "query"})
	if got := secondPages.Load(); got != 0 {
		t.Fatalf("check-cursor=false issued %d second-page requests", got)
	}
}

func requestCursor(t *testing.T, r *http.Request) string {
	t.Helper()
	var body struct {
		Cursor string `json:"cursor"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		t.Errorf("decode request: %v", err)
	}
	return body.Cursor
}

func TestWorkerEventsUseSourceFixturesAndGuaranteeUpsert(t *testing.T) {
	fixtures := []indexer.SourceEntityReference{
		{EntityType: indexer.EntityDocument, EntityID: "page-one"},
		{EntityType: indexer.EntityResource, EntityID: "resource-one"},
	}
	events, err := buildWorkerEvents(rand.New(rand.NewSource(7)), fixtures, 0, 20, 0.5, 7)
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{
		"document:page-one":     true,
		"resource:resource-one": true,
	}
	for _, event := range events {
		if !allowed[event.EntityType+":"+event.EntityID] {
			t.Fatalf("event targets entity not read from source fixtures: %#v", event)
		}
		if event.Action != "upsert" {
			t.Fatalf("worker stability generated destructive action: %#v", event)
		}
	}
	if len(events) == 0 {
		t.Fatal("worker sample contains no upsert events")
	}
}

func TestSourceEntityReferencesRejectUnboundedLimit(t *testing.T) {
	service := indexer.NewService(nil, nil)
	if _, err := service.SourceEntityReferences(context.Background(), indexer.EntityDocument, indexer.MaxSourceEntityReferences+1); err == nil {
		t.Fatal("source reference reader accepted an unbounded limit")
	}
}

func TestWorkerStabilityGenerationMergeAndProcessOnce(t *testing.T) {
	service, pool := stabilityIntegrationService(t)
	ctx := context.Background()
	updatedAt := time.Date(2026, 9, 22, 8, 0, 0, 0, time.UTC)
	_, err := pool.Exec(ctx, `INSERT INTO "CustomPage" (id,title,published,category,description,content,"createdAt","updatedAt")
		VALUES ('resource:x','Worker fixture',true,'guide','source adapter fixture','body',$1,$1)`, updatedAt)
	if err != nil {
		t.Fatal(err)
	}

	refs, err := service.SourceEntityReferences(ctx, indexer.EntityDocument, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 1 || refs[0].EntityID != "resource:x" || refs[0].Title != "Worker fixture" {
		t.Fatalf("unexpected source references: %#v", refs)
	}
	events, err := buildWorkerEvents(rand.New(rand.NewSource(7)), refs, 0, 2, 1, 7)
	if err != nil {
		t.Fatal(err)
	}
	for i, event := range events {
		if event.Action != "upsert" {
			t.Fatalf("generated destructive event: %#v", event)
		}
		_, err = pool.Exec(ctx, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order,source,payload)
			VALUES ($1,$2,$3,$4,$5,'stability-review',$6::jsonb)`, fmt.Sprintf("event-%d", i), event.EntityType, event.EntityID, event.Action, 41+i, event.Payload)
		if err != nil {
			t.Fatal(err)
		}
	}
	expectations, err := readWorkerExpectations(ctx, pool, "stability-review", refs)
	if err != nil {
		t.Fatal(err)
	}
	if err := indexer.NewWorker(service, indexer.WorkerOptions{BatchSize: 10}).ProcessOnce(ctx); err != nil {
		t.Fatal(err)
	}
	if err := verifyGeneratedEventsProcessed(ctx, pool, "stability-review"); err != nil {
		t.Fatal(err)
	}
	if err := verifyWorkerResults(ctx, pool, expectations); err != nil {
		t.Fatal(err)
	}
}

func stabilityIntegrationService(t *testing.T) (*indexer.Service, *pgxpool.Pool) {
	t.Helper()
	url := os.Getenv("NEXUSINDEX_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("NEXUSINDEX_TEST_DATABASE_URL is not configured")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	database := fmt.Sprintf("nexusindex_stability_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{database}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+identifier); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = database
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP DATABASE "+identifier)
		admin.Close()
	})
	if err := migrations.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `
		CREATE TABLE search_index_events (
			id text PRIMARY KEY, entity_type text NOT NULL, entity_id text NOT NULL, action text NOT NULL,
			event_order bigint NOT NULL, source text, payload jsonb,
			created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
			processed_at timestamptz, failed_at timestamptz, last_error text, locked_at timestamptz, attempts int NOT NULL DEFAULT 0
		);
		CREATE TABLE "CustomPage" (
			id text PRIMARY KEY, title text NOT NULL, published boolean NOT NULL, category text,
			description text, content text, "createdAt" timestamptz NOT NULL, "updatedAt" timestamptz NOT NULL,
			slug text, icon text, "sortOrder" integer NOT NULL DEFAULT 0
		)`)
	if err != nil {
		t.Fatal(err)
	}
	return indexer.NewService(pool, pool), pool
}
