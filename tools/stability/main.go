package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blockbridge/avmcbbs/apps/nexusindex/internal/indexer"
	"github.com/jackc/pgx/v5/pgxpool"
)

type workerOptions struct {
	DatabaseURL      string
	IndexDatabaseURL string
	Events           int
	DistinctDocs     int
	BatchSize        int
	DuplicateRatio   float64
	WatchSeconds     int
	PollMS           int
	Seed             int64
	RequireDrained   bool
}

type queryOptions struct {
	BaseURL      string
	Token        string
	PerClass     int
	Concurrency  int
	TimeoutMS    int
	Limit        int
	Seed         int64
	CheckCursor  bool
	OutputPretty bool
}

type eventSummary struct {
	Mode               string         `json:"mode"`
	StartedAt          string         `json:"started_at"`
	FinishedAt         string         `json:"finished_at"`
	Inserted           int            `json:"inserted"`
	DistinctDocs       int            `json:"distinct_docs"`
	ActionCounts       map[string]int `json:"action_counts"`
	EntityCounts       map[string]int `json:"entity_counts"`
	Before             eventStats     `json:"before"`
	After              eventStats     `json:"after"`
	Samples            []eventStats   `json:"samples"`
	BacklogGrowing     bool           `json:"backlog_growing"`
	PossibleLostEvents bool           `json:"possible_lost_events"`
	Errors             []string       `json:"errors,omitempty"`
}

type eventStats struct {
	At        string `json:"at"`
	Total     int64  `json:"total"`
	Processed int64  `json:"processed"`
	Failed    int64  `json:"failed"`
	Backlog   int64  `json:"backlog"`
	Locked    int64  `json:"locked"`
}

type workerEvent struct {
	EntityType string
	EntityID   string
	Action     string
	Payload    string
}

type workerExpectation struct {
	Reference     indexer.SourceEntityReference
	SourceVersion int64
}

type querySummary struct {
	Mode         string                       `json:"mode"`
	StartedAt    string                       `json:"started_at"`
	FinishedAt   string                       `json:"finished_at"`
	BaseURL      string                       `json:"base_url"`
	PerClass     int                          `json:"per_class"`
	Concurrency  int                          `json:"concurrency"`
	Overall      queryClassSummary            `json:"overall"`
	Classes      map[string]queryClassSummary `json:"classes"`
	Stability    []stabilityCheck             `json:"stability"`
	CursorChecks []cursorCheck                `json:"cursor_checks,omitempty"`
	Errors       []string                     `json:"errors,omitempty"`
}

type queryClassSummary struct {
	Requests         int     `json:"requests"`
	Failures         int     `json:"failures"`
	CacheHitRatio    float64 `json:"cache_hit_ratio"`
	LatencyP50MS     int64   `json:"latency_p50_ms"`
	LatencyP95MS     int64   `json:"latency_p95_ms"`
	LatencyP99MS     int64   `json:"latency_p99_ms"`
	PGP50MS          int64   `json:"pg_p50_ms"`
	ScoringP50MS     int64   `json:"scoring_p50_ms"`
	HighlightP50MS   int64   `json:"highlight_p50_ms"`
	CandidateSizeP50 int64   `json:"candidate_size_p50"`
	CandidateSizeP95 int64   `json:"candidate_size_p95"`
}

type querySample struct {
	Class            string
	Query            string
	LatencyMS        int64
	CacheHit         bool
	PGMS             int64
	ScoringMS        int64
	HighlightMS      int64
	CandidateSize    int64
	Failed           bool
	Error            string
	TopIDs           []string
	NextCursor       string
	SecondPageTopIDs []string
}

type stabilityCheck struct {
	Class             string   `json:"class"`
	Query             string   `json:"query"`
	Stable            bool     `json:"stable"`
	ExpectedTop10     []string `json:"expected_top10"`
	MismatchAt        int      `json:"mismatch_at,omitempty"`
	ActualTop10       []string `json:"actual_top10,omitempty"`
	Error             string   `json:"error,omitempty"`
	ExpectedStatus    int      `json:"expected_status,omitempty"`
	ExpectedErrorCode string   `json:"expected_error_code,omitempty"`
}

type cursorCheck struct {
	Class       string   `json:"class"`
	Query       string   `json:"query"`
	Stable      bool     `json:"stable"`
	FirstPage   []string `json:"first_page"`
	SecondPage  []string `json:"second_page"`
	Overlap     []string `json:"overlap,omitempty"`
	MismatchAt  int      `json:"mismatch_at,omitempty"`
	ActualPage2 []string `json:"actual_page2,omitempty"`
	Error       string   `json:"error,omitempty"`
}

type searchResponse struct {
	Items []struct {
		ID string `json:"id"`
	} `json:"items"`
	NextCursor string `json:"next_cursor"`
	Timing     struct {
		PGMS          int64 `json:"pg_ms"`
		ScoringMS     int64 `json:"scoring_ms"`
		HighlightMS   int64 `json:"highlight_ms"`
		CandidateSize int64 `json:"candidate_size"`
	} `json:"timing"`
	Debug struct {
		Timing struct {
			PGMS          int64 `json:"pg_ms"`
			ScoringMS     int64 `json:"scoring_ms"`
			HighlightMS   int64 `json:"highlight_ms"`
			CandidateSize int64 `json:"candidate_size"`
		} `json:"timing"`
	} `json:"debug"`
}

type queryCase struct {
	Query string
	// Zero expects a successful 2xx response with an items array.
	ExpectedStatus    int
	ExpectedErrorCode string
}

var queryClasses = map[string][]queryCase{
	"exact_title":   {{Query: "fabric 1.20.1"}, {Query: "optifine 光影"}, {Query: "sodium fabric"}, {Query: "paper 1.21"}},
	"tag":           {{Query: "mod"}, {Query: "plugin"}, {Query: "resourcepack"}, {Query: "shader"}},
	"chinese":       {{Query: "模组"}, {Query: "插件"}, {Query: "资源包"}, {Query: "光影"}},
	"version":       {{Query: "1.20.1"}, {Query: "1.21"}, {Query: "1.19.4"}, {Query: "1.18.2"}},
	"loader_domain": {{Query: "fabric mod"}, {Query: "paper plugin"}, {Query: "forge mod"}, {Query: "quilt mod"}},
	"garbage":       {{Query: "aaaa"}, {Query: "???", ExpectedStatus: http.StatusBadRequest, ExpectedErrorCode: "invalid_query_text"}, {Query: "hello"}, {Query: "zzzz unknown"}},
}

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	ctx := context.Background()
	switch os.Args[1] {
	case "worker":
		if err := runWorker(ctx, parseWorkerOptions(os.Args[2:])); err != nil {
			fmt.Fprintf(os.Stderr, "worker stability failed: %v\n", err)
			os.Exit(1)
		}
	case "query":
		if err := runQuery(ctx, parseQueryOptions(os.Args[2:])); err != nil {
			fmt.Fprintf(os.Stderr, "query stability failed: %v\n", err)
			os.Exit(1)
		}
	default:
		usage()
		os.Exit(2)
	}
}

func parseWorkerOptions(args []string) workerOptions {
	fs := flag.NewFlagSet("worker", flag.ExitOnError)
	opt := workerOptions{}
	fs.StringVar(&opt.DatabaseURL, "database-url", env("MAIN_DATABASE_READONLY_URL", ""), "forum PostgreSQL URL containing search_index_events")
	fs.StringVar(&opt.IndexDatabaseURL, "index-database-url", env("INDEX_DATABASE_URL", ""), "NexusIndex PostgreSQL URL used to verify indexed documents")
	fs.IntVar(&opt.Events, "events", 10000, "events to insert: 10000, 50000, 100000")
	fs.IntVar(&opt.DistinctDocs, "distinct-docs", 5000, "number of distinct documents to spread events across")
	fs.IntVar(&opt.BatchSize, "batch-size", 1000, "insert batch size")
	fs.Float64Var(&opt.DuplicateRatio, "duplicate-ratio", 0.35, "probability of repeating a previous doc_id")
	fs.IntVar(&opt.WatchSeconds, "watch-seconds", 60, "seconds to watch worker processing after insert")
	fs.IntVar(&opt.PollMS, "poll-ms", 1000, "worker stats poll interval ms")
	fs.Int64Var(&opt.Seed, "seed", time.Now().UnixNano(), "random seed")
	fs.BoolVar(&opt.RequireDrained, "require-drained", false, "exit non-zero if backlog remains after watch")
	_ = fs.Parse(args)
	return opt
}

func parseQueryOptions(args []string) queryOptions {
	fs := flag.NewFlagSet("query", flag.ExitOnError)
	opt := queryOptions{}
	fs.StringVar(&opt.BaseURL, "base-url", env("NEXUSINDEX_URL", "http://127.0.0.1:4412"), "NexusIndex base URL")
	fs.StringVar(&opt.Token, "token", env("NEXUSINDEX_TOKEN", ""), "NexusIndex API token")
	fs.IntVar(&opt.PerClass, "per-class", 200, "requests per query class")
	fs.IntVar(&opt.Concurrency, "concurrency", 8, "concurrent search workers")
	fs.IntVar(&opt.TimeoutMS, "timeout-ms", 5000, "request timeout ms")
	fs.IntVar(&opt.Limit, "limit", 10, "search result limit")
	fs.Int64Var(&opt.Seed, "seed", time.Now().UnixNano(), "random seed")
	fs.BoolVar(&opt.CheckCursor, "check-cursor", true, "check cursor pagination stability")
	fs.BoolVar(&opt.OutputPretty, "pretty", true, "pretty print JSON")
	_ = fs.Parse(args)
	return opt
}

func runWorker(ctx context.Context, opt workerOptions) error {
	if opt.DatabaseURL == "" {
		return fmt.Errorf("--database-url or MAIN_DATABASE_READONLY_URL is required")
	}
	if opt.IndexDatabaseURL == "" {
		return fmt.Errorf("--index-database-url or INDEX_DATABASE_URL is required")
	}
	if opt.Events < 1 {
		return errors.New("--events must be positive")
	}
	if opt.DistinctDocs < 1 {
		return errors.New("--distinct-docs must be positive")
	}
	if opt.DistinctDocs > indexer.MaxSourceEntityReferences {
		return fmt.Errorf("--distinct-docs must not exceed %d", indexer.MaxSourceEntityReferences)
	}
	if opt.BatchSize < 1 {
		return errors.New("--batch-size must be positive")
	}
	rng := rand.New(rand.NewSource(opt.Seed))
	mainPool, err := pgxpool.New(ctx, opt.DatabaseURL)
	if err != nil {
		return err
	}
	defer mainPool.Close()
	indexPool, err := pgxpool.New(ctx, opt.IndexDatabaseURL)
	if err != nil {
		return err
	}
	defer indexPool.Close()

	started := time.Now()
	runSource := fmt.Sprintf("stability-generator:%d:%d", opt.Seed, started.UnixNano())
	summary := eventSummary{
		Mode:         "worker",
		StartedAt:    started.Format(time.RFC3339Nano),
		ActionCounts: map[string]int{},
		EntityCounts: map[string]int{},
	}
	before, err := readEventStats(ctx, mainPool)
	if err != nil {
		return err
	}
	summary.Before = before

	service := indexer.NewService(mainPool, nil)
	refs, err := loadWorkerSourceReferences(ctx, service, opt.DistinctDocs)
	if err != nil {
		return err
	}

	inserted := 0
	for inserted < opt.Events {
		n := opt.BatchSize
		if remaining := opt.Events - inserted; remaining < n {
			n = remaining
		}
		events, err := buildWorkerEvents(rng, refs, inserted, n, opt.DuplicateRatio, opt.Seed)
		if err != nil {
			return err
		}
		rows := make([][]any, 0, len(events))
		for _, event := range events {
			rows = append(rows, []any{event.EntityType, event.EntityID, event.Action, runSource, event.Payload})
			summary.ActionCounts[event.Action]++
			summary.EntityCounts[event.EntityType]++
		}
		copied, err := mainPool.CopyFrom(ctx, []string{"search_index_events"}, []string{"entity_type", "entity_id", "action", "source", "payload"}, newCopyRows(rows))
		if err != nil {
			return err
		}
		inserted += int(copied)
		fmt.Printf("[worker] inserted=%d/%d\n", inserted, opt.Events)
	}
	summary.Inserted = inserted
	summary.DistinctDocs = len(refs)
	expectations, err := readWorkerExpectations(ctx, mainPool, runSource, refs)
	if err != nil {
		return err
	}

	deadline := time.Now().Add(time.Duration(opt.WatchSeconds) * time.Second)
	var previous eventStats
	for time.Now().Before(deadline) {
		stats, err := readEventStats(ctx, mainPool)
		if err != nil {
			summary.Errors = append(summary.Errors, err.Error())
			break
		}
		summary.Samples = append(summary.Samples, stats)
		fmt.Printf("[worker] processed=%d failed=%d backlog=%d locked=%d\n", stats.Processed, stats.Failed, stats.Backlog, stats.Locked)
		if previous.At != "" && stats.Backlog > previous.Backlog {
			summary.BacklogGrowing = true
		}
		previous = stats
		if stats.Backlog == 0 {
			break
		}
		time.Sleep(time.Duration(opt.PollMS) * time.Millisecond)
	}
	after, err := readEventStats(ctx, mainPool)
	if err != nil {
		return err
	}
	summary.After = after
	summary.FinishedAt = time.Now().Format(time.RFC3339Nano)
	expectedTotal := before.Total + int64(inserted)
	summary.PossibleLostEvents = after.Total < expectedTotal
	verificationErr := errors.Join(
		verifyGeneratedEventsProcessed(ctx, mainPool, runSource),
		verifyWorkerResults(ctx, indexPool, expectations),
	)
	if verificationErr != nil {
		summary.Errors = append(summary.Errors, verificationErr.Error())
	}
	writeReport(summary, true)
	if opt.RequireDrained && after.Backlog > 0 {
		return fmt.Errorf("backlog not drained: %d", after.Backlog)
	}
	if summary.PossibleLostEvents {
		return fmt.Errorf("possible lost events: expected total >= %d got %d", expectedTotal, after.Total)
	}
	if verificationErr != nil {
		return verificationErr
	}
	return nil
}

func loadWorkerSourceReferences(ctx context.Context, service *indexer.Service, limit int) ([]indexer.SourceEntityReference, error) {
	refs := make([]indexer.SourceEntityReference, 0, limit)
	for _, entityType := range indexer.EntityTypes {
		remaining := limit - len(refs)
		if remaining == 0 {
			break
		}
		batch, err := service.SourceEntityReferences(ctx, entityType, remaining)
		if err != nil {
			return nil, fmt.Errorf("read %s source fixtures: %w", entityType, err)
		}
		refs = append(refs, batch...)
	}
	if len(refs) == 0 {
		return nil, errors.New("no source entities are available for worker stability events")
	}
	return refs, nil
}

func buildWorkerEvents(rng *rand.Rand, refs []indexer.SourceEntityReference, start, count int, duplicateRatio float64, seed int64) ([]workerEvent, error) {
	if len(refs) == 0 {
		return nil, errors.New("worker events require source entity references")
	}
	events := make([]workerEvent, 0, count)
	for i := 0; i < count; i++ {
		refIndex := start + i
		if rng.Float64() < duplicateRatio {
			refIndex = rng.Intn(len(refs))
		}
		ref := refs[refIndex%len(refs)]
		payloadJSON, err := json.Marshal(map[string]any{
			"fixture": true,
			"seq":     start + i,
			"seed":    seed,
		})
		if err != nil {
			return nil, err
		}
		events = append(events, workerEvent{
			EntityType: ref.EntityType,
			EntityID:   ref.EntityID,
			Action:     "upsert",
			Payload:    string(payloadJSON),
		})
	}
	return events, nil
}

func readWorkerExpectations(ctx context.Context, mainDB *pgxpool.Pool, source string, refs []indexer.SourceEntityReference) ([]workerExpectation, error) {
	references := make(map[string]indexer.SourceEntityReference, len(refs))
	for _, ref := range refs {
		references[workerEntityKey(ref.EntityType, ref.EntityID)] = ref
	}
	rows, err := mainDB.Query(ctx, `SELECT entity_type,entity_id,max(event_order),bool_and(action='upsert')
		FROM search_index_events WHERE source=$1 GROUP BY entity_type,entity_id`, source)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	expectations := []workerExpectation{}
	for rows.Next() {
		var entityType, entityID string
		var sourceVersion int64
		var allUpserts bool
		if err := rows.Scan(&entityType, &entityID, &sourceVersion, &allUpserts); err != nil {
			return nil, err
		}
		if !allUpserts {
			return nil, fmt.Errorf("generated events include a destructive action for %s/%s", entityType, entityID)
		}
		ref, ok := references[workerEntityKey(entityType, entityID)]
		if !ok {
			return nil, fmt.Errorf("generated event targets an unsampled source entity %s/%s", entityType, entityID)
		}
		expectations = append(expectations, workerExpectation{Reference: ref, SourceVersion: sourceVersion})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(expectations) == 0 {
		return nil, errors.New("no generated worker events were found")
	}
	return expectations, nil
}

func verifyGeneratedEventsProcessed(ctx context.Context, mainDB *pgxpool.Pool, source string) error {
	var total, processed, failed int64
	if err := mainDB.QueryRow(ctx, `SELECT count(*),count(*) FILTER (WHERE processed_at IS NOT NULL),count(*) FILTER (WHERE failed_at IS NOT NULL)
		FROM search_index_events WHERE source=$1`, source).Scan(&total, &processed, &failed); err != nil {
		return err
	}
	if total == 0 || processed != total || failed != 0 {
		return fmt.Errorf("generated worker events not fully processed: total=%d processed=%d failed=%d", total, processed, failed)
	}
	return nil
}

func verifyWorkerResults(ctx context.Context, indexDB *pgxpool.Pool, expectations []workerExpectation) error {
	for _, expected := range expectations {
		var title, status string
		var updatedAt time.Time
		var sourceVersion int64
		err := indexDB.QueryRow(ctx, `SELECT s.title,s.status,s.updated_at,s.source_version
			FROM search_index s JOIN index_generations g ON g.id=s.generation_id
			WHERE g.status='active' AND s.entity_type=$1 AND s.entity_id=$2`,
			expected.Reference.EntityType, expected.Reference.EntityID,
		).Scan(&title, &status, &updatedAt, &sourceVersion)
		if err != nil {
			return fmt.Errorf("verify indexed source %s/%s: %w", expected.Reference.EntityType, expected.Reference.EntityID, err)
		}
		if status == "deleted" || title != expected.Reference.Title || !updatedAt.Equal(expected.Reference.UpdatedAt) {
			return fmt.Errorf("indexed source content mismatch for %s/%s", expected.Reference.EntityType, expected.Reference.EntityID)
		}
		if sourceVersion < expected.SourceVersion {
			return fmt.Errorf("indexed source version %d is older than generated event %d for %s/%s", sourceVersion, expected.SourceVersion, expected.Reference.EntityType, expected.Reference.EntityID)
		}
	}
	return nil
}

func workerEntityKey(entityType, entityID string) string {
	return entityType + "\x00" + entityID
}

func runQuery(ctx context.Context, opt queryOptions) error {
	if opt.PerClass < 1 {
		opt.PerClass = 1
	}
	if opt.Concurrency < 1 {
		opt.Concurrency = 1
	}
	client := &http.Client{Timeout: time.Duration(opt.TimeoutMS) * time.Millisecond}
	started := time.Now()
	jobs := make(chan struct {
		class string
		query queryCase
	}, opt.PerClass*len(queryClasses))
	results := make(chan querySample, cap(jobs))
	var wg sync.WaitGroup
	for i := 0; i < opt.Concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for job := range jobs {
				results <- executeQuery(ctx, client, opt, job.class, job.query, "")
			}
		}(i)
	}
	rng := rand.New(rand.NewSource(opt.Seed))
	totalJobs := 0
	for class, queries := range queryClasses {
		for i := 0; i < opt.PerClass; i++ {
			jobs <- struct {
				class string
				query queryCase
			}{class: class, query: queries[rng.Intn(len(queries))]}
			totalJobs++
		}
	}
	close(jobs)
	wg.Wait()
	close(results)

	samples := make([]querySample, 0, totalJobs)
	for sample := range results {
		samples = append(samples, sample)
	}
	summary := querySummary{
		Mode:        "query",
		StartedAt:   started.Format(time.RFC3339Nano),
		FinishedAt:  time.Now().Format(time.RFC3339Nano),
		BaseURL:     opt.BaseURL,
		PerClass:    opt.PerClass,
		Concurrency: opt.Concurrency,
		Classes:     map[string]queryClassSummary{},
	}
	summary.Overall = summarizeSamples(samples)
	for class := range queryClasses {
		classSamples := []querySample{}
		for _, sample := range samples {
			if sample.Class == class {
				classSamples = append(classSamples, sample)
			}
		}
		summary.Classes[class] = summarizeSamples(classSamples)
	}
	for class, queries := range queryClasses {
		for _, q := range queries[:min(2, len(queries))] {
			check, cursor := checkQueryStability(ctx, client, opt, class, q)
			summary.Stability = append(summary.Stability, check)
			if opt.CheckCursor && q.ExpectedErrorCode == "" {
				summary.CursorChecks = append(summary.CursorChecks, cursor)
			}
		}
	}
	if summary.Overall.Failures > 0 {
		summary.Errors = append(summary.Errors, fmt.Sprintf("%d query samples failed", summary.Overall.Failures))
	}
	for _, check := range summary.Stability {
		if !check.Stable {
			summary.Errors = append(summary.Errors, fmt.Sprintf("unstable top results for %s/%s", check.Class, check.Query))
		}
	}
	for _, check := range summary.CursorChecks {
		if !check.Stable {
			summary.Errors = append(summary.Errors, fmt.Sprintf("unstable cursor results for %s/%s", check.Class, check.Query))
		}
	}
	writeReport(summary, opt.OutputPretty)
	if len(summary.Errors) > 0 {
		return fmt.Errorf("query validation failed: %s", strings.Join(summary.Errors, "; "))
	}
	return nil
}

func executeQuery(ctx context.Context, client *http.Client, opt queryOptions, class string, queryCase queryCase, cursor string) querySample {
	started := time.Now()
	query := queryCase.Query
	body := map[string]any{"q": query, "limit": opt.Limit, "explain": true}
	if cursor != "" {
		body["cursor"] = cursor
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(opt.BaseURL, "/")+"/api/search", bytes.NewReader(raw))
	if err != nil {
		return querySample{Class: class, Query: query, Failed: true, Error: err.Error()}
	}
	req.Header.Set("Content-Type", "application/json")
	if opt.Token != "" {
		req.Header.Set("X-NexusIndex-Token", opt.Token)
	}
	resp, err := client.Do(req)
	latency := time.Since(started).Milliseconds()
	if err != nil {
		return querySample{Class: class, Query: query, LatencyMS: latency, Failed: true, Error: err.Error()}
	}
	defer resp.Body.Close()
	if resp.Request != nil && resp.Request.Response != nil {
		return querySample{Class: class, Query: query, LatencyMS: latency, Failed: true, Error: "search request followed a redirect"}
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return querySample{Class: class, Query: query, LatencyMS: latency, Failed: true, Error: err.Error()}
	}
	if queryCase.ExpectedErrorCode != "" {
		var parsed struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		if err := json.Unmarshal(data, &parsed); err != nil {
			return querySample{Class: class, Query: query, LatencyMS: latency, Failed: true, Error: err.Error()}
		}
		if resp.StatusCode != queryCase.ExpectedStatus || parsed.Error.Code != queryCase.ExpectedErrorCode {
			return querySample{Class: class, Query: query, LatencyMS: latency, Failed: true,
				Error: fmt.Sprintf("expected HTTP %d error %q, got HTTP %d error %q", queryCase.ExpectedStatus, queryCase.ExpectedErrorCode, resp.StatusCode, parsed.Error.Code)}
		}
		return querySample{Class: class, Query: query, LatencyMS: latency}
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return querySample{Class: class, Query: query, LatencyMS: latency, Failed: true, Error: string(data)}
	}
	parsed, err := decodeSearchResponse(data)
	if err != nil {
		return querySample{Class: class, Query: query, LatencyMS: latency, Failed: true, Error: err.Error()}
	}
	timing := parsed.Timing
	if timing.CandidateSize == 0 && parsed.Debug.Timing.CandidateSize > 0 {
		timing = parsed.Debug.Timing
	}
	topIDs := make([]string, 0, min(10, len(parsed.Items)))
	for i := 0; i < len(parsed.Items) && i < 10; i++ {
		topIDs = append(topIDs, parsed.Items[i].ID)
	}
	cacheHit := strings.EqualFold(resp.Header.Get("X-NexusIndex-Cache-Hit"), "true") || strings.EqualFold(resp.Header.Get("X-NexusIndex-Cache"), "HIT")
	candidateSize := timing.CandidateSize
	if candidateSize == 0 {
		candidateSize = parseHeaderInt(resp.Header.Get("X-NexusIndex-Candidate-Size"))
	}
	return querySample{
		Class:         class,
		Query:         query,
		LatencyMS:     latency,
		CacheHit:      cacheHit,
		PGMS:          firstNonZero(timing.PGMS, parseHeaderInt(resp.Header.Get("X-NexusIndex-PG-Ms"))),
		ScoringMS:     firstNonZero(timing.ScoringMS, parseHeaderInt(resp.Header.Get("X-NexusIndex-Scoring-Ms"))),
		HighlightMS:   firstNonZero(timing.HighlightMS, parseHeaderInt(resp.Header.Get("X-NexusIndex-Highlight-Ms"))),
		CandidateSize: candidateSize,
		TopIDs:        topIDs,
		NextCursor:    parsed.NextCursor,
	}
}

func decodeSearchResponse(data []byte) (searchResponse, error) {
	var envelope map[string]json.RawMessage
	if err := json.Unmarshal(data, &envelope); err != nil {
		return searchResponse{}, err
	}
	if envelope == nil {
		return searchResponse{}, errors.New("search response must be an object")
	}
	items, ok := envelope["items"]
	if !ok || bytes.Equal(bytes.TrimSpace(items), []byte("null")) {
		return searchResponse{}, errors.New("search response items must be an array")
	}
	var itemList []json.RawMessage
	if err := json.Unmarshal(items, &itemList); err != nil {
		return searchResponse{}, errors.New("search response items must be an array")
	}
	var parsed searchResponse
	if err := json.Unmarshal(data, &parsed); err != nil {
		return searchResponse{}, err
	}
	return parsed, nil
}

func checkQueryStability(ctx context.Context, client *http.Client, opt queryOptions, class string, query queryCase) (stabilityCheck, cursorCheck) {
	var expected []string
	check := stabilityCheck{Class: class, Query: query.Query, Stable: true, ExpectedStatus: query.ExpectedStatus, ExpectedErrorCode: query.ExpectedErrorCode}
	cursor := cursorCheck{Class: class, Query: query.Query, Stable: true}
	var expectedSecond []string
	expectedHasCursor := false
	for i := 0; i < 10; i++ {
		first := executeQuery(ctx, client, opt, class, query, "")
		if first.Failed {
			check.Stable = false
			check.MismatchAt = i
			check.Error = first.Error
			cursor.Stable = false
			cursor.MismatchAt = i
			cursor.Error = first.Error
			break
		}
		if query.ExpectedErrorCode != "" {
			// Each sample must satisfy the same status/code contract. Error
			// responses have no result ordering or cursor pagination to test.
			continue
		}
		if i == 0 {
			expected = first.TopIDs
			check.ExpectedTop10 = expected
			cursor.FirstPage = expected
			expectedHasCursor = first.NextCursor != ""
			if opt.CheckCursor && expectedHasCursor {
				second := executeQuery(ctx, client, opt, class, query, first.NextCursor)
				if second.Failed {
					cursor.Stable = false
					cursor.Error = second.Error
					continue
				}
				expectedSecond = second.TopIDs
				cursor.SecondPage = expectedSecond
				cursor.Overlap = intersect(expected, expectedSecond)
				if len(cursor.Overlap) > 0 {
					cursor.Stable = false
				}
			}
			continue
		}
		if opt.CheckCursor {
			hasCursor := first.NextCursor != ""
			if hasCursor != expectedHasCursor {
				cursor.Stable = false
				cursor.MismatchAt = i
				cursor.Error = "first-page cursor presence changed"
			} else if expectedHasCursor {
				second := executeQuery(ctx, client, opt, class, query, first.NextCursor)
				if second.Failed {
					cursor.Stable = false
					cursor.MismatchAt = i
					cursor.Error = second.Error
				} else if !sameStrings(expectedSecond, second.TopIDs) {
					cursor.Stable = false
					cursor.MismatchAt = i
					cursor.ActualPage2 = second.TopIDs
				}
			}
		}
		if !sameStrings(expected, first.TopIDs) {
			check.Stable = false
			check.MismatchAt = i
			check.ActualTop10 = first.TopIDs
			break
		}
	}
	return check, cursor
}

func summarizeSamples(samples []querySample) queryClassSummary {
	out := queryClassSummary{Requests: len(samples)}
	latencies := []int64{}
	pg := []int64{}
	scoring := []int64{}
	highlight := []int64{}
	candidates := []int64{}
	cacheHits := 0
	for _, sample := range samples {
		if sample.Failed {
			out.Failures++
			continue
		}
		if sample.CacheHit {
			cacheHits++
		}
		latencies = append(latencies, sample.LatencyMS)
		pg = append(pg, sample.PGMS)
		scoring = append(scoring, sample.ScoringMS)
		highlight = append(highlight, sample.HighlightMS)
		candidates = append(candidates, sample.CandidateSize)
	}
	successes := out.Requests - out.Failures
	if successes > 0 {
		out.CacheHitRatio = float64(cacheHits) / float64(successes)
	}
	out.LatencyP50MS = percentile(latencies, 50)
	out.LatencyP95MS = percentile(latencies, 95)
	out.LatencyP99MS = percentile(latencies, 99)
	out.PGP50MS = percentile(pg, 50)
	out.ScoringP50MS = percentile(scoring, 50)
	out.HighlightP50MS = percentile(highlight, 50)
	out.CandidateSizeP50 = percentile(candidates, 50)
	out.CandidateSizeP95 = percentile(candidates, 95)
	return out
}

func readEventStats(ctx context.Context, pool *pgxpool.Pool) (eventStats, error) {
	stats := eventStats{At: time.Now().Format(time.RFC3339Nano)}
	err := pool.QueryRow(ctx, `SELECT
		count(*)::bigint,
		count(*) FILTER (WHERE processed_at IS NOT NULL)::bigint,
		count(*) FILTER (WHERE failed_at IS NOT NULL AND processed_at IS NULL)::bigint,
		count(*) FILTER (WHERE processed_at IS NULL)::bigint,
		count(*) FILTER (WHERE processed_at IS NULL AND locked_at IS NOT NULL)::bigint
		FROM search_index_events`).Scan(&stats.Total, &stats.Processed, &stats.Failed, &stats.Backlog, &stats.Locked)
	return stats, err
}

type copyRows struct {
	rows [][]any
	idx  int
}

func newCopyRows(rows [][]any) *copyRows {
	return &copyRows{rows: rows, idx: -1}
}

func (r *copyRows) Next() bool {
	r.idx++
	return r.idx < len(r.rows)
}

func (r *copyRows) Values() ([]any, error) {
	if r.idx < 0 || r.idx >= len(r.rows) {
		return nil, fmt.Errorf("copy row out of range")
	}
	return r.rows[r.idx], nil
}

func (r *copyRows) Err() error { return nil }

func percentile(values []int64, p int) int64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]int64(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := (len(sorted)*p+99)/100 - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func writeReport(value any, pretty bool) {
	var data []byte
	if pretty {
		data, _ = json.MarshalIndent(value, "", "  ")
	} else {
		data, _ = json.Marshal(value)
	}
	fmt.Println(string(data))
}

func usage() {
	fmt.Println(`Usage:
  go run ./tools/stability worker --events 10000 --database-url postgresql://...
  go run ./tools/stability query --base-url http://127.0.0.1:4412 --per-class 200

Worker flags:
  --events 10000|50000|100000
  --distinct-docs 5000
  --duplicate-ratio 0.35
  --watch-seconds 60
  --require-drained

Query flags:
  --per-class 100..1000
  --concurrency 8
  --limit 10
  --check-cursor=true`)
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func parseHeaderInt(value string) int64 {
	parsed, _ := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	return parsed
}

func firstNonZero(values ...int64) int64 {
	for _, value := range values {
		if value != 0 {
			return value
		}
	}
	return 0
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func intersect(a, b []string) []string {
	seen := map[string]struct{}{}
	for _, value := range a {
		seen[value] = struct{}{}
	}
	out := []string{}
	for _, value := range b {
		if _, ok := seen[value]; ok {
			out = append(out, value)
		}
	}
	return out
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
