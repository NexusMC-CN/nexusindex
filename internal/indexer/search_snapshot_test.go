package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Trace real driver boundaries: QueryEnd for items runs after Next/Scan closes
// rows. This permits deterministic interleavings and transfer delays without
// replacing Search, its SQL, or PostgreSQL's snapshot/transaction behavior.
type searchTrace struct {
	start      func(context.Context, string) context.Context
	end        func(string)
	statements []string
}
type searchTraceSQLKey struct{}

func (tr *searchTrace) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tr.statements = append(tr.statements, data.SQL)
	if tr.start != nil {
		ctx = tr.start(ctx, data.SQL)
	}
	return context.WithValue(ctx, searchTraceSQLKey{}, data.SQL)
}
func (tr *searchTrace) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if tr.end != nil {
		tr.end(ctx.Value(searchTraceSQLKey{}).(string))
	}
}

func tracedSearchService(t *testing.T, source *Service, tr *searchTrace) *Service {
	t.Helper()
	cfg := source.indexDB.Config()
	cfg.MaxConns = 1
	cfg.ConnConfig.Tracer = tr
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	return NewServiceWithRuntimeAndCursor(pool, pool, source.RuntimeConfig(), "search-test")
}

func searchStage(sql string) string {
	switch {
	case strings.HasPrefix(strings.ToLower(strings.TrimSpace(sql)), "commit"):
		return "commit"
	case strings.HasPrefix(sql, "begin"):
		return "begin"
	case strings.HasPrefix(sql, "SELECT id, status"):
		return "generation"
	case strings.HasPrefix(sql, "SELECT count(*) FROM search_index WHERE"):
		return "count"
	case strings.HasPrefix(sql, "WITH ranked AS"):
		return "items"
	case strings.HasPrefix(sql, "SELECT entity_type, count(*)"):
		return "entity"
	case strings.HasPrefix(sql, "SELECT coalesce(category_id"):
		return "category"
	case strings.HasPrefix(sql, "SELECT tag, count(*)"):
		return "tag"
	}
	return sql
}

func TestSearchStageNormalizesCommitTraceSQL(t *testing.T) {
	for _, sql := range []string{"commit", " COMMIT ", "\nCommit\t"} {
		if got := searchStage(sql); got != "commit" {
			t.Fatalf("searchStage(%q)=%q, want commit", sql, got)
		}
	}
}

// Independent count/items/facet statements see a worker's intervening delete
// and disagree; a repeatable-read transaction keeps all three at the old view.
func TestSearchSnapshotIntegrationConcurrentWorker(t *testing.T) {
	for _, boundary := range []string{"count", "items"} {
		t.Run(boundary, func(t *testing.T) {
			writer := commitService(t)
			for _, id := range []string{"a", "b"} {
				putSearchTestDocument(t, writer, SearchDocument{EntityID: id, Tags: []string{id}, CategoryID: id})
			}
			execCommitSQL(t, writer, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('e','document','b','delete',1)`)
			worker := NewWorker(writer, WorkerOptions{OperationTimeout: 2 * time.Second})
			tr := &searchTrace{}
			reader := tracedSearchService(t, writer, tr)
			var workerErr error
			committed := false
			tr.end = func(sql string) {
				if committed || searchStage(sql) != boundary {
					return
				}
				committed = true
				done := make(chan error, 1)
				go func() { done <- worker.ProcessOnce(context.Background()) }()
				select {
				case workerErr = <-done:
				case <-time.After(3 * time.Second):
					workerErr = fmt.Errorf("worker blocked by search transaction")
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			page, err := reader.Search(ctx, SearchQuery{})
			if err != nil {
				t.Fatal(err)
			}
			if !committed || workerErr != nil {
				t.Fatalf("worker did not commit: committed=%v err=%v", committed, workerErr)
			}
			if page.Total != 2 || len(page.Items) != 2 || len(page.Facets.Entity) != 1 || page.Facets.Entity[0].Count != 2 || len(page.Facets.Category) != 2 || len(page.Facets.Tag) != 2 {
				t.Fatalf("response mixed snapshots after %s: %#v", boundary, page)
			}
			if len(tr.statements) == 0 || tr.statements[0] != "begin isolation level repeatable read read only" || tr.statements[len(tr.statements)-1] != "commit" {
				t.Fatalf("missing read-only repeatable-read transaction: %v", tr.statements)
			}
			page, err = reader.Search(ctx, SearchQuery{})
			if err != nil || page.Total != 1 || len(page.Items) != 1 || len(page.Facets.Tag) != 1 {
				t.Fatalf("next search must see committed deletion: %#v err=%v", page, err)
			}
		})
	}
}

// Omitting count/facets or charging driver row consumption to scoring loses the
// injected delay in PGMS. Every stage is checked independently to avoid one
// correctly measured stage hiding another missing one.
func TestSearchTimingIntegrationDatabaseStages(t *testing.T) {
	source := searchIntegrationService(t)
	putSearchTestDocument(t, source, SearchDocument{Title: "fabric", Tags: []string{"fabric"}, CategoryID: "mods"})
	for _, stage := range []string{"begin", "generation", "count", "query", "scan", "entity", "category", "tag", "commit"} {
		t.Run(stage, func(t *testing.T) {
			tr := &searchTrace{}
			injected := false
			tr.start = func(ctx context.Context, sql string) context.Context {
				if stage == "query" && searchStage(sql) == "items" {
					time.Sleep(70 * time.Millisecond)
					injected = true
				}
				return ctx
			}
			tr.end = func(sql string) {
				got := searchStage(sql)
				if got == stage || (stage == "scan" && got == "items") {
					time.Sleep(70 * time.Millisecond)
					injected = true
				}
			}
			service := tracedSearchService(t, source, tr)
			page, err := service.Search(context.Background(), SearchQuery{Query: "fabric"})
			if err != nil {
				t.Fatal(err)
			}
			if !injected {
				t.Fatal("fixture did not delay a database stage")
			}
			if page.Timing.PGMS < 65 {
				t.Errorf("%s omitted from pg_ms: %#v", stage, page.Timing)
			}
			if page.Timing.ScoringMS >= 65 || page.Timing.HighlightMS >= 65 {
				t.Errorf("%s database delay charged to Go work: %#v", stage, page.Timing)
			}
		})
	}
}

// A large but unrelated dictionary makes clause preparation deliberately slow
// without changing SQL recall or adding artificial hooks to production code.
// The tracer measures the actual Go-only gap between generation and count.
func TestSearchTimingIntegrationExcludesQueryPreparation(t *testing.T) {
	source := searchIntegrationService(t)
	putSearchTestDocument(t, source, SearchDocument{Title: "fabric"})
	tr := &searchTrace{}
	service := tracedSearchService(t, source, tr)
	snapshot := service.RuntimeSnapshot()
	snapshot.Config.Query.Synonyms = make(map[string][]string, 8192)
	for i := 0; i < 8192; i++ {
		snapshot.Config.Query.Synonyms[fmt.Sprintf("unrelated%06d%s", i, strings.Repeat("x", 40))] = []string{"unused"}
	}
	req := SearchQuery{Query: "fabric"}
	for i := 0; i < 32; i++ {
		req.Must = append(req.Must, QueryClause{Field: "title", Value: "fabric"})
	}
	var preparationStarted time.Time
	var preparationDuration time.Duration
	tr.end = func(sql string) {
		if searchStage(sql) == "generation" {
			preparationStarted = time.Now()
		}
	}
	tr.start = func(ctx context.Context, sql string) context.Context {
		if searchStage(sql) == "count" {
			preparationDuration = time.Since(preparationStarted)
		}
		return ctx
	}
	page, err := service.SearchWithSnapshot(context.Background(), req, snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 1 || len(page.Items) != 1 {
		t.Fatalf("preparation fixture changed recall: %#v", page)
	}
	if preparationDuration < 100*time.Millisecond {
		t.Fatalf("fixture did not produce a slow preparation phase: %s", preparationDuration)
	}
	if page.Timing.PGMS >= preparationDuration.Milliseconds()/2 {
		t.Fatalf("Go query preparation charged to pg_ms: preparation=%s timing=%#v", preparationDuration, page.Timing)
	}
	t.Logf("Go preparation=%s; timing=%#v", preparationDuration, page.Timing)
}

func TestSearchLagIntegrationAvailability(t *testing.T) {
	for _, available := range []bool{false, true} {
		t.Run(fmt.Sprint(available), func(t *testing.T) {
			source := searchIntegrationService(t)
			if available {
				execCommitSQL(t, source, `CREATE TABLE search_index_events(created_at timestamptz, processed_at timestamptz)`)
			}
			tr := &searchTrace{}
			service := tracedSearchService(t, source, tr)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			page, err := service.Search(ctx, SearchQuery{Explain: true})
			if err != nil {
				t.Fatalf("optional lag observation failed search: %v", err)
			}
			raw, err := json.Marshal(page.Timing)
			if err != nil {
				t.Fatal(err)
			}
			var timing map[string]any
			if err := json.Unmarshal(raw, &timing); err != nil {
				t.Fatal(err)
			}
			value, exists := timing["index_lag_ms"]
			if available && (!exists || value != float64(0)) {
				t.Errorf("empty backlog must be explicit zero: %s", raw)
			}
			if !available && exists && value != nil {
				t.Errorf("unavailable lag masquerades as a number: %s", raw)
			}
			for i, sql := range tr.statements {
				if strings.Contains(sql, "min(created_at)") && (i == 0 || tr.statements[i-1] != "commit") {
					t.Errorf("lag should run after releasing search transaction: %v", tr.statements)
				}
			}
			tr.statements = nil
			if _, err := service.Search(ctx, SearchQuery{}); err != nil {
				t.Fatal(err)
			}
			for _, sql := range tr.statements {
				if strings.Contains(sql, "min(created_at)") {
					t.Fatal("non-explain search attempted optional lag observation")
				}
			}
		})
	}
}

// A cancelled query must end the transaction and return the original deadline;
// reusing the one-connection pool then proves cleanup did not strand its lease.
func TestSearchSnapshotIntegrationRollbackOnDeadline(t *testing.T) {
	source := searchIntegrationService(t)
	for _, stage := range []string{"generation", "count", "items", "entity", "category", "tag", "commit"} {
		t.Run(stage, func(t *testing.T) {
			tr := &searchTrace{}
			tr.start = func(ctx context.Context, sql string) context.Context {
				if searchStage(sql) == stage {
					ctx, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
					cancel()
					return ctx
				}
				return ctx
			}
			service := tracedSearchService(t, source, tr)
			_, err := service.Search(context.Background(), SearchQuery{})
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("deadline was not propagated at %s: %v", stage, err)
			}
			if !containsString(tr.statements, "begin isolation level repeatable read read only") {
				t.Errorf("no transaction for %s error", stage)
			}
			if stage != "commit" && !containsString(tr.statements, "rollback") {
				t.Errorf("no rollback for %s error: %v", stage, tr.statements)
			}
			tr.start = nil
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if _, err := service.Search(ctx, SearchQuery{}); err != nil {
				t.Fatalf("pool lease stranded after failure: %v", err)
			}
		})
	}
}

func TestSearchSnapshotIntegrationRollbackOnScanError(t *testing.T) {
	source := searchIntegrationService(t)
	putSearchTestDocument(t, source, SearchDocument{EntityID: "infinite"})
	execCommitSQL(t, source, `UPDATE search_index SET created_at='infinity' WHERE entity_id='infinite'`)
	tr := &searchTrace{}
	service := tracedSearchService(t, source, tr)
	if _, err := service.Search(context.Background(), SearchQuery{}); err == nil {
		t.Fatal("fixture did not trigger timestamp scan error")
	}
	if !containsString(tr.statements, "rollback") || containsString(tr.statements, "commit") {
		t.Errorf("scan failure must rollback: %v", tr.statements)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := service.indexDB.Ping(ctx); err != nil {
		t.Fatalf("scan failure stranded pool lease: %v", err)
	}
}

func TestSearchSnapshotIntegrationRequestCancellation(t *testing.T) {
	source := searchIntegrationService(t)
	tr := &searchTrace{}
	service := tracedSearchService(t, source, tr)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr.end = func(sql string) {
		if searchStage(sql) == "count" {
			cancel()
		}
	}
	if _, err := service.Search(ctx, SearchQuery{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("request cancellation lost: %v", err)
	}
	if !containsString(tr.statements, "rollback") {
		t.Errorf("cancelled request skipped rollback: %v", tr.statements)
	}
	tr.end = nil
	checkCtx, checkCancel := context.WithTimeout(context.Background(), time.Second)
	defer checkCancel()
	if _, err := service.Search(checkCtx, SearchQuery{}); err != nil {
		t.Fatalf("cancelled request stranded lease: %v", err)
	}
}

func TestSearchLagIntegrationErrorsAndBacklog(t *testing.T) {
	service := searchIntegrationService(t)
	if _, err := service.IndexLagMS(context.Background()); err == nil {
		t.Fatal("missing source table error was swallowed")
	}
	execCommitSQL(t, service, `CREATE TABLE search_index_events(created_at timestamptz, processed_at timestamptz)`)
	if value, err := service.IndexLagMS(context.Background()); err != nil || value != 0 {
		t.Fatalf("empty backlog: value=%d err=%v", value, err)
	}
	execCommitSQL(t, service, `INSERT INTO search_index_events VALUES(now()-interval '1 hour',now()),(now()-interval '10 seconds',NULL)`)
	if value, err := service.IndexLagMS(context.Background()); err != nil || value < 10000 || value > 30000 {
		t.Fatalf("oldest unprocessed event: value=%d err=%v", value, err)
	}
	execCommitSQL(t, service, `UPDATE search_index_events SET created_at=now()+interval '1 hour' WHERE processed_at IS NULL`)
	if value, err := service.IndexLagMS(context.Background()); err != nil || value != 0 {
		t.Fatalf("future event clock skew: value=%d err=%v", value, err)
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := service.IndexLagMS(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("lag deadline was swallowed: %v", err)
	}

	tr := &searchTrace{start: func(ctx context.Context, sql string) context.Context {
		if strings.Contains(sql, "min(created_at)") {
			failed, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
			cancel()
			return failed
		}
		return ctx
	}}
	reader := tracedSearchService(t, service, tr)
	page, err := reader.Search(context.Background(), SearchQuery{Explain: true})
	if err != nil || page.Timing.IndexLagMS != nil {
		t.Fatalf("telemetry deadline must leave completed search available: %#v err=%v", page.Timing, err)
	}
}
