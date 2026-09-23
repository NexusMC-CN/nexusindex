package indexer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"
)

func commitService(t *testing.T) *Service {
	t.Helper()
	s := searchIntegrationService(t)
	_, err := s.mainDB.Exec(context.Background(), `CREATE TABLE search_index_events (
 id text PRIMARY KEY, entity_type text NOT NULL, entity_id text NOT NULL, action text NOT NULL,
 event_order bigint NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now(),
 processed_at timestamptz, failed_at timestamptz, last_error text, locked_at timestamptz, attempts int NOT NULL DEFAULT 0)`)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func execCommitSQL(t *testing.T, s *Service, sql string, args ...any) {
	t.Helper()
	if _, err := s.indexDB.Exec(context.Background(), sql, args...); err != nil {
		t.Fatal(err)
	}
}

// Missing direct tag refresh leaves a visible tag after deletion.
func TestCommitDirectWritesRefreshTags(t *testing.T) {
	s := commitService(t)
	putSearchTestDocument(t, s, SearchDocument{EntityID: "doc", Tags: []string{"fixture"}})
	tags, total, err := s.Tags(context.Background(), "", "latest", 20, 0)
	if err != nil || total != 1 || len(tags) != 1 {
		t.Fatalf("upsert tags: %#v total=%d err=%v", tags, total, err)
	}
	if _, err := s.MarkDeleted(context.Background(), EntityDocument, "doc"); err != nil {
		t.Fatal(err)
	}
	_, total, err = s.Tags(context.Background(), "", "latest", 20, 0)
	if err != nil || total != 0 {
		t.Fatalf("deleted tags: total=%d err=%v", total, err)
	}
}

func TestCommitPlaceholderHasSearchableTimestamps(t *testing.T) {
	s := commitService(t)
	if _, err := s.MarkDeleted(context.Background(), EntityDocument, "missing"); err != nil {
		t.Fatal(err)
	}
	page, err := s.Search(context.Background(), SearchQuery{Status: "deleted"})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].CreatedAt.IsZero() || page.Items[0].UpdatedAt.IsZero() {
		t.Fatalf("invalid placeholder: %#v", page)
	}
}

func TestCommitMissingCreatedAtPreservesOriginal(t *testing.T) {
	s := commitService(t)
	putSearchTestDocument(t, s, SearchDocument{EntityID: "doc", Title: "before"})
	if _, err := s.Upsert(context.Background(), SearchDocument{EntityID: "doc", Title: "after"}); err != nil {
		t.Fatal(err)
	}
	var created time.Time
	if err := s.indexDB.QueryRow(context.Background(), `SELECT created_at FROM search_index WHERE entity_id='doc'`).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if !created.Equal(time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("created_at overwritten: %v", created)
	}
}

func TestCommitTagsUseSourceUpdatedAt(t *testing.T) {
	s := commitService(t)
	putSearchTestDocument(t, s, SearchDocument{EntityID: "old", Tags: []string{"z"}})
	putSearchTestDocument(t, s, SearchDocument{EntityID: "new", Tags: []string{"a"}})
	execCommitSQL(t, s, `UPDATE search_index SET updated_at='2026-09-10T00:00:00Z' WHERE entity_id='old'`)
	if err := s.RebuildTags(context.Background()); err != nil {
		t.Fatal(err)
	}
	tags, _, err := s.Tags(context.Background(), "", "latest", 20, 0)
	if err != nil || len(tags) != 2 || tags[0].Tag != "z" || !tags[0].UpdatedAt.Equal(time.Date(2026, 9, 10, 0, 0, 0, 0, time.UTC)) {
		t.Fatalf("tags: %#v err=%v", tags, err)
	}
}

func TestCommitTagFailureDoesNotAck(t *testing.T) {
	s := commitService(t)
	putSearchTestDocument(t, s, SearchDocument{EntityID: "doc", Tags: []string{"fixture"}})
	if err := s.RebuildTags(context.Background()); err != nil {
		t.Fatal(err)
	}
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('e','document','doc','delete',1)`)
	execCommitSQL(t, s, `CREATE FUNCTION fail_tags() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'tags unavailable'; END $$; CREATE TRIGGER fail_tags BEFORE DELETE ON tag_index FOR EACH ROW EXECUTE FUNCTION fail_tags()`)
	err := NewWorker(s, WorkerOptions{}).ProcessOnce(context.Background())
	var processed *time.Time
	if scanErr := s.mainDB.QueryRow(context.Background(), `SELECT processed_at FROM search_index_events WHERE id='e'`).Scan(&processed); scanErr != nil {
		t.Fatal(scanErr)
	}
	if processed != nil || err == nil {
		t.Fatalf("tag failure acked: processed=%v err=%v", processed, err)
	}
	var status string
	if err = s.indexDB.QueryRow(context.Background(), `SELECT status FROM search_index WHERE entity_id='doc'`).Scan(&status); err != nil || status != "active" {
		t.Fatalf("tag failure did not roll back document: %s %v", status, err)
	}
	execCommitSQL(t, s, `DROP TRIGGER fail_tags ON tag_index; UPDATE search_index_events SET failed_at=NULL`)
	if err = NewWorker(s, WorkerOptions{}).ProcessOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err = s.mainDB.QueryRow(context.Background(), `SELECT processed_at FROM search_index_events WHERE id='e'`).Scan(&processed); err != nil || processed == nil {
		t.Fatalf("retry did not ack: %v %v", processed, err)
	}
	_, total, err := s.Tags(context.Background(), "", "", 20, 0)
	if err != nil || total != 0 {
		t.Fatalf("retry retained deleted tags: %d %v", total, err)
	}
}

func TestCommitWorkerFailurePersistenceIsReturned(t *testing.T) {
	s := commitService(t)
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('e','invalid','doc','upsert',1)`)
	execCommitSQL(t, s, `CREATE FUNCTION fail_event_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.failed_at IS NOT NULL THEN RAISE EXCEPTION 'failure persistence unavailable'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_event_failure BEFORE UPDATE ON search_index_events FOR EACH ROW EXECUTE FUNCTION fail_event_failure()`)
	if err := NewWorker(s, WorkerOptions{}).ProcessOnce(context.Background()); err == nil || !strings.Contains(err.Error(), "failure persistence unavailable") {
		t.Fatalf("lost failure update: %v", err)
	}
}

func TestCommitWorkerTruncatesUTF8(t *testing.T) {
	got := truncateError(errors.New(strings.Repeat("错", 400)))
	if !utf8.ValidString(got) || len(got) > 1000 {
		t.Fatalf("invalid UTF8 truncation bytes=%d valid=%v", len(got), utf8.ValidString(got))
	}
}

func TestCommitDirectWriteWaitsForGenerationSwitch(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	tx, err := s.indexDB.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, generationSwitchLockKey); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { _, err := s.Upsert(ctx, SearchDocument{EntityID: "during-switch", Title: "new"}); done <- err }()
	select {
	case err := <-done:
		t.Fatalf("write escaped generation lock: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	var target int64
	if err = tx.QueryRow(ctx, `INSERT INTO index_generations(status) VALUES ('building') RETURNING id`).Scan(&target); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE index_generations SET status='retired' WHERE status='active'; UPDATE index_generations SET status='active' WHERE status='building'`); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil {
		t.Fatal(err)
	}
	var actual int64
	if err = s.indexDB.QueryRow(ctx, `SELECT generation_id FROM search_index WHERE entity_id='during-switch'`).Scan(&actual); err != nil {
		t.Fatal(err)
	}
	if actual != target {
		t.Fatalf("write reached retired generation %d instead of %d", actual, target)
	}
}

func TestCommitRecoverOrphanBuilding(t *testing.T) {
	s := commitService(t)
	execCommitSQL(t, s, `INSERT INTO index_generations(status,created_at) VALUES ('building',now()-interval '1 day')`)
	if _, err := s.generations.CreateBuilding(context.Background(), 0, 0); err != nil {
		t.Fatalf("orphan blocks rebuild: %v", err)
	}
}

func TestCommitCatchUpSkipsQuarantinedEvent(t *testing.T) {
	s := commitService(t)
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order,attempts,failed_at) VALUES ('poison','unsupported','bad','upsert',1,10,now()),('valid','document','gone','delete',2,0,NULL)`)
	g, err := s.generations.CreateBuilding(context.Background(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = (&serviceRebuildBackend{service: s}).CatchUp(context.Background(), g.ID, 0); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = s.indexDB.QueryRow(context.Background(), `SELECT count(*) FROM search_index WHERE generation_id=$1 AND entity_id='gone'`, g.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("valid event not replayed: %d %v", count, err)
	}
}

func TestCommitStaleWritesReportNotApplied(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	result, err := s.UpsertVersioned(ctx, SearchDocument{EntityID: "doc", Title: "latest", Tags: []string{"current"}}, 10)
	if err != nil || !result.Applied {
		t.Fatalf("initial version: %#v %v", result, err)
	}
	writes := []func() (WriteResult, error){
		func() (WriteResult, error) {
			return s.UpsertVersioned(ctx, SearchDocument{EntityID: "doc", Title: "stale"}, 9)
		},
		func() (WriteResult, error) { return s.MarkDeletedVersioned(ctx, EntityDocument, "doc", 9) },
		func() (WriteResult, error) {
			return s.Upsert(ctx, SearchDocument{EntityID: "doc", Title: "unversioned"})
		},
		func() (WriteResult, error) { return s.MarkDeleted(ctx, EntityDocument, "doc") },
	}
	for _, write := range writes {
		if result, err = write(); err != nil || result.Applied {
			t.Fatalf("stale write: %#v %v", result, err)
		}
	}
	var title, status string
	if err = s.indexDB.QueryRow(ctx, `SELECT title,status FROM search_index WHERE entity_id='doc'`).Scan(&title, &status); err != nil || title != "latest" || status != "active" {
		t.Fatalf("stale mutated row: %s %s %v", title, status, err)
	}
}

func TestCommitRecoveryPreservesLeasedJob(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	job, err := s.startJob(ctx, "full_rebuild", "", "")
	if err != nil {
		t.Fatal(err)
	}
	lease, err := s.indexDB.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	if _, err = lease.Exec(ctx, `SELECT pg_advisory_lock($1)`, -job); err != nil {
		t.Fatal(err)
	}
	defer lease.Exec(ctx, `SELECT pg_advisory_unlock($1)`, -job)
	execCommitSQL(t, s, `INSERT INTO index_generations(status,build_job_id,created_at) VALUES ('building',$1,now()-interval '1 day')`, job)
	if _, err = s.generations.CreateBuilding(ctx, 0, 0); !errors.Is(err, ErrRebuildInProgress) {
		t.Fatalf("live build replaced: %v", err)
	}
	if _, err = lease.Exec(ctx, `SELECT pg_advisory_unlock($1)`, -job); err != nil {
		t.Fatal(err)
	}
	if _, err = s.generations.CreateBuilding(ctx, 0, 0); err != nil {
		t.Fatalf("crashed job blocked recovery: %v", err)
	}
	var status string
	if err = s.indexDB.QueryRow(ctx, `SELECT status FROM index_sync_log WHERE id=$1`, job).Scan(&status); err != nil || status != "failed" {
		t.Fatalf("abandoned job status: %s %v", status, err)
	}
}

func TestCommitCatchUpUsesFixedUpperWatermark(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('first','document','gone','delete',1)`)
	execCommitSQL(t, s, `CREATE FUNCTION append_late_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('late','unsupported','late','upsert',2) ON CONFLICT DO NOTHING; RETURN NEW; END $$; CREATE TRIGGER append_late_event AFTER INSERT ON search_index FOR EACH ROW EXECUTE FUNCTION append_late_event()`)
	g, err := s.generations.CreateBuilding(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err = (&serviceRebuildBackend{service: s}).CatchUp(ctx, g.ID, 0); err != nil {
		t.Fatalf("catch-up chased event appended after upper watermark: %v", err)
	}
	var count int
	if err = s.mainDB.QueryRow(ctx, `SELECT count(*) FROM search_index_events`).Scan(&count); err != nil || count != 2 {
		t.Fatalf("concurrent fixture missing: %d %v", count, err)
	}
}

func TestCommitActivationDoesNotReplayHistory(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('historical','unsupported','old','upsert',1)`)
	g, err := s.generations.CreateBuilding(ctx, 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	backend := &serviceRebuildBackend{service: s}
	if err = backend.CatchUp(ctx, g.ID, 1); err != nil {
		t.Fatal(err)
	}
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('tail','document','gone','delete',2)`)
	if err = backend.Activate(ctx, g.ID, 0); err != nil {
		t.Fatalf("activation replayed history: %v", err)
	}
	active, err := s.generations.Active(ctx)
	if err != nil || active.ID != g.ID || active.SourceHighWatermark != 2 || active.DocumentCount != 1 {
		t.Fatalf("activation result: %#v %v", active, err)
	}
}

func TestCommitActivationLargeTailReleasesSwitchLock(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	g, err := s.generations.CreateBuilding(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	backend := &serviceRebuildBackend{service: s}
	if err = backend.CatchUp(ctx, g.ID, 0); err != nil {
		t.Fatal(err)
	}
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) SELECT 'e'||n,'document','doc'||n,'delete',n FROM generate_series(1,501) n`)
	execCommitSQL(t, s, fmt.Sprintf(`CREATE FUNCTION reject_locked_large_replay() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted AND classid=%d::oid AND objid=%d::oid) THEN RAISE EXCEPTION 'large tail replay held generation switch lock'; END IF; RETURN NEW; END $$; CREATE TRIGGER reject_locked_large_replay BEFORE INSERT ON search_index FOR EACH ROW EXECUTE FUNCTION reject_locked_large_replay()`, generationSwitchLockKey>>32, generationSwitchLockKey&0xffffffff))
	if err = backend.Activate(ctx, g.ID, 0); err != nil {
		t.Fatal(err)
	}
	active, err := s.generations.Active(ctx)
	if err != nil || active.DocumentCount != 501 {
		t.Fatalf("large tail: %#v %v", active, err)
	}
}

func TestCommitConcurrentTagRebuildsSerialize(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	putSearchTestDocument(t, s, SearchDocument{EntityID: "doc", Tags: []string{"fixture"}})
	execCommitSQL(t, s, `CREATE FUNCTION slow_tag_delete() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.1); RETURN OLD; END $$; CREATE TRIGGER slow_tag_delete BEFORE DELETE ON tag_index FOR EACH ROW EXECUTE FUNCTION slow_tag_delete()`)
	start := make(chan struct{})
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { <-start; errs <- s.RebuildTags(ctx) }()
	}
	close(start)
	for i := 0; i < 2; i++ {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	tags, total, err := s.Tags(ctx, "", "", 20, 0)
	if err != nil || total != 1 || tags[0].TotalCount != 1 {
		t.Fatalf("concurrent tags: %#v %d %v", tags, total, err)
	}
}

func TestCommitWorkerDeadlineCoversDatabaseOperation(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('e','document','doc','delete',1)`)
	execCommitSQL(t, s, `CREATE FUNCTION slow_index_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(2); RETURN NEW; END $$; CREATE TRIGGER slow_index_insert BEFORE INSERT ON search_index FOR EACH ROW EXECUTE FUNCTION slow_index_insert()`)
	started := time.Now()
	err := NewWorker(s, WorkerOptions{OperationTimeout: 100 * time.Millisecond}).ProcessOnce(ctx)
	if err == nil || time.Since(started) > time.Second {
		t.Fatalf("DB operation escaped deadline: elapsed=%v err=%v", time.Since(started), err)
	}
	var processed *time.Time
	if err = s.mainDB.QueryRow(ctx, `SELECT processed_at FROM search_index_events WHERE id='e'`).Scan(&processed); err != nil || processed != nil {
		t.Fatalf("timed-out event acked: %v %v", processed, err)
	}
}

func TestCommitWorkerDrainsFullBatchWithoutPolling(t *testing.T) {
	s := commitService(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('e1','document','one','delete',1),('e2','document','two','delete',2)`)
	worker := NewWorker(s, WorkerOptions{Enabled: true, BatchSize: 1, PollInterval: 5 * time.Second})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); worker.loop(ctx) }()
	defer func() { cancel(); wg.Wait() }()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var count int
		if err := s.mainDB.QueryRow(ctx, `SELECT count(*) FROM search_index_events WHERE processed_at IS NOT NULL`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("full batch waited for poll interval")
}

func TestCommitWorkerCacheFailureDoesNotAck(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('e','document','gone','delete',1)`)
	failure := errors.New("cache completion failed")
	w := NewWorker(s, WorkerOptions{MutationRunner: func(_ context.Context, fn func() error) error {
		if err := fn(); err != nil {
			return err
		}
		return failure
	}})
	if err := w.ProcessOnce(ctx); !errors.Is(err, failure) {
		t.Fatalf("cache failure swallowed: %v", err)
	}
	var processed *time.Time
	if err := s.mainDB.QueryRow(ctx, `SELECT processed_at FROM search_index_events WHERE id='e'`).Scan(&processed); err != nil || processed != nil {
		t.Fatalf("failed cache completion acked: %v %v", processed, err)
	}
}

func TestCommitActivationCacheFailureIsReturnedAfterCommit(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	g, err := s.generations.CreateBuilding(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("activation cache completion failed")
	s.SetMutationRunner(func(_ context.Context, fn func() error) error {
		if err := fn(); err != nil {
			return err
		}
		active, err := s.generations.Active(ctx)
		if err != nil || active.ID != g.ID {
			t.Fatalf("completion before activation: %#v %v", active, err)
		}
		return failure
	})
	if err = (&serviceRebuildBackend{service: s}).Activate(ctx, g.ID, 0); !errors.Is(err, failure) {
		t.Fatalf("activation cache failure swallowed: %v", err)
	}
}

func TestCommitDeletedEntityRebuildRefreshesTags(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	execCommitSQL(t, s, `CREATE TABLE "CustomPage"(id text,title text,published boolean,category text,description text,content text,"createdAt" timestamptz,"updatedAt" timestamptz,slug text,icon text,"sortOrder" int)`)
	putSearchTestDocument(t, s, SearchDocument{EntityID: "gone", Tags: []string{"removed"}})
	job, err := s.startJob(ctx, "entity_rebuild", EntityDocument, "gone")
	if err != nil {
		t.Fatal(err)
	}
	s.runEntityRebuild(ctx, job, EntityDocument, "gone")
	_, total, err := s.Tags(ctx, "", "", 20, 0)
	if err != nil || total != 0 {
		t.Fatalf("entity delete retained tags: %d %v", total, err)
	}
	var status string
	if err = s.indexDB.QueryRow(ctx, `SELECT status FROM index_sync_log WHERE id=$1`, job).Scan(&status); err != nil || status != "success" {
		t.Fatalf("job failed: %s %v", status, err)
	}
}

func TestCommitActivationDoesNotRebuildAllTagsUnderSwitchLock(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	g, err := s.generations.CreateBuilding(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.upsertIntoGeneration(ctx, g.ID, SearchDocument{EntityID: "doc", Tags: []string{"fixture"}}, 0, true); err != nil {
		t.Fatal(err)
	}
	if err = s.rebuildTagsForGeneration(ctx, g.ID); err != nil {
		t.Fatal(err)
	}
	execCommitSQL(t, s, fmt.Sprintf(`CREATE FUNCTION reject_locked_tag_rebuild() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND pid=pg_backend_pid() AND granted AND classid=%d::oid AND objid=%d::oid) THEN RAISE EXCEPTION 'full tag rebuild held switch lock'; END IF; RETURN OLD; END $$; CREATE TRIGGER reject_locked_tag_rebuild BEFORE DELETE ON tag_index FOR EACH ROW EXECUTE FUNCTION reject_locked_tag_rebuild()`, generationSwitchLockKey>>32, generationSwitchLockKey&0xffffffff))
	if err = (&serviceRebuildBackend{service: s}).Activate(ctx, g.ID, 1); err != nil {
		t.Fatal(err)
	}
}

func TestCommitWorkerAcknowledgesSuccessfulSiblingOfPoison(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('poison','unsupported','bad','upsert',1),('good','document','gone','delete',2)`)
	err := NewWorker(s, WorkerOptions{}).ProcessOnce(ctx)
	if err == nil {
		t.Fatal("poison failure missing")
	}
	var processed *time.Time
	if err = s.mainDB.QueryRow(ctx, `SELECT processed_at FROM search_index_events WHERE id='good'`).Scan(&processed); err != nil || processed == nil {
		t.Fatalf("valid sibling lost acknowledgement: %v %v", processed, err)
	}
}

func TestCommitActivationTailRepairsAffectedTags(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	g, err := s.generations.CreateBuilding(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"gone", "keep"} {
		if _, err = s.upsertIntoGeneration(ctx, g.ID, SearchDocument{EntityID: id, Tags: []string{id}}, 0, true); err != nil {
			t.Fatal(err)
		}
	}
	backend := &serviceRebuildBackend{service: s}
	if err = backend.CatchUp(ctx, g.ID, 0); err != nil {
		t.Fatal(err)
	}
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('tail','document','gone','delete',1)`)
	if err = backend.Activate(ctx, g.ID, 2); err != nil {
		t.Fatal(err)
	}
	tags, total, err := s.Tags(ctx, "", "", 20, 0)
	if err != nil || total != 1 || len(tags) != 1 || tags[0].Tag != "keep" {
		t.Fatalf("activation tail tags: %#v total=%d err=%v", tags, total, err)
	}
}

func TestCommitActivationBoundsSlowTailAndReleasesLock(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	g, err := s.generations.CreateBuilding(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('tail','document','gone','delete',1)`)
	execCommitSQL(t, s, `CREATE FUNCTION slow_tail_insert() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(6); RETURN NEW; END $$; CREATE TRIGGER slow_tail_insert BEFORE INSERT ON search_index FOR EACH ROW EXECUTE FUNCTION slow_tail_insert()`)
	started := time.Now()
	err = (&serviceRebuildBackend{service: s}).Activate(ctx, g.ID, 0)
	if err == nil || time.Since(started) > 5500*time.Millisecond {
		t.Fatalf("slow tail held switch unbounded: elapsed=%v err=%v", time.Since(started), err)
	}
	tx, err := s.indexDB.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	var available bool
	if err = tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, generationSwitchLockKey).Scan(&available); err != nil || !available {
		t.Fatalf("activation leaked switch lock: %v %v", available, err)
	}
}
