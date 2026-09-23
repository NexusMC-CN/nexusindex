package indexer

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReviewOutboxCommitOrderCannotSkipAcknowledgedEvent(t *testing.T) {
	// Separate databases preserve the actual cross-database transaction boundary.
	mainService, indexService := commitService(t), commitService(t)
	s := NewService(mainService.mainDB, indexService.indexDB)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	execCommitSQL(t, mainService, `CREATE SEQUENCE search_index_event_order_seq START 10`)
	tx10, err := s.mainDB.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx10.Rollback(context.Background())
	if _, err = tx10.Exec(ctx, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('e10','document','late','delete',nextval('search_index_event_order_seq'))`); err != nil {
		t.Fatal(err)
	}
	execCommitSQL(t, mainService, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('e11','document','early','delete',nextval('search_index_event_order_seq'))`)
	g, err := s.generations.CreateBuilding(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	backend := &serviceRebuildBackend{service: s}
	catchUp := make(chan error, 1)
	go func() { catchUp <- backend.CatchUp(ctx, g.ID, 0) }()
	// Old code returns after observing 11; corrected code waits for the actual
	// outbox INSERT transaction 10 before it can capture a complete boundary.
	var caughtUp bool
	select {
	case err = <-catchUp:
		if err != nil {
			t.Fatal(err)
		}
		caughtUp = true
	case <-time.After(100 * time.Millisecond):
	}
	if err = tx10.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if !caughtUp {
		if err = <-catchUp; err != nil {
			t.Fatal(err)
		}
	}
	if err = NewWorker(s, WorkerOptions{}).ProcessOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var acknowledged int
	if err = s.mainDB.QueryRow(ctx, `SELECT count(*) FROM search_index_events WHERE processed_at IS NOT NULL`).Scan(&acknowledged); err != nil || acknowledged != 2 {
		t.Fatalf("old generation worker did not ack fixture: %d %v", acknowledged, err)
	}
	if err = backend.Activate(ctx, g.ID, 0); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = s.indexDB.QueryRow(ctx, `SELECT count(*) FROM search_index WHERE generation_id=$1 AND entity_id='late' AND status='deleted'`, g.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("acknowledged late-committing event 10 missing from active generation: count=%d err=%v", count, err)
	}
}

func TestReviewFullRebuildAdmissionPreventsPoolExhaustion(t *testing.T) {
	fixture := commitService(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	cfg := fixture.indexDB.Config().Copy()
	cfg.MaxConns = 2
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := NewService(pool, pool)
	jobs := make([]int64, 2)
	for i := range jobs {
		jobs[i], err = s.startJob(ctx, "full_rebuild", "", "")
		if err != nil {
			t.Fatal(err)
		}
	}
	// Both old goroutines hold a lease before waiting for this main-table lock.
	blocker, err := fixture.mainDB.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Rollback(context.Background())
	if _, err = blocker.Exec(ctx, `LOCK TABLE search_index_events IN ACCESS EXCLUSIVE MODE`); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for _, job := range jobs {
		wg.Add(1)
		go func(job int64) { defer wg.Done(); s.runFullRebuild(ctx, job) }(job)
	}
	time.Sleep(100 * time.Millisecond)
	if err = blocker.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(700 * time.Millisecond):
		cancel()
		<-done
		t.Fatal("concurrent rebuilds consumed both lease connections and could not progress")
	}
}

func TestReviewWorkerPrecommitFailureDoesNotExhaustAttempts(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('e','document','gone','delete',1)`)
	available := false
	failure := errors.New("cache precommit unavailable")
	w := NewWorker(s, WorkerOptions{MaxAttempts: 1, MutationRunner: func(_ context.Context, fn func() error) error {
		if !available {
			return failure
		}
		return fn()
	}})
	if err := w.ProcessOnce(ctx); !errors.Is(err, failure) {
		t.Fatalf("missing precommit error: %v", err)
	}
	available = true
	if err := w.ProcessOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var processed *time.Time
	if err := s.mainDB.QueryRow(ctx, `SELECT processed_at FROM search_index_events WHERE id='e'`).Scan(&processed); err != nil || processed == nil {
		t.Fatalf("restored cache could not retry MaxAttempts=1 event: %v %v", processed, err)
	}
}

func TestReviewWorkerCompletionFailureDoesNotExhaustAttempts(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('e','document','gone','delete',1)`)
	available := false
	failure := errors.New("cache completion unavailable")
	w := NewWorker(s, WorkerOptions{MaxAttempts: 1, MutationRunner: func(_ context.Context, fn func() error) error {
		if err := fn(); err != nil {
			return err
		}
		if !available {
			return failure
		}
		return nil
	}})
	if err := w.ProcessOnce(ctx); !errors.Is(err, failure) {
		t.Fatalf("missing completion error: %v", err)
	}
	available = true
	if err := w.ProcessOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var processed *time.Time
	if err := s.mainDB.QueryRow(ctx, `SELECT processed_at FROM search_index_events WHERE id='e'`).Scan(&processed); err != nil || processed == nil {
		t.Fatalf("completion failure exhausted attempts: %v %v", processed, err)
	}
}

func TestReviewWorkerAckFailureDoesNotExhaustAttempts(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('e','document','gone','delete',1)`)
	execCommitSQL(t, s, `CREATE FUNCTION fail_ack() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.processed_at IS NOT NULL THEN RAISE EXCEPTION 'ack unavailable'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_ack BEFORE UPDATE ON search_index_events FOR EACH ROW EXECUTE FUNCTION fail_ack()`)
	w := NewWorker(s, WorkerOptions{MaxAttempts: 1})
	if err := w.ProcessOnce(ctx); err == nil {
		t.Fatal("ack failure swallowed")
	}
	execCommitSQL(t, s, `DROP TRIGGER fail_ack ON search_index_events`)
	if err := w.ProcessOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var processed *time.Time
	if err := s.mainDB.QueryRow(ctx, `SELECT processed_at FROM search_index_events WHERE id='e'`).Scan(&processed); err != nil || processed == nil {
		t.Fatalf("ack failure exhausted attempts: %v %v", processed, err)
	}
}

func TestReviewCommitBarrierRejectsCachedSequence(t *testing.T) {
	s := commitService(t)
	execCommitSQL(t, s, `CREATE SEQUENCE search_index_event_order_seq CACHE 2`)
	_, err := s.committedOutboxWatermark(context.Background())
	if err == nil || !strings.Contains(err.Error(), "CACHE 1") {
		t.Fatalf("cached sequence cannot support this commit boundary: %v", err)
	}
}

func TestReviewWorkerOnlyBusinessFailureConsumesAttempts(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('e','unsupported','bad','upsert',1)`)
	w := NewWorker(s, WorkerOptions{MaxAttempts: 1})
	if err := w.ProcessOnce(ctx); err == nil {
		t.Fatal("expected unsupported entity failure")
	}
	var attempts int
	var failed, locked, processed *time.Time
	if err := s.mainDB.QueryRow(ctx, `SELECT attempts,failed_at,locked_at,processed_at FROM search_index_events WHERE id='e'`).Scan(&attempts, &failed, &locked, &processed); err != nil || attempts != 1 || failed == nil || locked != nil || processed != nil {
		t.Fatalf("bad poison state: attempts=%d failed=%v locked=%v processed=%v err=%v", attempts, failed, locked, processed, err)
	}
	if err := w.ProcessOnce(ctx); err != nil {
		t.Fatalf("quarantined poison retried: %v", err)
	}
}

func TestReviewActivationHoldsOutboxBarrierThroughIndexCommit(t *testing.T) {
	mainService, indexService := commitService(t), commitService(t)
	s := NewService(mainService.mainDB, indexService.indexDB)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	execCommitSQL(t, mainService, `CREATE SEQUENCE search_index_event_order_seq START 10; INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('initial','document','before','delete',nextval('search_index_event_order_seq'))`)
	g, err := s.generations.CreateBuilding(ctx, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	b := &serviceRebuildBackend{service: s}
	if err = b.CatchUp(ctx, g.ID, 0); err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	s.SetMutationRunner(func(_ context.Context, fn func() error) error {
		go func() {
			_, err := s.mainDB.Exec(ctx, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('after','document','after','delete',nextval('search_index_event_order_seq'))`)
			written <- err
		}()
		select {
		case err := <-written:
			t.Errorf("INSERT passed activation's commit barrier: %v", err)
		case <-time.After(100 * time.Millisecond):
		}
		var sequence int64
		if err := s.mainDB.QueryRow(ctx, `SELECT last_value FROM search_index_event_order_seq`).Scan(&sequence); err != nil || sequence != 10 {
			t.Errorf("blocked insert allocated order early: %d %v", sequence, err)
		}
		if err := fn(); err != nil {
			return err
		}
		active, err := s.generations.Active(ctx)
		if err != nil || active.ID != g.ID {
			t.Errorf("index transaction did not commit: %#v %v", active, err)
		}
		return nil
	})
	if err = b.Activate(ctx, g.ID, 1); err != nil {
		t.Fatal(err)
	}
	if err = <-written; err != nil {
		t.Fatal(err)
	}
	if err = NewWorker(s, WorkerOptions{}).ProcessOnce(ctx); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = s.indexDB.QueryRow(ctx, `SELECT count(*) FROM search_index WHERE generation_id=$1 AND entity_id='after'`, g.ID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("post-barrier event missed new generation: %d %v", count, err)
	}
}

func TestReviewWorkerDoesNotAcknowledgeReclaimedLease(t *testing.T) {
	s := commitService(t)
	ctx := context.Background()
	execCommitSQL(t, s, `INSERT INTO search_index_events(id,entity_type,entity_id,action,event_order) VALUES ('e','document','gone','delete',1)`)
	w := NewWorker(s, WorkerOptions{MaxAttempts: 1, MutationRunner: func(_ context.Context, fn func() error) error {
		if err := fn(); err != nil {
			return err
		}
		_, err := s.mainDB.Exec(ctx, `UPDATE search_index_events SET locked_at=now()+interval '1 minute' WHERE id='e'`)
		return err
	}})
	if err := w.ProcessOnce(ctx); err == nil {
		t.Fatal("lost lease was reported acknowledged")
	}
	var locked, processed *time.Time
	if err := s.mainDB.QueryRow(ctx, `SELECT locked_at,processed_at FROM search_index_events WHERE id='e'`).Scan(&locked, &processed); err != nil || locked == nil || processed != nil {
		t.Fatalf("old cleanup changed new claim: locked=%v processed=%v err=%v", locked, processed, err)
	}
}
