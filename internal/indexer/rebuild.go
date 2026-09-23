package indexer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

var ErrRebuildInProgress = errors.New("nexusindex full rebuild is already running")

const generationSwitchLockKey int64 = 0x4e5847454e535743

type RebuildBackend interface {
	CreateBuilding(ctx context.Context, jobID, watermark int64) (int64, error)
	Build(ctx context.Context, generationID int64) (int64, error)
	Validate(ctx context.Context, generationID, indexed int64) error
	CatchUp(ctx context.Context, generationID, watermark int64) error
	Activate(ctx context.Context, generationID, indexed int64) error
	Fail(ctx context.Context, generationID int64, cause error) error
}

type ShadowRebuildResult struct {
	GenerationID int64 `json:"generationId"`
	Indexed      int64 `json:"indexed"`
}

type RebuildCoordinator struct {
	backend RebuildBackend
	running atomic.Bool
}

func NewRebuildCoordinator(backend RebuildBackend) *RebuildCoordinator {
	return &RebuildCoordinator{backend: backend}
}

func (c *RebuildCoordinator) Run(ctx context.Context, jobID, watermark int64) (ShadowRebuildResult, error) {
	if c == nil || c.backend == nil {
		return ShadowRebuildResult{}, errors.New("rebuild backend is required")
	}
	if !c.running.CompareAndSwap(false, true) {
		return ShadowRebuildResult{}, ErrRebuildInProgress
	}
	defer c.running.Store(false)
	generationID, err := c.backend.CreateBuilding(ctx, jobID, watermark)
	if err != nil {
		return ShadowRebuildResult{}, err
	}
	fail := func(cause error) (ShadowRebuildResult, error) {
		failureCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if markErr := c.backend.Fail(failureCtx, generationID, cause); markErr != nil {
			return ShadowRebuildResult{}, fmt.Errorf("%w; mark generation failed: %v", cause, markErr)
		}
		return ShadowRebuildResult{}, cause
	}
	indexed, err := c.backend.Build(ctx, generationID)
	if err != nil {
		return fail(err)
	}
	if err := c.backend.Validate(ctx, generationID, indexed); err != nil {
		return fail(err)
	}
	if err := c.backend.CatchUp(ctx, generationID, watermark); err != nil {
		return fail(err)
	}
	if err := c.backend.Activate(ctx, generationID, indexed); err != nil {
		return fail(err)
	}
	return ShadowRebuildResult{GenerationID: generationID, Indexed: indexed}, nil
}

type serviceRebuildBackend struct {
	service          *Service
	catchUpWatermark int64
}

func (b *serviceRebuildBackend) CreateBuilding(ctx context.Context, jobID, watermark int64) (int64, error) {
	generation, err := b.service.generations.CreateBuilding(ctx, jobID, watermark)
	if err != nil {
		return 0, err
	}
	return generation.ID, nil
}

func (b *serviceRebuildBackend) Build(ctx context.Context, generationID int64) (int64, error) {
	var total int64
	for _, entityType := range EntityTypes {
		count, err := b.service.rebuildEntityTypeInto(ctx, entityType, generationID)
		if err != nil {
			return total, err
		}
		total += int64(count)
	}
	if err := b.service.rebuildTagsForGeneration(ctx, generationID); err != nil {
		return total, err
	}
	return total, nil
}

func (b *serviceRebuildBackend) Validate(ctx context.Context, generationID, indexed int64) error {
	var count int64
	if err := b.service.indexDB.QueryRow(ctx, `SELECT count(*) FROM search_index WHERE generation_id = $1`, generationID).Scan(&count); err != nil {
		return err
	}
	if count != indexed {
		return fmt.Errorf("generation validation failed: indexed=%d stored=%d", indexed, count)
	}
	var invalid int64
	if err := b.service.indexDB.QueryRow(ctx, `SELECT count(*) FROM search_index WHERE generation_id = $1 AND (entity_type = '' OR entity_id = '')`, generationID).Scan(&invalid); err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("generation validation failed: %d invalid documents", invalid)
	}
	return nil
}

func (b *serviceRebuildBackend) CatchUp(ctx context.Context, generationID, watermark int64) error {
	upper, err := b.service.committedOutboxWatermark(ctx)
	if err != nil {
		return err
	}
	if err := b.catchUpThrough(ctx, generationID, watermark, upper, b.service.indexDB); err != nil {
		return err
	}
	b.catchUpWatermark = upper
	return nil
}

func (b *serviceRebuildBackend) catchUpThrough(ctx context.Context, generationID, watermark, upper int64, writer indexWriter) error {
	after := watermark
	for {
		events, err := b.eventsAfter(ctx, after, upper, 500)
		if err != nil {
			return err
		}
		if len(events) == 0 {
			return nil
		}
		latest := mergeLatestEvents(events)
		keys := make([]string, 0, len(latest))
		for key := range latest {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			if err := b.applyEvent(ctx, writer, generationID, latest[key]); err != nil {
				return err
			}
		}
		after = events[len(events)-1].EventOrder
	}
}

func (b *serviceRebuildBackend) eventsAfter(ctx context.Context, after, upper int64, limit int) ([]IndexEvent, error) {
	return b.eventsAfterFrom(ctx, b.service.mainDB, after, upper, limit)
}

func (b *serviceRebuildBackend) eventsAfterFrom(ctx context.Context, reader sourceReader, after, upper int64, limit int) ([]IndexEvent, error) {
	maxAttempts := b.service.maxEventAttempts
	if maxAttempts <= 0 {
		maxAttempts = 10
	}
	rows, err := reader.Query(ctx, `SELECT id, entity_type, entity_id, action, event_order, created_at FROM search_index_events WHERE event_order > $1 AND event_order <= $2 AND NOT (attempts >= $4 AND failed_at IS NOT NULL AND processed_at IS NULL) ORDER BY event_order ASC LIMIT $3`, after, upper, limit, maxAttempts)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []IndexEvent{}
	for rows.Next() {
		var event IndexEvent
		if err := rows.Scan(&event.ID, &event.EntityType, &event.EntityID, &event.Action, &event.EventOrder, &event.CreatedAt); err != nil {
			return nil, err
		}
		events = append(events, event)
	}
	return events, rows.Err()
}

func (b *serviceRebuildBackend) applyEvent(ctx context.Context, writer indexWriter, generationID int64, event IndexEvent) error {
	return b.applyEventFrom(ctx, writer, b.service.mainDB, generationID, event)
}

func (b *serviceRebuildBackend) applyEventFrom(ctx context.Context, writer indexWriter, reader sourceReader, generationID int64, event IndexEvent) error {
	if event.Action == "delete" {
		_, err := b.service.markDeletedWithWriter(ctx, writer, generationID, event.EntityType, event.EntityID, event.EventOrder, true)
		return err
	}
	doc, found, err := b.service.fetchOneFrom(ctx, reader, event.EntityType, event.EntityID)
	if err != nil {
		return err
	}
	if !found {
		_, err := b.service.markDeletedWithWriter(ctx, writer, generationID, event.EntityType, event.EntityID, event.EventOrder, true)
		return err
	}
	_, err = b.service.upsertWithWriter(ctx, writer, generationID, doc, event.EventOrder, true)
	return err
}

func (b *serviceRebuildBackend) Activate(ctx context.Context, generationID, indexed int64) error {
	for {
		// Whole-generation aggregates are computed before the switch lock. Only
		// tags touched by the bounded tail are repaired inside activation.
		if err := b.service.rebuildTagsForGeneration(ctx, generationID); err != nil {
			return err
		}
		var finalCount int64
		if err := b.service.indexDB.QueryRow(ctx, `SELECT count(*) FROM search_index WHERE generation_id=$1`, generationID).Scan(&finalCount); err != nil {
			return err
		}
		tx, err := b.service.indexDB.Begin(ctx)
		if err != nil {
			return err
		}
		upper, ready, err := b.activateTail(ctx, tx, generationID, finalCount)
		_ = tx.Rollback(context.Background())
		if err != nil || ready {
			return err
		}
		// A large tail is replayed without blocking active-generation writers.
		if err := b.catchUpThrough(ctx, generationID, b.catchUpWatermark, upper, b.service.indexDB); err != nil {
			return err
		}
		b.catchUpWatermark = upper
	}
}

func (b *serviceRebuildBackend) activateTail(ctx context.Context, tx pgx.Tx, generationID, finalCount int64) (int64, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	// Bound PostgreSQL execution too: cancellation of a client socket alone
	// does not necessarily stop a running statement or release its lock.
	if _, err := tx.Exec(ctx, `SET LOCAL statement_timeout = '4s'`); err != nil {
		return 0, false, err
	}
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, generationSwitchLockKey); err != nil {
		return 0, false, err
	}
	// Hold the main-database commit barrier through the index transaction's
	// commit. This requires no distributed commit: the main transaction contains
	// only a table lock and reads. Writers released afterward target the new
	// active generation, even if the rebuild process dies after index commit.
	mainTx, upper, err := b.service.outboxCommitBarrier(ctx)
	if err != nil {
		return 0, false, err
	}
	defer rollbackOutboxBarrier(mainTx)
	events, err := b.eventsAfterFrom(ctx, mainTx, b.catchUpWatermark, upper, 501)
	if err != nil {
		return upper, false, err
	}
	if len(events) > 500 {
		return upper, false, nil
	}
	err = b.service.runMutation(ctx, func() error {
		affectedTags := []string{}
		for _, event := range mergeLatestEvents(events) {
			var tags []string
			err := tx.QueryRow(ctx, `SELECT tags FROM search_index WHERE generation_id=$1 AND entity_type=$2 AND entity_id=$3`, generationID, event.EntityType, event.EntityID).Scan(&tags)
			if errors.Is(err, pgx.ErrNoRows) {
				finalCount++
			} else if err != nil {
				return err
			}
			affectedTags = append(affectedTags, tags...)
			if err := b.applyEventFrom(ctx, tx, mainTx, generationID, event); err != nil {
				return err
			}
			if err := tx.QueryRow(ctx, `SELECT tags FROM search_index WHERE generation_id=$1 AND entity_type=$2 AND entity_id=$3`, generationID, event.EntityType, event.EntityID).Scan(&tags); err != nil {
				return err
			}
			affectedTags = append(affectedTags, tags...)
		}
		if len(affectedTags) > 0 {
			if err := b.service.rebuildSelectedTagsInTransaction(ctx, tx, generationID, normalizeStrings(affectedTags)); err != nil {
				return err
			}
		}
		if _, err := tx.Exec(ctx, `UPDATE index_generations SET document_count=$2, source_high_watermark=$3 WHERE id=$1`, generationID, finalCount, upper); err != nil {
			return err
		}
		if err := b.service.generations.activateInTransaction(ctx, tx, generationID); err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
	return upper, true, err
}

func (b *serviceRebuildBackend) Fail(ctx context.Context, generationID int64, cause error) error {
	return b.service.generations.Fail(ctx, generationID, cause)
}

// Production allocates nextval inside INSERT, with the default CACHE 1 sequence.
// INSERT first obtains ROW EXCLUSIVE on this table. SHARE waits for every such
// transaction to commit/abort and stops new inserts before they allocate an
// order. Only a maximum read under this barrier is a complete commit boundary.
func (s *Service) outboxCommitBarrier(ctx context.Context) (pgx.Tx, int64, error) {
	tx, err := s.mainDB.Begin(ctx)
	if err != nil {
		return nil, 0, err
	}
	fail := func(err error) (pgx.Tx, int64, error) { rollbackOutboxBarrier(tx); return nil, 0, err }
	if _, err = tx.Exec(ctx, `SET LOCAL lock_timeout = '4s'; SET LOCAL statement_timeout = '4s'; LOCK TABLE search_index_events IN SHARE MODE`); err != nil {
		return fail(fmt.Errorf("outbox commit barrier: %w", err))
	}
	var cacheSize *int64
	if err = tx.QueryRow(ctx, `SELECT (SELECT seqcache FROM pg_sequence WHERE seqrelid=to_regclass('public.search_index_event_order_seq'))`).Scan(&cacheSize); err != nil {
		return fail(err)
	}
	if cacheSize != nil && *cacheSize != 1 {
		return fail(errors.New("outbox commit boundary requires search_index_event_order_seq CACHE 1"))
	}
	var upper int64
	if err = tx.QueryRow(ctx, `SELECT coalesce(max(event_order),0) FROM search_index_events`).Scan(&upper); err != nil {
		return fail(err)
	}
	return tx, upper, nil
}

func rollbackOutboxBarrier(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := tx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		_ = tx.Conn().Close(ctx)
	}
}

func (s *Service) committedOutboxWatermark(ctx context.Context) (int64, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, upper, err := s.outboxCommitBarrier(ctx)
	if err != nil {
		return 0, err
	}
	rollbackOutboxBarrier(tx)
	return upper, nil
}
