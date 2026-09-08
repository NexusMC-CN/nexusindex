package indexer

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrRebuildInProgress = errors.New("nexusindex full rebuild is already running")

const generationSwitchLockKey int64 = 0x4e5847454e535743

func withGenerationSwitchLock(ctx context.Context, pool *pgxpool.Pool, fn func() error) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, generationSwitchLockKey); err != nil {
		return err
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, generationSwitchLockKey)
	}()
	return fn()
}

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
		if markErr := c.backend.Fail(ctx, generationID, cause); markErr != nil {
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

type serviceRebuildBackend struct{ service *Service }

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
	after := watermark
	for {
		events, err := b.eventsAfter(ctx, after, 500)
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
			if err := b.applyEvent(ctx, generationID, latest[key]); err != nil {
				return err
			}
		}
		after = events[len(events)-1].EventOrder
	}
}

func (b *serviceRebuildBackend) eventsAfter(ctx context.Context, after int64, limit int) ([]IndexEvent, error) {
	rows, err := b.service.mainDB.Query(ctx, `SELECT id, entity_type, entity_id, action, event_order, created_at FROM search_index_events WHERE event_order > $1 ORDER BY event_order ASC LIMIT $2`, after, limit)
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

func (b *serviceRebuildBackend) applyEvent(ctx context.Context, generationID int64, event IndexEvent) error {
	if event.Action == "delete" {
		return b.service.markDeletedInGeneration(ctx, generationID, event.EntityType, event.EntityID, event.EventOrder, true)
	}
	doc, found, err := b.service.fetchOne(ctx, event.EntityType, event.EntityID)
	if err != nil {
		return err
	}
	if !found {
		return b.service.markDeletedInGeneration(ctx, generationID, event.EntityType, event.EntityID, event.EventOrder, true)
	}
	return b.service.upsertIntoGeneration(ctx, generationID, doc, event.EventOrder, true)
}

func (b *serviceRebuildBackend) Activate(ctx context.Context, generationID, indexed int64) error {
	return withGenerationSwitchLock(ctx, b.service.mainDB, func() error {
		if err := b.CatchUp(ctx, generationID, 0); err != nil {
			return err
		}
		if err := b.service.rebuildTagsForGeneration(ctx, generationID); err != nil {
			return err
		}
		var finalCount int64
		if err := b.service.indexDB.QueryRow(ctx, `SELECT count(*) FROM search_index WHERE generation_id = $1`, generationID).Scan(&finalCount); err != nil {
			return err
		}
		if err := b.service.generations.SetDocumentCount(ctx, generationID, finalCount); err != nil {
			return err
		}
		return b.service.generations.Activate(ctx, generationID)
	})
}

func (b *serviceRebuildBackend) Fail(ctx context.Context, generationID int64, cause error) error {
	return b.service.generations.Fail(ctx, generationID, cause)
}
