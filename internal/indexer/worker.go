package indexer

import (
	"context"
	"errors"
	"log"
	"strings"
	"time"
)

type CacheInvalidator func(context.Context)

type WorkerOptions struct {
	Enabled          bool
	BatchSize        int
	PollInterval     time.Duration
	LockTTL          time.Duration
	RetryDelay       time.Duration
	MaxAttempts      int
	CacheInvalidator CacheInvalidator
	Logger           *log.Logger
}

type IndexEvent struct {
	ID         string
	EntityType string
	EntityID   string
	Action     string
	EventOrder int64
	CreatedAt  time.Time
}

type Worker struct {
	service          *Service
	options          WorkerOptions
	cacheInvalidator CacheInvalidator
}

func NewWorker(service *Service, options WorkerOptions) *Worker {
	if options.BatchSize <= 0 {
		options.BatchSize = 200
	}
	if options.BatchSize > 500 {
		options.BatchSize = 500
	}
	if options.PollInterval <= 0 {
		options.PollInterval = 2 * time.Second
	}
	if options.LockTTL <= 0 {
		options.LockTTL = 2 * time.Minute
	}
	if options.RetryDelay <= 0 {
		options.RetryDelay = 30 * time.Second
	}
	if options.MaxAttempts <= 0 {
		options.MaxAttempts = 10
	}
	return &Worker{service: service, options: options, cacheInvalidator: options.CacheInvalidator}
}

func (w *Worker) Start(ctx context.Context) {
	if w == nil || w.service == nil || !w.options.Enabled {
		return
	}
	go w.loop(ctx)
}

func (w *Worker) loop(ctx context.Context) {
	ticker := time.NewTicker(w.options.PollInterval)
	defer ticker.Stop()
	for {
		if err := w.ProcessOnce(ctx); err != nil {
			w.logf("nexusindex worker process failed: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (w *Worker) ProcessOnce(ctx context.Context) error {
	events, err := w.claimEvents(ctx)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	latestByDoc := mergeLatestEvents(events)

	changed := false
	for _, event := range latestByDoc {
		if err := w.processEvent(ctx, event); err != nil {
			w.markFailed(ctx, collectEventIDs(events, event.EntityType, event.EntityID), err)
			continue
		}
		if err := w.markProcessed(ctx, collectEventIDs(events, event.EntityType, event.EntityID)); err != nil {
			return err
		}
		changed = true
	}
	if changed {
		_ = w.service.RebuildTags(ctx)
		if w.cacheInvalidator != nil {
			w.cacheInvalidator(ctx)
		}
	}
	return nil
}

func mergeLatestEvents(events []IndexEvent) map[string]IndexEvent {
	latestByDoc := map[string]IndexEvent{}
	for _, event := range events {
		key := event.EntityType + ":" + event.EntityID
		existing, ok := latestByDoc[key]
		newerFallback := event.CreatedAt.After(existing.CreatedAt) || (event.CreatedAt.Equal(existing.CreatedAt) && event.ID > existing.ID)
		if !ok || event.EventOrder > existing.EventOrder || (event.EventOrder == existing.EventOrder && newerFallback) {
			latestByDoc[key] = event
		}
	}
	return latestByDoc
}

func (w *Worker) processEvent(ctx context.Context, event IndexEvent) error {
	entityType := strings.TrimSpace(event.EntityType)
	entityID := strings.TrimSpace(event.EntityID)
	if entityType == "" || entityID == "" {
		return errors.New("invalid index event entity")
	}
	if event.Action == "delete" {
		return w.service.MarkDeletedVersioned(ctx, entityType, entityID, event.EventOrder)
	}
	doc, found, err := w.service.fetchOne(ctx, entityType, entityID)
	if err != nil {
		return err
	}
	if !found {
		return w.service.MarkDeletedVersioned(ctx, entityType, entityID, event.EventOrder)
	}
	return w.service.UpsertVersioned(ctx, doc, event.EventOrder)
}

func (w *Worker) claimEvents(ctx context.Context) ([]IndexEvent, error) {
	lockTTLMs := w.options.LockTTL.Milliseconds()
	retryDelayMs := w.options.RetryDelay.Milliseconds()
	rows, err := w.service.mainDB.Query(ctx, `WITH picked AS (
		SELECT id
		FROM search_index_events
		WHERE processed_at IS NULL
			AND (locked_at IS NULL OR locked_at < now() - make_interval(secs => ($2::double precision / 1000.0)))
			AND attempts < $3
			AND (failed_at IS NULL OR failed_at < now() - make_interval(secs => ($4::double precision / 1000.0)))
		ORDER BY event_order ASC
		LIMIT $1
		FOR UPDATE SKIP LOCKED
	)
	UPDATE search_index_events e
	SET locked_at = now(), attempts = attempts + 1, updated_at = now()
	FROM picked
	WHERE e.id = picked.id
	RETURNING e.id, e.entity_type, e.entity_id, e.action, e.event_order, e.created_at`,
		w.options.BatchSize, lockTTLMs, w.options.MaxAttempts, retryDelayMs)
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
		event.Action = strings.TrimSpace(strings.ToLower(event.Action))
		events = append(events, event)
	}
	return events, rows.Err()
}

func (w *Worker) markProcessed(ctx context.Context, ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	_, err := w.service.mainDB.Exec(ctx, `UPDATE search_index_events
		SET processed_at = now(), failed_at = NULL, last_error = NULL, locked_at = NULL, updated_at = now()
		WHERE id = ANY($1)`, ids)
	return err
}

func (w *Worker) markFailed(ctx context.Context, ids []string, err error) {
	if len(ids) == 0 {
		return
	}
	_, _ = w.service.mainDB.Exec(ctx, `UPDATE search_index_events
		SET failed_at = now(), last_error = $2, locked_at = NULL, updated_at = now()
		WHERE id = ANY($1)`, ids, truncateError(err))
}

func (w *Worker) logf(format string, args ...any) {
	if w.options.Logger != nil {
		w.options.Logger.Printf(format, args...)
		return
	}
	log.Printf(format, args...)
}

func collectEventIDs(events []IndexEvent, entityType, entityID string) []string {
	ids := []string{}
	for _, event := range events {
		if event.EntityType == entityType && event.EntityID == entityID {
			ids = append(ids, event.ID)
		}
	}
	return ids
}

func truncateError(err error) string {
	if err == nil {
		return ""
	}
	text := err.Error()
	if len(text) > 1000 {
		return text[:1000]
	}
	return text
}
