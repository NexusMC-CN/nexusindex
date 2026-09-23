package indexer

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"
	"unicode/utf8"
)

type CacheInvalidator func(context.Context) error

type WorkerOptions struct {
	Enabled          bool
	BatchSize        int
	PollInterval     time.Duration
	LockTTL          time.Duration
	RetryDelay       time.Duration
	MaxAttempts      int
	OperationTimeout time.Duration
	MutationRunner   MutationRunner
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
	LockedAt   time.Time
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
	if options.OperationTimeout <= 0 || options.OperationTimeout >= options.LockTTL {
		options.OperationTimeout = options.LockTTL / 2
	}
	if service != nil {
		service.maxEventAttempts = options.MaxAttempts
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
	for {
		count, err := w.processBatch(ctx)
		if err != nil {
			w.logf("nexusindex worker process failed: %v", err)
		}
		if ctx.Err() != nil {
			return
		}
		if count > 0 && err == nil {
			continue
		}
		timer := time.NewTimer(w.options.PollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

func (w *Worker) ProcessOnce(ctx context.Context) error {
	_, err := w.processBatch(ctx)
	return err
}

func (w *Worker) processBatch(ctx context.Context) (count int, resultErr error) {
	ctx, cancel := context.WithTimeout(ctx, w.options.OperationTimeout)
	defer cancel()
	events, err := w.claimEvents(ctx)
	if err != nil {
		return 0, err
	}
	if len(events) == 0 {
		return 0, nil
	}
	// A lease is not an indexing attempt. Cache precommit, completion, and ack
	// failures must leave the event eligible even with MaxAttempts=1. Release
	// only this claim's timestamp so a delayed cleanup cannot clear a new lease.
	claimedAt := events[0].LockedAt
	defer func() {
		releaseCtx, releaseCancel := context.WithTimeout(context.WithoutCancel(ctx), w.options.OperationTimeout)
		defer releaseCancel()
		ids := make([]string, 0, len(events))
		for _, event := range events {
			ids = append(ids, event.ID)
		}
		_, err := w.service.mainDB.Exec(releaseCtx, `UPDATE search_index_events SET locked_at=NULL,updated_at=now() WHERE id=ANY($1) AND processed_at IS NULL AND locked_at=$2`, ids, claimedAt)
		if err != nil {
			resultErr = errors.Join(resultErr, fmt.Errorf("release event claim: %w", err))
		}
	}()

	latestByDoc := mergeLatestEvents(events)

	completed := []string{}
	var processingErr error
	operation := func() error {
		for _, event := range latestByDoc {
			ids := collectEventIDs(events, event.EntityType, event.EntityID)
			if err := w.processEvent(ctx, event); err != nil {
				failureCtx, failureCancel := context.WithTimeout(context.WithoutCancel(ctx), w.options.OperationTimeout)
				markErr := w.markFailed(failureCtx, ids, claimedAt, err)
				failureCancel()
				processingErr = errors.Join(processingErr, err, markErr)
				if ctx.Err() != nil {
					break
				}
				continue
			}
			completed = append(completed, ids...)
		}
		return nil
	}
	if w.options.MutationRunner != nil {
		err = w.options.MutationRunner(ctx, operation)
	} else {
		err = operation()
		if w.cacheInvalidator != nil {
			if invalidationErr := w.cacheInvalidator(ctx); invalidationErr != nil {
				return len(events), errors.Join(processingErr, err, invalidationErr)
			}
		}
	}
	// A fence completion failure is not an acknowledged commit. Successful
	// document/tag updates are idempotent and will be retried after the lease.
	if err != nil {
		return len(events), errors.Join(processingErr, err)
	}
	if markErr := w.markProcessed(ctx, completed, claimedAt); markErr != nil {
		return len(events), errors.Join(processingErr, markErr)
	}
	return len(events), processingErr
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
		_, err := w.service.MarkDeletedVersioned(ctx, entityType, entityID, event.EventOrder)
		return err
	}
	doc, found, err := w.service.fetchOne(ctx, entityType, entityID)
	if err != nil {
		return err
	}
	if !found {
		_, err := w.service.MarkDeletedVersioned(ctx, entityType, entityID, event.EventOrder)
		return err
	}
	_, err = w.service.UpsertVersioned(ctx, doc, event.EventOrder)
	return err
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
	SET locked_at = now(), updated_at = now()
	FROM picked
	WHERE e.id = picked.id
	RETURNING e.id, e.entity_type, e.entity_id, e.action, e.event_order, e.created_at,e.locked_at`,
		w.options.BatchSize, lockTTLMs, w.options.MaxAttempts, retryDelayMs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events := []IndexEvent{}
	for rows.Next() {
		var event IndexEvent
		if err := rows.Scan(&event.ID, &event.EntityType, &event.EntityID, &event.Action, &event.EventOrder, &event.CreatedAt, &event.LockedAt); err != nil {
			return nil, err
		}
		event.Action = strings.TrimSpace(strings.ToLower(event.Action))
		events = append(events, event)
	}
	return events, rows.Err()
}

func (w *Worker) markProcessed(ctx context.Context, ids []string, claimedAt time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	tag, err := w.service.mainDB.Exec(ctx, `UPDATE search_index_events
		SET processed_at = now(), failed_at = NULL, last_error = NULL, locked_at = NULL, updated_at = now()
		WHERE id = ANY($1) AND locked_at=$2`, ids, claimedAt)
	if err == nil && tag.RowsAffected() != int64(len(ids)) {
		return errors.New("event acknowledgement lost claim ownership")
	}
	return err
}

func (w *Worker) markFailed(ctx context.Context, ids []string, claimedAt time.Time, cause error) error {
	if len(ids) == 0 {
		return nil
	}
	tag, err := w.service.mainDB.Exec(ctx, `UPDATE search_index_events
		SET failed_at = now(), last_error = $2, locked_at = NULL, attempts=attempts+1, updated_at = now()
		WHERE id = ANY($1) AND locked_at=$3`, ids, truncateError(cause), claimedAt)
	if err != nil {
		return fmt.Errorf("persist event failure: %w", err)
	}
	if tag.RowsAffected() != int64(len(ids)) {
		return errors.New("persist event failure lost claim ownership")
	}
	return nil
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
		end := 1000
		for end > 0 && !utf8.RuneStart(text[end]) {
			end--
		}
		return text[:end]
	}
	return text
}
