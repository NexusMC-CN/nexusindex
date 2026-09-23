package indexer

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Service struct {
	mainDB             *pgxpool.Pool
	indexDB            *pgxpool.Pool
	generations        *GenerationStore
	runtime            *runtimeConfigStore
	pipeline           *QueryPipeline
	cursor             cursorCodec
	mutationRunner     MutationRunner
	maxEventAttempts   int
	fullRebuildRunning atomic.Bool
}

type WriteResult struct {
	Applied bool `json:"applied"`
}
type MutationRunner func(context.Context, func() error) error
type indexWriter interface {
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
}

func (s *Service) SetMutationRunner(runner MutationRunner) { s.mutationRunner = runner }
func (s *Service) RecoverStaleRebuilds(ctx context.Context) error {
	return s.generations.RecoverStaleBuilding(ctx)
}
func (s *Service) runMutation(ctx context.Context, fn func() error) error {
	if s.mutationRunner != nil {
		return s.mutationRunner(ctx, fn)
	}
	return fn()
}

type sourceRow struct {
	entityType    string
	entityID      string
	title         string
	status        string
	visibility    string
	categoryID    string
	authorID      string
	tagsJSON      string
	extraTagsJSON string
	keywordsJSON  string
	description   string
	viewCount     int
	likeCount     int
	downloadCount int
	favoriteCount int
	createdAt     time.Time
	updatedAt     time.Time
	payload       map[string]any
}

type RebuildResult struct {
	JobID int64 `json:"jobId"`
}

var wordPattern = regexp.MustCompile(`[[:alnum:]_]+`)
var opaqueIDPattern = regexp.MustCompile(`(?i)^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func NewService(mainDB, indexDB *pgxpool.Pool) *Service {
	return NewServiceWithRuntime(mainDB, indexDB, DefaultRuntimeConfig())
}

func NewServiceWithRuntime(mainDB, indexDB *pgxpool.Pool, cfg RuntimeConfig) *Service {
	return NewServiceWithRuntimeAndCursor(mainDB, indexDB, cfg, "")
}

func NewServiceWithRuntimeAndCursor(mainDB, indexDB *pgxpool.Pool, cfg RuntimeConfig, cursorSecret string) *Service {
	return &Service{
		mainDB:      mainDB,
		indexDB:     indexDB,
		generations: NewGenerationStore(indexDB),
		runtime:     newRuntimeConfigStore(cfg),
		pipeline:    NewQueryPipeline(),
		cursor:      newCursorCodec(cursorSecret),
	}
}

func (s *Service) RuntimeConfig() RuntimeConfig {
	return s.runtime.Get()
}

func (s *Service) UpdateRuntimeConfig(ctx context.Context, cfg RuntimeConfig) RuntimeConfig {
	return s.runtime.Update(ctx, cfg)
}

func (s *Service) RuntimeConfigVersion() uint64 {
	return s.runtime.Version()
}

func (s *Service) RuntimeSnapshot() RuntimeSnapshot {
	return s.runtime.Snapshot()
}

func (s *Service) PatchRuntimeConfig(ctx context.Context, override RuntimeConfig) RuntimeConfig {
	return s.runtime.Patch(ctx, override)
}

func (s *Service) Ping(ctx context.Context) error {
	if err := s.mainDB.Ping(ctx); err != nil {
		return fmt.Errorf("main readonly database: %w", err)
	}
	if err := s.indexDB.Ping(ctx); err != nil {
		return fmt.Errorf("index database: %w", err)
	}
	return nil
}

func (s *Service) Status(ctx context.Context) (Status, error) {
	status := Status{ByEntityType: map[string]int64{}}
	generation, err := s.generations.Active(ctx)
	if err != nil {
		return status, err
	}
	if err := s.indexDB.QueryRow(ctx, `SELECT count(*) FROM search_index WHERE generation_id = $1`, generation.ID).Scan(&status.Total); err != nil {
		return status, err
	}

	rows, err := s.indexDB.Query(ctx, `SELECT entity_type, count(*) FROM search_index WHERE generation_id = $1 GROUP BY entity_type ORDER BY entity_type`, generation.ID)
	if err != nil {
		return status, err
	}
	defer rows.Close()
	for rows.Next() {
		var entityType string
		var count int64
		if err := rows.Scan(&entityType, &count); err != nil {
			return status, err
		}
		status.ByEntityType[entityType] = count
	}
	if err := rows.Err(); err != nil {
		return status, err
	}

	var recent *time.Time
	if err := s.indexDB.QueryRow(ctx, `SELECT max(finished_at) FROM index_sync_log WHERE status = 'success'`).Scan(&recent); err != nil {
		return status, err
	}
	status.RecentSyncTime = recent

	if err := s.indexDB.QueryRow(ctx, `SELECT count(*) FROM index_sync_log WHERE status = 'failed'`).Scan(&status.FailedJobCount); err != nil {
		return status, err
	}
	if err := s.indexDB.QueryRow(ctx, `SELECT count(*) FROM index_sync_log WHERE status = 'running'`).Scan(&status.RunningJobCount); err != nil {
		return status, err
	}
	_ = s.indexDB.QueryRow(ctx, `SELECT coalesce(error, message, '') FROM index_sync_log WHERE status = 'failed' ORDER BY started_at DESC LIMIT 1`).Scan(&status.LastFailedMessage)
	return status, nil
}

func (s *Service) Search(ctx context.Context, req SearchQuery) (SearchPage, error) {
	return s.SearchWithSnapshot(ctx, req, s.RuntimeSnapshot())
}

// SearchWithSnapshot preserves the request's captured configuration identity
// through query building, scoring and cursor validation/creation.
func (s *Service) SearchWithSnapshot(ctx context.Context, req SearchQuery, snapshot RuntimeSnapshot) (SearchPage, error) {
	var err error
	req, err = normalizeSearchQuery(req)
	if err != nil {
		return SearchPage{}, err
	}
	req.Query = strings.TrimSpace(req.Query)
	req.EntityType = strings.TrimSpace(req.EntityType)
	req.CategoryID = strings.TrimSpace(req.CategoryID)
	req.Status = strings.TrimSpace(req.Status)
	req.Limit = boundInt(req.Limit, 20, 1, 100)
	sortMode, err := normalizeSearchSort(req.Sort)
	if err != nil {
		return SearchPage{}, err
	}
	req.Sort = sortMode

	cfg := snapshot.Config
	structured := s.pipeline.Build(req.Query, cfg.Query)
	if req.Query != "" && structured.TSQueryText == "" {
		return SearchPage{}, &QueryError{Code: "invalid_query_text", Message: "query must contain searchable text"}
	}
	resultQuery := structured
	resultQuery.ExpandedTerms = append([]string{}, structured.ExpandedTerms...)
	for _, clauses := range [][]QueryClause{req.Must, req.Should} {
		for _, clause := range clauses {
			resultQuery.ExpandedTerms = append(resultQuery.ExpandedTerms, s.pipeline.Build(clause.Value, cfg.Query).ExpandedTerms...)
		}
	}
	resultQuery.ExpandedTerms = normalizeStringsPreserveOrder(resultQuery.ExpandedTerms)
	cursor, err := s.cursor.decode(req.Cursor)
	if err != nil {
		return SearchPage{}, err
	}
	if cursor != nil && (cursor.ConfigHash == "" || cursor.ConfigHash != snapshot.Hash) {
		return SearchPage{}, ErrCursorMismatch
	}
	pgStarted := time.Now()
	tx, err := s.indexDB.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	queryDB := &timedQuerier{db: tx, elapsed: time.Since(pgStarted)}
	if err != nil {
		return SearchPage{}, err
	}
	defer func() {
		// A request deadline must not prevent releasing the transaction lease.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanupCtx)
	}()
	generation, err := loadActiveGeneration(ctx, queryDB)
	if err != nil {
		return SearchPage{}, err
	}
	queryHash := searchQueryHash(req)
	configVersion := snapshot.Version
	scoringNow := time.Now()
	if cursor != nil {
		if cursor.Sort != req.Sort || cursor.GenerationID != generation.ID || cursor.QueryHash != queryHash {
			return SearchPage{}, ErrCursorMismatch
		}
		parsed, err := cursor.scoringTime()
		if err != nil {
			return SearchPage{}, ErrInvalidCursor
		}
		scoringNow = parsed
	}

	where := []string{"s.generation_id = $1"}
	args := []any{generation.ID}
	queryArg := 0

	if structured.TSQueryText != "" {
		args = append(args, structured.TSQueryText)
		queryArg = len(args)
		where = append(where, fmt.Sprintf("s.search_vector @@ to_tsquery('simple', $%d)", queryArg))
	}
	where, args = appendSearchFilters(where, args, req.Filters)
	var clauseErr error
	where, args, clauseErr = appendClauses(where, args, req.Must, req.Should, req.MustNot, cfg.Query)
	if clauseErr != nil {
		return SearchPage{}, clauseErr
	}

	whereSQL := strings.Join(where, " AND ")
	filterArgs := append([]any(nil), args...)
	var total int64
	countWhereSQL := strings.ReplaceAll(whereSQL, "s.", "")
	if err := queryDB.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM search_index WHERE %s`, countWhereSQL), filterArgs...).Scan(&total); err != nil {
		return SearchPage{}, err
	}

	fromSQL := "search_index s"
	if queryArg > 0 {
		fromSQL = fmt.Sprintf("search_index s, (SELECT to_tsquery('simple', $%d) AS query) q", queryArg)
	}
	scoringNowArg := ""
	if req.Sort != SortLatest {
		args = append(args, scoringNow.UTC())
		scoringNowArg = fmt.Sprintf("$%d", len(args))
	}
	plan := buildSearchSQLPlan(req.Sort, queryArg > 0, cfg.Scoring, scoringNowArg, func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	}, structured)
	keyset := keysetPredicate(req.Sort, cursor, &args)
	if keyset != "" {
		keyset = "WHERE " + keyset
	}
	args = append(args, req.Limit+1)

	rows, err := queryDB.Query(ctx, fmt.Sprintf(`WITH ranked AS (
		SELECT s.entity_type, s.entity_id, s.title, s.status, coalesce(s.category_id, '') AS category_id, coalesce(s.author_id, '') AS author_id,
			s.tags, s.keywords, s.weight, s.view_count, s.like_count, s.download_count, s.payload, s.created_at, s.updated_at, s.indexed_at,
			%s AS sort_score, coalesce(s.updated_at, s.created_at, s.indexed_at) AS sort_time
		FROM %s WHERE %s
	)
	SELECT entity_type, entity_id, title, status, category_id, author_id, tags, keywords, weight, view_count, like_count, download_count,
		payload, created_at, updated_at, indexed_at, sort_score, sort_time
	FROM ranked %s
	ORDER BY %s
	LIMIT $%d`, plan.scoreExpr, fromSQL, whereSQL, keyset, plan.orderBy, len(args)), args...)
	if err != nil {
		return SearchPage{}, err
	}
	defer rows.Close()

	results := make([]SearchResult, 0, req.Limit+1)
	sortTimes := make([]time.Time, 0, req.Limit+1)
	payloads := make([][]byte, 0, req.Limit+1)
	for rows.Next() {
		var item SearchResult
		var payload []byte
		var sortTime time.Time
		if err := rows.Scan(
			&item.EntityType,
			&item.EntityID,
			&item.Title,
			&item.Status,
			&item.CategoryID,
			&item.AuthorID,
			&item.Tags,
			&item.Keywords,
			&item.Weight,
			&item.ViewCount,
			&item.LikeCount,
			&item.DownloadCount,
			&payload,
			&item.CreatedAt,
			&item.UpdatedAt,
			&item.IndexedAt,
			&item.Score,
			&sortTime,
		); err != nil {
			return SearchPage{}, err
		}
		results = append(results, item)
		sortTimes = append(sortTimes, sortTime)
		payloads = append(payloads, payload)
	}
	if err := rows.Err(); err != nil {
		return SearchPage{}, err
	}
	rows.Close()
	facets, err := loadFacets(ctx, queryDB, where, filterArgs)
	if err != nil {
		return SearchPage{}, err
	}
	// Scores are computed in SQL. Only local decoding, matched fields and
	// response metadata belong to scoring_ms; driver Next/Scan belongs to pg_ms.
	scoringStarted := time.Now()
	for i := range results {
		item := &results[i]
		item.ID = documentID(item.EntityType, item.EntityID)
		item.Payload = map[string]any{}
		if err := decodePayload(payloads[i], &item.Payload); err != nil {
			return SearchPage{}, err
		}
		item.payloadText = flattenPayloadText(item.Payload)
		item.MatchedFields = matchedFields(*item, resultQuery)
		if req.Explain {
			item.Explanation = map[string]any{
				"sort": req.Sort, "scoring_time": scoringNow.UTC().Format(time.RFC3339Nano),
				"score_source": "postgresql", "weights": cfg.Scoring,
			}
		}
	}

	hasMore := len(results) > req.Limit
	if hasMore {
		results = results[:req.Limit]
		sortTimes = sortTimes[:req.Limit]
	}

	nextCursor := ""
	if hasMore && len(results) > 0 {
		last := results[len(results)-1]
		nextCursor, err = s.cursor.encode(searchCursor{
			Sort: req.Sort, Score: last.Score, SortTime: sortTimes[len(sortTimes)-1].UTC().Format(time.RFC3339Nano),
			EntityType: last.EntityType, EntityID: last.EntityID, ScoredAt: scoringNow.UTC().Format(time.RFC3339Nano),
			GenerationID: generation.ID, ConfigVersion: configVersion, ConfigHash: snapshot.Hash, QueryHash: queryHash,
		})
		if err != nil {
			return SearchPage{}, err
		}
	}
	scoringMS := time.Since(scoringStarted).Milliseconds()

	highlightStarted := time.Now()
	for i := range results {
		results[i].Highlight = buildHighlight(results[i], resultQuery.ExpandedTerms)
		results[i].payloadText = ""
	}
	highlightMS := time.Since(highlightStarted).Milliseconds()
	commitStarted := time.Now()
	err = tx.Commit(ctx)
	queryDB.record(commitStarted)
	if err != nil {
		return SearchPage{}, err
	}
	pgMS := queryDB.elapsed.Milliseconds()

	// Lag is optional telemetry from the source database, outside the index
	// snapshot. Release the search connection first (the pools may be shared).
	// Failure, including a telemetry deadline, leaves it unavailable; it does
	// not discard an otherwise completed search. Normal searches do not query it.
	var indexLagMS *int64
	if req.Explain {
		if lag, err := s.IndexLagMS(ctx); err == nil {
			indexLagMS = &lag
		}
	}
	return SearchPage{
		Items:                 results,
		Total:                 total,
		Limit:                 req.Limit,
		NextCursor:            nextCursor,
		CandidateWindow:       len(results),
		ExhaustedCandidateSet: false,
		Sort:                  req.Sort,
		GenerationID:          generation.ID,
		Facets:                facets,
		Timing: SearchTiming{
			PGMS:          pgMS,
			ScoringMS:     scoringMS,
			HighlightMS:   highlightMS,
			CandidateSize: len(results),
			IndexLagMS:    indexLagMS,
		},
	}, nil
}

func (s *Service) IndexLagMS(ctx context.Context) (int64, error) {
	var oldest *time.Time
	if err := s.mainDB.QueryRow(ctx, `SELECT min(created_at) FROM search_index_events WHERE processed_at IS NULL`).Scan(&oldest); err != nil {
		return 0, err
	}
	if oldest == nil {
		return 0, nil
	}
	lag := time.Since(*oldest).Milliseconds()
	if lag < 0 {
		return 0, nil
	}
	return lag, nil
}

func (s *Service) Tags(ctx context.Context, query, sortBy string, limit, offset int) ([]TagResult, int64, error) {
	generation, err := s.generations.Active(ctx)
	if err != nil {
		return nil, 0, err
	}
	args := []any{generation.ID}
	where := "generation_id = $1"
	if q := strings.TrimSpace(query); q != "" {
		args = append(args, "%"+strings.NewReplacer(`\`, `\\`, "%", `\%`, "_", `\_`).Replace(q)+"%")
		where += fmt.Sprintf(" AND tag ILIKE $%d", len(args))
	}
	var total int64
	countSQL := fmt.Sprintf(`SELECT count(*) FROM tag_index WHERE %s`, where)
	if err := s.indexDB.QueryRow(ctx, countSQL, args...).Scan(&total); err != nil {
		return nil, 0, err
	}

	orderBy := "total_count DESC, tag ASC"
	switch strings.TrimSpace(sortBy) {
	case "name":
		orderBy = "tag ASC"
	case "latest":
		orderBy = "updated_at DESC, tag ASC"
	}

	args = append(args, limit, offset)
	rows, err := s.indexDB.Query(ctx, fmt.Sprintf(`SELECT tag, entity_types, total_count, resource_count, post_count, server_count,
		video_count, document_count, user_count, organization_count, tag_count, official_tag_count, category_counts, payload, updated_at
		FROM tag_index WHERE %s ORDER BY %s LIMIT $%d OFFSET $%d`, where, orderBy, len(args)-1, len(args)), args...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()

	results := []TagResult{}
	for rows.Next() {
		var item TagResult
		var categoryCounts []byte
		var payload []byte
		if err := rows.Scan(
			&item.Tag,
			&item.EntityTypes,
			&item.TotalCount,
			&item.ResourceCount,
			&item.PostCount,
			&item.ServerCount,
			&item.VideoCount,
			&item.DocumentCount,
			&item.UserCount,
			&item.OrganizationCount,
			&item.TagCount,
			&item.OfficialTagCount,
			&categoryCounts,
			&payload,
			&item.UpdatedAt,
		); err != nil {
			return nil, 0, err
		}
		item.CategoryCounts = map[string]int{}
		item.Payload = map[string]any{}
		_ = json.Unmarshal(categoryCounts, &item.CategoryCounts)
		_ = json.Unmarshal(payload, &item.Payload)
		results = append(results, item)
	}
	return results, total, rows.Err()
}

func (s *Service) StartFullRebuild(ctx context.Context) (RebuildResult, error) {
	jobID, err := s.startJob(ctx, "full_rebuild", "", "")
	if err != nil {
		return RebuildResult{}, err
	}
	go s.runFullRebuild(context.Background(), jobID)
	return RebuildResult{JobID: jobID}, nil
}

func (s *Service) StartEntityRebuild(ctx context.Context, entityType, entityID string) (RebuildResult, error) {
	if !validEntityType(entityType) {
		return RebuildResult{}, fmt.Errorf("unsupported entityType: %s", entityType)
	}
	if strings.TrimSpace(entityID) == "" {
		return RebuildResult{}, errors.New("entityId is required")
	}
	jobID, err := s.startJob(ctx, "entity_rebuild", entityType, entityID)
	if err != nil {
		return RebuildResult{}, err
	}
	go s.runEntityRebuild(context.Background(), jobID, entityType, entityID)
	return RebuildResult{JobID: jobID}, nil
}

func (s *Service) runFullRebuild(ctx context.Context, jobID int64) {
	// Admission precedes the session lease. Otherwise concurrent jobs can each
	// reserve a connection and then wait forever for a second pool connection.
	ctx, cancel := context.WithTimeout(ctx, 30*time.Minute)
	defer cancel()
	if !s.fullRebuildRunning.CompareAndSwap(false, true) {
		rejectCtx, rejectCancel := context.WithTimeout(ctx, 5*time.Second)
		defer rejectCancel()
		s.finishJob(rejectCtx, jobID, "failed", "full rebuild already running", ErrRebuildInProgress, nil)
		return
	}
	defer s.fullRebuildRunning.Store(false)
	start := time.Now()
	leaseCtx, leaseCancel := context.WithTimeout(ctx, 5*time.Second)
	lease, err := s.indexDB.Acquire(leaseCtx)
	leaseCancel()
	if err != nil {
		s.finishJob(ctx, jobID, "failed", "full rebuild lease failed", err, nil)
		return
	}
	defer lease.Release()
	var acquired bool
	if err = lease.QueryRow(ctx, `SELECT pg_try_advisory_lock($1)`, -jobID).Scan(&acquired); err != nil || !acquired {
		if err == nil {
			err = ErrRebuildInProgress
		}
		s.finishJob(ctx, jobID, "failed", "full rebuild lease failed", err, nil)
		return
	}
	defer func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if _, err := lease.Exec(unlockCtx, `SELECT pg_advisory_unlock($1)`, -jobID); err != nil {
			_ = lease.Conn().Close(context.Background())
		}
	}()
	watermark, err := s.committedOutboxWatermark(ctx)
	if err != nil {
		s.finishJob(ctx, jobID, "failed", "full rebuild failed", err, map[string]any{"durationMs": time.Since(start).Milliseconds()})
		return
	}
	result, err := NewRebuildCoordinator(&serviceRebuildBackend{service: s}).Run(ctx, jobID, watermark)
	if err != nil {
		s.finishJob(ctx, jobID, "failed", "full rebuild failed", err, map[string]any{"durationMs": time.Since(start).Milliseconds()})
		return
	}
	s.finishJob(ctx, jobID, "success", "full rebuild completed", nil, map[string]any{"durationMs": time.Since(start).Milliseconds(), "indexed": result.Indexed, "generationId": result.GenerationID})
}

func (s *Service) runEntityRebuild(ctx context.Context, jobID int64, entityType, entityID string) {
	start := time.Now()
	err := s.runMutation(ctx, func() error {
		sourceVersion, err := s.sourceVersion(ctx, entityType, entityID)
		if err != nil {
			return err
		}
		doc, found, err := s.fetchOne(ctx, entityType, entityID)
		if err != nil {
			return err
		}
		if !found {
			_, err := s.markDeleted(ctx, entityType, entityID, sourceVersion, true)
			return err
		}
		_, err = s.UpsertVersioned(ctx, doc, sourceVersion)
		return err
	})
	if err != nil {
		s.finishJob(ctx, jobID, "failed", "entity rebuild failed", err, map[string]any{"durationMs": time.Since(start).Milliseconds()})
		return
	}
	s.finishJob(ctx, jobID, "success", "entity rebuild completed", nil, map[string]any{"durationMs": time.Since(start).Milliseconds()})
}

func (s *Service) rebuildEntityTypeInto(ctx context.Context, entityType string, generationID int64) (int, error) {
	const batchSize = 500
	total := 0
	afterID := ""
	for {
		docs, err := s.fetchMany(ctx, entityType, afterID, batchSize)
		if err != nil {
			return total, err
		}
		if len(docs) == 0 {
			return total, nil
		}
		if err := s.populateSourceVersions(ctx, entityType, docs); err != nil {
			return total, err
		}
		for _, doc := range docs {
			if _, err := s.upsertIntoGeneration(ctx, generationID, doc, doc.SourceVersion, true); err != nil {
				return total, err
			}
			total++
		}
		afterID = docs[len(docs)-1].EntityID
	}
}

func (s *Service) Upsert(ctx context.Context, doc SearchDocument) (WriteResult, error) {
	entityType, err := normalizeEntityType(doc.EntityType)
	if err != nil {
		return WriteResult{}, err
	}
	doc.EntityType = entityType
	return s.upsert(ctx, doc, 0, false)
}

func (s *Service) UpsertVersioned(ctx context.Context, doc SearchDocument, sourceVersion int64) (WriteResult, error) {
	if sourceVersion < 0 {
		return WriteResult{}, errors.New("sourceVersion must not be negative")
	}
	entityType, err := normalizeEntityType(doc.EntityType)
	if err != nil {
		return WriteResult{}, err
	}
	doc.EntityType = entityType
	return s.upsert(ctx, doc, sourceVersion, true)
}

func normalizeEntityType(entityType string) (string, error) {
	entityType = strings.TrimSpace(entityType)
	if entityType == "" {
		entityType = EntityDocument
	}
	if !validEntityType(entityType) {
		return "", fmt.Errorf("unsupported entityType: %s", entityType)
	}
	return entityType, nil
}

func sourceVersionConflict(versioned bool) (string, string) {
	if versioned {
		return "source_version = EXCLUDED.source_version", " WHERE search_index.source_version <= EXCLUDED.source_version"
	}
	return "source_version = search_index.source_version", " WHERE search_index.source_version = 0"
}

func (s *Service) upsert(ctx context.Context, doc SearchDocument, sourceVersion int64, versioned bool) (WriteResult, error) {
	return s.writeActive(ctx, func(tx pgx.Tx, generationID int64) (WriteResult, error) {
		return s.upsertWithWriter(ctx, tx, generationID, doc, sourceVersion, versioned)
	})
}

// The active generation is selected after taking the same transaction lock used
// by activation. Documents and their derived tags commit together.
func (s *Service) writeActive(ctx context.Context, write func(pgx.Tx, int64) (WriteResult, error)) (WriteResult, error) {
	tx, err := s.indexDB.Begin(ctx)
	if err != nil {
		return WriteResult{}, err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, generationSwitchLockKey); err != nil {
		return WriteResult{}, err
	}
	var generationID int64
	if err = tx.QueryRow(ctx, `SELECT id FROM index_generations WHERE status='active'`).Scan(&generationID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			err = ErrIndexUnavailable
		}
		return WriteResult{}, err
	}
	result, err := write(tx, generationID)
	if err != nil {
		return WriteResult{}, err
	}
	if result.Applied {
		if err = s.rebuildTagsInTransaction(ctx, tx, generationID); err != nil {
			return WriteResult{}, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return WriteResult{}, err
	}
	return result, nil
}

func (s *Service) upsertIntoGeneration(ctx context.Context, generationID int64, doc SearchDocument, sourceVersion int64, versioned bool) (WriteResult, error) {
	return s.upsertWithWriter(ctx, s.indexDB, generationID, doc, sourceVersion, versioned)
}

func (s *Service) upsertWithWriter(ctx context.Context, writer indexWriter, generationID int64, doc SearchDocument, sourceVersion int64, versioned bool) (WriteResult, error) {
	payload, err := json.Marshal(doc.Payload)
	if err != nil {
		return WriteResult{}, err
	}
	doc.EntityType = strings.TrimSpace(doc.EntityType)
	doc.EntityID = strings.TrimSpace(doc.EntityID)
	if doc.EntityType == "" {
		doc.EntityType = EntityDocument
	}
	if doc.EntityID == "" {
		doc.EntityID = strings.TrimSpace(doc.DocID)
	}
	if doc.EntityID == "" {
		return WriteResult{}, errors.New("entityId or docId is required")
	}
	doc.Status = strings.ToLower(strings.TrimSpace(doc.Status))
	if doc.Status == "" {
		doc.Status = "active"
	}
	doc.Tags = normalizeStrings(doc.Tags)
	doc.Keywords = normalizeStrings(doc.Keywords)
	titleIndexText := ftsText(doc.Title)
	tagsIndexText := ftsText(strings.Join(doc.Tags, " "))
	keywordsIndexText := ftsText(strings.Join(doc.Keywords, " "))
	payloadIndexText := ftsText(flattenPayloadText(doc.Payload) + " " + doc.indexText)
	versionUpdate, versionGuard := sourceVersionConflict(versioned)
	query := fmt.Sprintf(`INSERT INTO search_index (
		generation_id, entity_type, entity_id, title, status, category_id, author_id, tags, keywords, weight,
		view_count, like_count, download_count, payload, created_at, updated_at, source_version, indexed_at, search_vector
	) VALUES (
		$1, $2, $3, $4, $5, nullif($6, ''), nullif($7, ''), $8, $9, $10, $11, $12, $13, $14::jsonb, coalesce($15::timestamptz,now()), coalesce($16::timestamptz,now()), $17, now(),
		setweight(to_tsvector('simple', $18), 'A') ||
		setweight(to_tsvector('simple', $19), 'B') ||
		setweight(to_tsvector('simple', $20), 'C') ||
		setweight(to_tsvector('simple', $21), 'D')
	)
	ON CONFLICT (generation_id, entity_type, entity_id) DO UPDATE SET
		title = EXCLUDED.title,
		status = EXCLUDED.status,
		category_id = EXCLUDED.category_id,
		author_id = EXCLUDED.author_id,
		tags = EXCLUDED.tags,
		keywords = EXCLUDED.keywords,
		weight = EXCLUDED.weight,
		view_count = EXCLUDED.view_count,
		like_count = EXCLUDED.like_count,
		download_count = EXCLUDED.download_count,
		payload = EXCLUDED.payload,
		created_at = coalesce($15::timestamptz, search_index.created_at, EXCLUDED.created_at),
		updated_at = EXCLUDED.updated_at,
		%s,
		indexed_at = now(),
		search_vector = EXCLUDED.search_vector%s`, versionUpdate, versionGuard)
	tag, err := writer.Exec(ctx, query,
		generationID,
		doc.EntityType,
		doc.EntityID,
		doc.Title,
		doc.Status,
		doc.CategoryID,
		doc.AuthorID,
		doc.Tags,
		doc.Keywords,
		doc.Weight,
		doc.ViewCount,
		doc.LikeCount,
		doc.DownloadCount,
		string(payload),
		nullTime(doc.CreatedAt),
		nullTime(doc.UpdatedAt),
		sourceVersion,
		titleIndexText,
		tagsIndexText,
		keywordsIndexText,
		payloadIndexText,
	)
	return WriteResult{Applied: err == nil && tag.RowsAffected() == 1}, err
}

func nullTime(value time.Time) any {
	if value.IsZero() {
		return nil
	}
	return value
}

func (s *Service) RebuildTags(ctx context.Context) error {
	generation, err := s.generations.Active(ctx)
	if err != nil {
		return err
	}
	return s.rebuildTagsForGeneration(ctx, generation.ID)
}

func (s *Service) rebuildTagsForGeneration(ctx context.Context, generationID int64) error {
	tx, err := s.indexDB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err := s.rebuildTagsInTransaction(ctx, tx, generationID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) rebuildTagsInTransaction(ctx context.Context, tx pgx.Tx, generationID int64) error {
	return s.rebuildSelectedTagsInTransaction(ctx, tx, generationID, nil)
}

func (s *Service) rebuildSelectedTagsInTransaction(ctx context.Context, tx pgx.Tx, generationID int64, tags []string) error {
	// Lock the generation row before taking the aggregate snapshot. This also
	// serializes standalone rebuilds when there are no tag rows yet to lock.
	if _, err := tx.Exec(ctx, `SELECT id FROM index_generations WHERE id=$1 FOR UPDATE`, generationID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM tag_index WHERE generation_id = $1 AND ($2::text[] IS NULL OR tag=ANY($2))`, generationID, tags); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `WITH raw AS (
		SELECT lower(trim(tag)) AS tag, entity_type, category_id, coalesce(updated_at,created_at,indexed_at) AS source_updated_at
		FROM search_index
		CROSS JOIN LATERAL unnest(tags) AS tag
		WHERE generation_id = $1
			AND ($2::text[] IS NULL OR tags && $2)
			AND ($2::text[] IS NULL OR lower(trim(tag))=ANY($2))
			AND status NOT IN ('hidden', 'deleted')
			AND entity_type NOT IN ('tag', 'official_tag')
			AND trim(tag) <> ''
			AND trim(tag) !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
	),
	category_stats AS (
		SELECT tag, category_id, count(*)::int AS category_count
		FROM raw
		WHERE category_id IS NOT NULL AND category_id <> ''
		GROUP BY tag, category_id
	),
	category_totals AS (
		SELECT tag, jsonb_object_agg(category_id, category_count) AS category_counts
		FROM category_stats
		GROUP BY tag
	),
	tag_stats AS (
		SELECT
			tag,
			max(source_updated_at) AS updated_at,
			array_agg(DISTINCT entity_type ORDER BY entity_type) AS entity_types,
			count(*)::int AS total_count,
			count(*) FILTER (WHERE entity_type = 'resource')::int AS resource_count,
			count(*) FILTER (WHERE entity_type = 'post')::int AS post_count,
			count(*) FILTER (WHERE entity_type = 'server')::int AS server_count,
			count(*) FILTER (WHERE entity_type = 'video')::int AS video_count,
			count(*) FILTER (WHERE entity_type = 'document')::int AS document_count,
			count(*) FILTER (WHERE entity_type = 'user')::int AS user_count,
			count(*) FILTER (WHERE entity_type = 'organization')::int AS organization_count,
			count(*) FILTER (WHERE entity_type = 'tag')::int AS tag_count,
			count(*) FILTER (WHERE entity_type = 'official_tag')::int AS official_tag_count
		FROM raw
		GROUP BY tag
	)
	INSERT INTO tag_index (
		generation_id, tag, entity_types, total_count, resource_count, post_count, server_count, video_count,
		document_count, user_count, organization_count, tag_count, official_tag_count,
		category_counts, payload, updated_at
	)
	SELECT $1, tag, entity_types, total_count, resource_count, post_count, server_count, video_count,
		document_count, user_count, organization_count, tag_count, official_tag_count,
		coalesce(category_totals.category_counts, '{}'::jsonb),
		jsonb_build_object('source', 'search_index'),
		tag_stats.updated_at
	FROM tag_stats
	LEFT JOIN category_totals USING (tag)`, generationID, tags)
	if err != nil {
		return err
	}
	return nil
}

func (s *Service) markDeleted(ctx context.Context, entityType, entityID string, sourceVersion int64, versioned bool) (WriteResult, error) {
	return s.writeActive(ctx, func(tx pgx.Tx, generationID int64) (WriteResult, error) {
		return s.markDeletedWithWriter(ctx, tx, generationID, entityType, entityID, sourceVersion, versioned)
	})
}

func (s *Service) markDeletedWithWriter(ctx context.Context, writer indexWriter, generationID int64, entityType, entityID string, sourceVersion int64, versioned bool) (WriteResult, error) {
	versionUpdate, versionGuard := sourceVersionConflict(versioned)
	query := fmt.Sprintf(`INSERT INTO search_index (
		generation_id, entity_type, entity_id, title, status, tags, keywords, payload, source_version, indexed_at, search_vector,created_at,updated_at
	) VALUES ($1, $2, $3, '', 'deleted', '{}', '{}', '{"reason":"source entity not found"}'::jsonb, $4, now(), ''::tsvector,now(),now())
	ON CONFLICT (generation_id, entity_type, entity_id) DO UPDATE SET
		status = 'deleted',
		payload = search_index.payload || '{"reason":"source entity not found"}'::jsonb,
		%s,
		search_vector = ''::tsvector,
		indexed_at = now()%s`, versionUpdate, versionGuard)
	tag, err := writer.Exec(ctx, query, generationID, entityType, entityID, sourceVersion)
	return WriteResult{Applied: err == nil && tag.RowsAffected() == 1}, err
}

func (s *Service) MarkDeleted(ctx context.Context, entityType, entityID string) (WriteResult, error) {
	var err error
	entityType, err = normalizeEntityType(entityType)
	if err != nil {
		return WriteResult{}, err
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return WriteResult{}, errors.New("entityId is required")
	}
	return s.markDeleted(ctx, entityType, entityID, 0, false)
}

func (s *Service) MarkDeletedVersioned(ctx context.Context, entityType, entityID string, sourceVersion int64) (WriteResult, error) {
	var err error
	entityType, err = normalizeEntityType(entityType)
	if err != nil {
		return WriteResult{}, err
	}
	entityID = strings.TrimSpace(entityID)
	if entityID == "" {
		return WriteResult{}, errors.New("entityId is required")
	}
	if sourceVersion < 0 {
		return WriteResult{}, errors.New("sourceVersion must not be negative")
	}
	return s.markDeleted(ctx, entityType, entityID, sourceVersion, true)
}

func (s *Service) sourceVersion(ctx context.Context, entityType, entityID string) (int64, error) {
	var version int64
	err := s.mainDB.QueryRow(ctx, `SELECT coalesce(max(event_order), 0)
		FROM search_index_events
		WHERE entity_type = $1 AND entity_id = $2`, entityType, entityID).Scan(&version)
	return version, err
}

func (s *Service) populateSourceVersions(ctx context.Context, entityType string, docs []SearchDocument) error {
	if len(docs) == 0 {
		return nil
	}
	ids := make([]string, 0, len(docs))
	for _, doc := range docs {
		ids = append(ids, doc.EntityID)
	}
	rows, err := s.mainDB.Query(ctx, `SELECT entity_id, max(event_order)
		FROM search_index_events
		WHERE entity_type = $1 AND entity_id = ANY($2)
		GROUP BY entity_id`, entityType, ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	versions := make(map[string]int64, len(docs))
	for rows.Next() {
		var entityID string
		var version int64
		if err := rows.Scan(&entityID, &version); err != nil {
			return err
		}
		versions[entityID] = version
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for i := range docs {
		docs[i].SourceVersion = versions[docs[i].EntityID]
	}
	return nil
}

func (s *Service) startJob(ctx context.Context, jobType, entityType, entityID string) (int64, error) {
	var id int64
	err := s.indexDB.QueryRow(ctx, `INSERT INTO index_sync_log (job_type, entity_type, entity_id, status, message)
		VALUES ($1, nullif($2, ''), nullif($3, ''), 'running', 'job queued')
		RETURNING id`, jobType, entityType, entityID).Scan(&id)
	return id, err
}

func (s *Service) finishJob(ctx context.Context, id int64, status, message string, jobErr error, details map[string]any) {
	detailsJSON, _ := json.Marshal(details)
	errText := ""
	if jobErr != nil {
		errText = jobErr.Error()
	}
	_, _ = s.indexDB.Exec(ctx, `UPDATE index_sync_log SET status = $2, message = $3, error = nullif($4, ''),
		finished_at = now(), details = $5::jsonb WHERE id = $1`, id, status, message, errText, string(detailsJSON))
}

func (s *Service) fetchMany(ctx context.Context, entityType, afterID string, limit int) ([]SearchDocument, error) {
	sql, err := selectSQL(entityType, false)
	if err != nil {
		return nil, err
	}
	rows, err := s.mainDB.Query(ctx, sql, afterID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanDocuments(rows)
}

const MaxSourceEntityReferences = 10_000

type SourceEntityReference struct {
	EntityType string
	EntityID   string
	Title      string
	UpdatedAt  time.Time
}

// SourceEntityReferences reads only the identity and content version needed by
// the stability tool. Worker processing still re-reads the full source row.
func (s *Service) SourceEntityReferences(ctx context.Context, entityType string, limit int) ([]SourceEntityReference, error) {
	entityType = strings.TrimSpace(entityType)
	if !validEntityType(entityType) {
		return nil, fmt.Errorf("unsupported entityType: %s", entityType)
	}
	if limit < 1 {
		return nil, errors.New("source entity reference limit must be positive")
	}
	if limit > MaxSourceEntityReferences {
		return nil, fmt.Errorf("source entity reference limit exceeds %d", MaxSourceEntityReferences)
	}
	query, err := sourceEntityReferenceSQL(entityType)
	if err != nil {
		return nil, err
	}
	rows, err := s.mainDB.Query(ctx, query, entityType, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	refs := make([]SourceEntityReference, 0, limit)
	for rows.Next() {
		var ref SourceEntityReference
		if err := rows.Scan(&ref.EntityType, &ref.EntityID, &ref.Title, &ref.UpdatedAt); err != nil {
			return nil, err
		}
		refs = append(refs, ref)
	}
	return refs, rows.Err()
}

func sourceEntityReferenceSQL(entityType string) (string, error) {
	table := ""
	titleColumn := ""
	updatedColumn := "updatedAt"
	switch entityType {
	case EntityResource:
		table, titleColumn = "Resource", "title"
	case EntityPost:
		table, titleColumn = "Post", "title"
	case EntityServer:
		table, titleColumn = "PlayerServer", "title"
	case EntityVideo:
		table, titleColumn = "Video", "title"
	case EntityDocument:
		table, titleColumn = "CustomPage", "title"
	case EntityUser:
		table, titleColumn = "User", "username"
	case EntityOrganization:
		table, titleColumn = "Organization", "name"
	case EntityTag:
		table, titleColumn, updatedColumn = "Tag", "name", "createdAt"
	case EntityOfficialTag:
		table, titleColumn = "OfficialTag", "name"
	default:
		return "", fmt.Errorf("unsupported entityType: %s", entityType)
	}
	return fmt.Sprintf(`SELECT $1::text, src.id::text, coalesce(src.%s::text, ''), src.%s
		FROM %s src ORDER BY src.id ASC LIMIT $2`,
		pgx.Identifier{titleColumn}.Sanitize(),
		pgx.Identifier{updatedColumn}.Sanitize(),
		pgx.Identifier{table}.Sanitize(),
	), nil
}

func (s *Service) fetchOne(ctx context.Context, entityType, entityID string) (SearchDocument, bool, error) {
	return s.fetchOneFrom(ctx, s.mainDB, entityType, entityID)
}

type sourceReader interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
}

func (s *Service) fetchOneFrom(ctx context.Context, reader sourceReader, entityType, entityID string) (SearchDocument, bool, error) {
	sql, err := selectSQL(entityType, true)
	if err != nil {
		return SearchDocument{}, false, err
	}
	rows, err := reader.Query(ctx, sql, entityID)
	if err != nil {
		return SearchDocument{}, false, err
	}
	defer rows.Close()
	docs, err := scanDocuments(rows)
	if err != nil {
		return SearchDocument{}, false, err
	}
	if len(docs) == 0 {
		return SearchDocument{}, false, nil
	}
	return docs[0], true, nil
}

func scanDocuments(rows pgx.Rows) ([]SearchDocument, error) {
	docs := []SearchDocument{}
	for rows.Next() {
		row := sourceRow{payload: map[string]any{}}
		var payload []byte
		if err := rows.Scan(
			&row.entityType,
			&row.entityID,
			&row.title,
			&row.status,
			&row.visibility,
			&row.categoryID,
			&row.authorID,
			&row.tagsJSON,
			&row.extraTagsJSON,
			&row.keywordsJSON,
			&row.description,
			&row.viewCount,
			&row.likeCount,
			&row.downloadCount,
			&row.favoriteCount,
			&row.createdAt,
			&row.updatedAt,
			&payload,
		); err != nil {
			return nil, err
		}
		if err := decodePayload(payload, &row.payload); err != nil {
			return nil, err
		}
		docs = append(docs, buildDocument(row))
	}
	return docs, rows.Err()
}

func buildDocument(row sourceRow) SearchDocument {
	extraTags := parseStringArray(row.extraTagsJSON)
	tags := mergeStrings(parseStringArray(row.tagsJSON))
	if row.entityType == EntityResource {
		tags = mergeStrings(tags, extraTags)
	}
	descriptionKeywords := keywordTokens(row.description)
	indexText := ""
	if row.entityType == EntityPost {
		// Paid access is independent of accessMode. Keep every post body out of
		// public payload/keywords and therefore out of response highlights.
		descriptionKeywords = nil
		indexText = row.description
	}
	keywords := mergeStrings(parseStringArray(row.keywordsJSON), tags, extraTags, keywordTokens(row.title), descriptionKeywords, []string{row.categoryID, row.entityType})
	status := normalizeStatus(row.status, row.visibility)
	weight := row.viewCount + row.likeCount*5 + row.downloadCount*3 + row.favoriteCount*2
	if status == "hidden" || status == "deleted" {
		weight = 0
	}
	if row.payload == nil {
		row.payload = map[string]any{}
	}
	row.payload["statusSource"] = row.status
	row.payload["visibility"] = row.visibility

	return SearchDocument{
		DocID:         documentID(row.entityType, row.entityID),
		EntityType:    row.entityType,
		EntityID:      row.entityID,
		Title:         row.title,
		Status:        status,
		CategoryID:    row.categoryID,
		AuthorID:      row.authorID,
		Tags:          tags,
		Keywords:      keywords,
		Weight:        weight,
		ViewCount:     row.viewCount,
		LikeCount:     row.likeCount,
		DownloadCount: row.downloadCount,
		Payload:       row.payload,
		CreatedAt:     row.createdAt,
		UpdatedAt:     row.updatedAt,
		indexText:     indexText,
	}
}

func normalizeStatus(status, visibility string) string {
	s := strings.ToLower(strings.TrimSpace(status))
	v := strings.ToLower(strings.TrimSpace(visibility))
	if s == "deleted" || s == "removed" {
		return "deleted"
	}
	if v != "" && v != "public" {
		return "hidden"
	}
	if s == "approved" || s == "published" || s == "active" {
		return "active"
	}
	return "hidden"
}

func parseStringArray(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var arr []any
	if err := json.Unmarshal([]byte(raw), &arr); err == nil {
		values := make([]string, 0, len(arr))
		for _, item := range arr {
			switch value := item.(type) {
			case string:
				values = append(values, value)
			case map[string]any:
				values = append(values, firstStringField(value, "name", "tag", "title", "slug"))
			}
		}
		return values
	}
	parts := strings.Split(raw, ",")
	for i := range parts {
		parts[i] = strings.TrimSpace(parts[i])
	}
	return parts
}

func firstStringField(value map[string]any, keys ...string) string {
	for _, key := range keys {
		if field, ok := value[key].(string); ok {
			field = strings.TrimSpace(field)
			if field != "" {
				return field
			}
		}
	}
	return ""
}

func keywordTokens(value string) []string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return nil
	}
	tokens := wordPattern.FindAllString(value, -1)
	if len([]rune(value)) <= 64 {
		tokens = append(tokens, value)
	}
	return tokens
}

func mergeStrings(groups ...[]string) []string {
	seen := map[string]struct{}{}
	merged := []string{}
	for _, group := range groups {
		for _, item := range group {
			item = strings.ToLower(strings.TrimSpace(item))
			if !isDisplayTagCandidate(item) {
				continue
			}
			if _, ok := seen[item]; ok {
				continue
			}
			seen[item] = struct{}{}
			merged = append(merged, item)
		}
	}
	sort.Strings(merged)
	return merged
}

func isDisplayTagCandidate(item string) bool {
	if item == "" {
		return false
	}
	if opaqueIDPattern.MatchString(item) {
		return false
	}
	if utf8.RuneCountInString(item) > 64 {
		return false
	}
	return true
}

func validEntityType(entityType string) bool {
	for _, item := range EntityTypes {
		if item == entityType {
			return true
		}
	}
	return false
}

func selectSQL(entityType string, single bool) (string, error) {
	where := `WHERE src.id > $1`
	limit := `LIMIT $2`
	if single {
		where = `WHERE src.id = $1`
		limit = ""
	}
	switch entityType {
	case EntityResource:
		return fmt.Sprintf(`SELECT 'resource', src.id, src.title, src.status, src.visibility, coalesce(src.category, ''),
			src."authorId", coalesce(src.tags, '[]'),
			coalesce((
				SELECT jsonb_agg(DISTINCT ot.name)::text
				FROM jsonb_array_elements_text(coalesce(nullif(src."officialTags", ''), '[]')::jsonb) selected_official_tag(tag_id)
				JOIN "OfficialTag" ot ON ot.id = selected_official_tag.tag_id AND ot.enabled
				JOIN "OfficialTagGroup" otg ON otg.id = ot."groupId" AND otg.enabled
			), '[]'),
			coalesce(src."mcVersions", '[]'),
			coalesce(src.description, ''), src.views, src."likeCount", src."downloadCount", src."favoriteCount",
			src."createdAt", src."updatedAt",
			jsonb_build_object(
				'publicId', src."publicId", 'slug', src.slug, 'customSlug', src."customSlug", 'description', src.description,
				'platform', src.platform, 'subCategory', src."subCategory", 'thirdLevelCats', src."thirdLevelCats",
				'mcVersions', src."mcVersions", 'officialTags', src."officialTags", 'coverImage', src."coverImage",
				'ratingAverage', src."ratingAverage", 'ratingCount', src."ratingCount", 'version', src.version,
				'isPaid', src."isPaid", 'attributionType', src."attributionType",
				'author', jsonb_build_object('id', usr.id, 'uid', usr.uid, 'username', usr.username, 'slug', usr.slug, 'avatar', usr.avatar, 'role', usr.role),
				'category', jsonb_build_object('id', cat.id, 'name', cat.name, 'slug', cat.slug, 'icon', cat.icon, 'color', cat.color)
			)
			FROM "Resource" src
			LEFT JOIN "User" usr ON usr.id = src."authorId"
			LEFT JOIN LATERAL (SELECT id, name, slug, icon, color FROM "Category" c WHERE c.id = src.category OR c.slug = src.category LIMIT 1) cat ON true
			%s ORDER BY src.id ASC %s`, where, limit), nil
	case EntityPost:
		return fmt.Sprintf(`SELECT 'post', src.id, src.title,
			CASE WHEN src."publishedAt" IS NOT NULL AND src."accessMode" = 'none' THEN src.status ELSE 'hidden' END,
			src.visibility, src."boardId",
			src."authorId", coalesce(src.tags, '[]'), coalesce(src."topicTags", '[]'), '[]',
			coalesce(src.content, ''), src.views, src."likeCount", 0, src."favoriteCount",
			src."createdAt", src."updatedAt",
			jsonb_build_object(
				'publicId', src."publicId", 'slug', src.slug, 'customSlug', src."customSlug", 'postType', src."postType",
				'isPinned', src."isPinned", 'isHighlighted', src."isHighlighted", 'topicTagId', src."topicTagId",
				'topicTagName', src."topicTagName", 'publishedAt', src."publishedAt", 'accessMode', src."accessMode",
				'author', jsonb_build_object('id', usr.id, 'uid', usr.uid, 'username', usr.username, 'slug', usr.slug, 'avatar', usr.avatar, 'role', usr.role),
				'category', jsonb_build_object('id', board.id, 'name', board.name, 'slug', board.slug, 'icon', board.icon, 'color', board.color)
			)
			FROM "Post" src
			LEFT JOIN "User" usr ON usr.id = src."authorId"
			LEFT JOIN "Board" board ON board.id = src."boardId"
			%s ORDER BY src.id ASC %s`, where, limit), nil
	case EntityServer:
		return fmt.Sprintf(`SELECT 'server', src.id, src.title, src.status, src.visibility, coalesce(src.category, ''),
			src."authorId", coalesce(src.tags, '[]'), coalesce(src.versions, '[]'), coalesce(src."networkEnvironments", '[]'),
			coalesce(src.description, ''), src.views, src."likeCount", 0, src."favoriteCount",
			src."createdAt", src."updatedAt",
			jsonb_build_object(
				'publicId', src."publicId", 'slug', src.slug, 'customSlug', src."customSlug", 'description', src.description,
				'platform', src.platform, 'versions', src.versions, 'networkEnvironments', src."networkEnvironments",
				'onlineMode', src."onlineMode", 'hideConnectionAddress', src."hideConnectionAddress", 'website', src.website,
				'coverImage', src."coverImage", 'onlineCount', src."onlineCount", 'maxPlayers', src."maxPlayers",
				'ratingAverage', src."ratingAverage", 'ratingCount', src."ratingCount",
				'author', jsonb_build_object('id', usr.id, 'uid', usr.uid, 'username', usr.username, 'slug', usr.slug, 'avatar', usr.avatar, 'role', usr.role),
				'category', jsonb_build_object('id', cat.id, 'name', cat.name, 'slug', cat.slug, 'icon', cat.icon, 'color', cat.color)
			)
			FROM "PlayerServer" src
			LEFT JOIN "User" usr ON usr.id = src."authorId"
			LEFT JOIN LATERAL (SELECT id, name, slug, icon, color FROM "PlayerServerCategory" c WHERE c.id = src.category OR c.slug = src.category LIMIT 1) cat ON true
			%s ORDER BY src.id ASC %s`, where, limit), nil
	case EntityVideo:
		return fmt.Sprintf(`SELECT 'video', src.id, src.title, src.status, src.visibility, src."categoryId",
			src."authorId", coalesce(src.tags, '[]'), '[]', '[]',
			coalesce(src.description, ''), src.views, src."likeCount", 0, src."favoriteCount",
			src."createdAt", src."updatedAt",
			jsonb_build_object(
				'publicId', src."publicId", 'slug', src.slug, 'customSlug', src."customSlug", 'description', src.description,
				'sourceUrl', src."sourceUrl", 'sourceType', src."sourceType", 'sourceId', src."sourceId",
				'coverImage', src."coverImage", 'replyCount', src."replyCount",
				'author', jsonb_build_object('id', usr.id, 'uid', usr.uid, 'username', usr.username, 'slug', usr.slug, 'avatar', usr.avatar, 'role', usr.role),
				'category', jsonb_build_object('id', cat.id, 'name', cat.name, 'slug', cat.slug, 'icon', cat.icon, 'color', cat.color)
			)
			FROM "Video" src
			LEFT JOIN "User" usr ON usr.id = src."authorId"
			LEFT JOIN "VideoCategory" cat ON cat.id = src."categoryId"
			%s ORDER BY src.id ASC %s`, where, limit), nil
	case EntityDocument:
		return fmt.Sprintf(`SELECT 'document', src.id, src.title,
			CASE WHEN src.published THEN 'published' ELSE 'draft' END,
			CASE WHEN src.published THEN 'public' ELSE 'private' END,
			coalesce(src.category, ''),
			'', '[]', jsonb_build_array(coalesce(nullif(src.category, ''), 'document'))::text, '[]',
			coalesce(src.description, '') || ' ' || coalesce(src.content, ''), 0, 0, 0, 0,
			src."createdAt", src."updatedAt",
			jsonb_build_object(
				'slug', src.slug, 'description', src.description, 'category', src.category,
				'icon', src.icon, 'published', src.published, 'sortOrder', src."sortOrder"
			)
			FROM "CustomPage" src
			%s ORDER BY src.id ASC %s`, where, limit), nil
	case EntityUser:
		return fmt.Sprintf(`SELECT 'user', src.id, src.username, src.status, 'public', coalesce(src.role, ''),
			src.id, '[]', jsonb_build_array('user', coalesce(src.role, 'user'))::text,
			jsonb_build_array(src.username, coalesce(src.slug, ''), src.uid::text)::text,
			coalesce(src.bio, '') || ' ' || coalesce(src."profileReadme", ''), 0, 0, 0, 0,
			src."createdAt", src."updatedAt",
			jsonb_build_object(
				'id', src.id, 'uid', src.uid, 'username', src.username, 'slug', src.slug,
				'avatar', src.avatar, 'bio', src.bio, 'profileReadme', src."profileReadme",
				'role', src.role, 'status', src.status, 'createdAt', src."createdAt"
			)
			FROM "User" src
			%s ORDER BY src.id ASC %s`, where, limit), nil
	case EntityOrganization:
		return fmt.Sprintf(`SELECT 'organization', src.id, src.name, src.status, src.visibility, coalesce(src.category, ''),
			src."ownerId", '[]', jsonb_build_array('organization', coalesce(nullif(src.category, ''), 'general'))::text,
			jsonb_build_array(src.name, src.slug, coalesce(src.tagline, ''))::text,
			coalesce(src.tagline, '') || ' ' || coalesce(src.description, '') || ' ' || coalesce(src."homepageContent", ''),
			0, 0, 0, src."memberCount",
			src."createdAt", src."updatedAt",
			jsonb_build_object(
				'slug', src.slug, 'name', src.name, 'tagline', src.tagline, 'description', src.description,
				'avatarImage', src."avatarImage", 'coverImage', src."coverImage", 'category', src.category,
				'joinPolicy', src."joinPolicy", 'memberCount', src."memberCount",
				'owner', jsonb_build_object('id', owner.id, 'uid', owner.uid, 'username', owner.username, 'slug', owner.slug, 'avatar', owner.avatar, 'role', owner.role)
			)
			FROM "Organization" src
			LEFT JOIN "User" owner ON owner.id = src."ownerId"
			%s ORDER BY src.id ASC %s`, where, limit), nil
	case EntityTag:
		return fmt.Sprintf(`SELECT 'tag', src.id, src.name, 'active', 'public', 'tag',
			'', jsonb_build_array(src.name)::text, '[]', jsonb_build_array(src.name, src.slug)::text,
			src.name, src.count, 0, 0, 0,
			src."createdAt", src."createdAt",
			jsonb_build_object('name', src.name, 'slug', src.slug, 'count', src.count)
			FROM "Tag" src
			%s ORDER BY src.id ASC %s`, where, limit), nil
	case EntityOfficialTag:
		return fmt.Sprintf(`SELECT 'official_tag', src.id, src.name,
			CASE WHEN src.enabled AND grp.enabled THEN 'active' ELSE 'hidden' END,
			CASE WHEN src.enabled AND grp.enabled THEN 'public' ELSE 'private' END,
			src."groupId",
			'', jsonb_build_array(src.name, grp.name)::text, '[]', jsonb_build_array(src.name, src.slug, grp.name, grp.slug)::text,
			coalesce(src.description, '') || ' ' || coalesce(grp.description, ''), 0, 0, 0, 0,
			src."createdAt", src."updatedAt",
			jsonb_build_object(
				'name', src.name, 'slug', src.slug, 'icon', src.icon, 'description', src.description,
				'sortOrder', src."sortOrder", 'enabled', src.enabled,
				'group', jsonb_build_object('id', grp.id, 'name', grp.name, 'slug', grp.slug, 'description', grp.description, 'enabled', grp.enabled)
			)
			FROM "OfficialTag" src
			LEFT JOIN "OfficialTagGroup" grp ON grp.id = src."groupId"
			%s ORDER BY src.id ASC %s`, where, limit), nil
	default:
		return "", fmt.Errorf("unsupported entityType: %s", entityType)
	}
}

func boundInt(value, fallback, min, max int) int {
	if value == 0 && fallback > 0 {
		return fallback
	}
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func normalizeStrings(values []string) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func documentID(entityType, entityID string) string {
	entityType = strings.TrimSpace(entityType)
	entityID = strings.TrimSpace(entityID)
	if entityType == "" {
		entityType = EntityDocument
	}
	return entityType + ":" + entityID
}

func searchTerms(query string) []string {
	return normalizeStrings(tokenizeForSearch(query))
}

func ftsText(value string) string {
	return strings.Join(tokenizeForSearch(value), " ")
}

func tokenizeForSearch(value string) []string {
	return tokenizeWithDictionary(value, nil)
}

func isHan(r rune) bool {
	return unicode.In(r, unicode.Han)
}

func scoreResult(item SearchResult, query StructuredQuery, cfg ScoringConfig, explain bool, pgRank float64, scoringNow time.Time) (float64, []string, map[string]any) {
	terms := query.Terms
	if len(query.ExpandedTerms) > 0 {
		terms = query.ExpandedTerms
	}
	titleScore := fieldMatchScore(item.Title, query.Normalized, terms) * cfg.TitleWeight
	tagsScore := fieldMatchScore(strings.Join(item.Tags, " "), query.Normalized, terms) * cfg.TagsWeight
	keywordsScore := fieldMatchScore(strings.Join(item.Keywords, " "), query.Normalized, terms) * cfg.KeywordsWeight
	payloadScore := fieldMatchScore(item.payloadText, query.Normalized, terms) * cfg.PayloadWeight

	matched := []string{}
	if titleScore > 0 {
		matched = append(matched, "title")
	}
	if tagsScore > 0 {
		matched = append(matched, "tags")
	}
	if keywordsScore > 0 {
		matched = append(matched, "keywords")
	}
	if payloadScore > 0 {
		matched = append(matched, "payload")
	}

	textScore := titleScore + tagsScore + keywordsScore + payloadScore
	boostScore := queryBoostScore(item, query)
	popularityScore := math.Log1p(float64(item.ViewCount))*cfg.ViewCountWeight +
		math.Log1p(float64(item.DownloadCount))*cfg.DownloadCountWeight +
		math.Log1p(float64(item.LikeCount))*cfg.LikeCountWeight +
		math.Log1p(float64(item.Weight))*0.05
	freshnessScore := 0.0
	if !item.UpdatedAt.IsZero() {
		ageDays := scoringNow.Sub(item.UpdatedAt).Hours() / 24
		if ageDays < 0 {
			ageDays = 0
		}
		freshnessScore = math.Exp(-ageDays / cfg.TimeDecayFactor)
	}
	if len(terms) == 0 {
		textScore = 0
	}
	score := (textScore+boostScore)*cfg.TextScoreWeight + popularityScore*cfg.PopularityScoreWeight + freshnessScore*cfg.FreshnessScoreWeight
	score = math.Round(score*1000000) / 1000000
	if !explain {
		return score, matched, nil
	}
	return score, matched, map[string]any{
		"pg_rank":           pgRank,
		"title_score":       titleScore,
		"tags_score":        tagsScore,
		"keywords_score":    keywordsScore,
		"payload_score":     payloadScore,
		"query_boost_score": boostScore,
		"text_score":        textScore,
		"popularity_score":  popularityScore,
		"freshness_score":   freshnessScore,
		"weights":           cfg,
		"query":             query,
	}
}

func queryBoostScore(item SearchResult, query StructuredQuery) float64 {
	if len(query.Boosts) == 0 {
		return 0
	}
	searchable := strings.ToLower(item.Title + " " + strings.Join(item.Tags, " ") + " " + strings.Join(item.Keywords, " ") + " " + item.payloadText)
	score := 0.0
	for key, boost := range query.Boosts {
		parts := strings.SplitN(key, ":", 2)
		if len(parts) != 2 {
			continue
		}
		if strings.Contains(searchable, parts[1]) {
			score += boost
		}
	}
	return score
}

func fieldMatchScore(value, query string, terms []string) float64 {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || len(terms) == 0 {
		return 0
	}
	score := 0.0
	for _, term := range terms {
		if term == "" {
			continue
		}
		if strings.Contains(value, term) {
			score += 1
		}
	}
	query = strings.ToLower(strings.TrimSpace(query))
	if query != "" && strings.Contains(value, query) {
		score += 1.5
	}
	return score / float64(len(terms)+1)
}

func flattenPayloadText(payload map[string]any) string {
	var b strings.Builder
	appendPayloadText(&b, payload, 0)
	text := b.String()
	const maxRunes = 20000
	if utf8.RuneCountInString(text) <= maxRunes {
		return text
	}
	runes := []rune(text)
	return string(runes[:maxRunes])
}

func decodePayload(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	return decoder.Decode(target)
}

func decimalNumberText(number json.Number) string {
	raw := number.String()
	parts := strings.FieldsFunc(raw, func(r rune) bool { return r == 'e' || r == 'E' })
	if len(parts) != 2 {
		return raw
	}
	exponent, err := strconv.Atoi(parts[1])
	// Payload search text has a 20,000-rune cap; do not allocate an unbounded
	// decimal expansion for an adversarial exponent.
	if err != nil || exponent < -20000 || exponent > 20000 {
		return raw
	}
	mantissa, sign := parts[0], ""
	if strings.HasPrefix(mantissa, "-") {
		sign, mantissa = "-", mantissa[1:]
	}
	point := strings.IndexByte(mantissa, '.')
	if point < 0 {
		point = len(mantissa)
	}
	digits := strings.ReplaceAll(mantissa, ".", "")
	point += exponent
	switch {
	case point <= 0:
		return sign + "0." + strings.Repeat("0", -point) + digits
	case point >= len(digits):
		return sign + digits + strings.Repeat("0", point-len(digits))
	default:
		return sign + digits[:point] + "." + digits[point:]
	}
}

func appendPayloadText(b *strings.Builder, value any, depth int) {
	if depth > 8 || b.Len() > 120000 {
		return
	}
	switch v := value.(type) {
	case string:
		if strings.TrimSpace(v) != "" {
			b.WriteByte(' ')
			b.WriteString(v)
		}
	case json.Number:
		b.WriteByte(' ')
		b.WriteString(decimalNumberText(v))
	case float64:
		b.WriteByte(' ')
		b.WriteString(strconv.FormatFloat(v, 'f', -1, 64))
	case float32:
		b.WriteByte(' ')
		b.WriteString(strconv.FormatFloat(float64(v), 'f', -1, 32))
	case int, int64, int32, bool:
		b.WriteByte(' ')
		b.WriteString(fmt.Sprint(v))
	case []any:
		for _, item := range v {
			appendPayloadText(b, item, depth+1)
		}
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			appendPayloadText(b, v[key], depth+1)
		}
	}
}

func buildHighlight(item SearchResult, terms []string) map[string]any {
	highlight := map[string]any{}
	if len(terms) == 0 {
		return highlight
	}
	if hasAnyTerm(item.Title, terms) {
		highlight["title"] = highlightText(item.Title, terms, 160)
	}
	if hasAnyTerm(strings.Join(item.Tags, " "), terms) {
		values := make([]string, 0, len(item.Tags))
		for _, tag := range item.Tags {
			if hasAnyTerm(tag, terms) {
				values = append(values, highlightText(tag, terms, 80))
			}
		}
		highlight["tags"] = values
	}
	if hasAnyTerm(strings.Join(item.Keywords, " "), terms) {
		values := make([]string, 0, len(item.Keywords))
		for _, keyword := range item.Keywords {
			if hasAnyTerm(keyword, terms) {
				values = append(values, highlightText(keyword, terms, 80))
			}
		}
		highlight["keywords"] = values
	}
	payloadText := item.payloadText
	if payloadText == "" {
		payloadText = flattenPayloadText(item.Payload)
	}
	if hasAnyTerm(payloadText, terms) {
		highlight["payload"] = []string{highlightText(payloadText, terms, 220)}
	}
	return highlight
}

func hasAnyTerm(value string, terms []string) bool {
	value = strings.ToLower(value)
	for _, term := range terms {
		if term != "" && strings.Contains(value, term) {
			return true
		}
	}
	return false
}

func highlightText(value string, terms []string, maxRunes int) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	runes := []rune(value)
	lower := []rune(strings.ToLower(value))
	type span struct{ start, end int }
	spans := []span{}
	for _, term := range terms {
		needle := []rune(strings.ToLower(term))
		if len(needle) == 0 {
			continue
		}
		for i := 0; i+len(needle) <= len(lower); i++ {
			matches := true
			for j, r := range needle {
				if lower[i+j] != r {
					matches = false
					break
				}
			}
			if matches {
				spans = append(spans, span{i, i + len(needle)})
			}
		}
	}
	sort.Slice(spans, func(i, j int) bool {
		if spans[i].start == spans[j].start {
			return spans[i].end > spans[j].end
		}
		return spans[i].start < spans[j].start
	})
	merged := []span{}
	for _, match := range spans {
		if len(merged) > 0 && match.start <= merged[len(merged)-1].end {
			if match.end > merged[len(merged)-1].end {
				merged[len(merged)-1].end = match.end
			}
		} else {
			merged = append(merged, match)
		}
	}
	windowStart, windowEnd := 0, len(runes)
	if maxRunes > 0 && len(runes) > maxRunes {
		if len(merged) > 0 {
			windowStart = merged[0].start - maxRunes/3
		}
		if windowStart < 0 {
			windowStart = 0
		}
		windowEnd = windowStart + maxRunes
		if windowEnd > len(runes) {
			windowEnd = len(runes)
			windowStart = windowEnd - maxRunes
		}
	}
	var out strings.Builder
	if windowStart > 0 {
		out.WriteString("...")
	}
	position := windowStart
	for _, match := range merged {
		if match.end <= windowStart || match.start >= windowEnd {
			continue
		}
		start, end := max(match.start, windowStart), min(match.end, windowEnd)
		out.WriteString(html.EscapeString(string(runes[position:start])))
		out.WriteString("<mark>")
		out.WriteString(html.EscapeString(string(runes[start:end])))
		out.WriteString("</mark>")
		position = end
	}
	out.WriteString(html.EscapeString(string(runes[position:windowEnd])))
	if windowEnd < len(runes) {
		out.WriteString("...")
	}
	return out.String()
}
