package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Service struct {
	mainDB      *pgxpool.Pool
	indexDB     *pgxpool.Pool
	generations *GenerationStore
	runtime     *runtimeConfigStore
	pipeline    *QueryPipeline
	cursor      cursorCodec
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

	cfg := s.RuntimeConfig()
	structured := s.pipeline.Build(req.Query, cfg.Query)
	cursor, err := s.cursor.decode(req.Cursor)
	if err != nil {
		return SearchPage{}, err
	}
	generation, err := s.generations.Active(ctx)
	if err != nil {
		return SearchPage{}, err
	}
	queryHash := searchQueryHash(req)
	configVersion := s.RuntimeConfigVersion()
	scoringNow := time.Now()
	if cursor != nil {
		if cursor.Sort != req.Sort || cursor.GenerationID != generation.ID || cursor.ConfigVersion != configVersion || cursor.QueryHash != queryHash {
			return SearchPage{}, errors.New("cursor does not match this search")
		}
		parsed, err := cursor.scoringTime()
		if err != nil {
			return SearchPage{}, errors.New("invalid cursor")
		}
		scoringNow = parsed
	}

	where := []string{"s.generation_id = $1"}
	args := []any{generation.ID}
	queryArg := 0

	if structured.TSQueryText != "" {
		args = append(args, structured.TSQueryText)
		queryArg = len(args)
		where = append(where, fmt.Sprintf("s.search_vector @@ plainto_tsquery('simple', $%d)", queryArg))
	}
	where, args = appendSearchFilters(where, args, req.Filters)
	var clauseErr error
	where, args, clauseErr = appendClauses(where, args, req.Must, req.Should, req.MustNot)
	if clauseErr != nil {
		return SearchPage{}, clauseErr
	}

	whereSQL := strings.Join(where, " AND ")
	filterArgs := append([]any(nil), args...)
	var total int64
	countWhereSQL := strings.ReplaceAll(whereSQL, "s.", "")
	if err := s.indexDB.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM search_index WHERE %s`, countWhereSQL), filterArgs...).Scan(&total); err != nil {
		return SearchPage{}, err
	}

	fromSQL := "search_index s"
	if queryArg > 0 {
		fromSQL = fmt.Sprintf("search_index s, (SELECT plainto_tsquery('simple', $%d) AS query) q", queryArg)
	}
	scoringNowArg := ""
	if req.Sort != SortLatest {
		args = append(args, scoringNow.UTC())
		scoringNowArg = fmt.Sprintf("$%d", len(args))
	}
	plan := buildSearchSQLPlan(req.Sort, queryArg > 0, cfg.Scoring, scoringNowArg, func(value any) string {
		args = append(args, value)
		return fmt.Sprintf("$%d", len(args))
	})
	keyset := keysetPredicate(req.Sort, cursor, &args)
	if keyset != "" {
		keyset = "WHERE " + keyset
	}
	args = append(args, req.Limit+1)

	pgStarted := time.Now()
	rows, err := s.indexDB.Query(ctx, fmt.Sprintf(`WITH ranked AS (
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
	scoringStarted := time.Now()
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
		item.ID = documentID(item.EntityType, item.EntityID)
		item.Payload = map[string]any{}
		_ = json.Unmarshal(payload, &item.Payload)
		item.payloadText = flattenPayloadText(item.Payload)
		item.MatchedFields = matchedFields(item, structured)
		if req.Explain {
			item.Explanation = map[string]any{
				"sort": req.Sort, "scoring_time": scoringNow.UTC().Format(time.RFC3339Nano),
				"score_source": "postgresql", "weights": cfg.Scoring,
			}
		}
		results = append(results, item)
		sortTimes = append(sortTimes, sortTime)
	}
	if err := rows.Err(); err != nil {
		return SearchPage{}, err
	}
	pgMS := scoringStarted.Sub(pgStarted).Milliseconds()
	scoringMS := time.Since(scoringStarted).Milliseconds()

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
			GenerationID: generation.ID, ConfigVersion: configVersion, QueryHash: queryHash,
		})
		if err != nil {
			return SearchPage{}, err
		}
	}

	highlightStarted := time.Now()
	for i := range results {
		results[i].Highlight = buildHighlight(results[i], structured.Terms)
		results[i].payloadText = ""
	}
	highlightMS := time.Since(highlightStarted).Milliseconds()
	facets, err := s.loadFacets(ctx, where, filterArgs)
	if err != nil {
		return SearchPage{}, err
	}

	indexLagMS := int64(0)
	if req.Explain {
		indexLagMS = s.IndexLagMS(ctx)
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

func (s *Service) IndexLagMS(ctx context.Context) int64 {
	var oldest *time.Time
	if err := s.mainDB.QueryRow(ctx, `SELECT min(created_at) FROM search_index_events WHERE processed_at IS NULL`).Scan(&oldest); err != nil {
		return 0
	}
	if oldest == nil {
		return 0
	}
	lag := time.Since(*oldest).Milliseconds()
	if lag < 0 {
		return 0
	}
	return lag
}

func (s *Service) Tags(ctx context.Context, query, sortBy string, limit, offset int) ([]TagResult, int64, error) {
	generation, err := s.generations.Active(ctx)
	if err != nil {
		return nil, 0, err
	}
	args := []any{generation.ID}
	where := "generation_id = $1"
	if q := strings.TrimSpace(query); q != "" {
		args = append(args, "%"+q+"%")
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
	start := time.Now()
	var watermark int64
	if err := s.mainDB.QueryRow(ctx, `SELECT coalesce(max(event_order), 0) FROM search_index_events`).Scan(&watermark); err != nil {
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
	err := func() error {
		sourceVersion, err := s.sourceVersion(ctx, entityType, entityID)
		if err != nil {
			return err
		}
		doc, found, err := s.fetchOne(ctx, entityType, entityID)
		if err != nil {
			return err
		}
		if !found {
			return s.markDeleted(ctx, entityType, entityID, sourceVersion, true)
		}
		if err := s.UpsertVersioned(ctx, doc, sourceVersion); err != nil {
			return err
		}
		return s.RebuildTags(ctx)
	}()
	if err != nil {
		s.finishJob(ctx, jobID, "failed", "entity rebuild failed", err, map[string]any{"durationMs": time.Since(start).Milliseconds()})
		return
	}
	s.finishJob(ctx, jobID, "success", "entity rebuild completed", nil, map[string]any{"durationMs": time.Since(start).Milliseconds()})
}

func (s *Service) rebuildEntityType(ctx context.Context, entityType string) (int, error) {
	generation, err := s.generations.Active(ctx)
	if err != nil {
		return 0, err
	}
	return s.rebuildEntityTypeInto(ctx, entityType, generation.ID)
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
			if err := s.upsertIntoGeneration(ctx, generationID, doc, doc.SourceVersion, true); err != nil {
				return total, err
			}
			total++
		}
		afterID = docs[len(docs)-1].EntityID
	}
}

func (s *Service) Upsert(ctx context.Context, doc SearchDocument) error {
	return s.upsert(ctx, doc, 0, false)
}

func (s *Service) UpsertVersioned(ctx context.Context, doc SearchDocument, sourceVersion int64) error {
	if sourceVersion < 0 {
		return errors.New("sourceVersion must not be negative")
	}
	return s.upsert(ctx, doc, sourceVersion, true)
}

func sourceVersionConflict(versioned bool) (string, string) {
	if versioned {
		return "source_version = EXCLUDED.source_version", " WHERE search_index.source_version <= EXCLUDED.source_version"
	}
	return "source_version = search_index.source_version", " WHERE search_index.source_version = 0"
}

func (s *Service) upsert(ctx context.Context, doc SearchDocument, sourceVersion int64, versioned bool) error {
	generation, err := s.generations.Active(ctx)
	if err != nil {
		return err
	}
	return s.upsertIntoGeneration(ctx, generation.ID, doc, sourceVersion, versioned)
}

func (s *Service) upsertIntoGeneration(ctx context.Context, generationID int64, doc SearchDocument, sourceVersion int64, versioned bool) error {
	payload, err := json.Marshal(doc.Payload)
	if err != nil {
		return err
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
		return errors.New("entityId or docId is required")
	}
	if strings.TrimSpace(doc.Status) == "" {
		doc.Status = "active"
	}
	doc.Tags = normalizeStrings(doc.Tags)
	doc.Keywords = normalizeStrings(doc.Keywords)
	titleIndexText := ftsText(doc.Title)
	tagsIndexText := ftsText(strings.Join(doc.Tags, " "))
	keywordsIndexText := ftsText(strings.Join(doc.Keywords, " "))
	payloadIndexText := ftsText(flattenPayloadText(doc.Payload))
	versionUpdate, versionGuard := sourceVersionConflict(versioned)
	query := fmt.Sprintf(`INSERT INTO search_index (
		generation_id, entity_type, entity_id, title, status, category_id, author_id, tags, keywords, weight,
		view_count, like_count, download_count, payload, created_at, updated_at, source_version, indexed_at, search_vector
	) VALUES (
		$1, $2, $3, $4, $5, nullif($6, ''), nullif($7, ''), $8, $9, $10, $11, $12, $13, $14::jsonb, $15, $16, $17, now(),
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
		created_at = EXCLUDED.created_at,
		updated_at = EXCLUDED.updated_at,
		%s,
		indexed_at = now(),
		search_vector = EXCLUDED.search_vector%s`, versionUpdate, versionGuard)
	_, err = s.indexDB.Exec(ctx, query,
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
		doc.CreatedAt,
		doc.UpdatedAt,
		sourceVersion,
		titleIndexText,
		tagsIndexText,
		keywordsIndexText,
		payloadIndexText,
	)
	return err
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
	if _, err := tx.Exec(ctx, `DELETE FROM tag_index WHERE generation_id = $1`, generationID); err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `WITH raw AS (
		SELECT lower(trim(tag)) AS tag, entity_type, category_id
		FROM search_index
		CROSS JOIN LATERAL unnest(tags) AS tag
		WHERE generation_id = $1
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
		now()
	FROM tag_stats
	LEFT JOIN category_totals USING (tag)`, generationID)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) markDeleted(ctx context.Context, entityType, entityID string, sourceVersion int64, versioned bool) error {
	generation, err := s.generations.Active(ctx)
	if err != nil {
		return err
	}
	return s.markDeletedInGeneration(ctx, generation.ID, entityType, entityID, sourceVersion, versioned)
}

func (s *Service) markDeletedInGeneration(ctx context.Context, generationID int64, entityType, entityID string, sourceVersion int64, versioned bool) error {
	versionUpdate, versionGuard := sourceVersionConflict(versioned)
	query := fmt.Sprintf(`INSERT INTO search_index (
		generation_id, entity_type, entity_id, title, status, tags, keywords, payload, source_version, indexed_at, search_vector
	) VALUES ($1, $2, $3, '', 'deleted', '{}', '{}', '{"reason":"source entity not found"}'::jsonb, $4, now(), ''::tsvector)
	ON CONFLICT (generation_id, entity_type, entity_id) DO UPDATE SET
		status = 'deleted',
		payload = search_index.payload || '{"reason":"source entity not found"}'::jsonb,
		%s,
		search_vector = ''::tsvector,
		indexed_at = now()%s`, versionUpdate, versionGuard)
	_, err := s.indexDB.Exec(ctx, query, generationID, entityType, entityID, sourceVersion)
	return err
}

func (s *Service) MarkDeleted(ctx context.Context, entityType, entityID string) error {
	entityType = strings.TrimSpace(entityType)
	entityID = strings.TrimSpace(entityID)
	if entityType == "" {
		entityType = EntityDocument
	}
	if entityID == "" {
		return errors.New("entityId is required")
	}
	return s.markDeleted(ctx, entityType, entityID, 0, false)
}

func (s *Service) MarkDeletedVersioned(ctx context.Context, entityType, entityID string, sourceVersion int64) error {
	entityType = strings.TrimSpace(entityType)
	entityID = strings.TrimSpace(entityID)
	if entityType == "" {
		entityType = EntityDocument
	}
	if entityID == "" {
		return errors.New("entityId is required")
	}
	if sourceVersion < 0 {
		return errors.New("sourceVersion must not be negative")
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

func (s *Service) fetchOne(ctx context.Context, entityType, entityID string) (SearchDocument, bool, error) {
	sql, err := selectSQL(entityType, true)
	if err != nil {
		return SearchDocument{}, false, err
	}
	rows, err := s.mainDB.Query(ctx, sql, entityID)
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
		_ = json.Unmarshal(payload, &row.payload)
		docs = append(docs, buildDocument(row))
	}
	return docs, rows.Err()
}

func buildDocument(row sourceRow) SearchDocument {
	tags := mergeStrings(parseStringArray(row.tagsJSON))
	extraTags := parseStringArray(row.extraTagsJSON)
	keywords := mergeStrings(parseStringArray(row.keywordsJSON), tags, extraTags, keywordTokens(row.title), keywordTokens(row.description), []string{row.categoryID, row.entityType})
	status := normalizeStatus(row.status, row.visibility)
	weight := row.viewCount + row.likeCount*5 + row.downloadCount*3 + row.favoriteCount*2
	if status == "hidden" || status == "deleted" {
		weight = 0
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
	if entityType == "" || entityType == EntityDocument {
		return entityID
	}
	return entityType + ":" + entityID
}

func searchTerms(query string) []string {
	return normalizeStrings(tokenizeForSearch(query))
}

func ftsText(value string) string {
	return strings.Join(normalizeStrings(tokenizeForSearch(value)), " ")
}

func tokenizeForSearch(value string) []string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return nil
	}
	tokens := []string{}
	var current strings.Builder
	flush := func() {
		if current.Len() == 0 {
			return
		}
		tokens = append(tokens, current.String())
		current.Reset()
	}

	for _, r := range value {
		switch {
		case isHan(r):
			flush()
			tokens = append(tokens, string(r))
		case unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-':
			current.WriteRune(r)
		default:
			flush()
		}
	}
	flush()

	if utf8.RuneCountInString(value) <= 64 {
		compacted := strings.Join(wordPattern.FindAllString(value, -1), "")
		if compacted != "" {
			tokens = append(tokens, compacted)
		}
	}
	return tokens
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
	case float64, float32, int, int64, int32, bool:
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
	lower := strings.ToLower(value)
	start := 0
	for _, term := range terms {
		if term == "" {
			continue
		}
		if idx := strings.Index(lower, term); idx >= 0 {
			start = idx
			break
		}
	}
	runes := []rune(value)
	if maxRunes > 0 && len(runes) > maxRunes {
		byteStart := 0
		if start > 0 {
			byteStart = utf8.RuneCountInString(value[:start])
		}
		windowStart := byteStart - maxRunes/3
		if windowStart < 0 {
			windowStart = 0
		}
		windowEnd := windowStart + maxRunes
		if windowEnd > len(runes) {
			windowEnd = len(runes)
			windowStart = windowEnd - maxRunes
			if windowStart < 0 {
				windowStart = 0
			}
		}
		value = string(runes[windowStart:windowEnd])
		if windowStart > 0 {
			value = "..." + value
		}
		if windowEnd < len(runes) {
			value += "..."
		}
	}

	escaped := html.EscapeString(value)
	for _, term := range terms {
		if term == "" {
			continue
		}
		escaped = highlightEscapedTerm(escaped, html.EscapeString(term))
	}
	return escaped
}

func highlightEscapedTerm(value, term string) string {
	lowerValue := strings.ToLower(value)
	lowerTerm := strings.ToLower(term)
	if lowerTerm == "" {
		return value
	}
	var out strings.Builder
	for {
		idx := strings.Index(lowerValue, lowerTerm)
		if idx < 0 {
			out.WriteString(value)
			break
		}
		out.WriteString(value[:idx])
		out.WriteString("<mark>")
		out.WriteString(value[idx : idx+len(term)])
		out.WriteString("</mark>")
		value = value[idx+len(term):]
		lowerValue = lowerValue[idx+len(term):]
	}
	return out.String()
}
