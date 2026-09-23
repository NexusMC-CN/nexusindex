package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blockbridge/avmcbbs/apps/nexusindex/internal/indexer"
)

type indexService interface {
	Ping(context.Context) error
	Status(context.Context) (indexer.Status, error)
	SearchWithSnapshot(context.Context, indexer.SearchQuery, indexer.RuntimeSnapshot) (indexer.SearchPage, error)
	Tags(context.Context, string, string, int, int) ([]indexer.TagResult, int64, error)
	RuntimeConfig() indexer.RuntimeConfig
	UpdateRuntimeConfig(context.Context, indexer.RuntimeConfig) indexer.RuntimeConfig
	PatchRuntimeConfig(context.Context, indexer.RuntimeConfig) indexer.RuntimeConfig
	RuntimeSnapshot() indexer.RuntimeSnapshot
	MarkDeleted(context.Context, string, string) (indexer.WriteResult, error)
	Upsert(context.Context, indexer.SearchDocument) (indexer.WriteResult, error)
	StartFullRebuild(context.Context) (indexer.RebuildResult, error)
	StartEntityRebuild(context.Context, string, string) (indexer.RebuildResult, error)
}

type HTTPServer struct {
	indexer             indexService
	cache               CacheOptions
	authToken           string
	requestTimeout      time.Duration
	runtimeConfigFile   string
	runtimeConfigLoader func() (indexer.RuntimeConfig, error)
	cacheVersion        atomic.Int64
	mutationStarted     atomic.Int64
	mutationCompleted   atomic.Int64
	mux                 *http.ServeMux
}

type Options struct {
	Cache               CacheOptions
	AuthToken           string
	RequestTimeout      time.Duration
	RuntimeConfigFile   string
	RuntimeConfigLoader func() (indexer.RuntimeConfig, error)
}

type CacheOptions struct {
	Cache               Cache
	TTLSeconds          int64
	QueryTTLSeconds     int64
	FilterTTLSeconds    int64
	HighlightTTLSeconds int64
	Namespace           string
}

type searchRequest struct {
	Q          string                `json:"q"`
	Sort       string                `json:"sort"`
	Must       []indexer.QueryClause `json:"must"`
	Should     []indexer.QueryClause `json:"should"`
	MustNot    []indexer.QueryClause `json:"must_not"`
	Filters    indexer.SearchFilters `json:"filters"`
	EntityType string                `json:"entityType"`
	CategoryID string                `json:"categoryId"`
	Status     string                `json:"status"`
	Tags       []string              `json:"tags"`
	Limit      int                   `json:"limit"`
	Offset     int                   `json:"offset"`
	Cursor     string                `json:"cursor"`
	Explain    bool                  `json:"explain"`
}

type prewarmRequest struct {
	Queries []searchRequest `json:"queries"`
}

type indexDocumentRequest struct {
	EntityType    string         `json:"entityType"`
	Title         string         `json:"title"`
	Status        string         `json:"status"`
	CategoryID    string         `json:"categoryId"`
	AuthorID      string         `json:"authorId"`
	Tags          []string       `json:"tags"`
	Keywords      []string       `json:"keywords"`
	Weight        int            `json:"weight"`
	ViewCount     int            `json:"viewCount"`
	LikeCount     int            `json:"likeCount"`
	DownloadCount int            `json:"downloadCount"`
	Payload       map[string]any `json:"payload"`
	CreatedAt     *time.Time     `json:"createdAt"`
	UpdatedAt     *time.Time     `json:"updatedAt"`
}

type tagsRequest struct {
	Q      string `json:"q"`
	Sort   string `json:"sort"`
	Limit  int    `json:"limit"`
	Offset int    `json:"offset"`
}

type apiError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewHTTPServer(indexer indexService, options Options) *HTTPServer {
	options = options.normalized()
	s := &HTTPServer{
		indexer:             indexer,
		cache:               options.Cache,
		authToken:           strings.TrimSpace(options.AuthToken),
		requestTimeout:      options.RequestTimeout,
		runtimeConfigFile:   strings.TrimSpace(options.RuntimeConfigFile),
		runtimeConfigLoader: options.RuntimeConfigLoader,
		mux:                 http.NewServeMux(),
	}
	s.cacheVersion.Store(time.Now().UnixNano())
	s.routes()
	return s
}

func (s *HTTPServer) Handler() http.Handler {
	return s.mux
}

func (s *HTTPServer) InvalidateSearchCache(ctx context.Context) error {
	return s.runMutation(ctx, func() error { return nil })
}

// Background mutations use the same shared fence as HTTP mutations. Completion
// errors reach the worker/job, so an incomplete fence can never imply success.
func (s *HTTPServer) RunMutation(ctx context.Context, operation func() error) error {
	return s.runMutation(ctx, operation)
}

func (o Options) normalized() Options {
	o.Cache = o.Cache.normalized()
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 5 * time.Second
	}
	return o
}

func (c CacheOptions) normalized() CacheOptions {
	if c.TTLSeconds <= 0 {
		c.TTLSeconds = 60
	}
	if c.QueryTTLSeconds <= 0 {
		c.QueryTTLSeconds = c.TTLSeconds
	}
	if c.FilterTTLSeconds <= 0 {
		c.FilterTTLSeconds = c.TTLSeconds
	}
	if c.HighlightTTLSeconds <= 0 {
		c.HighlightTTLSeconds = c.TTLSeconds
	}
	if strings.TrimSpace(c.Namespace) == "" {
		c.Namespace = "nexusindex:v2"
	}
	return c
}

func (s *HTTPServer) routes() {
	s.mux.HandleFunc("/health", s.wrap(s.handleHealth, false))
	s.mux.HandleFunc("/search", s.wrap(s.handleSearch, true))
	s.mux.HandleFunc("/api/index/status", s.wrap(s.handleStatus, true))
	s.mux.HandleFunc("/api/search", s.wrap(s.handleSearch, true))
	s.mux.HandleFunc("/api/tags", s.wrap(s.handleTags, true))
	s.mux.HandleFunc("/api/config", s.wrap(s.handleRuntimeConfig, true))
	s.mux.HandleFunc("/api/config/reload", s.wrap(s.handleRuntimeConfigReload, true))
	s.mux.HandleFunc("/api/cache/stats", s.wrap(s.handleCacheStats, true))
	s.mux.HandleFunc("/api/cache/prewarm", s.wrap(s.handleCachePrewarm, true))
	s.mux.HandleFunc("/index/", s.wrap(s.handleIndexDocument, true))
	s.mux.HandleFunc("/api/index/", s.wrap(s.handleIndexDocument, true))
	s.mux.HandleFunc("/api/index/rebuild", s.wrap(s.handleFullRebuild, true))
	s.mux.HandleFunc("/api/index/rebuild/", s.wrap(s.handleEntityRebuild, true))
}

func (s *HTTPServer) wrap(handler http.HandlerFunc, requireAuth bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), s.requestTimeout)
		defer cancel()
		r = r.WithContext(ctx)

		if requireAuth && !s.authorized(r) {
			writeError(w, http.StatusUnauthorized, "unauthorized", "missing or invalid nexusindex token")
			return
		}
		handler(w, r)
	}
}

func (s *HTTPServer) authorized(r *http.Request) bool {
	if s.authToken == "" {
		return true
	}
	token := strings.TrimSpace(r.Header.Get("X-NexusIndex-Token"))
	if token == "" {
		auth := strings.TrimSpace(r.Header.Get("Authorization"))
		if strings.HasPrefix(strings.ToLower(auth), "bearer ") {
			token = strings.TrimSpace(auth[7:])
		}
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(s.authToken)) == 1
}

func (s *HTTPServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if err := s.indexer.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database_unavailable", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *HTTPServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	status, err := s.indexer.Status(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, "index_status_failed", "index status failed")
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (s *HTTPServer) handleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	req, err := parseSearchRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cacheValues := req.cacheValues()
	cacheResource := req.cacheResource()
	r = r.WithContext(s.withCacheScope(r.Context()))
	if cached, ok := s.getCached(r.Context(), cacheResource, cacheValues); ok {
		cached = cacheHitPayload(cached)
		w.Header().Set("X-NexusIndex-Cache", "HIT")
		w.Header().Set("X-NexusIndex-Cache-State", "HIT")
		w.Header().Set("X-NexusIndex-Cache-Hit", "true")
		setTimingHeadersFromRaw(w, cached)
		writeRawJSON(w, http.StatusOK, cached)
		return
	}
	payload, err := s.searchPayload(r.Context(), req)
	if err != nil {
		var queryErr *indexer.QueryError
		if errors.As(err, &queryErr) {
			writeError(w, http.StatusBadRequest, queryErr.Code, queryErr.Message)
			return
		}
		if errors.Is(err, indexer.ErrInvalidCursor) || errors.Is(err, indexer.ErrCursorMismatch) {
			writeError(w, http.StatusBadRequest, "invalid_cursor", "invalid cursor")
			return
		}
		writeError(w, http.StatusBadGateway, "search_failed", "search failed")
		return
	}
	s.setCached(r.Context(), cacheResource, cacheValues, payload, s.cacheTTL(cacheResource))
	cacheState := s.cacheState(r.Context())
	w.Header().Set("X-NexusIndex-Cache", cacheState)
	w.Header().Set("X-NexusIndex-Cache-State", cacheState)
	w.Header().Set("X-NexusIndex-Cache-Hit", "false")
	setTimingHeaders(w, payload)
	writeJSON(w, http.StatusOK, payload)
}

func (s *HTTPServer) handleTags(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	req, err := parseTagsRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	cacheValues := req.cacheValues()
	r = r.WithContext(s.withCacheScope(r.Context()))
	if cached, ok := s.getCached(r.Context(), "tags", cacheValues); ok {
		w.Header().Set("X-NexusIndex-Cache", "HIT")
		writeRawJSON(w, http.StatusOK, cached)
		return
	}
	results, total, err := s.indexer.Tags(r.Context(), req.Q, req.Sort, req.Limit, req.Offset)
	if err != nil {
		writeError(w, http.StatusBadGateway, "tag_query_failed", "tag query failed")
		return
	}
	payload := map[string]any{
		"items":  results,
		"total":  total,
		"limit":  req.Limit,
		"offset": req.Offset,
		"sort":   req.Sort,
	}
	s.setCached(r.Context(), "tags", cacheValues, payload, s.cache.TTLSeconds)
	w.Header().Set("X-NexusIndex-Cache", s.cacheState(r.Context()))
	writeJSON(w, http.StatusOK, payload)
}

func (s *HTTPServer) searchPayload(ctx context.Context, req searchRequest) (map[string]any, error) {
	scope := ctx.Value(cacheScopeKey{}).(cacheScope)
	page, err := s.indexer.SearchWithSnapshot(ctx, indexer.SearchQuery{
		Query:      req.Q,
		Sort:       indexer.SearchSort(req.Sort),
		Must:       req.Must,
		Should:     req.Should,
		MustNot:    req.MustNot,
		Filters:    req.Filters,
		EntityType: req.EntityType,
		CategoryID: req.CategoryID,
		Status:     req.Status,
		Tags:       req.Tags,
		Limit:      req.Limit,
		Cursor:     req.Cursor,
		Explain:    req.Explain,
	}, scope.snapshot)
	if err != nil {
		return nil, err
	}
	payload := map[string]any{
		"items":                   page.Items,
		"total":                   page.Total,
		"limit":                   page.Limit,
		"offset":                  0,
		"next_cursor":             page.NextCursor,
		"sort":                    page.Sort,
		"generation_id":           page.GenerationID,
		"facets":                  page.Facets,
		"candidate_window":        page.CandidateWindow,
		"exhausted_candidate_set": page.ExhaustedCandidateSet,
	}
	if req.Explain {
		debug := map[string]any{
			"timing":         page.Timing,
			"cache_hit":      false,
			"candidate_size": page.Timing.CandidateSize,
		}
		if page.Timing.IndexLagMS != nil {
			debug["index_lag_ms"] = *page.Timing.IndexLagMS
		}
		payload["debug"] = debug
	} else {
		payload["timing"] = page.Timing
	}
	s.cacheHighlights(ctx, req, page.Items)
	return payload, nil
}

func (s *HTTPServer) handleRuntimeConfig(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		writeJSON(w, http.StatusOK, s.indexer.RuntimeConfig())
	case http.MethodPut, http.MethodPost:
		var override indexer.RuntimeConfig
		if err := json.NewDecoder(r.Body).Decode(&override); err != nil {
			writeError(w, http.StatusBadRequest, "invalid_json", "invalid json")
			return
		}
		var updated indexer.RuntimeConfig
		if err := s.runMutation(r.Context(), func() error { updated = s.indexer.PatchRuntimeConfig(r.Context(), override); return nil }); err != nil {
			writeCacheMutationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, updated)
	default:
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
	}
}

func (s *HTTPServer) handleRuntimeConfigReload(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if s.runtimeConfigFile == "" {
		writeError(w, http.StatusBadRequest, "runtime_config_file_missing", "NEXUSINDEX_RUNTIME_CONFIG_FILE is not set")
		return
	}
	loader := s.runtimeConfigLoader
	if loader == nil {
		loader = func() (indexer.RuntimeConfig, error) { return indexer.LoadRuntimeConfigFile(s.runtimeConfigFile) }
	}
	cfg, err := loader()
	if err != nil {
		writeError(w, http.StatusBadRequest, "runtime_config_reload_failed", err.Error())
		return
	}
	var updated indexer.RuntimeConfig
	if err := s.runMutation(r.Context(), func() error { updated = s.indexer.UpdateRuntimeConfig(r.Context(), cfg); return nil }); err != nil {
		writeCacheMutationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, updated)
}

func (s *HTTPServer) handleCacheStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if statsProvider, ok := s.cache.Cache.(interface{ Stats() CacheStats }); ok {
		writeJSON(w, http.StatusOK, map[string]any{
			"cacheVersion": s.cacheVersion.Load(),
			"stats":        statsProvider.Stats(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"available": false, "cacheVersion": s.cacheVersion.Load()})
}

func (s *HTTPServer) handleCachePrewarm(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	var req prewarmRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "invalid json")
		return
	}
	if len(req.Queries) > 50 {
		req.Queries = req.Queries[:50]
	}
	warmed := 0
	for _, item := range req.Queries {
		item = item.normalized()
		cacheResource := item.cacheResource()
		ctx := s.withCacheScope(r.Context())
		payload, err := s.searchPayload(ctx, item)
		if err != nil {
			continue
		}
		s.setCached(ctx, cacheResource, item.cacheValues(), payload, s.cacheTTL(cacheResource))
		warmed++
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "warmed": warmed})
}

func (s *HTTPServer) handleIndexDocument(w http.ResponseWriter, r *http.Request) {
	docID := indexDocIDFromRequest(r)
	if docID == "" {
		writeError(w, http.StatusBadRequest, "invalid_route", "expected /index/:doc_id")
		return
	}
	queryEntityType := strings.TrimSpace(r.URL.Query().Get("entityType"))
	entityType := queryEntityType
	if entityType == "" {
		entityType = indexer.EntityDocument
	}
	if queryEntityType != "" && !isValidEntityType(entityType) {
		writeError(w, http.StatusBadRequest, "invalid_entity_type", "unsupported entity type")
		return
	}
	if r.Method == http.MethodDelete {
		var result indexer.WriteResult
		if err := s.runMutation(r.Context(), func() error {
			var err error
			result, err = s.indexer.MarkDeleted(r.Context(), entityType, docID)
			return err
		}); err != nil {
			if writeCacheMutationError(w, err) {
				return
			}
			writeError(w, http.StatusBadGateway, "index_delete_failed", "index delete failed")
			return
		}
		if !result.Applied {
			writeError(w, http.StatusConflict, "stale_source_version", "document has a newer source version")
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "id": docID, "status": "deleted", "applied": result.Applied})
		return
	}
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}

	var body indexDocumentRequest
	r.Body = http.MaxBytesReader(w, r.Body, 64<<10)
	if err := decodeSingleJSON(r.Body, &body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "invalid json")
		return
	}
	doc := body.toSearchDocument(docID)
	if queryEntityType != "" {
		doc.EntityType = queryEntityType
	}
	if !isValidEntityType(doc.EntityType) {
		writeError(w, http.StatusBadRequest, "invalid_entity_type", "unsupported entity type")
		return
	}
	var result indexer.WriteResult
	if err := s.runMutation(r.Context(), func() error { var err error; result, err = s.indexer.Upsert(r.Context(), doc); return err }); err != nil {
		if writeCacheMutationError(w, err) {
			return
		}
		writeError(w, http.StatusBadGateway, "index_upsert_failed", "index upsert failed")
		return
	}
	if !result.Applied {
		writeError(w, http.StatusConflict, "stale_source_version", "document has a newer source version")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "id": docID, "entityType": doc.EntityType, "applied": result.Applied})
}

func (s *HTTPServer) handleFullRebuild(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/api/index/rebuild" {
		writeError(w, http.StatusNotFound, "not_found", "not found")
		return
	}
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	var result indexer.RebuildResult
	err := s.runMutation(r.Context(), func() error { var err error; result, err = s.indexer.StartFullRebuild(r.Context()); return err })
	if err != nil {
		if writeCacheMutationError(w, err) {
			return
		}
		writeError(w, http.StatusBadGateway, "rebuild_start_failed", "rebuild failed to start")
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "jobId": result.JobID})
}

func (s *HTTPServer) handleEntityRebuild(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/index/rebuild/"), "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		writeError(w, http.StatusBadRequest, "invalid_route", "expected /api/index/rebuild/:entityType/:entityId")
		return
	}
	var result indexer.RebuildResult
	err := s.runMutation(r.Context(), func() error {
		var err error
		result, err = s.indexer.StartEntityRebuild(r.Context(), parts[0], parts[1])
		return err
	})
	if err != nil {
		if writeCacheMutationError(w, err) {
			return
		}
		writeError(w, http.StatusBadRequest, "entity_rebuild_start_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{"ok": true, "jobId": result.JobID})
}

func parseSearchRequest(r *http.Request) (searchRequest, error) {
	req := searchRequest{Limit: 20, Offset: 0}
	if r.Method == http.MethodPost {
		r.Body = http.MaxBytesReader(nil, r.Body, 64<<10)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return req, fmt.Errorf("invalid json")
		}
	} else {
		q := r.URL.Query()
		req.Q = q.Get("q")
		req.Sort = q.Get("sort")
		req.EntityType = q.Get("entityType")
		req.CategoryID = q.Get("categoryId")
		req.Status = q.Get("status")
		req.Tags = q["tags"]
		req.Limit = boundedInt(q.Get("limit"), 20, 1, 100)
		req.Cursor = q.Get("cursor")
		req.Explain = q.Get("explain") == "1" || strings.EqualFold(q.Get("explain"), "true")
		return req.normalized(), nil
	}
	return req.normalized(), nil
}

func parseTagsRequest(r *http.Request) (tagsRequest, error) {
	req := tagsRequest{Limit: 50}
	if r.Method == http.MethodPost {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			return req, fmt.Errorf("invalid json")
		}
	} else {
		q := r.URL.Query()
		req.Q = q.Get("q")
		req.Sort = q.Get("sort")
		req.Limit = boundedInt(q.Get("limit"), 50, 1, 200)
		req.Offset = boundedInt(q.Get("offset"), 0, 0, 10000)
		return req.normalized(), nil
	}
	return req.normalized(), nil
}

func (r searchRequest) normalized() searchRequest {
	r.Q = strings.TrimSpace(r.Q)
	r.EntityType = strings.TrimSpace(r.EntityType)
	r.CategoryID = strings.TrimSpace(r.CategoryID)
	r.Status = strings.TrimSpace(r.Status)
	r.Sort = strings.ToLower(strings.TrimSpace(r.Sort))
	if r.Sort == "" {
		if r.Q == "" {
			r.Sort = string(indexer.SortLatest)
		} else {
			r.Sort = string(indexer.SortRelevance)
		}
	}
	r.Cursor = strings.TrimSpace(r.Cursor)
	r.Tags = cleanList(r.Tags)
	r.Limit = boundValue(r.Limit, 20, 1, 100)
	r.Offset = 0
	return r
}

func (r searchRequest) cacheResource() string {
	if strings.TrimSpace(r.Q) == "" {
		return "filter"
	}
	return "query"
}

func (r tagsRequest) normalized() tagsRequest {
	r.Q = strings.TrimSpace(r.Q)
	r.Sort = strings.TrimSpace(r.Sort)
	if r.Sort != "name" && r.Sort != "latest" {
		r.Sort = "count"
	}
	r.Limit = boundValue(r.Limit, 50, 1, 200)
	r.Offset = boundValue(r.Offset, 0, 0, 10000)
	return r
}

func (r searchRequest) cacheValues() url.Values {
	values := url.Values{}
	values.Set("q", r.Q)
	values.Set("sort", r.Sort)
	values.Set("entityType", r.EntityType)
	values.Set("categoryId", r.CategoryID)
	values.Set("status", r.Status)
	for _, tag := range r.Tags {
		values.Add("tags", tag)
	}
	values.Set("limit", strconv.Itoa(r.Limit))
	values.Set("cursor", r.Cursor)
	setJSONCacheValue(values, "must", r.Must)
	setJSONCacheValue(values, "should", r.Should)
	setJSONCacheValue(values, "must_not", r.MustNot)
	setJSONCacheValue(values, "filters", r.Filters)
	if r.Explain {
		values.Set("explain", "true")
	}
	return values
}

func setJSONCacheValue(values url.Values, key string, value any) {
	raw, err := json.Marshal(value)
	if err == nil {
		values.Set(key, string(raw))
	}
}

func (r searchRequest) highlightCacheValues(id string) url.Values {
	values := url.Values{}
	values.Set("id", id)
	values.Set("q", r.Q)
	return values
}

func (r tagsRequest) cacheValues() url.Values {
	values := url.Values{}
	values.Set("q", r.Q)
	values.Set("sort", r.Sort)
	values.Set("limit", strconv.Itoa(r.Limit))
	values.Set("offset", strconv.Itoa(r.Offset))
	return values
}

func boundedInt(value string, fallback, min, max int) int {
	parsed, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return fallback
	}
	return boundValue(parsed, fallback, min, max)
}

func boundValue(value, fallback, min, max int) int {
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

func (s *HTTPServer) getCached(ctx context.Context, resource string, values url.Values) (json.RawMessage, bool) {
	key, enabled := s.scopedCacheKey(ctx, resource, values)
	if !enabled {
		return nil, false
	}
	return s.cache.Cache.Get(ctx, key)
}

func (s *HTTPServer) setCached(ctx context.Context, resource string, values url.Values, payload any, ttlSeconds int64) {
	key, enabled := s.scopedCacheKey(ctx, resource, values)
	if !enabled {
		return
	}
	s.cache.Cache.Set(ctx, key, payload, ttlSeconds)
}

func (s *HTTPServer) cacheState(ctx context.Context) string {
	scope, ok := ctx.Value(cacheScopeKey{}).(cacheScope)
	if !ok || !scope.enabled {
		return "BYPASS"
	}
	return "MISS"
}

func (s *HTTPServer) clearCachedSearch(ctx context.Context) {
	if s.cache.Cache == nil {
		return
	}
	s.cacheVersion.Add(1)
	if shared, ok := s.cache.Cache.(namespaceVersions); ok {
		version, err := shared.BumpVersion(ctx, s.cache.Namespace)
		if err == nil {
			s.cacheVersion.Store(version)
		}
	}
	s.cache.Cache.Clear(ctx, s.cache.Namespace+":query:*")
	s.cache.Cache.Clear(ctx, s.cache.Namespace+":filter:*")
	s.cache.Cache.Clear(ctx, s.cache.Namespace+":highlight:*")
	s.cache.Cache.Clear(ctx, s.cache.Namespace+":tags:*")
}

type cacheMutationError struct {
	phase string
	err   error
}

func (e *cacheMutationError) Error() string {
	return "cache mutation " + e.phase + ": " + e.err.Error()
}
func (e *cacheMutationError) Unwrap() error { return e.err }

func writeCacheMutationError(w http.ResponseWriter, err error) bool {
	var cacheErr *cacheMutationError
	if !errors.As(err, &cacheErr) {
		return false
	}
	message := "shared cache unavailable; mutation was not executed"
	if cacheErr.phase == "completion" {
		message = "mutation may have completed; shared cache completion failed"
	}
	writeError(w, http.StatusServiceUnavailable, "cache_mutation_"+cacheErr.phase+"_failed", message)
	return true
}

// Independent started/completed counters keep every instance out of cache
// during writes. Count completions (not sequence assignment), so concurrent
// operations can finish in any order. Never retry an ambiguous bump: an
// unbalanced fence safely remains BYPASS until an operator reconciles it.
func (s *HTTPServer) runMutation(ctx context.Context, operation func() error) error {
	if s.cache.Cache == nil {
		return operation()
	}
	shared, hasShared := s.cache.Cache.(namespaceVersions)
	if hasShared {
		// GET initializes a missing Redis version to 1, while INCR starts at
		// 1 itself. Initialize both keys before incrementing started so the
		// first in-flight mutation cannot appear completed to a cold reader.
		for _, suffix := range []string{":mutation:started", ":mutation:completed"} {
			if _, err := shared.GetVersion(ctx, s.cache.Namespace+suffix); err != nil {
				return &cacheMutationError{phase: "precommit", err: err}
			}
		}
		if _, err := shared.BumpVersion(ctx, s.cache.Namespace+":mutation:started"); err != nil {
			return &cacheMutationError{phase: "precommit", err: err}
		}
	} else {
		s.mutationStarted.Add(1)
	}
	err := operation()
	if hasShared {
		completionCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.requestTimeout)
		defer cancel()
		if _, completionErr := shared.BumpVersion(completionCtx, s.cache.Namespace+":mutation:completed"); completionErr != nil {
			return &cacheMutationError{phase: "completion", err: errors.Join(err, completionErr)}
		}
	} else {
		s.mutationCompleted.Add(1)
	}
	return err
}

func (s *HTTPServer) stableMutationVersion(ctx context.Context) (int64, bool) {
	if shared, ok := s.cache.Cache.(namespaceVersions); ok {
		started, err := shared.GetVersion(ctx, s.cache.Namespace+":mutation:started")
		if err != nil {
			return 0, false
		}
		completed, err := shared.GetVersion(ctx, s.cache.Namespace+":mutation:completed")
		if err != nil {
			return 0, false
		}
		confirmed, err := shared.GetVersion(ctx, s.cache.Namespace+":mutation:started")
		return started, err == nil && started == confirmed && started == completed
	}
	started := s.mutationStarted.Load()
	completed := s.mutationCompleted.Load()
	return started, started == completed && started == s.mutationStarted.Load()
}

func (s *HTTPServer) cacheTTL(resource string) int64 {
	switch resource {
	case "filter":
		return s.cache.FilterTTLSeconds
	case "highlight":
		return s.cache.HighlightTTLSeconds
	default:
		return s.cache.QueryTTLSeconds
	}
}

func (s *HTTPServer) cacheKeyAt(resource string, values url.Values, version int64, configHash string) string {
	normalized := cloneQuery(values).Encode()
	sum := sha256.Sum256([]byte(resource + "?v=" + strconv.FormatInt(version, 10) + "&config=" + configHash + "&" + normalized))
	return fmt.Sprintf("%s:%s:%x", s.cache.Namespace, resource, sum[:16])
}

type cacheScopeKey struct{}
type cacheScope struct {
	version         int64
	snapshot        indexer.RuntimeSnapshot
	enabled         bool
	mutationVersion int64
}

// Capture once before the miss and keep this identity through every write.
// A failed shared-version read bypasses both cache tiers, including warm L1.
func (s *HTTPServer) withCacheScope(ctx context.Context) context.Context {
	scope := cacheScope{version: s.cacheVersion.Load(), enabled: s.cache.Cache != nil}
	if s.indexer != nil {
		scope.snapshot = s.indexer.RuntimeSnapshot()
	}
	if scope.enabled {
		scope.mutationVersion, scope.enabled = s.stableMutationVersion(ctx)
	}
	if shared, ok := s.cache.Cache.(namespaceVersions); ok {
		version, err := shared.GetVersion(ctx, s.cache.Namespace)
		scope.version = version
		scope.enabled = scope.enabled && err == nil
		if err == nil {
			s.cacheVersion.Store(scope.version)
		}
	}
	return context.WithValue(ctx, cacheScopeKey{}, scope)
}

func (s *HTTPServer) scopedCacheKey(ctx context.Context, resource string, values url.Values) (string, bool) {
	scope, ok := ctx.Value(cacheScopeKey{}).(cacheScope)
	if !ok {
		return "", false
	}
	return s.cacheKeyAt(resource, values, scope.version, scope.snapshot.Hash+"&mutation="+strconv.FormatInt(scope.mutationVersion, 10)), scope.enabled
}

func (s *HTTPServer) cacheHighlights(ctx context.Context, req searchRequest, items []indexer.SearchResult) {
	entries := make([]CacheEntry, 0, len(items))
	for _, item := range items {
		key, enabled := s.scopedCacheKey(ctx, "highlight", req.highlightCacheValues(item.ID))
		if !enabled {
			return
		}
		entries = append(entries, CacheEntry{Key: key, Value: item.Highlight, TTLSeconds: s.cache.HighlightTTLSeconds})
	}
	if batch, ok := s.cache.Cache.(interface {
		SetBatch(context.Context, []CacheEntry)
	}); ok {
		batch.SetBatch(ctx, entries)
		return
	}
	ctx, cancel := context.WithTimeout(ctx, highlightCacheBudget)
	defer cancel()
	for _, entry := range entries {
		if ctx.Err() != nil {
			return
		}
		s.cache.Cache.Set(ctx, entry.Key, entry.Value, entry.TTLSeconds)
	}
}

func cacheHitPayload(raw json.RawMessage) json.RawMessage {
	var body map[string]json.RawMessage
	if json.Unmarshal(raw, &body) != nil {
		return raw
	}
	var debug map[string]json.RawMessage
	if json.Unmarshal(body["debug"], &debug) != nil || debug == nil {
		return raw
	}
	debug["cache_hit"] = json.RawMessage("true")
	body["debug"], _ = json.Marshal(debug)
	updated, err := json.Marshal(body)
	if err != nil {
		return raw
	}
	return updated
}

func cloneQuery(values url.Values) url.Values {
	cloned := url.Values{}
	for key, list := range values {
		for _, value := range list {
			cloned.Add(key, value)
		}
	}
	return cloned
}

func cleanList(values []string) []string {
	cleaned := []string{}
	seen := map[string]struct{}{}
	for _, value := range values {
		for _, part := range strings.Split(value, ",") {
			part = strings.ToLower(strings.TrimSpace(part))
			if part == "" {
				continue
			}
			if _, ok := seen[part]; ok {
				continue
			}
			seen[part] = struct{}{}
			cleaned = append(cleaned, part)
		}
	}
	return cleaned
}

func indexDocIDFromRequest(r *http.Request) string {
	return indexDocIDFromEscapedPath(r.URL.EscapedPath())
}

func indexDocIDFromEscapedPath(path string) string {
	for _, prefix := range []string{"/api/index/", "/index/"} {
		if strings.HasPrefix(path, prefix) {
			rest := strings.TrimPrefix(path, prefix)
			if rest == "" || strings.Contains(rest, "/") {
				return ""
			}
			value, err := url.PathUnescape(rest)
			if err != nil || strings.Contains(value, "/") {
				return ""
			}
			value = strings.TrimSpace(value)
			if value == "" || value == "rebuild" || strings.HasPrefix(value, "rebuild/") {
				return ""
			}
			return value
		}
	}
	return ""
}

func isValidEntityType(entityType string) bool {
	for _, allowed := range indexer.EntityTypes {
		if entityType == allowed {
			return true
		}
	}
	return false
}

func decodeSingleJSON(reader io.Reader, destination any) error {
	decoder := json.NewDecoder(reader)
	decoder.UseNumber()
	var raw json.RawMessage
	if err := decoder.Decode(&raw); err != nil {
		return err
	}
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return errors.New("json value must not be null")
	}
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return errors.New("multiple json values")
		}
		return err
	}
	valueDecoder := json.NewDecoder(bytes.NewReader(trimmed))
	valueDecoder.UseNumber()
	return valueDecoder.Decode(destination)
}

func (r indexDocumentRequest) toSearchDocument(docID string) indexer.SearchDocument {
	now := time.Now().UTC()
	createdAt := time.Time{}
	updatedAt := now
	if r.CreatedAt != nil {
		createdAt = *r.CreatedAt
	}
	if r.UpdatedAt != nil {
		updatedAt = *r.UpdatedAt
	}
	status := strings.TrimSpace(r.Status)
	if status == "" {
		status = "active"
	}
	entityType := strings.TrimSpace(r.EntityType)
	if entityType == "" {
		entityType = indexer.EntityDocument
	}
	payload := r.Payload
	if payload == nil {
		payload = map[string]any{}
	}
	return indexer.SearchDocument{
		DocID:         docID,
		EntityType:    entityType,
		EntityID:      docID,
		Title:         strings.TrimSpace(r.Title),
		Status:        status,
		CategoryID:    strings.TrimSpace(r.CategoryID),
		AuthorID:      strings.TrimSpace(r.AuthorID),
		Tags:          cleanList(r.Tags),
		Keywords:      cleanList(r.Keywords),
		Weight:        r.Weight,
		ViewCount:     r.ViewCount,
		LikeCount:     r.LikeCount,
		DownloadCount: r.DownloadCount,
		Payload:       payload,
		CreatedAt:     createdAt,
		UpdatedAt:     updatedAt,
	}
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeRawJSON(w http.ResponseWriter, status int, payload json.RawMessage) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(payload)
	_, _ = w.Write([]byte("\n"))
}

func setTimingHeaders(w http.ResponseWriter, payload map[string]any) {
	timing, ok := payload["timing"].(indexer.SearchTiming)
	if !ok {
		if debug, exists := payload["debug"].(map[string]any); exists {
			timing, ok = debug["timing"].(indexer.SearchTiming)
		}
	}
	if !ok {
		return
	}
	writeTimingHeaders(w, timing)
}

func writeTimingHeaders(w http.ResponseWriter, timing indexer.SearchTiming) {
	w.Header().Set("X-NexusIndex-PG-Ms", strconv.FormatInt(timing.PGMS, 10))
	w.Header().Set("X-NexusIndex-Scoring-Ms", strconv.FormatInt(timing.ScoringMS, 10))
	w.Header().Set("X-NexusIndex-Highlight-Ms", strconv.FormatInt(timing.HighlightMS, 10))
	w.Header().Set("X-NexusIndex-Candidate-Size", strconv.Itoa(timing.CandidateSize))
	if timing.IndexLagMS != nil {
		w.Header().Set("X-NexusIndex-Index-Lag-Ms", strconv.FormatInt(*timing.IndexLagMS, 10))
	}
}

func setTimingHeadersFromRaw(w http.ResponseWriter, payload json.RawMessage) {
	var body struct {
		Timing *indexer.SearchTiming `json:"timing"`
		Debug  struct {
			Timing *indexer.SearchTiming `json:"timing"`
		} `json:"debug"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return
	}
	timing := body.Timing
	if timing == nil {
		timing = body.Debug.Timing
	}
	if timing != nil {
		writeTimingHeaders(w, *timing)
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]apiError{
		"error": {Code: code, Message: message},
	})
}
