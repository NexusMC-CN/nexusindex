package server

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/blockbridge/avmcbbs/apps/nexusindex/internal/indexer"
)

type HTTPServer struct {
	indexer           *indexer.Service
	cache             CacheOptions
	authToken         string
	requestTimeout    time.Duration
	runtimeConfigFile string
	cacheVersion      atomic.Int64
	mux               *http.ServeMux
}

type Options struct {
	Cache             CacheOptions
	AuthToken         string
	RequestTimeout    time.Duration
	RuntimeConfigFile string
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

func NewHTTPServer(indexer *indexer.Service, options Options) *HTTPServer {
	options = options.normalized()
	s := &HTTPServer{
		indexer:           indexer,
		cache:             options.Cache,
		authToken:         strings.TrimSpace(options.AuthToken),
		requestTimeout:    options.RequestTimeout,
		runtimeConfigFile: strings.TrimSpace(options.RuntimeConfigFile),
		mux:               http.NewServeMux(),
	}
	s.cacheVersion.Store(time.Now().UnixNano())
	s.routes()
	return s
}

func (s *HTTPServer) Handler() http.Handler {
	return s.mux
}

func (s *HTTPServer) InvalidateSearchCache(ctx context.Context) {
	s.clearCachedSearch(ctx)
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
	if cached, ok := s.getCached(r.Context(), cacheResource, cacheValues); ok {
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
		if strings.Contains(err.Error(), "invalid cursor") {
			writeError(w, http.StatusBadRequest, "invalid_cursor", "invalid cursor")
			return
		}
		writeError(w, http.StatusBadGateway, "search_failed", "search failed")
		return
	}
	s.setCached(r.Context(), cacheResource, cacheValues, payload, s.cacheTTL(cacheResource))
	cacheState := s.cacheState()
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
	w.Header().Set("X-NexusIndex-Cache", s.cacheState())
	writeJSON(w, http.StatusOK, payload)
}

func (s *HTTPServer) searchPayload(ctx context.Context, req searchRequest) (map[string]any, error) {
	page, err := s.indexer.Search(ctx, indexer.SearchQuery{
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
	})
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
		payload["debug"] = map[string]any{
			"timing":         page.Timing,
			"cache_hit":      false,
			"candidate_size": page.Timing.CandidateSize,
			"index_lag_ms":   page.Timing.IndexLagMS,
		}
	} else {
		payload["timing"] = page.Timing
	}
	for _, item := range page.Items {
		s.setCached(ctx, "highlight", req.highlightCacheValues(item.ID), item.Highlight, s.cache.HighlightTTLSeconds)
	}
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
		updated := s.indexer.UpdateRuntimeConfig(r.Context(), s.indexer.RuntimeConfig().Merge(override))
		s.clearCachedSearch(r.Context())
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
	cfg, err := indexer.LoadRuntimeConfigFile(s.runtimeConfigFile)
	if err != nil {
		writeError(w, http.StatusBadRequest, "runtime_config_reload_failed", err.Error())
		return
	}
	updated := s.indexer.UpdateRuntimeConfig(r.Context(), cfg)
	s.clearCachedSearch(r.Context())
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
		payload, err := s.searchPayload(r.Context(), item)
		if err != nil {
			continue
		}
		s.setCached(r.Context(), cacheResource, item.cacheValues(), payload, s.cacheTTL(cacheResource))
		warmed++
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "warmed": warmed})
}

func (s *HTTPServer) handleIndexDocument(w http.ResponseWriter, r *http.Request) {
	docID := indexDocIDFromPath(r.URL.Path)
	if docID == "" {
		writeError(w, http.StatusBadRequest, "invalid_route", "expected /index/:doc_id")
		return
	}
	entityType := strings.TrimSpace(r.URL.Query().Get("entityType"))
	if r.Method == http.MethodDelete {
		if err := s.indexer.MarkDeleted(r.Context(), entityType, docID); err != nil {
			writeError(w, http.StatusBadGateway, "index_delete_failed", "index delete failed")
			return
		}
		s.clearCachedSearch(r.Context())
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "id": docID, "status": "deleted"})
		return
	}
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}

	var body indexDocumentRequest
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid_json", "invalid json")
		return
	}
	doc := body.toSearchDocument(docID)
	if entityType != "" {
		doc.EntityType = entityType
	}
	if err := s.indexer.Upsert(r.Context(), doc); err != nil {
		writeError(w, http.StatusBadGateway, "index_upsert_failed", "index upsert failed")
		return
	}
	s.clearCachedSearch(r.Context())
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "id": docID, "entityType": doc.EntityType})
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
	result, err := s.indexer.StartFullRebuild(r.Context())
	if err != nil {
		writeError(w, http.StatusBadGateway, "rebuild_start_failed", "rebuild failed to start")
		return
	}
	s.clearCachedSearch(r.Context())
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
	result, err := s.indexer.StartEntityRebuild(r.Context(), parts[0], parts[1])
	if err != nil {
		writeError(w, http.StatusBadRequest, "entity_rebuild_start_failed", err.Error())
		return
	}
	s.clearCachedSearch(r.Context())
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
	if s.cache.Cache == nil {
		return nil, false
	}
	return s.cache.Cache.Get(ctx, s.cacheKey(resource, values))
}

func (s *HTTPServer) setCached(ctx context.Context, resource string, values url.Values, payload any, ttlSeconds int64) {
	if s.cache.Cache == nil {
		return
	}
	s.cache.Cache.Set(ctx, s.cacheKey(resource, values), payload, ttlSeconds)
}

func (s *HTTPServer) cacheState() string {
	if s.cache.Cache == nil {
		return "BYPASS"
	}
	return "MISS"
}

func (s *HTTPServer) clearCachedSearch(ctx context.Context) {
	if s.cache.Cache == nil {
		return
	}
	s.cacheVersion.Add(1)
	s.cache.Cache.Clear(ctx, s.cache.Namespace+":query:*")
	s.cache.Cache.Clear(ctx, s.cache.Namespace+":filter:*")
	s.cache.Cache.Clear(ctx, s.cache.Namespace+":highlight:*")
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

func (s *HTTPServer) cacheKey(resource string, values url.Values) string {
	normalized := cloneQuery(values).Encode()
	version := s.cacheVersion.Load()
	sum := sha256.Sum256([]byte(resource + "?v=" + strconv.FormatInt(version, 10) + "&" + normalized))
	return fmt.Sprintf("%s:%s:%x", s.cache.Namespace, resource, sum[:16])
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

func indexDocIDFromPath(path string) string {
	for _, prefix := range []string{"/api/index/", "/index/"} {
		if strings.HasPrefix(path, prefix) {
			rest := strings.Trim(strings.TrimPrefix(path, prefix), "/")
			if rest == "" || strings.Contains(rest, "/") || rest == "rebuild" || strings.HasPrefix(rest, "rebuild/") {
				return ""
			}
			if value, err := url.PathUnescape(rest); err == nil {
				return strings.TrimSpace(value)
			}
			return rest
		}
	}
	return ""
}

func (r indexDocumentRequest) toSearchDocument(docID string) indexer.SearchDocument {
	now := time.Now().UTC()
	createdAt := now
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
		return
	}
	w.Header().Set("X-NexusIndex-PG-Ms", strconv.FormatInt(timing.PGMS, 10))
	w.Header().Set("X-NexusIndex-Scoring-Ms", strconv.FormatInt(timing.ScoringMS, 10))
	w.Header().Set("X-NexusIndex-Highlight-Ms", strconv.FormatInt(timing.HighlightMS, 10))
	w.Header().Set("X-NexusIndex-Candidate-Size", strconv.Itoa(timing.CandidateSize))
	w.Header().Set("X-NexusIndex-Index-Lag-Ms", strconv.FormatInt(timing.IndexLagMS, 10))
}

func setTimingHeadersFromRaw(w http.ResponseWriter, payload json.RawMessage) {
	var body struct {
		Timing indexer.SearchTiming `json:"timing"`
		Debug  struct {
			Timing indexer.SearchTiming `json:"timing"`
		} `json:"debug"`
	}
	if err := json.Unmarshal(payload, &body); err != nil {
		return
	}
	timing := body.Timing
	if timing.PGMS == 0 && timing.ScoringMS == 0 && timing.HighlightMS == 0 && body.Debug.Timing.CandidateSize > 0 {
		timing = body.Debug.Timing
	}
	w.Header().Set("X-NexusIndex-PG-Ms", strconv.FormatInt(timing.PGMS, 10))
	w.Header().Set("X-NexusIndex-Scoring-Ms", strconv.FormatInt(timing.ScoringMS, 10))
	w.Header().Set("X-NexusIndex-Highlight-Ms", strconv.FormatInt(timing.HighlightMS, 10))
	w.Header().Set("X-NexusIndex-Candidate-Size", strconv.Itoa(timing.CandidateSize))
	w.Header().Set("X-NexusIndex-Index-Lag-Ms", strconv.FormatInt(timing.IndexLagMS, 10))
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]apiError{
		"error": {Code: code, Message: message},
	})
}
