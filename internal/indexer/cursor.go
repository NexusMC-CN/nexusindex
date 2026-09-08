package indexer

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

type SearchSort string

const (
	SortRelevance SearchSort = "relevance"
	SortLatest    SearchSort = "latest"
	SortPopular   SearchSort = "popular"
)

func normalizeSearchSort(value SearchSort) (SearchSort, error) {
	switch SearchSort(strings.ToLower(strings.TrimSpace(string(value)))) {
	case "", SortRelevance:
		return SortRelevance, nil
	case SortLatest:
		return SortLatest, nil
	case SortPopular:
		return SortPopular, nil
	default:
		return "", errors.New("invalid sort")
	}
}

type searchCursor struct {
	Sort          SearchSort `json:"sort"`
	Score         float64    `json:"score"`
	SortTime      string     `json:"sortTime"`
	EntityType    string     `json:"entityType"`
	EntityID      string     `json:"entityId"`
	ScoredAt      string     `json:"scoredAt"`
	GenerationID  int64      `json:"generationId"`
	ConfigVersion uint64     `json:"configVersion"`
	QueryHash     string     `json:"queryHash"`
}

type cursorCodec struct {
	secret []byte
}

func newCursorCodec(secret string) cursorCodec {
	secret = strings.TrimSpace(secret)
	if secret == "" {
		secret = "nexusindex-development-cursor-secret"
	}
	return cursorCodec{secret: []byte(secret)}
}

func (c cursorCodec) encode(cursor searchCursor) (string, error) {
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, c.secret)
	_, _ = mac.Write(payload)
	signature := mac.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func (c cursorCodec) decode(value string) (*searchCursor, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, nil
	}
	parts := strings.Split(value, ".")
	if len(parts) != 2 {
		return nil, errors.New("invalid cursor")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, errors.New("invalid cursor")
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, errors.New("invalid cursor")
	}
	mac := hmac.New(sha256.New, c.secret)
	_, _ = mac.Write(payload)
	if !hmac.Equal(signature, mac.Sum(nil)) {
		return nil, errors.New("invalid cursor")
	}
	var cursor searchCursor
	if err := json.Unmarshal(payload, &cursor); err != nil || cursor.ScoredAt == "" || cursor.QueryHash == "" {
		return nil, errors.New("invalid cursor")
	}
	return &cursor, nil
}

func (c searchCursor) scoringTime() (time.Time, error) {
	return time.Parse(time.RFC3339Nano, c.ScoredAt)
}

func searchQueryHash(req SearchQuery) string {
	payload, _ := json.Marshal(struct {
		Query      string        `json:"query"`
		Sort       SearchSort    `json:"sort"`
		EntityType string        `json:"entityType"`
		CategoryID string        `json:"categoryId"`
		Status     string        `json:"status"`
		Tags       []string      `json:"tags"`
		Must       []QueryClause `json:"must"`
		Should     []QueryClause `json:"should"`
		MustNot    []QueryClause `json:"mustNot"`
		Filters    SearchFilters `json:"filters"`
	}{req.Query, req.Sort, req.EntityType, req.CategoryID, req.Status, normalizeStrings(req.Tags), req.Must, req.Should, req.MustNot, req.Filters})
	sum := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}
