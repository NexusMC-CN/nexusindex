package indexer

import "time"

const (
	EntityResource     = "resource"
	EntityPost         = "post"
	EntityServer       = "server"
	EntityVideo        = "video"
	EntityDocument     = "document"
	EntityUser         = "user"
	EntityOrganization = "organization"
	EntityTag          = "tag"
	EntityOfficialTag  = "official_tag"
)

var EntityTypes = []string{
	EntityResource,
	EntityPost,
	EntityServer,
	EntityVideo,
	EntityDocument,
	EntityUser,
	EntityOrganization,
	EntityTag,
	EntityOfficialTag,
}

type SearchDocument struct {
	DocID         string
	EntityType    string
	EntityID      string
	Title         string
	Status        string
	CategoryID    string
	AuthorID      string
	Tags          []string
	Keywords      []string
	Weight        int
	ViewCount     int
	LikeCount     int
	DownloadCount int
	Payload       map[string]any
	CreatedAt     time.Time
	UpdatedAt     time.Time
	SourceVersion int64
}

type SearchResult struct {
	ID            string         `json:"id"`
	EntityType    string         `json:"entityType"`
	EntityID      string         `json:"entityId"`
	Title         string         `json:"title"`
	Score         float64        `json:"score"`
	MatchedFields []string       `json:"matched_fields"`
	Highlight     map[string]any `json:"highlight"`
	Explanation   map[string]any `json:"explanation,omitempty"`
	Status        string         `json:"status"`
	CategoryID    string         `json:"categoryId,omitempty"`
	AuthorID      string         `json:"authorId,omitempty"`
	Tags          []string       `json:"tags"`
	Keywords      []string       `json:"keywords"`
	Weight        int            `json:"weight"`
	ViewCount     int            `json:"viewCount"`
	LikeCount     int            `json:"likeCount"`
	DownloadCount int            `json:"downloadCount"`
	Payload       map[string]any `json:"payload"`
	CreatedAt     time.Time      `json:"createdAt"`
	UpdatedAt     time.Time      `json:"updatedAt"`
	IndexedAt     time.Time      `json:"indexedAt"`
	payloadText   string
}

type SearchQuery struct {
	Query      string
	EntityType string
	CategoryID string
	Status     string
	Tags       []string
	Limit      int
	Cursor     string
	Explain    bool
}

type SearchPage struct {
	Items                 []SearchResult `json:"items"`
	Total                 int64          `json:"total"`
	Limit                 int            `json:"limit"`
	NextCursor            string         `json:"next_cursor,omitempty"`
	CandidateWindow       int            `json:"candidate_window"`
	ExhaustedCandidateSet bool           `json:"exhausted_candidate_set"`
	Timing                SearchTiming   `json:"timing,omitempty"`
}

type SearchTiming struct {
	PGMS          int64 `json:"pg_ms"`
	ScoringMS     int64 `json:"scoring_ms"`
	HighlightMS   int64 `json:"highlight_ms"`
	CandidateSize int   `json:"candidate_size,omitempty"`
	IndexLagMS    int64 `json:"index_lag_ms,omitempty"`
}

type TagResult struct {
	Tag               string         `json:"tag"`
	EntityTypes       []string       `json:"entityTypes"`
	TotalCount        int            `json:"totalCount"`
	ResourceCount     int            `json:"resourceCount"`
	PostCount         int            `json:"postCount"`
	ServerCount       int            `json:"serverCount"`
	VideoCount        int            `json:"videoCount"`
	DocumentCount     int            `json:"documentCount"`
	UserCount         int            `json:"userCount"`
	OrganizationCount int            `json:"organizationCount"`
	TagCount          int            `json:"tagCount"`
	OfficialTagCount  int            `json:"officialTagCount"`
	CategoryCounts    map[string]int `json:"categoryCounts"`
	Payload           map[string]any `json:"payload"`
	UpdatedAt         time.Time      `json:"updatedAt"`
}

type Status struct {
	Total             int64            `json:"total"`
	ByEntityType      map[string]int64 `json:"byEntityType"`
	RecentSyncTime    *time.Time       `json:"recentSyncTime,omitempty"`
	FailedJobCount    int64            `json:"failedJobCount"`
	RunningJobCount   int64            `json:"runningJobCount"`
	LastFailedMessage string           `json:"lastFailedMessage,omitempty"`
}
