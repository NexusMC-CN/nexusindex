package indexer

import "time"

type QueryClause struct {
	Field    string
	Operator string
	Value    string
}
type NumericRange struct {
	GTE *int
	GT  *int
	LTE *int
	LT  *int
}
type TimeRange struct {
	GTE *time.Time
	GT  *time.Time
	LTE *time.Time
	LT  *time.Time
}
type VersionRange struct {
	GTE string
	GT  string
	LTE string
	LT  string
}

type SearchFilters struct {
	EntityTypes      []string
	CategoryIDs      []string
	Statuses         []string
	TagsAny          []string
	TagsAll          []string
	Platforms        []string
	ViewCount        NumericRange
	LikeCount        NumericRange
	DownloadCount    NumericRange
	CreatedAt        TimeRange
	UpdatedAt        TimeRange
	MinecraftVersion VersionRange
}
