package indexer

import (
	"fmt"
	"strings"
)

const maxQueryCharacters = 256
const maxQueryTerms = 24
const maxQueryClauses = 32
const maxMustNotClauses = 8
const maxFilterValues = 50
const maxFilterValuesAll = 128

type QueryError struct {
	Code    string
	Message string
}

func (e *QueryError) Error() string { return e.Message }

func normalizeSearchQuery(req SearchQuery) (SearchQuery, error) {
	req.Query = strings.TrimSpace(req.Query)
	if len([]rune(req.Query)) > maxQueryCharacters {
		return SearchQuery{}, &QueryError{Code: "query_too_long", Message: "query exceeds the character limit"}
	}
	if strings.TrimSpace(string(req.Sort)) == "" && req.Query == "" {
		req.Sort = SortLatest
	}
	sortMode, err := normalizeSearchSort(req.Sort)
	if err != nil {
		return SearchQuery{}, &QueryError{Code: "invalid_sort", Message: "invalid sort"}
	}
	req.Sort = sortMode
	if req.Query == "" && req.Sort == SortRelevance {
		return SearchQuery{}, &QueryError{Code: "invalid_sort_for_query", Message: "relevance sort requires query text"}
	}
	if len(normalizeStrings(tokenizeForSearch(req.Query))) > maxQueryTerms {
		return SearchQuery{}, &QueryError{Code: "query_term_limit_exceeded", Message: "too many query terms"}
	}
	if req.EntityType != "" {
		req.Filters.EntityTypes = append(req.Filters.EntityTypes, req.EntityType)
	}
	if req.CategoryID != "" {
		req.Filters.CategoryIDs = append(req.Filters.CategoryIDs, req.CategoryID)
	}
	if req.Status != "" {
		req.Filters.Statuses = append(req.Filters.Statuses, req.Status)
	}
	if len(req.Tags) > 0 {
		req.Filters.TagsAll = append(req.Filters.TagsAll, req.Tags...)
	}
	if err := validateClauses(req.Must, req.Should, req.MustNot); err != nil {
		return SearchQuery{}, err
	}
	if err := validateFilters(req.Filters); err != nil {
		return SearchQuery{}, err
	}
	return req, nil
}

func validateClauses(groups ...[]QueryClause) error {
	total := 0
	fuzzyCount := 0
	for groupIndex, group := range groups {
		if groupIndex == 2 && len(group) > maxMustNotClauses {
			return &QueryError{Code: "must_not_limit_exceeded", Message: "too many must_not clauses"}
		}
		total += len(group)
		for _, clause := range group {
			field := strings.ToLower(strings.TrimSpace(clause.Field))
			operator := strings.ToLower(strings.TrimSpace(clause.Operator))
			if field != "_all" && field != "title" && field != "tags" && field != "keywords" {
				return &QueryError{Code: "invalid_query_field", Message: fmt.Sprintf("unsupported query field %q", clause.Field)}
			}
			if operator == "" {
				operator = "match"
			}
			if operator != "match" && operator != "phrase" && operator != "prefix" && operator != "fuzzy" {
				return &QueryError{Code: "invalid_query_operator", Message: fmt.Sprintf("unsupported query operator %q", clause.Operator)}
			}
			value := strings.TrimSpace(clause.Value)
			if value == "" {
				return &QueryError{Code: "invalid_query_clause", Message: "query clause value is required"}
			}
			if operator == "prefix" && len([]rune(value)) < 2 {
				return &QueryError{Code: "prefix_too_short", Message: "prefix requires at least two characters"}
			}
			if operator == "fuzzy" {
				fuzzyCount++
				if len([]rune(value)) < 3 {
					return &QueryError{Code: "fuzzy_too_short", Message: "fuzzy requires at least three characters"}
				}
			}
		}
	}
	if total > maxQueryClauses {
		return &QueryError{Code: "query_clause_limit_exceeded", Message: "too many query clauses"}
	}
	if fuzzyCount > 2 {
		return &QueryError{Code: "fuzzy_limit_exceeded", Message: "at most two fuzzy clauses are allowed"}
	}
	return nil
}

func validateFilters(filters SearchFilters) error {
	total := 0
	for _, values := range [][]string{filters.EntityTypes, filters.CategoryIDs, filters.Statuses, filters.TagsAny, filters.TagsAll, filters.Platforms} {
		if len(values) > maxFilterValues {
			return &QueryError{Code: "filter_value_limit_exceeded", Message: "too many values for one filter"}
		}
		total += len(values)
	}
	if total > maxFilterValuesAll {
		return &QueryError{Code: "filter_value_limit_exceeded", Message: "too many filter values"}
	}
	for _, value := range []string{filters.MinecraftVersion.GTE, filters.MinecraftVersion.GT, filters.MinecraftVersion.LTE, filters.MinecraftVersion.LT} {
		if strings.TrimSpace(value) != "" {
			if _, ok := minecraftVersionKey(value); !ok {
				return &QueryError{Code: "invalid_minecraft_version", Message: "minecraft version must be major.minor.patch"}
			}
		}
	}
	return nil
}
