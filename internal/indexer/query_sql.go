package indexer

import (
	"fmt"
	"strings"
)

type searchSQLPlan struct {
	scoreExpr string
	orderBy   string
}

func buildSearchSQLPlan(sort SearchSort, hasTextQuery bool, cfg ScoringConfig, scoringNowExpr string, arg func(any) string) searchSQLPlan {
	plan := searchSQLPlan{}
	switch sort {
	case SortLatest:
		plan.scoreExpr = "0::double precision"
		plan.orderBy = "sort_time DESC, entity_type ASC, entity_id ASC"
		return plan
	}
	freshness := fmt.Sprintf(
		"exp(-greatest(0, extract(epoch from (%s::timestamptz - coalesce(s.updated_at, s.created_at, s.indexed_at))) / 86400.0) / %s)",
		scoringNowExpr, arg(cfg.TimeDecayFactor),
	)
	popularity := fmt.Sprintf(
		"(ln(1 + greatest(s.view_count, 0)) * %s + ln(1 + greatest(s.download_count, 0)) * %s + ln(1 + greatest(s.like_count, 0)) * %s + ln(1 + greatest(s.weight, 0)) * 0.05)",
		arg(cfg.ViewCountWeight), arg(cfg.DownloadCountWeight), arg(cfg.LikeCountWeight),
	)
	if sort == SortPopular {
		plan.scoreExpr = fmt.Sprintf("(%s * %s + %s * %s)", popularity, arg(cfg.PopularityScoreWeight), freshness, arg(cfg.FreshnessScoreWeight))
		plan.orderBy = "sort_score DESC, sort_time DESC, entity_type ASC, entity_id ASC"
		return plan
	}
	textRank := "0::double precision"
	if hasTextQuery {
		totalTextWeight := cfg.PayloadWeight + cfg.KeywordsWeight + cfg.TagsWeight + cfg.TitleWeight
		if totalTextWeight <= 0 {
			totalTextWeight = 1
		}
		textRank = fmt.Sprintf(
			"ts_rank_cd(ARRAY[%s, %s, %s, %s]::real[], s.search_vector, q.query, 32)::double precision",
			arg(cfg.PayloadWeight/totalTextWeight), arg(cfg.KeywordsWeight/totalTextWeight),
			arg(cfg.TagsWeight/totalTextWeight), arg(cfg.TitleWeight/totalTextWeight),
		)
	}
	plan.scoreExpr = fmt.Sprintf("(%s * %s + %s * %s + %s * %s)", textRank, arg(cfg.TextScoreWeight), popularity, arg(cfg.PopularityScoreWeight), freshness, arg(cfg.FreshnessScoreWeight))
	plan.orderBy = "sort_score DESC, sort_time DESC, entity_type ASC, entity_id ASC"
	return plan
}

func keysetPredicate(sort SearchSort, cursor *searchCursor, args *[]any) string {
	if cursor == nil {
		return ""
	}
	add := func(value any) string {
		*args = append(*args, value)
		return fmt.Sprintf("$%d", len(*args))
	}
	entityType := add(cursor.EntityType)
	entityID := add(cursor.EntityID)
	sortTime := add(cursor.SortTime)
	if sort == SortLatest {
		return fmt.Sprintf("(sort_time < %s OR (sort_time = %s AND (entity_type > %s OR (entity_type = %s AND entity_id > %s))))", sortTime, sortTime, entityType, entityType, entityID)
	}
	score := add(cursor.Score)
	return fmt.Sprintf("(sort_score < %s OR (sort_score = %s AND (sort_time < %s OR (sort_time = %s AND (entity_type > %s OR (entity_type = %s AND entity_id > %s))))))", score, score, sortTime, sortTime, entityType, entityType, entityID)
}

func matchedFields(item SearchResult, query StructuredQuery) []string {
	terms := query.Terms
	if len(query.ExpandedTerms) > 0 {
		terms = query.ExpandedTerms
	}
	matched := []string{}
	if fieldMatchScore(item.Title, query.Normalized, terms) > 0 {
		matched = append(matched, "title")
	}
	if fieldMatchScore(strings.Join(item.Tags, " "), query.Normalized, terms) > 0 {
		matched = append(matched, "tags")
	}
	if fieldMatchScore(strings.Join(item.Keywords, " "), query.Normalized, terms) > 0 {
		matched = append(matched, "keywords")
	}
	if fieldMatchScore(item.payloadText, query.Normalized, terms) > 0 {
		matched = append(matched, "payload")
	}
	return matched
}
