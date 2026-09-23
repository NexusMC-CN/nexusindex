package indexer

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func appendSearchFilters(where []string, args []any, filters SearchFilters) ([]string, []any) {
	add := func(value any) string { args = append(args, value); return fmt.Sprintf("$%d", len(args)) }
	anyText := func(values []string) []string { return normalizeStrings(values) }
	if values := anyText(filters.EntityTypes); len(values) > 0 {
		where = append(where, "s.entity_type = ANY("+add(values)+"::text[])")
	}
	if values := literalFilterValues(filters.CategoryIDs); len(values) > 0 {
		where = append(where, "s.category_id = ANY("+add(values)+"::text[])")
	}
	if values := anyText(filters.Statuses); len(values) > 0 {
		where = append(where, "s.status = ANY("+add(values)+"::text[])")
	} else {
		where = append(where, "s.status NOT IN ('hidden', 'deleted')")
	}
	if values := anyText(filters.TagsAny); len(values) > 0 {
		where = append(where, "s.tags && "+add(values)+"::text[]")
	}
	if values := anyText(filters.TagsAll); len(values) > 0 {
		where = append(where, "s.tags @> "+add(values)+"::text[]")
	}
	if values := anyText(filters.Platforms); len(values) > 0 {
		where = append(where, "lower(coalesce(s.payload ->> 'platform', '')) = ANY("+add(values)+"::text[])")
	}
	where, args = appendNumericRange(where, args, "s.view_count", filters.ViewCount)
	where, args = appendNumericRange(where, args, "s.like_count", filters.LikeCount)
	where, args = appendNumericRange(where, args, "s.download_count", filters.DownloadCount)
	where, args = appendTimeRange(where, args, "s.created_at", filters.CreatedAt)
	where, args = appendTimeRange(where, args, "s.updated_at", filters.UpdatedAt)
	where, args = appendVersionRange(where, args, filters.MinecraftVersion)
	return where, args
}

func appendVersionRange(where []string, args []any, value VersionRange) ([]string, []any) {
	bounds := []struct{ op, raw string }{{">=", value.GTE}, {">", value.GT}, {"<=", value.LTE}, {"<", value.LT}}
	conditions := []string{}
	// CASE protects casts even when PostgreSQL reorders WHERE predicates.
	expression := `(CASE WHEN v ~ '^[0-9]{1,3}([.][0-9]{1,3}){0,2}$' THEN split_part(v, '.', 1)::int * 1000000 + coalesce(nullif(split_part(v, '.', 2), '')::int, 0) * 1000 + coalesce(nullif(split_part(v, '.', 3), '')::int, 0) END)`
	for _, bound := range bounds {
		key, ok := minecraftVersionKey(bound.raw)
		if !ok {
			continue
		}
		args = append(args, key)
		conditions = append(conditions, expression+" "+bound.op+" $"+fmt.Sprint(len(args)))
	}
	if len(conditions) == 0 {
		return where, args
	}
	versions := `CASE WHEN jsonb_typeof(s.payload->'mcVersions') = 'array' THEN s.payload->'mcVersions' WHEN jsonb_typeof(s.payload->'versions') = 'array' THEN s.payload->'versions' ELSE '[]'::jsonb END`
	where = append(where, "EXISTS (SELECT 1 FROM jsonb_array_elements_text("+versions+") AS versions(v) WHERE "+strings.Join(conditions, " AND ")+")")
	return where, args
}

var minecraftVersionPattern = regexp.MustCompile(`^[0-9]{1,3}(\.[0-9]{1,3}){0,2}$`)

func minecraftVersionKey(raw string) (int, bool) {
	raw = strings.TrimPrefix(strings.TrimSpace(raw), "v")
	if !minecraftVersionPattern.MatchString(raw) {
		return 0, false
	}
	parts := strings.Split(raw, ".")
	multipliers := []int{1000000, 1000, 1}
	key := 0
	for i, part := range parts {
		n, err := strconv.Atoi(part)
		if err != nil || n < 0 || n > 999 {
			return 0, false
		}
		key += n * multipliers[i]
	}
	return key, true
}

func literalFilterValues(values []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" && !seen[value] {
			seen[value] = true
			out = append(out, value)
		}
	}
	return out
}

func appendNumericRange(where []string, args []any, column string, value NumericRange) ([]string, []any) {
	for _, item := range []struct {
		op    string
		value *int
	}{{">=", value.GTE}, {">", value.GT}, {"<=", value.LTE}, {"<", value.LT}} {
		if item.value != nil {
			args = append(args, *item.value)
			where = append(where, fmt.Sprintf("%s %s $%d", column, item.op, len(args)))
		}
	}
	return where, args
}

func appendTimeRange(where []string, args []any, column string, value TimeRange) ([]string, []any) {
	for _, item := range []struct {
		op    string
		value *time.Time
	}{{">=", value.GTE}, {">", value.GT}, {"<=", value.LTE}, {"<", value.LT}} {
		if item.value != nil {
			args = append(args, *item.value)
			where = append(where, fmt.Sprintf("%s %s $%d", column, item.op, len(args)))
		}
	}
	return where, args
}

func appendClauses(where []string, args []any, must, should, mustNot []QueryClause, configs ...QueryConfig) ([]string, []any, error) {
	cfg := QueryConfig{}
	if len(configs) > 0 {
		cfg = configs[0]
	}
	addGroup := func(clauses []QueryClause, joiner string, negate bool) error {
		expressions := []string{}
		for _, clause := range clauses {
			operator := strings.ToLower(strings.TrimSpace(clause.Operator))
			if operator == "" {
				operator = "match"
			}
			field := strings.ToLower(strings.TrimSpace(clause.Field))
			column := "s.search_vector"
			textColumn := "s.title || ' ' || array_to_string(s.tags, ' ') || ' ' || array_to_string(s.keywords, ' ') || ' ' || coalesce(s.payload::text, '')"
			switch field {
			case "_all":
			case "title":
				column = `ts_filter(s.search_vector, '{A}')`
				textColumn = "s.title"
			case "tags":
				column = `ts_filter(s.search_vector, '{B}')`
				textColumn = "array_to_string(s.tags, ' ')"
			case "keywords":
				column = `ts_filter(s.search_vector, '{C}')`
				textColumn = "array_to_string(s.keywords, ' ')"
			default:
				return &QueryError{Code: "invalid_query_field", Message: "unsupported query field"}
			}
			value := strings.TrimSpace(clause.Value)
			tokens := tokenizeForSearch(value)
			if len(tokens) == 0 {
				return &QueryError{Code: "invalid_query_clause", Message: "query clause must contain searchable text"}
			}
			switch operator {
			case "match":
				value = NewQueryPipeline().Build(value, cfg).TSQueryText
			case "phrase":
				value = strings.Join(tokens, " ")
			case "prefix":
				value = compileSearchTokens(tokens, true)
			}
			args = append(args, value)
			argument := "$" + fmt.Sprint(len(args))
			expression := ""
			switch operator {
			case "match":
				expression = column + " @@ to_tsquery('simple', " + argument + ")"
			case "phrase":
				expression = column + " @@ phraseto_tsquery('simple', " + argument + ")"
			case "prefix":
				expression = column + " @@ to_tsquery('simple', " + argument + ")"
			case "fuzzy":
				expression = "similarity(lower(" + textColumn + "), lower(" + argument + ")) >= 0.35"
			default:
				return &QueryError{Code: "invalid_query_operator", Message: "unsupported query operator"}
			}
			if negate {
				expression = "NOT (" + expression + ")"
			}
			expressions = append(expressions, expression)
		}
		if len(expressions) > 0 {
			where = append(where, "("+strings.Join(expressions, " "+joiner+" ")+")")
		}
		return nil
	}
	if err := addGroup(must, "AND", false); err != nil {
		return nil, nil, err
	}
	if len(should) > 0 {
		if err := addGroup(should, "OR", false); err != nil {
			return nil, nil, err
		}
	}
	if err := addGroup(mustNot, "AND", true); err != nil {
		return nil, nil, err
	}
	return where, args, nil
}
