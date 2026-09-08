package indexer

import (
	"strings"
	"testing"
	"time"
)

func TestSearchFiltersCompileMultiValueAndRanges(t *testing.T) {
	now := time.Now()
	min := 10
	where, args := appendSearchFilters([]string{"s.generation_id = $1"}, []any{int64(1)}, SearchFilters{EntityTypes: []string{"resource", "server"}, TagsAll: []string{"fabric"}, Platforms: []string{"java"}, ViewCount: NumericRange{GTE: &min}, UpdatedAt: TimeRange{GTE: &now}})
	sql := strings.Join(where, " AND ")
	for _, expected := range []string{"ANY", "@>", "payload ->> 'platform'", "s.view_count >=", "s.updated_at >="} {
		if !strings.Contains(sql, expected) {
			t.Fatalf("missing %q: %s", expected, sql)
		}
	}
	if len(args) != 6 {
		t.Fatalf("unexpected args: %#v", args)
	}
}

func TestClausesCompileTextOperators(t *testing.T) {
	where, _, err := appendClauses(nil, nil, []QueryClause{
		{Field: "title", Operator: "phrase", Value: "fabric api"},
		{Field: "tags", Operator: "prefix", Value: "fabr"},
		{Field: "keywords", Operator: "fuzzy", Value: "fabric"},
	}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.Join(where, " ")
	for _, expected := range []string{"phraseto_tsquery", "to_tsquery", "similarity"} {
		if !strings.Contains(sql, expected) {
			t.Fatalf("missing %q in %s", expected, sql)
		}
	}
}

func TestMinecraftVersionKeyIsNumeric(t *testing.T) {
	low, ok := minecraftVersionKey("1.8.10")
	if !ok || low != 1008010 {
		t.Fatalf("unexpected key %d", low)
	}
	high, ok := minecraftVersionKey("1.20.1")
	if !ok || high <= low {
		t.Fatalf("expected numeric ordering")
	}
}

func TestInvalidMinecraftVersionIsRejected(t *testing.T) {
	_, err := normalizeSearchQuery(SearchQuery{Sort: SortLatest, Filters: SearchFilters{MinecraftVersion: VersionRange{GTE: "release"}}})
	if err == nil {
		t.Fatal("expected invalid version rejection")
	}
}
