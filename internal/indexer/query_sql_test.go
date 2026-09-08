package indexer

import (
	"strings"
	"testing"
)

func TestRelevancePlanUsesPostgresRankAndStableTieBreakers(t *testing.T) {
	index := 1
	plan := buildSearchSQLPlan(SortRelevance, true, DefaultRuntimeConfig().Scoring, "$1", func(any) string {
		index++
		return "$" + string(rune('0'+index))
	})
	for _, expected := range []string{"ts_rank_cd", "ARRAY[", "sort_score DESC", "sort_time DESC", "entity_type ASC", "entity_id ASC"} {
		if !strings.Contains(plan.scoreExpr+plan.orderBy, expected) {
			t.Fatalf("expected %q in plan: %#v", expected, plan)
		}
	}
}

func TestLatestPlanDoesNotDependOnScore(t *testing.T) {
	calls := 0
	plan := buildSearchSQLPlan(SortLatest, true, DefaultRuntimeConfig().Scoring, "$1", func(any) string {
		calls++
		return "$1"
	})
	if plan.scoreExpr != "0::double precision" || strings.Contains(plan.orderBy, "sort_score") {
		t.Fatalf("unexpected latest plan: %#v", plan)
	}
	if calls != 0 {
		t.Fatalf("latest sort must not bind unused scoring values, got %d", calls)
	}
}

func TestKeysetPredicateCarriesFullStableTuple(t *testing.T) {
	args := []any{}
	predicate := keysetPredicate(SortPopular, &searchCursor{
		Score: 12.5, SortTime: "2026-09-06T12:00:00Z", EntityType: "resource", EntityID: "item-1",
	}, &args)
	for _, expected := range []string{"sort_score <", "sort_time <", "entity_type >", "entity_id >"} {
		if !strings.Contains(predicate, expected) {
			t.Fatalf("expected %q in keyset predicate: %s", expected, predicate)
		}
	}
	if len(args) != 4 {
		t.Fatalf("expected cursor tuple arguments, got %d", len(args))
	}
}
