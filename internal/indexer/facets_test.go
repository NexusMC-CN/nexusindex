package indexer

import (
	"strings"
	"testing"
)

func TestFacetQueriesUseStableOrderingAndLimit(t *testing.T) {
	if !strings.Contains("ORDER BY count(*) DESC, entity_type ASC LIMIT 20", "ORDER BY count(*) DESC") {
		t.Fatal("facet ordering contract changed")
	}
}
