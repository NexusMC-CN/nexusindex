package indexer

import (
	"math"
	"testing"
	"time"
)

func TestQueryScoringStability(t *testing.T) {
	cfg := DefaultRuntimeConfig()
	pipeline := NewQueryPipeline()
	structured := pipeline.Build("Fabric 模组 1.20.1", cfg.Query)
	now := time.Date(2026, 6, 19, 12, 0, 0, 0, time.UTC)
	item := SearchResult{
		ID:            "resource:stable",
		EntityType:    EntityResource,
		EntityID:      "stable",
		Title:         "Fabric 模组 1.20.1",
		Tags:          []string{"fabric", "模组"},
		Keywords:      []string{"minecraft", "1.20.1"},
		ViewCount:     100,
		LikeCount:     10,
		DownloadCount: 20,
		UpdatedAt:     now.Add(-24 * time.Hour),
		Payload: map[string]any{
			"description": "Fabric 模组 for Minecraft 1.20.1",
		},
		payloadText: "Fabric 模组 for Minecraft 1.20.1",
	}

	var first float64
	for i := 0; i < 10; i++ {
		score, _, _ := scoreResult(item, structured, cfg.Scoring, false, 0.42, now)
		if i == 0 {
			first = score
			continue
		}
		if math.Abs(score-first) > 1e-9 {
			t.Fatalf("expected stable score, first=%f current=%f", first, score)
		}
	}
}
