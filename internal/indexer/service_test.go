package indexer

import (
	"strings"
	"testing"
)

func TestSourceVersionConflictRejectsOlderWrites(t *testing.T) {
	update, guard := sourceVersionConflict(true)
	if update != "source_version = EXCLUDED.source_version" {
		t.Fatalf("unexpected version update clause: %s", update)
	}
	if guard != " WHERE search_index.source_version <= EXCLUDED.source_version" {
		t.Fatalf("unexpected version guard: %s", guard)
	}

	unversionedUpdate, unversionedGuard := sourceVersionConflict(false)
	if unversionedUpdate != "source_version = search_index.source_version" || unversionedGuard != " WHERE search_index.source_version = 0" {
		t.Fatalf("unversioned writes must not overwrite outbox-managed documents")
	}
}

func TestBuildDocumentKeepsOnlyUserTagsInDisplayTags(t *testing.T) {
	uuidTag := "e23b8989-f252-452e-aa42-1256fff19f67"
	doc := buildDocument(sourceRow{
		entityType:    EntityPost,
		entityID:      "post-1",
		title:         "Topic tagged post",
		status:        "published",
		visibility:    "public",
		tagsJSON:      `["教程"]`,
		extraTagsJSON: `[{"id":"` + uuidTag + `","name":"Blockbench","icon":"/icons/blockbench.ico"},"` + uuidTag + `"]`,
		payload:       map[string]any{},
	})

	if containsString(doc.Tags, uuidTag) {
		t.Fatalf("expected opaque id to be filtered from display tags, got %#v", doc.Tags)
	}
	if containsString(doc.Tags, "blockbench") {
		t.Fatalf("expected extra topic/official tags to stay out of display tags, got %#v", doc.Tags)
	}
	if !containsString(doc.Tags, "教程") {
		t.Fatalf("expected ordinary tag to be preserved, got %#v", doc.Tags)
	}
	if !containsString(doc.Keywords, "blockbench") {
		t.Fatalf("expected extra topic/official tags to remain searchable, got %#v", doc.Keywords)
	}
}

func TestMergeStringsFiltersOpaqueIDs(t *testing.T) {
	merged := mergeStrings(
		[]string{"model", "9f540498-9d78-4a50-b8f7-050f5f42183d"},
		[]string{"模型官方分类"},
	)

	if containsString(merged, "9f540498-9d78-4a50-b8f7-050f5f42183d") {
		t.Fatalf("expected uuid-looking tag to be filtered, got %#v", merged)
	}
	if !containsString(merged, "model") || !containsString(merged, "模型官方分类") {
		t.Fatalf("expected readable tags to be preserved, got %#v", merged)
	}
}

func TestPostSelectSQLHidesUnpublishedAndProtectedPosts(t *testing.T) {
	sql, err := selectSQL(EntityPost, false)
	if err != nil {
		t.Fatalf("selectSQL returned an error: %v", err)
	}

	for _, expected := range []string{
		`src."publishedAt" IS NOT NULL`,
		`src."accessMode" = 'none'`,
		`'accessMode', src."accessMode"`,
	} {
		if !strings.Contains(sql, expected) {
			t.Fatalf("post source SQL is missing %q", expected)
		}
	}
}
