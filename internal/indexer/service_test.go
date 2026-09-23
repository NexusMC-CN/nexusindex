package indexer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/blockbridge/avmcbbs/apps/nexusindex/internal/migrations"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestSearchSymbolsAreRejectedBeforeDatabaseAccess(t *testing.T) {
	defer func() {
		if value := recover(); value != nil {
			t.Errorf("empty query reached database: %v", value)
		}
	}()
	_, err := NewService(nil, nil).Search(context.Background(), SearchQuery{Query: "???"})
	var queryError *QueryError
	if !errors.As(err, &queryError) {
		t.Fatalf("expected typed query error, got %v", err)
	}
}

func TestHighlightUsesOriginalRuneBoundaries(t *testing.T) {
	for _, tc := range []struct {
		value string
		terms []string
		want  string
	}{
		{"Ⱥ A", []string{"a"}, "Ⱥ <mark>A</mark>"},
		{"fabric mark", []string{"fabric", "mark"}, "<mark>fabric</mark> <mark>mark</mark>"},
		{"<fabric> & mark", []string{"fabric", "mark"}, "&lt;<mark>fabric</mark>&gt; &amp; <mark>mark</mark>"},
		{"fabric", []string{"fab", "fabric"}, "<mark>fabric</mark>"},
	} {
		t.Run(tc.value, func(t *testing.T) {
			defer func() {
				if value := recover(); value != nil {
					t.Errorf("highlight panicked: %v", value)
				}
			}()
			if got := highlightText(tc.value, tc.terms, 160); got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

func TestBuildDocumentPayloadNumbersStayDecimal(t *testing.T) {
	for _, tc := range []struct {
		value any
		want  string
	}{
		{float64(1234567), "1234567"},
		{json.Number("9007199254740993"), "9007199254740993"},
		{json.Number("1.234567e6"), "1234567"},
	} {
		if got := strings.TrimSpace(flattenPayloadText(map[string]any{"id": tc.value})); got != tc.want {
			t.Errorf("number text got %q, want %q", got, tc.want)
		}
	}
}

func TestBuildDocumentPostBodyIsNotSerializable(t *testing.T) {
	for _, body := range []string{"独角兽 HiddenPurchaseSecret", strings.Repeat("这是帖子正文。", 20) + "独角兽 HiddenPurchaseSecret"} {
		payload := map[string]any{"accessMode": "none"}
		doc := buildDocument(sourceRow{entityType: EntityPost, description: body, payload: payload})
		raw, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "独角兽") || strings.Contains(strings.ToLower(string(raw)), "hiddenpurchasesecret") {
			t.Errorf("post body serialized: %s", raw)
		}
		if _, exists := doc.Payload["content"]; exists {
			t.Errorf("post body added to public payload")
		}
	}
}

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

func TestSearchResultIDsAlwaysIncludeEntityType(t *testing.T) {
	for _, tc := range []struct {
		entityType string
		entityID   string
		want       string
	}{
		{EntityDocument, "resource:x", "document:resource:x"},
		{EntityResource, "x", "resource:x"},
		{EntityPost, "x", "post:x"},
	} {
		if got := documentID(tc.entityType, tc.entityID); got != tc.want {
			t.Errorf("documentID(%q, %q)=%q, want %q", tc.entityType, tc.entityID, got, tc.want)
		}
	}
	if documentID(EntityDocument, "resource:x") == documentID(EntityResource, "x") {
		t.Fatal("document/resource IDs collide across entity types")
	}
}

func TestWriteBoundaryRejectsUnknownEntityTypeBeforeDatabaseAccess(t *testing.T) {
	service := NewService(nil, nil)
	for name, write := range map[string]func() error{
		"upsert": func() error {
			_, err := service.Upsert(context.Background(), SearchDocument{EntityType: "document:resource", EntityID: "x"})
			return err
		},
		"versioned upsert": func() error {
			_, err := service.UpsertVersioned(context.Background(), SearchDocument{EntityType: "document:resource", EntityID: "x"}, 1)
			return err
		},
		"delete": func() error {
			_, err := service.MarkDeleted(context.Background(), "document:resource", "x")
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if recovered := recover(); recovered != nil {
					t.Fatalf("unknown entity type reached database: %v", recovered)
				}
			}()
			if err := write(); err == nil {
				t.Fatal("unknown entity type was accepted")
			}
		})
	}
}

func TestBuildDocumentIncludesReadableOfficialTags(t *testing.T) {
	uuidTag := "e23b8989-f252-452e-aa42-1256fff19f67"
	doc := buildDocument(sourceRow{
		entityType:    EntityResource,
		entityID:      "resource-1",
		title:         "Officially tagged resource",
		status:        "published",
		visibility:    "public",
		tagsJSON:      `["教程"]`,
		extraTagsJSON: `[{"id":"` + uuidTag + `","name":"Blockbench","icon":"/icons/blockbench.ico"},"` + uuidTag + `"]`,
		payload:       map[string]any{},
	})

	if containsString(doc.Tags, uuidTag) {
		t.Fatalf("expected opaque id to be filtered from display tags, got %#v", doc.Tags)
	}
	if !containsString(doc.Tags, "blockbench") {
		t.Fatalf("expected extra topic/official tags to support filters and facets, got %#v", doc.Tags)
	}
	if !containsString(doc.Tags, "教程") {
		t.Fatalf("expected ordinary tag to be preserved, got %#v", doc.Tags)
	}
	if !containsString(doc.Keywords, "blockbench") {
		t.Fatalf("expected extra topic/official tags to remain searchable, got %#v", doc.Keywords)
	}
}

func TestBuildDocumentKeepsNonResourceMetadataOutOfTags(t *testing.T) {
	for _, entity := range []string{EntityPost, EntityServer, EntityDocument, EntityUser} {
		doc := buildDocument(sourceRow{entityType: entity, tagsJSON: `["survival"]`, extraTagsJSON: `["1.20.1"]`})
		if len(doc.Tags) != 1 || doc.Tags[0] != "survival" {
			t.Errorf("%s metadata leaked into tags: %#v", entity, doc.Tags)
		}
		if !containsString(doc.Keywords, "1.20.1") {
			t.Errorf("%s metadata lost from keywords", entity)
		}
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

// Integration tests run the production Upsert/Search/Tags paths. Each test
// owns an isolated database, including its extensions. A missing test database
// is an explicit skip; the test role needs CREATEDB permission.
func searchIntegrationService(t *testing.T) *Service {
	t.Helper()
	url := os.Getenv("NEXUSINDEX_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("NEXUSINDEX_TEST_DATABASE_URL is not configured")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	database := fmt.Sprintf("nexusindex_search_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{database}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+identifier); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.Database = database
	cfg.ConnConfig.RuntimeParams["statement_timeout"] = "10000"
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP DATABASE "+identifier)
		admin.Close()
	})
	if err := migrations.Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	return NewServiceWithRuntimeAndCursor(pool, pool, DefaultRuntimeConfig(), "search-test")
}

func putSearchTestDocument(t *testing.T, service *Service, doc SearchDocument) {
	t.Helper()
	if doc.EntityID == "" {
		doc.EntityID = "fixture"
	}
	if doc.EntityType == "" {
		doc.EntityType = EntityDocument
	}
	if doc.Payload == nil {
		doc.Payload = map[string]any{}
	}
	doc.CreatedAt = time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	doc.UpdatedAt = doc.CreatedAt
	if _, err := service.Upsert(context.Background(), doc); err != nil {
		t.Fatal(err)
	}
}

func TestSearchIntegrationTextRecall(t *testing.T) {
	service := searchIntegrationService(t)
	putSearchTestDocument(t, service, SearchDocument{EntityID: "negative-control", Title: "unrelated"})
	for _, tc := range []struct{ title, query string }{
		{"模组", "模组"}, {"mod", "模组"}, {"mods", "模组"},
		{"hello世界", "hello世界"}, {"1.20.1", "1.20.1"},
		{"modern", "modern"}, {"テスト", "テスト"},
	} {
		t.Run(tc.title+"/"+tc.query, func(t *testing.T) {
			putSearchTestDocument(t, service, SearchDocument{Title: tc.title})
			page, err := service.Search(context.Background(), SearchQuery{Query: tc.query})
			if err != nil {
				t.Fatal(err)
			}
			if page.Total != 1 || len(page.Items) != 1 {
				t.Fatalf("query %q lost %q: %#v", tc.query, tc.title, page)
			}
		})
	}
}

func TestSearchIntegrationNoncontiguousSynonymHighlightsTokens(t *testing.T) {
	service := searchIntegrationService(t)
	putSearchTestDocument(t, service, SearchDocument{Title: "mod useful pack"})
	for _, req := range []SearchQuery{
		{Query: "整合包"},
		{Must: []QueryClause{{Field: "title", Value: "整合包"}}},
	} {
		page, err := service.Search(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if page.Total != 1 || len(page.Items) != 1 {
			t.Fatalf("noncontiguous synonym was not recalled: %#v", page)
		}
		item := page.Items[0]
		if !containsString(item.MatchedFields, "title") {
			t.Errorf("synonym title missing from matched fields: %#v", item.MatchedFields)
		}
		if got := item.Highlight["title"]; got != "<mark>mod</mark> useful <mark>pack</mark>" {
			t.Errorf("noncontiguous synonym highlight: %#v", got)
		}
	}
}

func TestSearchIntegrationClausesAndHighlights(t *testing.T) {
	service := searchIntegrationService(t)
	cfg := service.RuntimeConfig()
	cfg.Query.Synonyms = map[string][]string{}
	service.UpdateRuntimeConfig(context.Background(), cfg)
	putSearchTestDocument(t, service, SearchDocument{Title: "fabric api", Tags: []string{"guide"}, Payload: map[string]any{"": "fabric"}})
	for _, field := range []string{"_all", "title"} {
		page, err := service.Search(context.Background(), SearchQuery{Must: []QueryClause{{Field: field, Operator: "prefix", Value: "fabric api"}}})
		if err != nil {
			t.Error(err)
			continue
		}
		if page.Total != 1 {
			t.Fatalf("multiword prefix failed for %s", field)
		}
	}
	page, err := service.Search(context.Background(), SearchQuery{
		Must: []QueryClause{{Field: "title", Value: "fabric"}}, Should: []QueryClause{{Field: "tags", Value: "guide"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("clause result missing: %#v", page)
	}
	item := page.Items[0]
	if !containsString(item.MatchedFields, "title") || !containsString(item.MatchedFields, "tags") || item.Highlight["title"] != "<mark>fabric</mark> api" {
		t.Errorf("clause matches absent from result metadata: %#v", item)
	}
	putSearchTestDocument(t, service, SearchDocument{Payload: map[string]any{"": "fabric"}})
	page, err = service.Search(context.Background(), SearchQuery{Must: []QueryClause{{Field: "_all", Operator: "fuzzy", Value: "fabric"}}})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 1 {
		t.Fatalf("payload-only fuzzy match lost: %#v", page)
	}
}

func TestSearchIntegrationFiltersTagsAndStatus(t *testing.T) {
	service := searchIntegrationService(t)
	doc := buildDocument(sourceRow{entityType: EntityResource, entityID: "fixture", title: "model", status: "active", categoryID: "ModPackA", extraTagsJSON: `["Blockbench"]`, payload: map[string]any{}})
	putSearchTestDocument(t, service, doc)
	page, err := service.Search(context.Background(), SearchQuery{Filters: SearchFilters{CategoryIDs: []string{"ModPackA"}, TagsAny: []string{"Blockbench"}}})
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 1 || len(page.Facets.Tag) != 1 || page.Facets.Tag[0].Value != "blockbench" {
		t.Errorf("official tag/category not filterable: %#v", page)
	}
	if err := service.RebuildTags(context.Background()); err != nil {
		t.Fatal(err)
	}
	tags, total, err := service.Tags(context.Background(), "Blockbench", "name", 20, 0)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(tags) != 1 || tags[0].ResourceCount != 1 {
		t.Errorf("official tag not aggregated: %#v", tags)
	}
	for _, status := range []string{" HIDDEN ", "DELETED"} {
		doc.Status = status
		putSearchTestDocument(t, service, doc)
		page, err = service.Search(context.Background(), SearchQuery{})
		if err != nil {
			t.Fatal(err)
		}
		if page.Total != 0 {
			t.Errorf("status %q bypassed visibility: %#v", status, page)
		}
	}
}

func TestSearchIntegrationTagQueryIsLiteral(t *testing.T) {
	service := searchIntegrationService(t)
	putSearchTestDocument(t, service, SearchDocument{Tags: []string{"foo_bar", "fooxbar", "foo%bar", `foo\bar`}})
	if err := service.RebuildTags(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"foo_bar", "foo%bar", `foo\bar`} {
		tags, total, err := service.Tags(context.Background(), query, "name", 20, 0)
		if err != nil {
			t.Fatal(err)
		}
		if total != 1 || len(tags) != 1 || tags[0].Tag != query {
			t.Errorf("literal %q matched %#v (total %d)", query, tags, total)
		}
	}
}

func TestSearchIntegrationVersionPayloadCannotOverflow(t *testing.T) {
	service := searchIntegrationService(t)
	for _, version := range []string{"2148.0.0", "1.20.1000", "9999999999999999999999999", "1.20.1junk"} {
		t.Run(version, func(t *testing.T) {
			putSearchTestDocument(t, service, SearchDocument{Payload: map[string]any{"mcVersions": []any{version}}})
			page, err := service.Search(context.Background(), SearchQuery{Filters: SearchFilters{MinecraftVersion: VersionRange{GTE: "1.0.0"}}})
			if err != nil {
				t.Fatal(err)
			}
			if page.Total != 0 {
				t.Fatalf("invalid payload version %q matched", version)
			}
		})
	}
}

func TestScoreIntegrationIntegerMaxDoesNotOverflow(t *testing.T) {
	service := searchIntegrationService(t)
	putSearchTestDocument(t, service, SearchDocument{Title: "fixture", ViewCount: math.MaxInt32, LikeCount: math.MaxInt32, DownloadCount: math.MaxInt32, Weight: math.MaxInt32})
	for _, sort := range []SearchSort{SortPopular, SortRelevance} {
		page, err := service.Search(context.Background(), SearchQuery{Query: "fixture", Sort: sort})
		if err != nil {
			t.Errorf("%s: %v", sort, err)
			continue
		}
		if len(page.Items) != 1 || page.Items[0].Score <= 0 || math.IsInf(page.Items[0].Score, 0) {
			t.Errorf("invalid score: %#v", page)
		}
	}
}

func TestScoreIntegrationPipelineBoostsChangeOrdering(t *testing.T) {
	service := searchIntegrationService(t)
	cfg := service.RuntimeConfig()
	cfg.Query.Synonyms = map[string][]string{"fabric": {"loom"}, "1.20.1": {"release"}}
	service.UpdateRuntimeConfig(context.Background(), cfg)
	for _, tc := range []struct{ query, alternative string }{{"fabric", "loom"}, {"1.20.1", "release"}} {
		putSearchTestDocument(t, service, SearchDocument{EntityID: "a", Title: tc.alternative})
		putSearchTestDocument(t, service, SearchDocument{EntityID: "z", Title: tc.query})
		page, err := service.Search(context.Background(), SearchQuery{Query: tc.query})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Items) != 2 || page.Items[0].EntityID != "z" || page.Items[0].Score <= page.Items[1].Score {
			t.Errorf("boost for %q did not rank literal first: %#v", tc.query, page)
		}
	}
}

func TestBuildDocumentIntegrationSnapshotNumbersPreserveLexicalValue(t *testing.T) {
	service := searchIntegrationService(t)
	rows, err := service.mainDB.Query(context.Background(), `SELECT 'post', 'fixture', '', 'active', 'public', '', '', '[]', '[]', '[]', '', 0, 0, 0, 0, now(), now(), '{"id":9007199254740993,"count":1234567}'::jsonb`)
	if err != nil {
		t.Fatal(err)
	}
	docs, err := scanDocuments(rows)
	rows.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(docs) != 1 {
		t.Fatal("missing source fixture")
	}
	if got := fmt.Sprint(docs[0].Payload["id"]); got != "9007199254740993" {
		t.Fatalf("source numeric precision lost: %q", got)
	}
	putSearchTestDocument(t, service, docs[0])
	for _, query := range []string{"9007199254740993", "1234567"} {
		page, err := service.Search(context.Background(), SearchQuery{Query: query})
		if err != nil {
			t.Fatal(err)
		}
		if len(page.Items) != 1 {
			t.Fatalf("numeric text %q was not searchable", query)
		}
		if got := fmt.Sprint(page.Items[0].Payload["id"]); got != "9007199254740993" {
			t.Fatalf("result numeric precision lost: %q", got)
		}
	}
}

func TestBuildDocumentIntegrationLongChinesePostRecall(t *testing.T) {
	service := searchIntegrationService(t)
	body := strings.Repeat("这是帖子正文。", 20) + "独角兽 HiddenPurchaseSecret"
	for _, payload := range []string{`{"accessMode":"none"}`} {
		rows, err := service.mainDB.Query(context.Background(), `SELECT 'post', 'fixture', 'title', 'active', 'public', '', '', '[]', '[]', '[]', $1::text, 0, 0, 0, 0, now(), now(), $2::jsonb`, body, payload)
		if err != nil {
			t.Fatal(err)
		}
		docs, err := scanDocuments(rows)
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
		putSearchTestDocument(t, service, docs[0])
		for _, query := range []string{"独角兽", "HiddenPurchaseSecret"} {
			page, err := service.Search(context.Background(), SearchQuery{Query: query})
			if err != nil {
				t.Fatal(err)
			}
			if page.Total != 1 || len(page.Items) != 1 {
				t.Fatalf("post body was not recalled for %q", query)
			}
			raw, err := json.Marshal(page)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), "独角兽") || strings.Contains(strings.ToLower(string(raw)), "hiddenpurchasesecret") {
				t.Errorf("search returned private body for %s: %s", payload, raw)
			}
			if len(page.Items[0].Highlight) != 0 {
				t.Errorf("body-only match exposed snippets: %#v", page.Items[0].Highlight)
			}
			if _, exists := page.Items[0].Payload["content"]; exists {
				t.Error("search payload contains post content")
			}
		}
	}
}

func TestClauseIntegrationPreservesChineseAndPhrasePositions(t *testing.T) {
	service := searchIntegrationService(t)
	for _, tc := range []struct{ title, value, operator string }{
		{"hello世界", "hello世界", "match"}, {"模组", "模组", "match"},
		{"fabric api fabric", "fabric api fabric", "phrase"},
	} {
		putSearchTestDocument(t, service, SearchDocument{Title: tc.title})
		for _, field := range []string{"title", "_all"} {
			page, err := service.Search(context.Background(), SearchQuery{Must: []QueryClause{{Field: field, Value: tc.value, Operator: tc.operator}}})
			if err != nil {
				t.Fatal(err)
			}
			if page.Total != 1 {
				t.Errorf("%s %s %q failed to match %q", field, tc.operator, tc.value, tc.title)
			}
		}
	}
}
