package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/blockbridge/avmcbbs/apps/nexusindex/config"
	"github.com/blockbridge/avmcbbs/apps/nexusindex/internal/indexer"
	"github.com/blockbridge/avmcbbs/apps/nexusindex/server"
)

func TestRuntimeReloadRetainsStartupEnvironmentOverrides(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.json")
	if err := os.WriteFile(path, []byte(`{"scoring":{"title_weight":3,"tags_weight":2}}`), 0600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NEXUSINDEX_SCORING_TITLE_WEIGHT", "77")
	t.Setenv("NEXUSINDEX_SCORING_TAGS_WEIGHT", "")
	if err := os.Unsetenv("NEXUSINDEX_SCORING_TAGS_WEIGHT"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NEXUSINDEX_RUNTIME_CONFIG_FILE", path)
	t.Setenv("MAIN_DATABASE_READONLY_URL", "postgres://unused/unused")
	t.Setenv("INDEX_DATABASE_URL", "postgres://unused/unused")
	t.Setenv("NEXUSINDEX_CURSOR_SECRET", "reload-test-secret")
	cfg, err := config.Load()
	if err != nil {
		t.Fatal(err)
	}
	loader := func() (indexer.RuntimeConfig, error) { return runtimeConfigFromAppConfig(cfg) }
	initial, err := loader()
	if err != nil {
		t.Fatal(err)
	}
	if initial.Scoring.TitleWeight != 77 {
		t.Fatalf("environment fixture was not applied: %v", initial.Scoring.TitleWeight)
	}
	service := indexer.NewServiceWithRuntime(nil, nil, initial)
	srv := server.NewHTTPServer(service, server.Options{RuntimeConfigFile: path, RuntimeConfigLoader: loader})
	if err := os.WriteFile(path, []byte(`{"scoring":{"title_weight":4,"tags_weight":9}}`), 0600); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	srv.Handler().ServeHTTP(w, httptest.NewRequest(http.MethodPost, "/api/config/reload", bytes.NewBufferString(`{}`)))
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var actual indexer.RuntimeConfig
	if err := json.Unmarshal(w.Body.Bytes(), &actual); err != nil {
		t.Fatal(err)
	}
	if actual.Scoring.TitleWeight != 77 || actual.Scoring.TagsWeight != 9 {
		t.Fatalf("reload lost env or file settings: %+v", actual.Scoring)
	}
}

func TestParseInvocationDefaultsToServe(t *testing.T) {
	invocation, err := parseInvocation(nil)
	if err != nil {
		t.Fatal(err)
	}
	if invocation.mode != modeServe {
		t.Fatalf("expected serve mode, got %q", invocation.mode)
	}
}

func TestParseInvocationAcceptsMigrationCommands(t *testing.T) {
	for _, action := range []string{"status", "up"} {
		invocation, err := parseInvocation([]string{"migrate", action})
		if err != nil {
			t.Fatalf("parse migrate %s: %v", action, err)
		}
		if invocation.mode != modeMigrate || invocation.action != action {
			t.Fatalf("unexpected invocation: %#v", invocation)
		}
	}
}

func TestParseInvocationRejectsUnknownCommands(t *testing.T) {
	for _, args := range [][]string{{"unknown"}, {"migrate"}, {"migrate", "down"}} {
		if _, err := parseInvocation(args); err == nil {
			t.Fatalf("expected args %v to be rejected", args)
		}
	}
}
