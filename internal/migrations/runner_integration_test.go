package migrations

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRunnerIntegrationAppliesAndValidatesManifest(t *testing.T) {
	pool := integrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	status, err := Status(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if status.CurrentVersion != 0 || status.TargetVersion != TargetVersion() {
		t.Fatalf("unexpected empty status: %#v", status)
	}
	if err := Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if err := Up(ctx, pool); err != nil {
		t.Fatalf("second migration run must be idempotent: %v", err)
	}
	if err := ValidateSchemaVersion(ctx, pool); err != nil {
		t.Fatal(err)
	}

	status, err = Status(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if status.CurrentVersion != TargetVersion() || len(status.Installed) != TargetVersion() {
		t.Fatalf("unexpected migrated status: %#v", status)
	}
}

func TestRunnerIntegrationRejectsInstalledHashMismatch(t *testing.T) {
	pool := integrationPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := Up(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nexusindex_schema_migrations SET definition_hash = 'changed' WHERE version = 1`); err != nil {
		t.Fatal(err)
	}
	if err := ValidateSchemaVersion(ctx, pool); err == nil {
		t.Fatal("expected changed installed migration to be rejected")
	}
}

func integrationPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	url := os.Getenv("NEXUSINDEX_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("NEXUSINDEX_TEST_DATABASE_URL is not configured")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("nexusindex_test_%d", time.Now().UnixNano())
	identifier := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+identifier); err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		admin.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = admin.Exec(cleanupCtx, "DROP SCHEMA "+identifier+" CASCADE")
		admin.Close()
	})
	return pool
}
