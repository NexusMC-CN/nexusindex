package config

import "testing"

func TestMigrationDatabaseURLRequiresDedicatedOwnerURL(t *testing.T) {
	t.Setenv("INDEX_DATABASE_MIGRATION_URL", "")
	if _, err := MigrationDatabaseURL(); err == nil {
		t.Fatal("expected missing migration owner URL to fail")
	}

	t.Setenv("INDEX_DATABASE_MIGRATION_URL", "postgresql://migration-owner/db")
	url, err := MigrationDatabaseURL()
	if err != nil {
		t.Fatal(err)
	}
	if url != "postgresql://migration-owner/db" {
		t.Fatalf("unexpected URL %q", url)
	}
}
