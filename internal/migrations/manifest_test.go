package migrations

import (
	"errors"
	"strings"
	"testing"
)

func TestTextSearchMigrationUsesImmutableIndexExpressions(t *testing.T) {
	migration := Manifest[3]
	if migration.Version != 4 {
		t.Fatalf("expected text search migration at version 4, got %d", migration.Version)
	}
	if strings.Contains(migration.SQL, "array_to_string(") {
		t.Fatal("text search migration must not use array_to_string in an index expression")
	}
	if !strings.Contains(migration.SQL, "ON search_index USING gin (title gin_trgm_ops)") {
		t.Fatal("text search migration must keep an immutable title trigram index")
	}
}

func TestManifestIsOrderedAndHashBound(t *testing.T) {
	if len(Manifest) < 2 {
		t.Fatalf("expected at least baseline and generation migrations, got %d", len(Manifest))
	}
	for i, migration := range Manifest {
		if migration.Version != i+1 {
			t.Fatalf("migration at position %d has version %d", i, migration.Version)
		}
		if migration.Name == "" || migration.SQL == "" {
			t.Fatalf("migration %d is incomplete", migration.Version)
		}
		if migration.Hash != hashSQL(migration.SQL) {
			t.Fatalf("migration %d hash does not bind its SQL", migration.Version)
		}
	}
}

func TestPlanReturnsOnlyPendingMigrations(t *testing.T) {
	manifest := []Migration{
		{Version: 1, Name: "baseline", SQL: "SELECT 1", Hash: hashSQL("SELECT 1")},
		{Version: 2, Name: "generations", SQL: "SELECT 2", Hash: hashSQL("SELECT 2")},
	}

	pending, err := Plan(manifest, 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].Version != 2 {
		t.Fatalf("unexpected pending migrations: %#v", pending)
	}
}

func TestPlanRejectsDatabaseNewerThanBinary(t *testing.T) {
	_, err := Plan([]Migration{{Version: 1}}, 2, 1)
	if !errors.Is(err, ErrDatabaseNewer) {
		t.Fatalf("expected ErrDatabaseNewer, got %v", err)
	}
}

func TestPlanRejectsVersionGaps(t *testing.T) {
	_, err := Plan([]Migration{{Version: 1}, {Version: 3}}, 0, 3)
	if !errors.Is(err, ErrInvalidManifest) {
		t.Fatalf("expected ErrInvalidManifest, got %v", err)
	}
}

func TestValidateInstalledHashRejectsChangedMigration(t *testing.T) {
	err := ValidateInstalledHash(Migration{Version: 2, Hash: "new"}, "old")
	if !errors.Is(err, ErrDefinitionMismatch) {
		t.Fatalf("expected ErrDefinitionMismatch, got %v", err)
	}
}
