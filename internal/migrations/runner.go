package migrations

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const migrationLockKey int64 = 0x4e45585553494e44

type InstalledMigration struct {
	Version int    `json:"version"`
	Name    string `json:"name"`
	Hash    string `json:"hash"`
}

type SchemaStatus struct {
	CurrentVersion int                  `json:"currentVersion"`
	TargetVersion  int                  `json:"targetVersion"`
	Installed      []InstalledMigration `json:"installed"`
}

func Status(ctx context.Context, pool *pgxpool.Pool) (SchemaStatus, error) {
	status := SchemaStatus{TargetVersion: TargetVersion(), Installed: []InstalledMigration{}}
	rows, err := pool.Query(ctx, "SELECT version, name, definition_hash FROM nexusindex_schema_migrations ORDER BY version")
	if err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42P01" {
			return status, nil
		}
		return status, err
	}
	defer rows.Close()
	for rows.Next() {
		var installed InstalledMigration
		if err := rows.Scan(&installed.Version, &installed.Name, &installed.Hash); err != nil {
			return status, err
		}
		status.Installed = append(status.Installed, installed)
		status.CurrentVersion = installed.Version
	}
	return status, rows.Err()
}

func ValidateSchemaVersion(ctx context.Context, pool *pgxpool.Pool) error {
	status, err := Status(ctx, pool)
	if err != nil {
		return err
	}
	if status.CurrentVersion > status.TargetVersion {
		return ErrDatabaseNewer
	}
	if status.CurrentVersion < status.TargetVersion {
		return fmt.Errorf("nexusindex schema migration required: database=%d binary=%d; run 'nexusindex migrate up' with INDEX_DATABASE_MIGRATION_URL before starting the service", status.CurrentVersion, status.TargetVersion)
	}
	return validateInstalled(status.Installed)
}

func Up(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", migrationLockKey); err != nil {
		return err
	}
	defer func() { _, _ = conn.Exec(context.Background(), "SELECT pg_advisory_unlock($1)", migrationLockKey) }()
	if _, err := conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS nexusindex_schema_migrations (version integer PRIMARY KEY, name text NOT NULL, definition_hash text NOT NULL, installed_at timestamptz NOT NULL DEFAULT now())"); err != nil {
		return err
	}
	status, err := statusOnConn(ctx, conn)
	if err != nil {
		return err
	}
	if err := validateInstalled(status.Installed); err != nil {
		return err
	}
	pending, err := Plan(Manifest, status.CurrentVersion, TargetVersion())
	if err != nil {
		return err
	}
	for _, migration := range pending {
		if err := applyMigration(ctx, conn, migration); err != nil {
			return fmt.Errorf("apply migration %04d_%s: %w", migration.Version, migration.Name, err)
		}
	}
	return nil
}

func statusOnConn(ctx context.Context, conn *pgxpool.Conn) (SchemaStatus, error) {
	status := SchemaStatus{TargetVersion: TargetVersion(), Installed: []InstalledMigration{}}
	rows, err := conn.Query(ctx, "SELECT version, name, definition_hash FROM nexusindex_schema_migrations ORDER BY version")
	if err != nil {
		return status, err
	}
	defer rows.Close()
	for rows.Next() {
		var installed InstalledMigration
		if err := rows.Scan(&installed.Version, &installed.Name, &installed.Hash); err != nil {
			return status, err
		}
		status.Installed = append(status.Installed, installed)
		status.CurrentVersion = installed.Version
	}
	return status, rows.Err()
}

func validateInstalled(installed []InstalledMigration) error {
	if len(installed) > len(Manifest) {
		return ErrDatabaseNewer
	}
	for i, item := range installed {
		migration := Manifest[i]
		if item.Version != migration.Version || item.Name != migration.Name {
			return ErrInvalidManifest
		}
		if err := ValidateInstalledHash(migration, item.Hash); err != nil {
			return err
		}
	}
	return nil
}

func applyMigration(ctx context.Context, conn *pgxpool.Conn, migration Migration) error {
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, migration.SQL); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "INSERT INTO nexusindex_schema_migrations (version, name, definition_hash) VALUES ($1, $2, $3)", migration.Version, migration.Name, migration.Hash); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
