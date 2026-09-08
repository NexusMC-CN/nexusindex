package migrations

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	migrationfiles "github.com/blockbridge/avmcbbs/apps/nexusindex/migrations"
)

var (
	ErrDatabaseNewer      = errors.New("nexusindex schema is newer than this binary")
	ErrInvalidManifest    = errors.New("invalid nexusindex migration manifest")
	ErrDefinitionMismatch = errors.New("nexusindex migration definition mismatch")
)

type Migration struct {
	Version int
	Name    string
	SQL     string
	Hash    string
}

var Manifest = mustLoadManifest()

func TargetVersion() int {
	if len(Manifest) == 0 {
		return 0
	}
	return Manifest[len(Manifest)-1].Version
}

func hashSQL(sql string) string {
	sum := sha256.Sum256([]byte(sql))
	return hex.EncodeToString(sum[:])
}

func mustLoadManifest() []Migration {
	entries, err := migrationfiles.FS.ReadDir(".")
	if err != nil {
		panic(err)
	}
	migrations := make([]Migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		filename := entry.Name()
		if len(filename) < len("0001_x.sql") || filename[4] != '_' {
			panic(fmt.Errorf("parse migration filename %q: expected NNNN_name.sql", filename))
		}
		version, err := strconv.Atoi(filename[:4])
		if err != nil || version < 1 {
			panic(fmt.Errorf("parse migration filename %q: invalid version", filename))
		}
		name := strings.TrimSuffix(filename[5:], ".sql")
		if name == "" {
			panic(fmt.Errorf("parse migration filename %q: empty name", filename))
		}
		sqlBytes, err := migrationfiles.FS.ReadFile(entry.Name())
		if err != nil {
			panic(err)
		}
		sql := string(sqlBytes)
		migrations = append(migrations, Migration{Version: version, Name: name, SQL: sql, Hash: hashSQL(sql)})
	}
	sort.Slice(migrations, func(i, j int) bool { return migrations[i].Version < migrations[j].Version })
	if _, err := Plan(migrations, 0, len(migrations)); err != nil {
		panic(err)
	}
	return migrations
}

func Plan(manifest []Migration, current, target int) ([]Migration, error) {
	if current > target {
		return nil, ErrDatabaseNewer
	}
	if target < 0 || len(manifest) < target {
		return nil, ErrInvalidManifest
	}
	for i, migration := range manifest {
		if migration.Version != i+1 {
			return nil, ErrInvalidManifest
		}
	}
	pending := make([]Migration, 0, target-current)
	for _, migration := range manifest {
		if migration.Version > current && migration.Version <= target {
			pending = append(pending, migration)
		}
	}
	return pending, nil
}

func ValidateInstalledHash(migration Migration, installedHash string) error {
	if migration.Hash != installedHash {
		return fmt.Errorf("%w: version %d", ErrDefinitionMismatch, migration.Version)
	}
	return nil
}
