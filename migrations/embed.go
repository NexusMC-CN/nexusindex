package migrationfiles

import "embed"

// FS contains the immutable NexusIndex schema migrations.
//
//go:embed *.sql
var FS embed.FS
