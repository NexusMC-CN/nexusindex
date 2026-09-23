package indexer

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// timedQuerier accumulates time spent inside database calls, including deferred
// row consumption. Time between calls (query preparation and result processing)
// is excluded. Search uses it serially within one transaction.
type timedQuerier struct {
	db      Querier
	elapsed time.Duration
}

func (q *timedQuerier) record(started time.Time) { q.elapsed += time.Since(started) }

func (q *timedQuerier) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	defer q.record(time.Now())
	rows, err := q.db.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	return &timedRows{Rows: rows, timing: q}, nil
}

func (q *timedQuerier) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	defer q.record(time.Now())
	return &timedRow{Row: q.db.QueryRow(ctx, sql, args...), timing: q}
}

type timedRow struct {
	pgx.Row
	timing *timedQuerier
}

func (r *timedRow) Scan(dest ...any) error {
	defer r.timing.record(time.Now())
	return r.Row.Scan(dest...)
}

type timedRows struct {
	pgx.Rows
	timing *timedQuerier
}

func (r *timedRows) Next() bool {
	defer r.timing.record(time.Now())
	return r.Rows.Next()
}

func (r *timedRows) Scan(dest ...any) error {
	defer r.timing.record(time.Now())
	return r.Rows.Scan(dest...)
}

func (r *timedRows) Err() error {
	defer r.timing.record(time.Now())
	return r.Rows.Err()
}

func (r *timedRows) Close() {
	defer r.timing.record(time.Now())
	r.Rows.Close()
}
