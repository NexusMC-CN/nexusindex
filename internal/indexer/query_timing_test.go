package indexer

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// These substitutes model delayed driver errors; assertions exercise the real
// accumulator, including its error paths, without depending on a test database.
type timingErrorDB struct {
	Querier
	err error
}

func (db timingErrorDB) Query(context.Context, string, ...any) (pgx.Rows, error) {
	time.Sleep(25 * time.Millisecond)
	return nil, db.err
}

func (db timingErrorDB) QueryRow(context.Context, string, ...any) pgx.Row {
	time.Sleep(25 * time.Millisecond)
	return timingErrorRows{err: db.err}
}

type timingErrorRows struct {
	pgx.Rows
	err error
}

func (r timingErrorRows) Next() bool        { time.Sleep(25 * time.Millisecond); return false }
func (r timingErrorRows) Scan(...any) error { time.Sleep(25 * time.Millisecond); return r.err }
func (r timingErrorRows) Err() error        { time.Sleep(25 * time.Millisecond); return r.err }
func (r timingErrorRows) Close()            { time.Sleep(25 * time.Millisecond) }

func TestQueryTimingRecordsFailedDriverCalls(t *testing.T) {
	cause := errors.New("delayed driver failure")
	for _, stage := range []string{"query", "query row", "scan", "next", "rows scan", "rows error", "close"} {
		t.Run(stage, func(t *testing.T) {
			q := &timedQuerier{db: timingErrorDB{err: cause}}
			r := &timedRows{Rows: timingErrorRows{err: cause}, timing: q}
			var err error
			switch stage {
			case "query":
				_, err = q.Query(context.Background(), "fixture")
			case "query row":
				_ = q.QueryRow(context.Background(), "fixture")
			case "scan":
				err = (&timedRow{Row: timingErrorRows{err: cause}, timing: q}).Scan()
			case "next":
				if r.Next() {
					t.Fatal("error rows reported data")
				}
			case "rows scan":
				err = r.Scan()
			case "rows error":
				err = r.Err()
			case "close":
				r.Close()
			}
			if stage == "query" || stage == "scan" || stage == "rows scan" || stage == "rows error" {
				if !errors.Is(err, cause) {
					t.Fatalf("driver error was replaced: %v", err)
				}
			}
			if q.elapsed < 25*time.Millisecond {
				t.Fatalf("%s error wait missing from database time: %s", stage, q.elapsed)
			}
		})
	}
}

func TestQueryTimingExcludesWorkBetweenDriverCalls(t *testing.T) {
	q := &timedQuerier{db: timingErrorDB{err: errors.New("failure")}}
	row := q.QueryRow(context.Background(), "fixture")
	before := q.elapsed
	time.Sleep(70 * time.Millisecond)
	if q.elapsed != before {
		t.Fatal("time between driver calls changed the accumulator")
	}
	_ = row.Scan()
	if scanned := q.elapsed - before; scanned < 25*time.Millisecond || scanned >= 70*time.Millisecond {
		t.Fatalf("Scan must include its driver wait but not preceding Go work: %s", scanned)
	}
}
