package indexer

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrIndexUnavailable = errors.New("nexusindex has no active generation")

type GenerationStatus string

const (
	GenerationBuilding GenerationStatus = "building"
	GenerationActive   GenerationStatus = "active"
	GenerationRetired  GenerationStatus = "retired"
	GenerationFailed   GenerationStatus = "failed"
)

type Generation struct {
	ID                  int64            `json:"id"`
	Status              GenerationStatus `json:"status"`
	CreatedAt           time.Time        `json:"createdAt"`
	ActivatedAt         *time.Time       `json:"activatedAt,omitempty"`
	RetiredAt           *time.Time       `json:"retiredAt,omitempty"`
	BuildJobID          *int64           `json:"buildJobId,omitempty"`
	DocumentCount       int64            `json:"documentCount"`
	SourceHighWatermark int64            `json:"sourceHighWatermark"`
	Failure             string           `json:"failure,omitempty"`
}

type GenerationStore struct{ db *pgxpool.Pool }

func NewGenerationStore(db *pgxpool.Pool) *GenerationStore { return &GenerationStore{db: db} }

func canTransitionGeneration(from, to GenerationStatus) bool {
	switch from {
	case GenerationBuilding:
		return to == GenerationActive || to == GenerationFailed
	case GenerationActive:
		return to == GenerationRetired
	case GenerationRetired:
		return to == GenerationActive
	default:
		return false
	}
}

func requireActiveGeneration(generation *Generation) (int64, error) {
	if generation == nil || generation.Status != GenerationActive || generation.ID < 1 {
		return 0, ErrIndexUnavailable
	}
	return generation.ID, nil
}

func (s *GenerationStore) Active(ctx context.Context) (*Generation, error) {
	return loadActiveGeneration(ctx, s.db)
}

func loadActiveGeneration(ctx context.Context, db Querier) (*Generation, error) {
	generation := &Generation{}
	err := db.QueryRow(ctx, `SELECT id, status, created_at, activated_at, retired_at, build_job_id, document_count, source_high_watermark, coalesce(failure, '') FROM index_generations WHERE status = 'active'`).Scan(&generation.ID, &generation.Status, &generation.CreatedAt, &generation.ActivatedAt, &generation.RetiredAt, &generation.BuildJobID, &generation.DocumentCount, &generation.SourceHighWatermark, &generation.Failure)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrIndexUnavailable
	}
	if err != nil {
		return nil, err
	}
	return generation, nil
}

func (s *GenerationStore) CreateBuilding(ctx context.Context, jobID, watermark int64) (*Generation, error) {
	if err := s.RecoverStaleBuilding(ctx); err != nil {
		return nil, err
	}
	generation := &Generation{}
	err := s.db.QueryRow(ctx, `INSERT INTO index_generations (status, build_job_id, source_high_watermark) VALUES ('building', nullif($1, 0), $2) RETURNING id, status, created_at, build_job_id, document_count, source_high_watermark`, jobID, watermark).Scan(&generation.ID, &generation.Status, &generation.CreatedAt, &generation.BuildJobID, &generation.DocumentCount, &generation.SourceHighWatermark)
	if err != nil && strings.Contains(err.Error(), "index_generations_single_building_idx") {
		return nil, ErrRebuildInProgress
	}
	return generation, err
}

// Running rebuilds hold a session lease on -jobID for their entire lifetime.
// A status row alone cannot distinguish a crashed process from a live job.
func (s *GenerationStore) RecoverStaleBuilding(ctx context.Context) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	rows, err := tx.Query(ctx, `SELECT g.id,coalesce(g.build_job_id,0) FROM index_generations g WHERE g.status='building' FOR UPDATE`)
	if err != nil {
		return err
	}
	type orphan struct{ id, job int64 }
	candidates := []orphan{}
	for rows.Next() {
		var item orphan
		if err := rows.Scan(&item.id, &item.job); err != nil {
			rows.Close()
			return err
		}
		candidates = append(candidates, item)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for _, item := range candidates {
		available := true
		if item.job > 0 {
			if err := tx.QueryRow(ctx, `SELECT pg_try_advisory_xact_lock($1)`, -item.job).Scan(&available); err != nil {
				return err
			}
		}
		if !available {
			continue
		}
		if _, err := tx.Exec(ctx, `UPDATE index_generations SET status='failed',failure='recovered abandoned rebuild' WHERE id=$1`, item.id); err != nil {
			return err
		}
		if item.job > 0 {
			if _, err := tx.Exec(ctx, `UPDATE index_sync_log SET status='failed',finished_at=now(),error='recovered abandoned rebuild' WHERE id=$1 AND status='running'`, item.job); err != nil {
				return err
			}
		}
	}
	return tx.Commit(ctx)
}

func (s *GenerationStore) SetDocumentCount(ctx context.Context, id, count int64) error {
	_, err := s.db.Exec(ctx, `UPDATE index_generations SET document_count = $2 WHERE id = $1 AND status = 'building'`, id, count)
	return err
}

func (s *GenerationStore) Activate(ctx context.Context, id int64) error {
	tx, err := s.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, generationSwitchLockKey); err != nil {
		return err
	}
	if err := s.activateInTransaction(ctx, tx, id); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *GenerationStore) activateInTransaction(ctx context.Context, tx pgx.Tx, id int64) error {
	var status GenerationStatus
	if err := tx.QueryRow(ctx, `SELECT status FROM index_generations WHERE id = $1 FOR UPDATE`, id).Scan(&status); err != nil {
		return err
	}
	if !canTransitionGeneration(status, GenerationActive) {
		return fmt.Errorf("invalid generation transition %s -> %s", status, GenerationActive)
	}
	if _, err := tx.Exec(ctx, `UPDATE index_generations SET status = 'retired', retired_at = now() WHERE status = 'active'`); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE index_generations SET status = 'active', activated_at = now(), retired_at = NULL WHERE id = $1`, id); err != nil {
		return err
	}
	return nil
}

func (s *GenerationStore) Fail(ctx context.Context, id int64, cause error) error {
	message := ""
	if cause != nil {
		message = cause.Error()
	}
	tag, err := s.db.Exec(ctx, `UPDATE index_generations SET status = 'failed', failure = nullif($2, '') WHERE id = $1 AND status = 'building'`, id, message)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return fmt.Errorf("invalid generation transition to failed")
	}
	return nil
}

func (s *GenerationStore) Rollback(ctx context.Context, id int64) error { return s.Activate(ctx, id) }
