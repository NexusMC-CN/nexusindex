package db

import (
	"context"
	"fmt"
	"time"

	"github.com/blockbridge/avmcbbs/apps/nexusindex/config"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Pools struct {
	Main  *pgxpool.Pool
	Index *pgxpool.Pool
}

func Open(ctx context.Context, cfg config.Config) (*Pools, error) {
	mainConfig, err := pgxpool.ParseConfig(cfg.MainDatabaseReadonlyURL)
	if err != nil {
		return nil, fmt.Errorf("parse MAIN_DATABASE_READONLY_URL: %w", err)
	}
	mainConfig.MaxConns = 8
	mainConfig.MinConns = 1
	mainConfig.MaxConnIdleTime = 5 * time.Minute

	indexConfig, err := pgxpool.ParseConfig(cfg.IndexDatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse INDEX_DATABASE_URL: %w", err)
	}
	indexConfig.MaxConns = 8
	indexConfig.MinConns = 1
	indexConfig.MaxConnIdleTime = 5 * time.Minute

	mainPool, err := pgxpool.NewWithConfig(ctx, mainConfig)
	if err != nil {
		return nil, fmt.Errorf("open main readonly database: %w", err)
	}
	indexPool, err := pgxpool.NewWithConfig(ctx, indexConfig)
	if err != nil {
		mainPool.Close()
		return nil, fmt.Errorf("open index database: %w", err)
	}

	pools := &Pools{Main: mainPool, Index: indexPool}
	if err := pools.Ping(ctx); err != nil {
		pools.Close()
		return nil, err
	}
	return pools, nil
}

func (p *Pools) Ping(ctx context.Context) error {
	if err := p.Main.Ping(ctx); err != nil {
		return fmt.Errorf("ping main readonly database: %w", err)
	}
	if err := p.Index.Ping(ctx); err != nil {
		return fmt.Errorf("ping index database: %w", err)
	}
	return nil
}

func (p *Pools) Close() {
	if p.Main != nil {
		p.Main.Close()
	}
	if p.Index != nil {
		p.Index.Close()
	}
}
