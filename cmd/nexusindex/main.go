package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/blockbridge/avmcbbs/apps/nexusindex/config"
	"github.com/blockbridge/avmcbbs/apps/nexusindex/internal/db"
	"github.com/blockbridge/avmcbbs/apps/nexusindex/internal/indexer"
	"github.com/blockbridge/avmcbbs/apps/nexusindex/internal/migrations"
	edgecache "github.com/blockbridge/avmcbbs/apps/nexusindex/packages/edgecache-go-client"
	"github.com/blockbridge/avmcbbs/apps/nexusindex/server"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	modeServe   = "serve"
	modeMigrate = "migrate"
)

type invocation struct {
	mode   string
	action string
}

func parseInvocation(args []string) (invocation, error) {
	if len(args) == 0 {
		return invocation{mode: modeServe}, nil
	}
	if len(args) == 2 && args[0] == modeMigrate && (args[1] == "status" || args[1] == "up") {
		return invocation{mode: modeMigrate, action: args[1]}, nil
	}
	return invocation{}, fmt.Errorf("usage: nexusindex [migrate status|up]")
}

func main() {
	inv, err := parseInvocation(os.Args[1:])
	if err != nil {
		log.Fatal(err)
	}
	if inv.mode == modeMigrate {
		if err := runMigration(context.Background(), inv.action); err != nil {
			log.Fatal(err)
		}
		return
	}
	cfg, err := config.Load()
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()
	pools, err := db.Open(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer pools.Close()

	if err := migrations.ValidateSchemaVersion(ctx, pools.Index); err != nil {
		log.Fatal(err)
	}
	if err := indexer.ValidateMainOutboxSchema(ctx, pools.Main); err != nil {
		log.Fatal(err)
	}

	runtimeConfig, err := runtimeConfigFromAppConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}
	cursorSecret := cfg.CursorSecret
	if cursorSecret == "" {
		cursorSecret = cfg.APIToken
	}
	indexService := indexer.NewServiceWithRuntimeAndCursor(pools.Main, pools.Index, runtimeConfig, cursorSecret)
	var cacheClient *edgecache.Client
	if cfg.EdgeCacheEnabled {
		cacheClient = edgecache.New(edgecache.Options{
			BaseURL:   cfg.EdgeCacheURL,
			TimeoutMS: cfg.EdgeCacheTimeoutMS,
		})
	}
	cache := server.NewTieredCache(server.TieredCacheOptions{
		L1MaxEntries:        cfg.CacheL1MaxEntries,
		L2:                  server.NewEdgeCacheAdapter(cacheClient),
		DefaultTTLSeconds:   int64(cfg.EdgeCacheTTLSeconds),
		QueryTTLSeconds:     int64(cfg.EdgeCacheQueryTTLSeconds),
		FilterTTLSeconds:    int64(cfg.EdgeCacheFilterTTLSeconds),
		HighlightTTLSeconds: int64(cfg.EdgeCacheHighlightTTLSeconds),
	})
	httpSrv := server.NewHTTPServer(indexService, server.Options{
		AuthToken:         cfg.APIToken,
		RequestTimeout:    time.Duration(cfg.RequestTimeoutMS) * time.Millisecond,
		RuntimeConfigFile: cfg.RuntimeConfigFile,
		Cache: server.CacheOptions{
			Cache:               cache,
			TTLSeconds:          int64(cfg.EdgeCacheTTLSeconds),
			QueryTTLSeconds:     int64(cfg.EdgeCacheQueryTTLSeconds),
			FilterTTLSeconds:    int64(cfg.EdgeCacheFilterTTLSeconds),
			HighlightTTLSeconds: int64(cfg.EdgeCacheHighlightTTLSeconds),
			Namespace:           cfg.CacheNamespace,
		},
	})
	worker := indexer.NewWorker(indexService, indexer.WorkerOptions{
		Enabled:          cfg.WorkerEnabled,
		BatchSize:        cfg.WorkerBatchSize,
		PollInterval:     time.Duration(cfg.WorkerPollIntervalMS) * time.Millisecond,
		LockTTL:          time.Duration(cfg.WorkerLockTTLMS) * time.Millisecond,
		RetryDelay:       time.Duration(cfg.WorkerRetryDelayMS) * time.Millisecond,
		MaxAttempts:      cfg.WorkerMaxAttempts,
		CacheInvalidator: httpSrv.InvalidateSearchCache,
	})
	worker.Start(ctx)
	log.Printf("nexusindex listening on http://127.0.0.1:%d", cfg.Port)

	httpServer := &http.Server{
		Addr:              cfg.Address(),
		Handler:           httpSrv.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       time.Duration(cfg.RequestTimeoutMS+1000) * time.Millisecond,
		WriteTimeout:      time.Duration(cfg.RequestTimeoutMS+1000) * time.Millisecond,
	}
	err = httpServer.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal(err)
	}
}

func runMigration(ctx context.Context, action string) error {
	url, err := config.MigrationDatabaseURL()
	if err != nil {
		return err
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return fmt.Errorf("open migration database: %w", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping migration database: %w", err)
	}
	if action == "up" {
		if err := migrations.Up(ctx, pool); err != nil {
			return err
		}
	}
	status, err := migrations.Status(ctx, pool)
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(status)
}

func runtimeConfigFromAppConfig(cfg config.Config) (indexer.RuntimeConfig, error) {
	runtimeConfig, err := indexer.LoadRuntimeConfigFile(cfg.RuntimeConfigFile)
	if err != nil {
		return runtimeConfig, err
	}
	override := indexer.RuntimeConfig{CandidateLimit: cfg.CandidateLimit}
	override.Scoring.TitleWeight = cfg.ScoringTitleWeight
	override.Scoring.TagsWeight = cfg.ScoringTagsWeight
	override.Scoring.KeywordsWeight = cfg.ScoringKeywordsWeight
	override.Scoring.PayloadWeight = cfg.ScoringPayloadWeight
	override.Scoring.ViewCountWeight = cfg.ScoringViewCountWeight
	override.Scoring.DownloadCountWeight = cfg.ScoringDownloadCountWeight
	override.Scoring.LikeCountWeight = cfg.ScoringLikeCountWeight
	override.Scoring.TimeDecayFactor = cfg.ScoringTimeDecayFactor
	override = override.WithCandidateLimitOverride()
	scoringFields := []string{}
	if cfg.ScoringTitleWeightSet {
		scoringFields = append(scoringFields, "title_weight")
	}
	if cfg.ScoringTagsWeightSet {
		scoringFields = append(scoringFields, "tags_weight")
	}
	if cfg.ScoringKeywordsWeightSet {
		scoringFields = append(scoringFields, "keywords_weight")
	}
	if cfg.ScoringPayloadWeightSet {
		scoringFields = append(scoringFields, "payload_weight")
	}
	if cfg.ScoringViewCountWeightSet {
		scoringFields = append(scoringFields, "view_count_weight")
	}
	if cfg.ScoringDownloadCountWeightSet {
		scoringFields = append(scoringFields, "download_count_weight")
	}
	if cfg.ScoringLikeCountWeightSet {
		scoringFields = append(scoringFields, "like_count_weight")
	}
	if cfg.ScoringTimeDecayFactorSet {
		scoringFields = append(scoringFields, "time_decay_factor")
	}
	override = override.WithScoringOverride(scoringFields...)
	return runtimeConfig.Merge(override).Normalized(), nil
}
