package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	MainDatabaseReadonlyURL       string
	IndexDatabaseURL              string
	Port                          int
	APIToken                      string
	CursorSecret                  string
	RequestTimeoutMS              int
	EdgeCacheEnabled              bool
	EdgeCacheURL                  string
	EdgeCacheTTLSeconds           int
	EdgeCacheQueryTTLSeconds      int
	EdgeCacheFilterTTLSeconds     int
	EdgeCacheHighlightTTLSeconds  int
	EdgeCacheTimeoutMS            int
	CacheNamespace                string
	CacheL1MaxEntries             int
	WorkerEnabled                 bool
	WorkerBatchSize               int
	WorkerPollIntervalMS          int
	WorkerLockTTLMS               int
	WorkerRetryDelayMS            int
	WorkerMaxAttempts             int
	RuntimeConfigFile             string
	CandidateLimit                int
	ScoringTitleWeight            float64
	ScoringTagsWeight             float64
	ScoringKeywordsWeight         float64
	ScoringPayloadWeight          float64
	ScoringViewCountWeight        float64
	ScoringDownloadCountWeight    float64
	ScoringLikeCountWeight        float64
	ScoringTimeDecayFactor        float64
	ScoringTitleWeightSet         bool
	ScoringTagsWeightSet          bool
	ScoringKeywordsWeightSet      bool
	ScoringPayloadWeightSet       bool
	ScoringViewCountWeightSet     bool
	ScoringDownloadCountWeightSet bool
	ScoringLikeCountWeightSet     bool
	ScoringTimeDecayFactorSet     bool
}

func Load() (Config, error) {
	_ = loadDotEnv(".env")

	cfg := Config{
		MainDatabaseReadonlyURL:       strings.TrimSpace(os.Getenv("MAIN_DATABASE_READONLY_URL")),
		IndexDatabaseURL:              strings.TrimSpace(os.Getenv("INDEX_DATABASE_URL")),
		Port:                          envInt("NEXUSINDEX_PORT", 4412, 1, 65535),
		APIToken:                      strings.TrimSpace(os.Getenv("NEXUSINDEX_API_TOKEN")),
		CursorSecret:                  strings.TrimSpace(os.Getenv("NEXUSINDEX_CURSOR_SECRET")),
		RequestTimeoutMS:              envInt("NEXUSINDEX_REQUEST_TIMEOUT_MS", 5000, 100, 120000),
		EdgeCacheEnabled:              envBool("NEXUSINDEX_EDGECACHE_ENABLED", false),
		EdgeCacheURL:                  strings.TrimSpace(envOr("NEXUSINDEX_EDGECACHE_URL", "http://127.0.0.1:4410")),
		EdgeCacheTTLSeconds:           envInt("NEXUSINDEX_EDGECACHE_TTL_SECONDS", 60, 1, 3600),
		EdgeCacheQueryTTLSeconds:      envInt("NEXUSINDEX_CACHE_QUERY_TTL_SECONDS", 60, 1, 3600),
		EdgeCacheFilterTTLSeconds:     envInt("NEXUSINDEX_CACHE_FILTER_TTL_SECONDS", 120, 1, 3600),
		EdgeCacheHighlightTTLSeconds:  envInt("NEXUSINDEX_CACHE_HIGHLIGHT_TTL_SECONDS", 300, 1, 3600),
		EdgeCacheTimeoutMS:            envInt("NEXUSINDEX_EDGECACHE_TIMEOUT_MS", 300, 1, 10000),
		CacheNamespace:                strings.TrimSpace(envOr("NEXUSINDEX_CACHE_NAMESPACE", "nexusindex:v2")),
		CacheL1MaxEntries:             envInt("NEXUSINDEX_CACHE_L1_MAX_ENTRIES", 2048, 128, 100000),
		WorkerEnabled:                 envBool("NEXUSINDEX_WORKER_ENABLED", true),
		WorkerBatchSize:               envInt("NEXUSINDEX_WORKER_BATCH_SIZE", 200, 1, 500),
		WorkerPollIntervalMS:          envInt("NEXUSINDEX_WORKER_POLL_INTERVAL_MS", 2000, 100, 60000),
		WorkerLockTTLMS:               envInt("NEXUSINDEX_WORKER_LOCK_TTL_MS", 120000, 1000, 3600000),
		WorkerRetryDelayMS:            envInt("NEXUSINDEX_WORKER_RETRY_DELAY_MS", 30000, 100, 3600000),
		WorkerMaxAttempts:             envInt("NEXUSINDEX_WORKER_MAX_ATTEMPTS", 10, 1, 100),
		RuntimeConfigFile:             strings.TrimSpace(os.Getenv("NEXUSINDEX_RUNTIME_CONFIG_FILE")),
		CandidateLimit:                envInt("NEXUSINDEX_CANDIDATE_LIMIT", 1000, 100, 5000),
		ScoringTitleWeight:            envFloat("NEXUSINDEX_SCORING_TITLE_WEIGHT", 0),
		ScoringTagsWeight:             envFloat("NEXUSINDEX_SCORING_TAGS_WEIGHT", 0),
		ScoringKeywordsWeight:         envFloat("NEXUSINDEX_SCORING_KEYWORDS_WEIGHT", 0),
		ScoringPayloadWeight:          envFloat("NEXUSINDEX_SCORING_PAYLOAD_WEIGHT", 0),
		ScoringViewCountWeight:        envFloat("NEXUSINDEX_SCORING_VIEW_COUNT_WEIGHT", 0),
		ScoringDownloadCountWeight:    envFloat("NEXUSINDEX_SCORING_DOWNLOAD_COUNT_WEIGHT", 0),
		ScoringLikeCountWeight:        envFloat("NEXUSINDEX_SCORING_LIKE_COUNT_WEIGHT", 0),
		ScoringTimeDecayFactor:        envFloat("NEXUSINDEX_SCORING_TIME_DECAY_FACTOR", 0),
		ScoringTitleWeightSet:         envSet("NEXUSINDEX_SCORING_TITLE_WEIGHT"),
		ScoringTagsWeightSet:          envSet("NEXUSINDEX_SCORING_TAGS_WEIGHT"),
		ScoringKeywordsWeightSet:      envSet("NEXUSINDEX_SCORING_KEYWORDS_WEIGHT"),
		ScoringPayloadWeightSet:       envSet("NEXUSINDEX_SCORING_PAYLOAD_WEIGHT"),
		ScoringViewCountWeightSet:     envSet("NEXUSINDEX_SCORING_VIEW_COUNT_WEIGHT"),
		ScoringDownloadCountWeightSet: envSet("NEXUSINDEX_SCORING_DOWNLOAD_COUNT_WEIGHT"),
		ScoringLikeCountWeightSet:     envSet("NEXUSINDEX_SCORING_LIKE_COUNT_WEIGHT"),
		ScoringTimeDecayFactorSet:     envSet("NEXUSINDEX_SCORING_TIME_DECAY_FACTOR"),
	}

	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func MigrationDatabaseURL() (string, error) {
	_ = loadDotEnv(".env")
	value := strings.TrimSpace(os.Getenv("INDEX_DATABASE_MIGRATION_URL"))
	if value == "" {
		return "", errors.New("INDEX_DATABASE_MIGRATION_URL is required for migration commands")
	}
	return value, nil
}

func (c Config) Validate() error {
	if c.MainDatabaseReadonlyURL == "" {
		return errors.New("MAIN_DATABASE_READONLY_URL is required")
	}
	if c.IndexDatabaseURL == "" {
		return errors.New("INDEX_DATABASE_URL is required")
	}
	if c.CursorSecret == "" && c.APIToken == "" {
		return errors.New("NEXUSINDEX_CURSOR_SECRET or NEXUSINDEX_API_TOKEN is required")
	}
	if c.Port < 1 || c.Port > 65535 {
		return errors.New("NEXUSINDEX_PORT must be a valid TCP port")
	}
	if c.EdgeCacheEnabled && c.EdgeCacheURL == "" {
		return errors.New("NEXUSINDEX_EDGECACHE_URL is required when NEXUSINDEX_EDGECACHE_ENABLED=true")
	}
	if c.EdgeCacheEnabled && c.CacheNamespace == "" {
		return errors.New("NEXUSINDEX_CACHE_NAMESPACE is required when NEXUSINDEX_EDGECACHE_ENABLED=true")
	}
	return nil
}

func (c Config) Address() string {
	return fmt.Sprintf(":%d", c.Port)
}

func envInt(key string, fallback, min, max int) int {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(value)
	if err != nil {
		return fallback
	}
	if parsed < min {
		return min
	}
	if parsed > max {
		return max
	}
	return parsed
}

func envBool(key string, fallback bool) bool {
	value := strings.TrimSpace(strings.ToLower(os.Getenv(key)))
	if value == "" {
		return fallback
	}
	if value == "1" || value == "true" || value == "yes" || value == "on" {
		return true
	}
	if value == "0" || value == "false" || value == "no" || value == "off" {
		return false
	}
	return fallback
}

func envFloat(key string, fallback float64) float64 {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

func envSet(key string) bool {
	_, ok := os.LookupEnv(key)
	return ok
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func loadDotEnv(path string) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if strings.HasPrefix(line, "export ") {
			line = strings.TrimSpace(strings.TrimPrefix(line, "export "))
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		if key == "" {
			continue
		}
		if _, exists := os.LookupEnv(key); exists {
			continue
		}

		value := strings.TrimSpace(parts[1])
		value = trimEnvQuotes(value)
		if err := os.Setenv(key, value); err != nil {
			return fmt.Errorf("set env %s: %w", key, err)
		}
	}
	return scanner.Err()
}

func trimEnvQuotes(value string) string {
	if len(value) < 2 {
		return value
	}
	if (strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"")) ||
		(strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) {
		return value[1 : len(value)-1]
	}
	return value
}
