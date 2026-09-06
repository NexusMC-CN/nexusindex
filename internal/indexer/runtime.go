package indexer

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"strings"
	"sync/atomic"
)

type ScoringConfig struct {
	TitleWeight           float64 `json:"title_weight"`
	TagsWeight            float64 `json:"tags_weight"`
	KeywordsWeight        float64 `json:"keywords_weight"`
	PayloadWeight         float64 `json:"payload_weight"`
	ViewCountWeight       float64 `json:"view_count_weight"`
	DownloadCountWeight   float64 `json:"download_count_weight"`
	LikeCountWeight       float64 `json:"like_count_weight"`
	TimeDecayFactor       float64 `json:"time_decay_factor"`
	TextScoreWeight       float64 `json:"text_score_weight"`
	PopularityScoreWeight float64 `json:"popularity_score_weight"`
	FreshnessScoreWeight  float64 `json:"freshness_score_weight"`
	set                   map[string]bool
}

type QueryConfig struct {
	Synonyms map[string][]string `json:"synonyms"`
}

type RuntimeConfig struct {
	Scoring        ScoringConfig `json:"scoring"`
	Query          QueryConfig   `json:"query"`
	CandidateLimit int           `json:"candidate_limit"`
	set            map[string]bool
}

func (c *RuntimeConfig) UnmarshalJSON(data []byte) error {
	type rawRuntimeConfig struct {
		Scoring        json.RawMessage `json:"scoring"`
		Query          QueryConfig     `json:"query"`
		CandidateLimit *int            `json:"candidate_limit"`
	}
	var raw rawRuntimeConfig
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	c.set = map[string]bool{}
	if raw.CandidateLimit != nil {
		c.CandidateLimit = *raw.CandidateLimit
		c.set["candidate_limit"] = true
	}
	c.Query = raw.Query
	if len(raw.Scoring) > 0 && string(raw.Scoring) != "null" {
		if err := json.Unmarshal(raw.Scoring, &c.Scoring); err != nil {
			return err
		}
		c.set["scoring"] = true
	}
	return nil
}

func (c *ScoringConfig) UnmarshalJSON(data []byte) error {
	var raw map[string]*float64
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	c.set = map[string]bool{}
	assign := func(key string, target *float64) {
		if value, ok := raw[key]; ok {
			c.set[key] = true
			if value != nil {
				*target = *value
			}
		}
	}
	assign("title_weight", &c.TitleWeight)
	assign("tags_weight", &c.TagsWeight)
	assign("keywords_weight", &c.KeywordsWeight)
	assign("payload_weight", &c.PayloadWeight)
	assign("view_count_weight", &c.ViewCountWeight)
	assign("download_count_weight", &c.DownloadCountWeight)
	assign("like_count_weight", &c.LikeCountWeight)
	assign("time_decay_factor", &c.TimeDecayFactor)
	assign("text_score_weight", &c.TextScoreWeight)
	assign("popularity_score_weight", &c.PopularityScoreWeight)
	assign("freshness_score_weight", &c.FreshnessScoreWeight)
	return nil
}

func DefaultRuntimeConfig() RuntimeConfig {
	return RuntimeConfig{
		Scoring: ScoringConfig{
			TitleWeight:           8.0,
			TagsWeight:            5.0,
			KeywordsWeight:        3.0,
			PayloadWeight:         1.0,
			ViewCountWeight:       0.30,
			DownloadCountWeight:   0.50,
			LikeCountWeight:       0.70,
			TimeDecayFactor:       30.0,
			TextScoreWeight:       0.72,
			PopularityScoreWeight: 0.20,
			FreshnessScoreWeight:  0.08,
		},
		Query:          QueryConfig{Synonyms: defaultSynonyms()},
		CandidateLimit: 1000,
	}
}

func LoadRuntimeConfigFile(path string) (RuntimeConfig, error) {
	cfg := DefaultRuntimeConfig()
	path = strings.TrimSpace(path)
	if path == "" {
		return cfg, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return cfg, err
	}
	var override RuntimeConfig
	if err := json.Unmarshal(raw, &override); err != nil {
		return cfg, err
	}
	return cfg.Merge(override).Normalized(), nil
}

func (c RuntimeConfig) Merge(override RuntimeConfig) RuntimeConfig {
	c = c.DeepCopy()
	override = override.DeepCopy()
	if override.scoringFieldSet("title_weight") {
		c.Scoring.TitleWeight = override.Scoring.TitleWeight
	}
	if override.scoringFieldSet("tags_weight") {
		c.Scoring.TagsWeight = override.Scoring.TagsWeight
	}
	if override.scoringFieldSet("keywords_weight") {
		c.Scoring.KeywordsWeight = override.Scoring.KeywordsWeight
	}
	if override.scoringFieldSet("payload_weight") {
		c.Scoring.PayloadWeight = override.Scoring.PayloadWeight
	}
	if override.scoringFieldSet("view_count_weight") {
		c.Scoring.ViewCountWeight = override.Scoring.ViewCountWeight
	}
	if override.scoringFieldSet("download_count_weight") {
		c.Scoring.DownloadCountWeight = override.Scoring.DownloadCountWeight
	}
	if override.scoringFieldSet("like_count_weight") {
		c.Scoring.LikeCountWeight = override.Scoring.LikeCountWeight
	}
	if override.scoringFieldSet("time_decay_factor") {
		c.Scoring.TimeDecayFactor = override.Scoring.TimeDecayFactor
	}
	if override.scoringFieldSet("text_score_weight") {
		c.Scoring.TextScoreWeight = override.Scoring.TextScoreWeight
	}
	if override.scoringFieldSet("popularity_score_weight") {
		c.Scoring.PopularityScoreWeight = override.Scoring.PopularityScoreWeight
	}
	if override.scoringFieldSet("freshness_score_weight") {
		c.Scoring.FreshnessScoreWeight = override.Scoring.FreshnessScoreWeight
	}
	if len(override.Query.Synonyms) > 0 {
		if c.Query.Synonyms == nil {
			c.Query.Synonyms = map[string][]string{}
		}
		for key, values := range override.Query.Synonyms {
			c.Query.Synonyms[normalizeQueryText(key)] = normalizeStrings(values)
		}
	}
	if override.fieldSet("candidate_limit") {
		c.CandidateLimit = override.CandidateLimit
	}
	return c
}

func (c RuntimeConfig) Normalized() RuntimeConfig {
	c = c.DeepCopy()
	if c.CandidateLimit < 100 {
		c.CandidateLimit = 100
	}
	if c.CandidateLimit > 5000 {
		c.CandidateLimit = 5000
	}
	c.Scoring.TitleWeight = clampFinite(c.Scoring.TitleWeight, 0, 100, DefaultRuntimeConfig().Scoring.TitleWeight)
	c.Scoring.TagsWeight = clampFinite(c.Scoring.TagsWeight, 0, 100, DefaultRuntimeConfig().Scoring.TagsWeight)
	c.Scoring.KeywordsWeight = clampFinite(c.Scoring.KeywordsWeight, 0, 100, DefaultRuntimeConfig().Scoring.KeywordsWeight)
	c.Scoring.PayloadWeight = clampFinite(c.Scoring.PayloadWeight, 0, 100, DefaultRuntimeConfig().Scoring.PayloadWeight)
	c.Scoring.ViewCountWeight = clampFinite(c.Scoring.ViewCountWeight, 0, 10, DefaultRuntimeConfig().Scoring.ViewCountWeight)
	c.Scoring.DownloadCountWeight = clampFinite(c.Scoring.DownloadCountWeight, 0, 10, DefaultRuntimeConfig().Scoring.DownloadCountWeight)
	c.Scoring.LikeCountWeight = clampFinite(c.Scoring.LikeCountWeight, 0, 10, DefaultRuntimeConfig().Scoring.LikeCountWeight)
	c.Scoring.TimeDecayFactor = clampFinite(c.Scoring.TimeDecayFactor, 1, 3650, DefaultRuntimeConfig().Scoring.TimeDecayFactor)
	c.Scoring.TextScoreWeight = clampFinite(c.Scoring.TextScoreWeight, 0, 10, DefaultRuntimeConfig().Scoring.TextScoreWeight)
	c.Scoring.PopularityScoreWeight = clampFinite(c.Scoring.PopularityScoreWeight, 0, 10, DefaultRuntimeConfig().Scoring.PopularityScoreWeight)
	c.Scoring.FreshnessScoreWeight = clampFinite(c.Scoring.FreshnessScoreWeight, 0, 10, DefaultRuntimeConfig().Scoring.FreshnessScoreWeight)
	if c.Query.Synonyms == nil {
		c.Query.Synonyms = defaultSynonyms()
	}
	return c
}

func (c RuntimeConfig) DeepCopy() RuntimeConfig {
	out := c
	out.Query.Synonyms = copySynonyms(c.Query.Synonyms)
	out.set = copyBoolMap(c.set)
	out.Scoring.set = copyBoolMap(c.Scoring.set)
	return out
}

func (c RuntimeConfig) fieldSet(key string) bool {
	return c.set != nil && c.set[key]
}

func (c RuntimeConfig) scoringFieldSet(key string) bool {
	return c.Scoring.set != nil && c.Scoring.set[key]
}

func (c RuntimeConfig) WithCandidateLimitOverride() RuntimeConfig {
	c.set = copyBoolMap(c.set)
	if c.set == nil {
		c.set = map[string]bool{}
	}
	c.set["candidate_limit"] = true
	return c
}

func (c RuntimeConfig) WithScoringOverride(fields ...string) RuntimeConfig {
	c.Scoring.set = copyBoolMap(c.Scoring.set)
	if c.Scoring.set == nil {
		c.Scoring.set = map[string]bool{}
	}
	for _, field := range fields {
		c.Scoring.set[field] = true
	}
	return c
}

func copySynonyms(input map[string][]string) map[string][]string {
	if input == nil {
		return nil
	}
	out := make(map[string][]string, len(input))
	for key, values := range input {
		copied := append([]string(nil), values...)
		out[key] = copied
	}
	return out
}

func copyBoolMap(input map[string]bool) map[string]bool {
	if input == nil {
		return nil
	}
	out := make(map[string]bool, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
}

func clampFinite(value, min, max, fallback float64) float64 {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return fallback
	}
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}

func defaultSynonyms() map[string][]string {
	return map[string][]string{
		"mc":        {"minecraft", "我的世界"},
		"我的世界":      {"minecraft", "mc"},
		"minecraft": {"mc", "我的世界"},
		"fabric":    {"fabric loader", "fabric api"},
		"forge":     {"minecraft forge"},
		"neoforge":  {"neo forge"},
		"插件":        {"plugin", "bukkit", "spigot", "paper"},
		"plugin":    {"插件", "bukkit", "spigot", "paper"},
		"模组":        {"mod", "mods"},
		"mod":       {"模组", "mods"},
		"光影":        {"shader", "shaders"},
		"shader":    {"光影", "shaders"},
		"材质包":       {"resource pack", "texture pack"},
		"整合包":       {"modpack", "mod pack"},
		"服务端":       {"server", "paper", "spigot"},
		"基岩版":       {"bedrock"},
		"java版":     {"java edition"},
	}
}

type runtimeConfigStore struct {
	value atomic.Value
}

func newRuntimeConfigStore(cfg RuntimeConfig) *runtimeConfigStore {
	store := &runtimeConfigStore{}
	store.value.Store(cfg.Normalized())
	return store
}

func (s *runtimeConfigStore) Get() RuntimeConfig {
	if s == nil {
		return DefaultRuntimeConfig()
	}
	cfg, ok := s.value.Load().(RuntimeConfig)
	if !ok {
		return DefaultRuntimeConfig()
	}
	return cfg.DeepCopy()
}

func (s *runtimeConfigStore) Update(_ context.Context, cfg RuntimeConfig) RuntimeConfig {
	cfg = cfg.Normalized().DeepCopy()
	s.value.Store(cfg)
	return cfg.DeepCopy()
}
