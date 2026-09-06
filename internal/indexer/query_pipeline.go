package indexer

import (
	"regexp"
	"sort"
	"strings"
	"unicode"
)

type StructuredQuery struct {
	Raw           string
	Normalized    string
	Terms         []string
	ExpandedTerms []string
	TSQueryText   string
	Versions      []string
	Loaders       []string
	Boosts        map[string]float64
}

type QueryPipeline struct{}

var (
	versionPattern = regexp.MustCompile(`\b1\.\d+(?:\.\d+)?\b`)
	loaderTerms    = map[string]struct{}{
		"fabric": {}, "forge": {}, "neoforge": {}, "quilt": {}, "paper": {},
		"spigot": {}, "bukkit": {}, "velocity": {}, "bungeecord": {},
		"bedrock": {}, "java": {},
	}
)

func NewQueryPipeline() *QueryPipeline {
	return &QueryPipeline{}
}

func (p *QueryPipeline) Build(raw string, cfg QueryConfig) StructuredQuery {
	normalized := normalizeQueryText(raw)
	terms := normalizeStringsPreserveOrder(tokenizeWithDictionary(normalized, cfg.Synonyms))
	expanded := append([]string{}, terms...)
	for _, term := range terms {
		for _, synonym := range cfg.Synonyms[term] {
			expanded = append(expanded, tokenizeForSearch(synonym)...)
		}
	}
	expanded = normalizeStringsPreserveOrder(expanded)

	versions := normalizeStrings(versionPattern.FindAllString(normalized, -1))
	loaders := []string{}
	for _, term := range expanded {
		if _, ok := loaderTerms[term]; ok {
			loaders = append(loaders, term)
		}
	}
	loaders = normalizeStrings(loaders)

	boosts := map[string]float64{}
	for _, version := range versions {
		boosts["version:"+version] = 0.80
	}
	for _, loader := range loaders {
		boosts["loader:"+loader] = 0.65
	}
	if strings.Contains(normalized, "/") {
		boosts["slash_query"] = 0.20
	}

	return StructuredQuery{
		Raw:           raw,
		Normalized:    normalized,
		Terms:         terms,
		ExpandedTerms: expanded,
		TSQueryText:   strings.Join(expanded, " "),
		Versions:      versions,
		Loaders:       loaders,
		Boosts:        boosts,
	}
}

func normalizeQueryText(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}
	var b strings.Builder
	lastSpace := false
	for _, r := range value {
		keep := (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || isHan(r) || r == '.' || r == '-' || r == '_' || r == '/'
		if keep {
			b.WriteRune(r)
			lastSpace = false
			continue
		}
		if !lastSpace {
			b.WriteByte(' ')
			lastSpace = true
		}
	}
	parts := strings.Fields(b.String())
	return strings.Join(parts, " ")
}

func tokenizeWithDictionary(value string, dictionary map[string][]string) []string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return nil
	}
	keys := make([]string, 0, len(dictionary))
	for key := range dictionary {
		key = strings.TrimSpace(key)
		if key != "" {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		return len([]rune(keys[i])) > len([]rune(keys[j]))
	})

	tokens := []string{}
	runes := []rune(value)
	for i := 0; i < len(runes); {
		remaining := string(runes[i:])
		matched := ""
		for _, key := range keys {
			if strings.HasPrefix(remaining, key) {
				matched = key
				break
			}
		}
		if matched != "" {
			tokens = append(tokens, matched)
			i += len([]rune(matched))
			continue
		}
		r := runes[i]
		if isHan(r) {
			tokens = append(tokens, string(r))
			i++
			continue
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' || r == '.' {
			start := i
			i++
			for i < len(runes) {
				next := runes[i]
				if !(unicode.IsLetter(next) || unicode.IsDigit(next) || next == '_' || next == '-' || next == '.') {
					break
				}
				i++
			}
			token := strings.Trim(string(runes[start:i]), ".")
			if token != "" {
				tokens = append(tokens, token)
			}
			continue
		}
		i++
	}
	return tokens
}

func normalizeStringsPreserveOrder(values []string) []string {
	out := []string{}
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}
