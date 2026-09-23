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
	expanded := []string{}
	groups := make([]string, 0, len(terms))
	for _, term := range terms {
		tokens := tokenizeForSearch(term)
		expanded = append(expanded, tokens...)
		alternatives := []string{compileSearchTokens(tokens, false)}
		for _, synonym := range cfg.Synonyms[term] {
			synonymTokens := tokenizeForSearch(synonym)
			if expression := compileSearchTokens(synonymTokens, false); expression != "" {
				alternatives = append(alternatives, expression)
				expanded = append(expanded, synonymTokens...)
			}
		}
		if len(alternatives) > 1 {
			for i, alternative := range alternatives {
				if strings.Contains(alternative, " & ") {
					alternatives[i] = "(" + alternative + ")"
				}
			}
			groups = append(groups, "("+strings.Join(alternatives, " | ")+")")
		} else if alternatives[0] != "" {
			groups = append(groups, alternatives[0])
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
		TSQueryText:   strings.Join(groups, " & "),
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
		keep := unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsMark(r) || r == '.' || r == '-' || r == '_' || r == '/'
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
		if len([]rune(keys[i])) == len([]rune(keys[j])) {
			return keys[i] < keys[j]
		}
		return len([]rune(keys[i])) > len([]rune(keys[j]))
	})

	tokens := []string{}
	runes := []rune(value)
	for i := 0; i < len(runes); {
		remaining := ""
		if len(keys) > 0 {
			remaining = string(runes[i:])
		}
		matched := ""
		for _, key := range keys {
			keyRunes := []rune(key)
			end := i + len(keyRunes)
			leftBoundary := isHan(keyRunes[0]) || i == 0 || !isSearchWordRune(runes[i-1])
			rightBoundary := isHan(keyRunes[len(keyRunes)-1]) || end == len(runes) || (end < len(runes) && !isSearchWordRune(runes[end]))
			if leftBoundary && rightBoundary && strings.HasPrefix(remaining, key) {
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
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			start := i
			i++
			for i < len(runes) {
				next := runes[i]
				if isHan(next) || !isSearchWordRune(next) {
					break
				}
				i++
			}
			token := strings.TrimRight(string(runes[start:i]), ".-_")
			if token != "" {
				tokens = append(tokens, token)
			}
			continue
		}
		i++
	}
	return tokens
}

func isSearchWordRune(r rune) bool {
	return !isHan(r) && (unicode.IsLetter(r) || unicode.IsDigit(r) || unicode.IsMark(r) || r == '_' || r == '-' || r == '.')
}

func compileSearchTokens(tokens []string, prefix bool) string {
	parts := make([]string, 0, len(tokens))
	for _, token := range tokens {
		quoted := "'" + strings.NewReplacer(`\`, `\\`, "'", "''").Replace(token) + "'"
		if prefix {
			quoted += ":*"
		}
		parts = append(parts, quoted)
	}
	return strings.Join(parts, " & ")
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
