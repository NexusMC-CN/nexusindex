package indexer

import (
	"context"
	"fmt"
	"strings"
)

const facetLimit = 20

func (s *Service) loadFacets(ctx context.Context, where []string, args []any) (SearchFacets, error) {
	base := strings.ReplaceAll(strings.Join(where, " AND "), "s.", "")
	query := func(sql string) ([]FacetBucket, error) {
		rows, err := s.indexDB.Query(ctx, fmt.Sprintf(sql, base), args...)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		buckets := []FacetBucket{}
		for rows.Next() {
			var b FacetBucket
			if err := rows.Scan(&b.Value, &b.Count); err != nil {
				return nil, err
			}
			buckets = append(buckets, b)
		}
		return buckets, rows.Err()
	}
	facets := SearchFacets{}
	var err error
	facets.Entity, err = query(`SELECT entity_type, count(*) FROM search_index WHERE %s GROUP BY entity_type ORDER BY count(*) DESC, entity_type ASC LIMIT ` + fmt.Sprint(facetLimit))
	if err != nil {
		return facets, err
	}
	facets.Category, err = query(`SELECT coalesce(category_id, ''), count(*) FROM search_index WHERE %s AND category_id IS NOT NULL GROUP BY category_id ORDER BY count(*) DESC, category_id ASC LIMIT ` + fmt.Sprint(facetLimit))
	if err != nil {
		return facets, err
	}
	facets.Tag, err = query(`SELECT tag, count(*) FROM search_index, unnest(tags) AS tag WHERE %s GROUP BY tag ORDER BY count(*) DESC, tag ASC LIMIT ` + fmt.Sprint(facetLimit))
	return facets, err
}
