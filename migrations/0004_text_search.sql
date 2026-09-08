CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- Array flattening is not immutable on all supported PostgreSQL collations,
-- so it cannot be used in an index expression. Keep the immutable
-- title index here; tag and keyword fuzzy clauses remain correct and use the
-- existing array/full-text indexes where applicable.
CREATE INDEX IF NOT EXISTS search_index_title_trgm_idx
    ON search_index USING gin (title gin_trgm_ops);
