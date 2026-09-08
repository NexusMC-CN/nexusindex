CREATE TABLE IF NOT EXISTS search_index (
    entity_type text NOT NULL,
    entity_id text NOT NULL,
    title text NOT NULL DEFAULT '',
    status text NOT NULL DEFAULT 'active',
    category_id text,
    author_id text,
    tags text[] NOT NULL DEFAULT '{}',
    keywords text[] NOT NULL DEFAULT '{}',
    search_vector tsvector,
    weight integer NOT NULL DEFAULT 0,
    view_count integer NOT NULL DEFAULT 0,
    like_count integer NOT NULL DEFAULT 0,
    download_count integer NOT NULL DEFAULT 0,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz,
    updated_at timestamptz,
    source_version bigint NOT NULL DEFAULT 0,
    indexed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (entity_type, entity_id)
);

CREATE INDEX IF NOT EXISTS search_index_entity_status_weight_idx
    ON search_index (entity_type, status, weight DESC, updated_at DESC);
CREATE INDEX IF NOT EXISTS search_index_vector_gin_idx ON search_index USING gin (search_vector);
CREATE INDEX IF NOT EXISTS search_index_active_vector_gin_idx ON search_index USING gin (search_vector)
    WHERE status NOT IN ('hidden', 'deleted');
CREATE INDEX IF NOT EXISTS search_index_active_entity_category_idx
    ON search_index (entity_type, category_id, updated_at DESC)
    WHERE status NOT IN ('hidden', 'deleted');
CREATE INDEX IF NOT EXISTS search_index_active_updated_idx
    ON search_index (updated_at DESC, indexed_at DESC)
    WHERE status NOT IN ('hidden', 'deleted');
CREATE INDEX IF NOT EXISTS search_index_category_idx ON search_index (category_id);
CREATE INDEX IF NOT EXISTS search_index_author_idx ON search_index (author_id);
CREATE INDEX IF NOT EXISTS search_index_tags_gin_idx ON search_index USING gin (tags);
CREATE INDEX IF NOT EXISTS search_index_keywords_gin_idx ON search_index USING gin (keywords);
CREATE INDEX IF NOT EXISTS search_index_payload_gin_idx ON search_index USING gin (payload);

UPDATE search_index
SET search_vector =
    setweight(to_tsvector('simple', coalesce(title, '')), 'A') ||
    setweight(to_tsvector('simple', coalesce(array_to_string(tags, ' '), '')), 'B') ||
    setweight(to_tsvector('simple', coalesce(array_to_string(keywords, ' '), '')), 'C') ||
    setweight(to_tsvector('simple', coalesce(payload::text, '')), 'D')
WHERE search_vector IS NULL;

CREATE TABLE IF NOT EXISTS tag_index (
    tag text PRIMARY KEY,
    entity_types text[] NOT NULL DEFAULT '{}',
    total_count integer NOT NULL DEFAULT 0,
    resource_count integer NOT NULL DEFAULT 0,
    post_count integer NOT NULL DEFAULT 0,
    server_count integer NOT NULL DEFAULT 0,
    video_count integer NOT NULL DEFAULT 0,
    document_count integer NOT NULL DEFAULT 0,
    user_count integer NOT NULL DEFAULT 0,
    organization_count integer NOT NULL DEFAULT 0,
    tag_count integer NOT NULL DEFAULT 0,
    official_tag_count integer NOT NULL DEFAULT 0,
    category_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
    payload jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS tag_index_total_count_idx ON tag_index (total_count DESC);
CREATE INDEX IF NOT EXISTS tag_index_payload_gin_idx ON tag_index USING gin (payload);

CREATE TABLE IF NOT EXISTS index_sync_log (
    id bigserial PRIMARY KEY,
    job_type text NOT NULL,
    entity_type text,
    entity_id text,
    status text NOT NULL,
    message text,
    error text,
    started_at timestamptz NOT NULL DEFAULT now(),
    finished_at timestamptz,
    details jsonb NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS index_sync_log_status_idx ON index_sync_log (status, started_at DESC);
CREATE INDEX IF NOT EXISTS index_sync_log_entity_idx ON index_sync_log (entity_type, entity_id, started_at DESC);
