CREATE TABLE index_generations (
    id bigserial PRIMARY KEY,
    status text NOT NULL CHECK (status IN ('building', 'active', 'retired', 'failed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    activated_at timestamptz,
    retired_at timestamptz,
    build_job_id bigint,
    document_count bigint NOT NULL DEFAULT 0,
    entity_counts jsonb NOT NULL DEFAULT '{}'::jsonb,
    source_high_watermark bigint NOT NULL DEFAULT 0,
    failure text
);
CREATE UNIQUE INDEX index_generations_single_active_idx ON index_generations ((status)) WHERE status = 'active';

INSERT INTO index_generations (status, activated_at, document_count)
SELECT 'active', now(), count(*) FROM search_index;

ALTER TABLE search_index ADD COLUMN generation_id bigint;
UPDATE search_index SET generation_id = (SELECT id FROM index_generations WHERE status = 'active');
ALTER TABLE search_index ALTER COLUMN generation_id SET NOT NULL;
ALTER TABLE search_index ADD CONSTRAINT search_index_generation_fk
    FOREIGN KEY (generation_id) REFERENCES index_generations(id) ON DELETE CASCADE;
ALTER TABLE search_index DROP CONSTRAINT search_index_pkey;
ALTER TABLE search_index ADD PRIMARY KEY (generation_id, entity_type, entity_id);
CREATE INDEX search_index_generation_entity_status_idx
    ON search_index (generation_id, entity_type, status, updated_at DESC);

ALTER TABLE tag_index ADD COLUMN generation_id bigint;
UPDATE tag_index SET generation_id = (SELECT id FROM index_generations WHERE status = 'active');
ALTER TABLE tag_index ALTER COLUMN generation_id SET NOT NULL;
ALTER TABLE tag_index ADD CONSTRAINT tag_index_generation_fk
    FOREIGN KEY (generation_id) REFERENCES index_generations(id) ON DELETE CASCADE;
ALTER TABLE tag_index DROP CONSTRAINT tag_index_pkey;
ALTER TABLE tag_index ADD PRIMARY KEY (generation_id, tag);
CREATE INDEX tag_index_generation_count_idx ON tag_index (generation_id, total_count DESC, tag);

ALTER TABLE index_sync_log ADD COLUMN generation_id bigint REFERENCES index_generations(id);
