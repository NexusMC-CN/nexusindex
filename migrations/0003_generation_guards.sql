CREATE UNIQUE INDEX index_generations_single_building_idx ON index_generations ((status)) WHERE status = 'building';
