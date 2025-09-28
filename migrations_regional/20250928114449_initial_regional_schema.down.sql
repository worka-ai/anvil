DROP TRIGGER IF EXISTS objects_update_key_ltree_trigger ON objects;
DROP FUNCTION IF EXISTS update_key_ltree();
DROP TABLE IF EXISTS objects;
DROP EXTENSION IF EXISTS ltree;
DROP EXTENSION IF EXISTS pg_trgm;