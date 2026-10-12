DROP INDEX IF EXISTS idx_attachments_file_key_active;
DROP INDEX IF EXISTS idx_attachments_deletion;
ALTER TABLE attachments DROP CONSTRAINT IF EXISTS attachments_deletion_fkey;
DROP TABLE IF EXISTS asset_deletion_objects;
DROP INDEX IF EXISTS idx_asset_deletions_active;
DROP INDEX IF EXISTS idx_asset_deletions_asset;
DROP TABLE IF EXISTS asset_deletions;
ALTER TABLE attachments DROP COLUMN IF EXISTS deletion_id;
ALTER TABLE attachments DROP COLUMN IF EXISTS deleted_at;
