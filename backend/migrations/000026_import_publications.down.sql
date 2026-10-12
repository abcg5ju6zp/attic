DROP TRIGGER IF EXISTS update_import_publications_updated_at ON import_publications;
DROP TABLE IF EXISTS import_publications;
DROP INDEX IF EXISTS idx_attachments_asset_file_key;
DROP INDEX IF EXISTS idx_categories_org_plugin_unique;
