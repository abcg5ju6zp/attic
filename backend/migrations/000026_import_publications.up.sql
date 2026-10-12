-- Idempotent, recoverable plugin imports. One publication row per stable
-- identity (organization, plugin, external id); generation isolates a
-- delete-and-reimport from the publication it supersedes.
CREATE TABLE import_publications (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    organization_id UUID NOT NULL REFERENCES organizations(id),
    plugin_id VARCHAR(50) NOT NULL,
    external_id VARCHAR(255) NOT NULL,
    generation INTEGER NOT NULL DEFAULT 1,
    -- pending: waiting for (or resumable by) an owner;
    -- publishing: an owner currently holds the lease;
    -- published: core asset (and cover, when one existed) are complete.
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    asset_id UUID REFERENCES assets(id) ON DELETE SET NULL,
    -- Pinned snapshot of the external response for this generation so a
    -- recovery never mixes later Fetch results into the old publication.
    payload JSONB NOT NULL,
    external_version VARCHAR(64) NOT NULL,
    -- Cover stage bookkeeping: object metadata is persisted before the
    -- Attachment row is written, letting recovery recreate just the record
    -- instead of downloading/uploading the image again.
    cover_file_key VARCHAR(500),
    cover_file_name VARCHAR(255),
    cover_content_type VARCHAR(100),
    cover_file_size BIGINT,
    cover_attachment_id UUID REFERENCES attachments(id) ON DELETE SET NULL,
    lease_owner UUID,
    leased_until TIMESTAMPTZ,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    UNIQUE (organization_id, plugin_id, external_id)
);

CREATE INDEX idx_import_publications_status ON import_publications(status, leased_until);
CREATE INDEX idx_import_publications_asset ON import_publications(asset_id)
    WHERE asset_id IS NOT NULL;

CREATE TRIGGER update_import_publications_updated_at
    BEFORE UPDATE ON import_publications
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- One stored object per (asset, key) makes cover attachment recovery
-- idempotent even when the INSERT response was lost.
CREATE UNIQUE INDEX idx_attachments_asset_file_key
    ON attachments(asset_id, file_key);

-- Only one live plugin-managed category per organization/plugin; concurrent
-- first imports must not create duplicates.
CREATE UNIQUE INDEX idx_categories_org_plugin_unique
    ON categories(organization_id, plugin_id)
    WHERE plugin_id IS NOT NULL AND deleted_at IS NULL;
