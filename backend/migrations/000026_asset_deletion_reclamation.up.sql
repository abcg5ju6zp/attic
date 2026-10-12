-- Reference-aware reclamation for asset deletion.
--
-- A deletion first freezes the asset, its attachments, the main image and the
-- deduplicated object key manifest under a stable deletion identity. Database
-- lifecycle changes commit before any remote object is deleted, and each
-- object keeps a per-item reclaim state so partial failures stay queryable and
-- only unconfirmed objects are retried (also after a restart).

-- Attachment lifecycle: hidden together with the asset, and stamped with the
-- generation (deletion job) that claimed the row. New uploads interleave with
-- deletion through row locks, so rows created after the freeze are never
-- claimed by an older generation.
ALTER TABLE attachments ADD COLUMN deleted_at TIMESTAMPTZ;
ALTER TABLE attachments ADD COLUMN deletion_id UUID;

-- One row per asset deletion attempt; reused as the stable identity for
-- repeated deletes and for restart recovery.
CREATE TABLE asset_deletions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    asset_id UUID NOT NULL,
    organization_id UUID NOT NULL,
    -- pending: manifest frozen, object reclaim not started
    -- reclaiming: frozen objects are being reclaimed
    -- completed: every frozen object is confirmed deleted or still referenced
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    frozen_main_attachment_id UUID,
    attachment_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX idx_asset_deletions_asset ON asset_deletions(asset_id);
-- At most one unfinished deletion generation per asset; repeated deletes join
-- the existing identity instead of producing a new manifest.
CREATE UNIQUE INDEX idx_asset_deletions_active
    ON asset_deletions(asset_id) WHERE status <> 'completed';

-- Frozen object key manifest with per-item reclaim state.
CREATE TABLE asset_deletion_objects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    deletion_id UUID NOT NULL REFERENCES asset_deletions(id) ON DELETE CASCADE,
    file_key VARCHAR(500) NOT NULL,
    -- pending: awaiting reclaim
    -- deleted: object absence was confirmed against the storage backend
    -- retained: a live reference still exists, the object must not be deleted
    -- failed: deletion was attempted but could not be confirmed; retryable
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(deletion_id, file_key)
);

CREATE INDEX idx_asset_deletion_objects_reclaim
    ON asset_deletion_objects(deletion_id, status);

ALTER TABLE attachments
    ADD CONSTRAINT attachments_deletion_fkey
    FOREIGN KEY (deletion_id) REFERENCES asset_deletions(id) ON DELETE SET NULL;

CREATE INDEX idx_attachments_deletion
    ON attachments(deletion_id) WHERE deletion_id IS NOT NULL;
-- Supports "is this object key still referenced by a live attachment" checks
-- during reclaim.
CREATE INDEX idx_attachments_file_key_active
    ON attachments(file_key) WHERE deleted_at IS NULL;
