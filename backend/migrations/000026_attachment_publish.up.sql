-- Stable publishing identity for attachment uploads.
--
-- One row per upload attempt links the deterministic storage object key, the
-- attachments row, the asset main-image pointer and the HTTP result together.
-- Rows survive request cancellation and process restarts so an interrupted
-- publish can be resumed with the same identity instead of creating duplicates.
CREATE TABLE attachment_uploads (
    id UUID PRIMARY KEY,
    asset_id UUID NOT NULL REFERENCES assets(id) ON DELETE CASCADE,
    -- Set once the attachments row is committed. Attachment deletion clears
    -- this and the publish is aborted in the same transaction.
    attachment_id UUID REFERENCES attachments(id) ON DELETE SET NULL,
    file_key VARCHAR(500) NOT NULL,
    file_name VARCHAR(255) NOT NULL,
    file_size BIGINT NOT NULL DEFAULT 0,
    content_type VARCHAR(100),
    description TEXT,
    is_image BOOLEAN NOT NULL DEFAULT FALSE,
    -- True when the upload promised to become the asset main image.
    set_main BOOLEAN NOT NULL DEFAULT FALSE,
    -- pending_object -> object_uploaded -> attachment_created -> completed
    -- aborted is the terminal failure state.
    status VARCHAR(32) NOT NULL,
    -- set | skipped_manual, populated when set_main reaches a terminal state.
    main_result VARCHAR(32),
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

-- Recovery only ever scans unfinished publishes.
CREATE INDEX idx_attachment_uploads_pending
    ON attachment_uploads(updated_at)
    WHERE status NOT IN ('completed', 'aborted');

-- Delete arbitration looks up publishes by the attachment they reference.
CREATE INDEX idx_attachment_uploads_attachment
    ON attachment_uploads(attachment_id)
    WHERE attachment_id IS NOT NULL;

-- Records the last explicit (manual) main-image choice. Automatic publish
-- completion only claims the main image when no manual choice was made after
-- the upload was registered, so manual set/clear is never resurrected by a
-- late recovery.
ALTER TABLE assets ADD COLUMN main_attachment_manual_at TIMESTAMPTZ;
