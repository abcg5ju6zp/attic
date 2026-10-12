package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Deletion lifecycle statuses.
const (
	DeletionStatusPending    = "pending"
	DeletionStatusReclaiming = "reclaiming"
	DeletionStatusCompleted  = "completed"
)

// Per-object reclaim statuses.
const (
	ObjectStatusPending  = "pending"
	ObjectStatusDeleted  = "deleted"
	ObjectStatusRetained = "retained"
	ObjectStatusFailed   = "failed"
)

// AssetDeletion is the stable identity of one asset deletion. The same row is
// reused by repeated delete requests and by restart recovery.
type AssetDeletion struct {
	ID                     uuid.UUID
	AssetID                uuid.UUID
	OrganizationID         uuid.UUID
	Status                 string
	FrozenMainAttachmentID *uuid.UUID
	AttachmentCount        int
	CreatedAt              time.Time
	UpdatedAt              time.Time
	CompletedAt            *time.Time
}

// AssetDeletionObject is one frozen object key and its per-item reclaim
// state, making partial failures queryable and retryable.
type AssetDeletionObject struct {
	ID         uuid.UUID
	DeletionID uuid.UUID
	FileKey    string
	Status     string
	Attempts   int
	LastError  *string
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

type DeletionRepository struct {
	pool *pgxpool.Pool
}

func NewDeletionRepository(pool *pgxpool.Pool) *DeletionRepository {
	return &DeletionRepository{pool: pool}
}

const deletionColumns = `id, asset_id, organization_id, status, frozen_main_attachment_id,
    attachment_count, created_at, updated_at, completed_at`

func scanAssetDeletion(row pgx.Row) (*AssetDeletion, error) {
	var d AssetDeletion
	if err := row.Scan(
		&d.ID, &d.AssetID, &d.OrganizationID, &d.Status, &d.FrozenMainAttachmentID,
		&d.AttachmentCount, &d.CreatedAt, &d.UpdatedAt, &d.CompletedAt,
	); err != nil {
		return nil, err
	}
	return &d, nil
}

// BeginDeletion freezes the deletion manifest and commits the database side of
// the lifecycle before any remote object is touched:
//
//   - a missing asset row is reported as found=false and produces no job;
//   - an existing deletion for the asset (finished or in progress) is returned
//     unchanged, so repeated deletes and restarts share one identity;
//   - otherwise the asset row is locked, its live attachments and main image
//     are snapshotted into the manifest, the attachments are claimed by the
//     new generation and the asset is soft-deleted, all in one transaction.
//
// New attachments inserting concurrently serialize on the asset row lock and
// either are part of this snapshot or are rejected once the asset is hidden.
func (r *DeletionRepository) BeginDeletion(ctx context.Context, assetID uuid.UUID) (*AssetDeletion, bool, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback(ctx)

	var orgID uuid.UUID
	err = tx.QueryRow(ctx,
		`SELECT organization_id FROM assets WHERE id = $1`,
		assetID,
	).Scan(&orgID)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	// Same organization-wide write lock and ordering as the legacy asset
	// delete (advisory before asset rows), so it cannot deadlock against
	// concurrent asset updates.
	if err = lockAttributeWrites(ctx, tx, orgID); err != nil {
		return nil, false, err
	}
	// Read the main image only while holding the asset row lock, so a
	// concurrent main-image switch either fully precedes the freeze or lands
	// on zero rows afterwards - never in between.
	var mainID *uuid.UUID
	if err = tx.QueryRow(ctx,
		`SELECT main_attachment_id FROM assets WHERE id = $1 FOR UPDATE`,
		assetID,
	).Scan(&mainID); err != nil {
		return nil, false, err
	}

	// Reuse the existing deletion identity, including completed ones: the
	// manifest must never be rescanned into a different target set.
	if existing, err := scanAssetDeletion(tx.QueryRow(ctx,
		`SELECT `+deletionColumns+` FROM asset_deletions
         WHERE asset_id = $1 ORDER BY created_at DESC, id DESC LIMIT 1`,
		assetID,
	)); err == nil {
		return existing, true, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return nil, false, err
	}

	rows, err := tx.Query(ctx,
		`SELECT id, file_key FROM attachments
         WHERE asset_id = $1 AND deleted_at IS NULL
         ORDER BY created_at ASC, id ASC
         FOR UPDATE`,
		assetID,
	)
	if err != nil {
		return nil, false, err
	}
	type frozenAttachment struct {
		id  uuid.UUID
		key string
	}
	var attachments []frozenAttachment
	seenKeys := map[string]bool{}
	var keys []string
	for rows.Next() {
		var a frozenAttachment
		if err := rows.Scan(&a.id, &a.key); err != nil {
			rows.Close()
			return nil, false, err
		}
		attachments = append(attachments, a)
		if !seenKeys[a.key] {
			seenKeys[a.key] = true
			keys = append(keys, a.key)
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, false, err
	}

	deletion := &AssetDeletion{
		ID:                     uuid.New(),
		AssetID:                assetID,
		OrganizationID:         orgID,
		Status:                 DeletionStatusPending,
		FrozenMainAttachmentID: mainID,
		AttachmentCount:        len(attachments),
	}

	if _, err = tx.Exec(ctx, `
		INSERT INTO asset_deletions
		    (id, asset_id, organization_id, status, frozen_main_attachment_id, attachment_count)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		deletion.ID, assetID, orgID, deletion.Status, mainID, deletion.AttachmentCount,
	); err != nil {
		return nil, false, err
	}

	for _, key := range keys {
		if _, err = tx.Exec(ctx, `
			INSERT INTO asset_deletion_objects (id, deletion_id, file_key, status)
			VALUES (uuid_generate_v4(), $1, $2, $3)`,
			deletion.ID, key, ObjectStatusPending,
		); err != nil {
			return nil, false, err
		}
	}

	// Claim the frozen attachment rows for this generation and hide them with
	// the asset lifecycle.
	if _, err = tx.Exec(ctx,
		`UPDATE attachments
         SET deleted_at = NOW(), deletion_id = $2
         WHERE asset_id = $1 AND deleted_at IS NULL`,
		assetID, deletion.ID,
	); err != nil {
		return nil, false, err
	}

	if _, err = tx.Exec(ctx,
		`UPDATE assets SET deleted_at = NOW(), main_attachment_id = NULL WHERE id = $1`,
		assetID,
	); err != nil {
		return nil, false, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, false, err
	}
	d, err := r.GetDeletion(ctx, deletion.ID)
	if err != nil {
		return nil, false, err
	}
	return d, true, nil
}

// GetDeletion loads a deletion by its stable identity.
func (r *DeletionRepository) GetDeletion(ctx context.Context, id uuid.UUID) (*AssetDeletion, error) {
	return scanAssetDeletion(r.pool.QueryRow(ctx,
		`SELECT `+deletionColumns+` FROM asset_deletions WHERE id = $1`, id))
}

// GetDeletionByAsset returns the latest deletion recorded for an asset.
func (r *DeletionRepository) GetDeletionByAsset(ctx context.Context, assetID uuid.UUID) (*AssetDeletion, error) {
	d, err := scanAssetDeletion(r.pool.QueryRow(ctx,
		`SELECT `+deletionColumns+` FROM asset_deletions
         WHERE asset_id = $1 ORDER BY created_at DESC, id DESC LIMIT 1`, assetID))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return d, nil
}

// MarkReclaiming moves a pending deletion into the reclaiming phase.
func (r *DeletionRepository) MarkReclaiming(ctx context.Context, id uuid.UUID) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE asset_deletions SET status = $2, updated_at = NOW()
         WHERE id = $1 AND status = $3`,
		id, DeletionStatusReclaiming, DeletionStatusPending)
	return err
}

// ListActiveDeletions returns deletions that still have reclaim work pending,
// using the stored manifest rather than rescanning current database state.
func (r *DeletionRepository) ListActiveDeletions(ctx context.Context) ([]AssetDeletion, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT `+deletionColumns+` FROM asset_deletions
         WHERE status IN ($1, $2) ORDER BY created_at ASC, id ASC`,
		DeletionStatusPending, DeletionStatusReclaiming)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var deletions []AssetDeletion
	for rows.Next() {
		d, err := scanAssetDeletion(rows)
		if err != nil {
			return nil, err
		}
		deletions = append(deletions, *d)
	}
	return deletions, rows.Err()
}

// ListUnconfirmedObjects returns frozen objects that have not been confirmed
// deleted. Retained objects are included so their references can be rechecked
// on retry; only "deleted" objects are skipped.
func (r *DeletionRepository) ListUnconfirmedObjects(ctx context.Context, deletionID uuid.UUID) ([]AssetDeletionObject, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT id, deletion_id, file_key, status, attempts, last_error, created_at, updated_at
		FROM asset_deletion_objects
		WHERE deletion_id = $1 AND status <> $2
		ORDER BY created_at ASC, id ASC`,
		deletionID, ObjectStatusDeleted)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []AssetDeletionObject
	for rows.Next() {
		var o AssetDeletionObject
		if err := rows.Scan(
			&o.ID, &o.DeletionID, &o.FileKey, &o.Status, &o.Attempts,
			&o.LastError, &o.CreatedAt, &o.UpdatedAt,
		); err != nil {
			return nil, err
		}
		objects = append(objects, o)
	}
	return objects, rows.Err()
}

// CountLiveReferences counts non-deleted records that still reference an
// object key: live attachment rows plus main-image slots of live assets. A
// shared key or a new-generation reference therefore keeps the object alive.
func (r *DeletionRepository) CountLiveReferences(ctx context.Context, fileKey string) (int, error) {
	var count int
	err := r.pool.QueryRow(ctx, `
		SELECT
			(SELECT COUNT(*) FROM attachments
                WHERE file_key = $1 AND deleted_at IS NULL)
			+
			(SELECT COUNT(*) FROM assets a
                JOIN attachments ma ON ma.id = a.main_attachment_id
                WHERE ma.file_key = $1 AND a.deleted_at IS NULL)`,
		fileKey,
	).Scan(&count)
	return count, err
}

// MarkObjectOutcome records the latest per-item reclaim outcome, error detail
// and attempt count without touching any other frozen object.
func (r *DeletionRepository) MarkObjectOutcome(ctx context.Context, id uuid.UUID, status string, lastError *string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE asset_deletion_objects
		SET status = $2, last_error = $3, attempts = attempts + 1, updated_at = NOW()
		WHERE id = $1`,
		id, status, lastError)
	return err
}

// CompleteDeletionIfReady marks the deletion completed when no object remains
// pending or failed. Retained (still referenced) objects do not block
// completion, but stay recheckable on the next retry.
func (r *DeletionRepository) CompleteDeletionIfReady(ctx context.Context, id uuid.UUID) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE asset_deletions d
		SET status = $2, completed_at = NOW(), updated_at = NOW()
		WHERE d.id = $1 AND d.status <> $2
		  AND NOT EXISTS (
		      SELECT 1 FROM asset_deletion_objects o
		      WHERE o.deletion_id = d.id AND o.status IN ($3, $4)
		  )`,
		id, DeletionStatusCompleted, ObjectStatusPending, ObjectStatusFailed)
	return err
}
