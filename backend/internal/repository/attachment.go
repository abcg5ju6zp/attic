package repository

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lmmendes/attic/internal/domain"
)

// ErrAssetNotLive indicates that an attachment operation targeted an asset
// which is missing or already in a deleted lifecycle state.
var ErrAssetNotLive = errors.New("asset not found or deleted")

type AttachmentRepository struct {
	pool *pgxpool.Pool
}

func NewAttachmentRepository(pool *pgxpool.Pool) *AttachmentRepository {
	return &AttachmentRepository{pool: pool}
}

func (r *AttachmentRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.Attachment, error) {
	query := `
		SELECT id, asset_id, uploaded_by, file_key, file_name, file_size, content_type, description, created_at
		FROM attachments
		WHERE id = $1 AND deleted_at IS NULL
	`
	var a domain.Attachment
	err := r.pool.QueryRow(ctx, query, id).Scan(
		&a.ID, &a.AssetID, &a.UploadedBy, &a.FileKey, &a.FileName,
		&a.FileSize, &a.ContentType, &a.Description, &a.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

func (r *AttachmentRepository) ListByAsset(ctx context.Context, assetID uuid.UUID) ([]domain.Attachment, error) {
	query := `
		SELECT id, asset_id, uploaded_by, file_key, file_name, file_size, content_type, description, created_at
		FROM attachments
		WHERE asset_id = $1 AND deleted_at IS NULL
		ORDER BY created_at DESC
	`
	rows, err := r.pool.Query(ctx, query, assetID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var attachments []domain.Attachment
	for rows.Next() {
		var a domain.Attachment
		if err := rows.Scan(
			&a.ID, &a.AssetID, &a.UploadedBy, &a.FileKey, &a.FileName,
			&a.FileSize, &a.ContentType, &a.Description, &a.CreatedAt,
		); err != nil {
			return nil, err
		}
		attachments = append(attachments, a)
	}
	return attachments, rows.Err()
}

// Create persists an attachment for a live asset. The asset row is locked for
// the duration of the transaction so the insert is arbitrated against a
// concurrent asset deletion: when deletion commits first, the upload is
// rejected instead of producing an attachment belonging to an older or newer
// generation than the frozen manifest.
func (r *AttachmentRepository) Create(ctx context.Context, a *domain.Attachment) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var liveID uuid.UUID
	if err := tx.QueryRow(ctx,
		`SELECT id FROM assets WHERE id = $1 AND deleted_at IS NULL FOR UPDATE`,
		a.AssetID,
	).Scan(&liveID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrAssetNotLive
		}
		return err
	}

	query := `
		INSERT INTO attachments (id, asset_id, uploaded_by, file_key, file_name, file_size, content_type, description)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING created_at
	`
	if a.ID == uuid.Nil {
		a.ID = uuid.New()
	}
	if err := tx.QueryRow(ctx, query,
		a.ID, a.AssetID, a.UploadedBy, a.FileKey, a.FileName, a.FileSize, a.ContentType, a.Description,
	).Scan(&a.CreatedAt); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// Delete hard-deletes an attachment row. Prefer SoftDelete for lifecycle
// handling; this remains for paths that do not participate in reclamation.
func (r *AttachmentRepository) Delete(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM attachments WHERE id = $1`
	_, err := r.pool.Exec(ctx, query, id)
	return err
}

// SoftDelete hides a single attachment as part of its lifecycle. It locks the
// owning asset before the attachment row (the same asset->attachment order
// used by asset deletion, avoiding deadlocks), so it serializes against a
// deletion freeze, and clears the attachment from any live main image slot.
// Returns the object key and found=false when the attachment is missing or
// already hidden by a deletion generation.
func (r *AttachmentRepository) SoftDelete(ctx context.Context, id uuid.UUID) (fileKey string, found bool, err error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback(ctx)

	var assetID uuid.UUID
	if err = tx.QueryRow(ctx,
		`SELECT asset_id, file_key FROM attachments WHERE id = $1 AND deleted_at IS NULL`,
		id,
	).Scan(&assetID, &fileKey); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}
		return "", false, err
	}

	// Lock the asset first to serialize with a deletion freeze using the same
	// lock order. After the freeze commits, the conditional update below
	// affects zero rows and we report the attachment as not found.
	if _, err = tx.Exec(ctx, `SELECT id FROM assets WHERE id = $1 FOR UPDATE`, assetID); err != nil {
		return "", false, err
	}

	tag, err := tx.Exec(ctx,
		`UPDATE attachments SET deleted_at = NOW() WHERE id = $1 AND deleted_at IS NULL`,
		id)
	if err != nil {
		return "", false, err
	}
	if tag.RowsAffected() == 0 {
		return "", false, nil
	}

	if _, err = tx.Exec(ctx,
		`UPDATE assets SET main_attachment_id = NULL WHERE main_attachment_id = $1 AND deleted_at IS NULL`,
		id); err != nil {
		return "", false, err
	}

	if err = tx.Commit(ctx); err != nil {
		return "", false, err
	}
	return fileKey, true, nil
}
