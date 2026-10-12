package repository

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lmmendes/attic/internal/domain"
)

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
		WHERE id = $1
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
		WHERE asset_id = $1
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

func (r *AttachmentRepository) Create(ctx context.Context, a *domain.Attachment) error {
	query := `
		INSERT INTO attachments (id, asset_id, uploaded_by, file_key, file_name, file_size, content_type, description)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING created_at
	`
	if a.ID == uuid.Nil {
		a.ID = uuid.New()
	}
	return r.pool.QueryRow(ctx, query,
		a.ID, a.AssetID, a.UploadedBy, a.FileKey, a.FileName, a.FileSize, a.ContentType, a.Description,
	).Scan(&a.CreatedAt)
}

// GetByAssetAndFileKey returns an attachment on the asset with the given
// storage key. It is the idempotency anchor for recovered cover uploads.
func (r *AttachmentRepository) GetByAssetAndFileKey(ctx context.Context, assetID uuid.UUID, fileKey string) (*domain.Attachment, error) {
	query := `
		SELECT id, asset_id, uploaded_by, file_key, file_name, file_size, content_type, description, created_at
		FROM attachments
		WHERE asset_id = $1 AND file_key = $2
	`
	var a domain.Attachment
	err := r.pool.QueryRow(ctx, query, assetID, fileKey).Scan(
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

// CreateOrGet inserts an attachment, returning the existing row when an
// attachment with the same (asset, storage key) was created by a concurrent
// or earlier attempt. created reports whether a new row was inserted.
func (r *AttachmentRepository) CreateOrGet(ctx context.Context, a *domain.Attachment) (created bool, err error) {
	if a.ID == uuid.Nil {
		a.ID = uuid.New()
	}
	query := `
		INSERT INTO attachments (id, asset_id, uploaded_by, file_key, file_name, file_size, content_type, description)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (asset_id, file_key) DO NOTHING
		RETURNING created_at
	`
	err = r.pool.QueryRow(ctx, query,
		a.ID, a.AssetID, a.UploadedBy, a.FileKey, a.FileName, a.FileSize, a.ContentType, a.Description,
	).Scan(&a.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		existing, getErr := r.GetByAssetAndFileKey(ctx, a.AssetID, a.FileKey)
		if getErr != nil {
			return false, getErr
		}
		if existing == nil {
			return false, errors.New("attachment disappeared after conflict")
		}
		*a = *existing
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

func (r *AttachmentRepository) Delete(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM attachments WHERE id = $1`
	_, err := r.pool.Exec(ctx, query, id)
	return err
}
