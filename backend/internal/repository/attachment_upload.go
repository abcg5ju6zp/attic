package repository

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lmmendes/attic/internal/domain"
	"github.com/lmmendes/attic/internal/storage"
)

var (
	// ErrUploadNotFound indicates the stable upload identity does not exist.
	ErrUploadNotFound = errors.New("attachment upload not found")
	// ErrUploadAborted indicates the publish was superseded (typically because
	// the attachment was deleted before publication completed).
	ErrUploadAborted = errors.New("attachment upload aborted")
	// ErrObjectNotUploaded indicates the storage object stage never completed,
	// so the publish cannot be finished without a request body.
	ErrObjectNotUploaded = errors.New("storage object has not been uploaded")
)

const (
	// recoverMinPendingAge avoids taking over publishes that an in-flight HTTP
	// request is actively driving.
	recoverMinPendingAge = 30 * time.Second
	// recoverOrphanAge abandons object-pending records that never reached the
	// object_uploaded state (e.g. process died while streaming to storage).
	recoverOrphanAge = 15 * time.Minute
)

// AttachmentPublisher drives the recoverable upload state machine. Each
// publish has a stable identity that ties the storage object key, the
// attachments row, the asset main-image pointer and the HTTP result together.
type AttachmentPublisher struct {
	pool  *pgxpool.Pool
	store storage.FileStorage // may be nil when storage is not configured
}

// NewAttachmentPublisher creates a publisher. store may be nil; object
// upload/deletion steps are then the caller's responsibility to avoid.
func NewAttachmentPublisher(pool *pgxpool.Pool, store storage.FileStorage) *AttachmentPublisher {
	return &AttachmentPublisher{pool: pool, store: store}
}

// RegisterUploadInput carries the metadata captured before the object upload.
type RegisterUploadInput struct {
	ID          uuid.UUID
	AssetID     uuid.UUID
	FileName    string
	FileSize    int64
	ContentType string
	Description *string
	SetMain     bool
}

// uploadObjectKey returns the deterministic storage key for an upload. Retries
// with the same upload ID always hit the same object.
func uploadObjectKey(uploadID uuid.UUID, fileName string) string {
	return fmt.Sprintf("uploads/%s/%s", uploadID.String(), fileName)
}

// Register creates the stable publish record before any storage work starts,
// so an upload remains observable even if the process dies immediately. If the
// ID already exists the original record is returned unchanged.
func (p *AttachmentPublisher) Register(ctx context.Context, in RegisterUploadInput) (*domain.AttachmentUpload, error) {
	contentType := nullableString(in.ContentType)
	fileKey := uploadObjectKey(in.ID, in.FileName)

	query := `
		INSERT INTO attachment_uploads
			(id, asset_id, file_key, file_name, file_size, content_type, description, is_image, set_main, status, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())
		ON CONFLICT (id) DO NOTHING
	`
	tag, err := p.pool.Exec(ctx, query,
		in.ID, in.AssetID, fileKey, in.FileName, in.FileSize, contentType, in.Description,
		in.SetMain, in.SetMain, domain.AttachmentUploadPendingObject,
	)
	if err != nil {
		return nil, fmt.Errorf("registering attachment upload: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return p.Get(ctx, in.ID)
	}
	return p.Get(ctx, in.ID)
}

func nullableString(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

// Get returns the publish record for an upload identity.
func (p *AttachmentPublisher) Get(ctx context.Context, id uuid.UUID) (*domain.AttachmentUpload, error) {
	return scanUpload(p.pool.QueryRow(ctx, uploadSelect+` WHERE id = $1`, id))
}

const uploadSelect = `
	SELECT id, asset_id, attachment_id, file_key, file_name, file_size, content_type, description,
	       is_image, set_main, status, main_result, error, created_at, updated_at, completed_at
	FROM attachment_uploads
`

type rowScanner interface {
	Scan(dest ...any) error
}

func scanUpload(s rowScanner) (*domain.AttachmentUpload, error) {
	var u domain.AttachmentUpload
	err := s.Scan(
		&u.ID, &u.AssetID, &u.AttachmentID, &u.FileKey, &u.FileName, &u.FileSize,
		&u.ContentType, &u.Description, &u.IsImage, &u.SetMain, &u.Status, &u.MainResult,
		&u.Error, &u.CreatedAt, &u.UpdatedAt, &u.CompletedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// PutObject uploads the object under the deterministic key. Once the object
// stage has been recorded it is never re-uploaded: retries and recovery reuse
// the same object instead of creating a duplicate.
func (p *AttachmentPublisher) PutObject(ctx context.Context, id uuid.UUID, body io.Reader) (*domain.AttachmentUpload, error) {
	u, err := p.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if u == nil {
		return nil, ErrUploadNotFound
	}
	switch u.Status {
	case domain.AttachmentUploadAborted:
		return u, ErrUploadAborted
	case domain.AttachmentUploadCompleted:
		return u, nil
	case domain.AttachmentUploadPendingObject:
		// Proceed to upload below.
	default:
		// Object already recorded.
		return u, nil
	}

	if p.store == nil {
		return nil, errors.New("storage not configured")
	}
	contentType := ""
	if u.ContentType != nil {
		contentType = *u.ContentType
	}
	// Overwriting the same deterministic key on retry is idempotent for both
	// local storage and S3 PutObject.
	if err := p.store.UploadWithKey(ctx, u.FileKey, contentType, body); err != nil {
		return nil, fmt.Errorf("uploading object: %w", err)
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)

	current, err := lockUpload(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if current.Status == domain.AttachmentUploadPendingObject {
		if _, err := tx.Exec(ctx,
			`UPDATE attachment_uploads SET status = $2, updated_at = NOW() WHERE id = $1`,
			id, domain.AttachmentUploadObjectUploaded); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return p.Get(ctx, id)
}

// Commit drives the publish to its terminal state: it ensures the attachments
// row exists (reusing the existing object) and, for images, settles the asset
// main-image pointer. It is idempotent and safe to call from request retries
// and from recovery after request cancellation or process restart.
//
// The attachment row is committed separately (attachment_created) before the
// main-image stage runs, so a main-pointer failure retries the same
// attachment row instead of re-uploading or re-creating one.
//
// Success means the attachment row exists and, for images, the promised main
// image outcome was recorded (set or explicitly superseded by a manual
// choice).
func (p *AttachmentPublisher) Commit(ctx context.Context, id uuid.UUID) (*domain.AttachmentUpload, *domain.Attachment, error) {
	pre, err := p.Get(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	if pre == nil {
		return nil, nil, ErrUploadNotFound
	}
	if pre.Status == domain.AttachmentUploadAborted {
		return pre, nil, ErrUploadAborted
	}

	// Stage 1: persist the attachment row independently of the main image.
	attachmentID, err := p.ensureAttachmentCommitted(ctx, pre)
	if err != nil {
		return nil, nil, err
	}

	// Stage 2: settle the promised main-image outcome and complete.
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback(ctx)

	u, err := lockUpload(ctx, tx, id)
	if err != nil {
		return nil, nil, err
	}
	switch u.Status {
	case domain.AttachmentUploadAborted:
		return u, nil, ErrUploadAborted
	case domain.AttachmentUploadCompleted:
		att, err := loadAttachmentTx(ctx, tx, attachmentID)
		if err != nil {
			return u, nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return u, nil, err
		}
		return u, att, nil
	case domain.AttachmentUploadPendingObject:
		return u, nil, ErrObjectNotUploaded
	}

	mainResult := u.MainResult
	if u.SetMain && mainResult == nil {
		// Automatic publish only claims the main image when no explicit manual
		// set/clear happened after this upload was registered. A concurrent
		// manual choice therefore wins and is never overwritten by recovery.
		if _, err := tx.Exec(ctx, `
			UPDATE assets
			SET main_attachment_id = $2
			WHERE id = $1
			  AND deleted_at IS NULL
			  AND (main_attachment_manual_at IS NULL OR main_attachment_manual_at < $3)`,
			u.AssetID, attachmentID, u.CreatedAt); err != nil {
			return nil, nil, err
		}

		var currentMain *uuid.UUID
		if err := tx.QueryRow(ctx,
			`SELECT main_attachment_id FROM assets WHERE id = $1 FOR UPDATE`, u.AssetID,
		).Scan(&currentMain); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return nil, nil, err
		}
		outcome := domain.AttachmentUploadMainSet
		if currentMain == nil || *currentMain != attachmentID {
			outcome = domain.AttachmentUploadMainSkippedManual
		}
		mainResult = &outcome
	}

	if _, err := tx.Exec(ctx, `
		UPDATE attachment_uploads
		SET status = $2, main_result = $3, completed_at = NOW(), updated_at = NOW(), error = NULL
		WHERE id = $1`,
		id, domain.AttachmentUploadCompleted, mainResult); err != nil {
		return nil, nil, err
	}

	att, err := loadAttachmentTx(ctx, tx, attachmentID)
	if err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, nil, err
	}

	u.Status = domain.AttachmentUploadCompleted
	u.MainResult = mainResult
	now := time.Now()
	u.CompletedAt = &now
	u.AttachmentID = &attachmentID
	return u, att, nil
}

// ensureAttachmentCommitted drives the upload through attachment_created and
// returns the stable attachment ID. Locks are taken in attachment -> upload
// order, matching AbortAttachment, so publication and deletion cannot
// deadlock.
func (p *AttachmentPublisher) ensureAttachmentCommitted(ctx context.Context, pre *domain.AttachmentUpload) (uuid.UUID, error) {
	// Fast path: the row already exists. Lock it before the upload record to
	// stay consistent with deletion's lock order.
	if pre.AttachmentID != nil {
		tx, err := p.pool.Begin(ctx)
		if err != nil {
			return uuid.Nil, err
		}
		defer tx.Rollback(ctx)

		var existingID uuid.UUID
		err = tx.QueryRow(ctx,
			`SELECT id FROM attachments WHERE id = $1 FOR UPDATE`, *pre.AttachmentID,
		).Scan(&existingID)
		if errors.Is(err, pgx.ErrNoRows) {
			// The attachment row is gone; abort instead of resurrecting it.
			current, lockErr := lockUpload(ctx, tx, pre.ID)
			if lockErr != nil {
				return uuid.Nil, lockErr
			}
			if current.Status != domain.AttachmentUploadAborted {
				if err := abortUploadTx(ctx, tx, pre.ID, "attachment deleted"); err != nil {
					return uuid.Nil, err
				}
			}
			if err := tx.Commit(ctx); err != nil {
				return uuid.Nil, err
			}
			return uuid.Nil, ErrUploadAborted
		}
		if err != nil {
			return uuid.Nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return uuid.Nil, err
		}
		return existingID, nil
	}

	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer tx.Rollback(ctx)

	u, err := lockUpload(ctx, tx, pre.ID)
	if err != nil {
		return uuid.Nil, err
	}
	switch u.Status {
	case domain.AttachmentUploadAborted:
		return uuid.Nil, ErrUploadAborted
	case domain.AttachmentUploadCompleted:
		if u.AttachmentID == nil {
			if err := abortUploadTx(ctx, tx, pre.ID, "published attachment deleted"); err != nil {
				return uuid.Nil, err
			}
			if err := tx.Commit(ctx); err != nil {
				return uuid.Nil, err
			}
			return uuid.Nil, ErrUploadAborted
		}
		id := *u.AttachmentID
		if err := tx.Commit(ctx); err != nil {
			return uuid.Nil, err
		}
		return id, nil
	case domain.AttachmentUploadPendingObject:
		return uuid.Nil, ErrObjectNotUploaded
	}
	if u.AttachmentID != nil {
		// A concurrent attempt already inserted the row.
		id := *u.AttachmentID
		if err := tx.Commit(ctx); err != nil {
			return uuid.Nil, err
		}
		return id, nil
	}

	newID := uuid.New()
	if _, err := tx.Exec(ctx, `
		INSERT INTO attachments (id, asset_id, file_key, file_name, file_size, content_type, description)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		newID, u.AssetID, u.FileKey, u.FileName, u.FileSize, u.ContentType, u.Description,
	); err != nil {
		return uuid.Nil, fmt.Errorf("creating attachment record: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE attachment_uploads
		SET attachment_id = $2, status = $3, updated_at = NOW()
		WHERE id = $1`,
		pre.ID, newID, domain.AttachmentUploadAttachmentCreated); err != nil {
		return uuid.Nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, err
	}
	return newID, nil
}

// AbortAttachment arbitrates deletion against an unfinished publish. In one
// transaction it locks the attachment, aborts any non-terminal publish
// referencing it, clears a main-image pointer to it and removes the row. The
// storage object is deleted by the caller after commit using the returned
// key. A completed publish whose attachment is deleted can no longer be
// replayed.
func (p *AttachmentPublisher) AbortAttachment(ctx context.Context, attachmentID uuid.UUID) (assetID uuid.UUID, fileKey string, err error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, "", err
	}
	defer tx.Rollback(ctx)

	err = tx.QueryRow(ctx,
		`SELECT asset_id, file_key FROM attachments WHERE id = $1 FOR UPDATE`, attachmentID,
	).Scan(&assetID, &fileKey)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, "", ErrUploadNotFound
	}
	if err != nil {
		return uuid.Nil, "", err
	}

	if _, err := tx.Exec(ctx, `
		UPDATE attachment_uploads
		SET status = $2, completed_at = NOW(), updated_at = NOW(),
		    error = COALESCE(error, 'attachment deleted before publish completed')
		WHERE attachment_id = $1 AND status NOT IN ($2, $3)`,
		attachmentID, domain.AttachmentUploadAborted, domain.AttachmentUploadCompleted); err != nil {
		return uuid.Nil, "", err
	}

	// Defensive clear in the same transaction; the FK ON DELETE SET NULL would
	// also do this when the row is deleted below.
	if _, err := tx.Exec(ctx,
		`UPDATE assets SET main_attachment_id = NULL WHERE main_attachment_id = $1`,
		attachmentID); err != nil {
		return uuid.Nil, "", err
	}

	if _, err := tx.Exec(ctx, `DELETE FROM attachments WHERE id = $1`, attachmentID); err != nil {
		return uuid.Nil, "", err
	}

	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, "", err
	}
	return assetID, fileKey, nil
}

// Recover drives all stale unfinished publishes to their terminal state. It is
// safe to run concurrently with HTTP requests (FOR UPDATE SKIP LOCKED) and to
// run repeatedly after process restarts. Object-pending records older than
// the orphan age are aborted and their objects are best-effort deleted.
func (p *AttachmentPublisher) Recover(ctx context.Context) error {
	rows, err := p.pool.Query(ctx, `
		SELECT id, status, created_at
		FROM attachment_uploads
		WHERE status NOT IN ($2, $3)
		  AND updated_at < NOW() - $1::interval
		ORDER BY updated_at
		FOR UPDATE SKIP LOCKED`,
		fmt.Sprintf("%d seconds", int(recoverMinPendingAge/time.Second)),
		domain.AttachmentUploadCompleted, domain.AttachmentUploadAborted)
	if err != nil {
		return fmt.Errorf("scanning pending uploads: %w", err)
	}
	type pending struct {
		id        uuid.UUID
		status    string
		createdAt time.Time
	}
	var pendings []pending
	for rows.Next() {
		var pe pending
		if err := rows.Scan(&pe.id, &pe.status, &pe.createdAt); err != nil {
			rows.Close()
			return err
		}
		pendings = append(pendings, pe)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}

	for _, pe := range pendings {
		switch pe.status {
		case domain.AttachmentUploadObjectUploaded, domain.AttachmentUploadAttachmentCreated:
			if _, _, err := p.Commit(ctx, pe.id); err != nil {
				if errors.Is(err, ErrUploadAborted) || errors.Is(err, ErrObjectNotUploaded) {
					continue
				}
				slog.Error("failed to recover attachment upload", "upload_id", pe.id, "error", err)
			}
		case domain.AttachmentUploadPendingObject:
			if time.Since(pe.createdAt) < recoverOrphanAge {
				continue
			}
			u, err := p.Get(ctx, pe.id)
			if err != nil || u == nil {
				continue
			}
			if err := p.abortPendingObject(ctx, pe.id); err != nil {
				slog.Error("failed to abort orphaned attachment upload", "upload_id", pe.id, "error", err)
				continue
			}
			if p.store != nil && u.FileKey != "" {
				if err := p.store.Delete(ctx, u.FileKey); err != nil {
					slog.Warn("failed to delete orphaned upload object", "upload_id", pe.id, "key", u.FileKey, "error", err)
				}
			}
		}
	}
	return nil
}

func (p *AttachmentPublisher) abortPendingObject(ctx context.Context, id uuid.UUID) error {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	current, err := lockUpload(ctx, tx, id)
	if err != nil {
		return err
	}
	if current.Status != domain.AttachmentUploadPendingObject {
		return tx.Rollback(ctx)
	}
	if err := abortUploadTx(ctx, tx, id, "upload abandoned before object storage completed"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// RunRecovery runs Recover immediately and then on every tick until ctx is
// cancelled. It is intended to be started once per server process.
func (p *AttachmentPublisher) RunRecovery(ctx context.Context, interval time.Duration) {
	if err := p.Recover(ctx); err != nil {
		slog.Warn("startup attachment upload recovery failed", "error", err)
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.Recover(ctx); err != nil {
				slog.Warn("attachment upload recovery failed", "error", err)
			}
		}
	}
}

func lockUpload(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*domain.AttachmentUpload, error) {
	u, err := scanUpload(tx.QueryRow(ctx, uploadSelect+` WHERE id = $1 FOR UPDATE`, id))
	if err != nil {
		return nil, err
	}
	if u == nil {
		return nil, ErrUploadNotFound
	}
	return u, nil
}

func abortUploadTx(ctx context.Context, tx pgx.Tx, id uuid.UUID, reason string) error {
	_, err := tx.Exec(ctx, `
		UPDATE attachment_uploads
		SET status = $2, completed_at = NOW(), updated_at = NOW(),
		    error = COALESCE(error, $3)
		WHERE id = $1`,
		id, domain.AttachmentUploadAborted, reason)
	return err
}

const attachmentSelectCols = `
	id, asset_id, uploaded_by, file_key, file_name, file_size, content_type, description, created_at
`

func loadAttachmentTx(ctx context.Context, tx pgx.Tx, id uuid.UUID) (*domain.Attachment, error) {
	var a domain.Attachment
	err := tx.QueryRow(ctx,
		`SELECT `+attachmentSelectCols+` FROM attachments WHERE id = $1`, id,
	).Scan(
		&a.ID, &a.AssetID, &a.UploadedBy, &a.FileKey, &a.FileName,
		&a.FileSize, &a.ContentType, &a.Description, &a.CreatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrUploadAborted
	}
	if err != nil {
		return nil, err
	}
	return &a, nil
}

// SetMainAttachmentManual records an explicit admin set/clear of the main
// image. main_attachment_manual_at blocks any earlier in-flight automatic
// publish from overwriting the manual choice, while later uploads still win.
func (r *AssetRepository) SetMainAttachmentManual(ctx context.Context, assetID uuid.UUID, attachmentID *uuid.UUID) error {
	query := `
		UPDATE assets
		SET main_attachment_id = $2, main_attachment_manual_at = NOW()
		WHERE id = $1 AND deleted_at IS NULL
	`
	_, err := r.pool.Exec(ctx, query, assetID, attachmentID)
	return err
}
