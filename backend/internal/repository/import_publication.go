package repository

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lmmendes/attic/internal/domain"
)

// ImportPublicationRepository persists recoverable plugin import state.
//
// Every mutating step is keyed by the row's current generation so an owner
// that lost ownership (delete + reimport bumped the generation) never writes
// its stale result over the newer publication.
type ImportPublicationRepository struct {
	pool *pgxpool.Pool
}

func NewImportPublicationRepository(pool *pgxpool.Pool) *ImportPublicationRepository {
	return &ImportPublicationRepository{pool: pool}
}

const importPublicationColumns = `
	id, organization_id, plugin_id, external_id, generation, status, asset_id,
	payload, external_version, cover_file_key, cover_file_name, cover_content_type,
	cover_file_size, cover_attachment_id, lease_owner, leased_until, attempts,
	last_error, created_at, updated_at, published_at
`

func scanImportPublication(row pgx.Row) (*domain.ImportPublication, error) {
	var pub domain.ImportPublication
	err := row.Scan(
		&pub.ID, &pub.OrganizationID, &pub.PluginID, &pub.ExternalID, &pub.Generation,
		&pub.Status, &pub.AssetID, &pub.Payload, &pub.ExternalVersion,
		&pub.CoverFileKey, &pub.CoverFileName, &pub.CoverContentType, &pub.CoverFileSize,
		&pub.CoverAttachmentID, &pub.LeaseOwner, &pub.LeasedUntil, &pub.Attempts,
		&pub.LastError, &pub.CreatedAt, &pub.UpdatedAt, &pub.PublishedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &pub, nil
}

// GetByID returns the publication with the given id, or nil when absent.
func (r *ImportPublicationRepository) GetByID(ctx context.Context, id uuid.UUID) (*domain.ImportPublication, error) {
	return scanImportPublication(r.pool.QueryRow(ctx,
		"SELECT "+importPublicationColumns+" FROM import_publications WHERE id = $1", id))
}

// GetByIdentity returns the publication for a stable import identity, or nil
// when this (organization, plugin, external id) has never been imported.
func (r *ImportPublicationRepository) GetByIdentity(ctx context.Context, organizationID uuid.UUID, pluginID, externalID string) (*domain.ImportPublication, error) {
	return scanImportPublication(r.pool.QueryRow(ctx, `
		SELECT `+importPublicationColumns+`
		FROM import_publications
		WHERE organization_id = $1 AND plugin_id = $2 AND external_id = $3
	`, organizationID, pluginID, externalID))
}

// CreateIfAbsent stores a new pending publication for its identity. It returns
// created=false when a concurrent request already inserted the same identity;
// in that case the caller should re-read and adopt the existing row.
func (r *ImportPublicationRepository) CreateIfAbsent(ctx context.Context, pub *domain.ImportPublication) (bool, error) {
	if pub.ID == uuid.Nil {
		pub.ID = uuid.New()
	}
	row := r.pool.QueryRow(ctx, `
		INSERT INTO import_publications
			(id, organization_id, plugin_id, external_id, generation, status,
			 payload, external_version)
		VALUES ($1, $2, $3, $4, 1, 'pending', $5, $6)
		ON CONFLICT (organization_id, plugin_id, external_id) DO NOTHING
		RETURNING `+importPublicationColumns,
		pub.ID, pub.OrganizationID, pub.PluginID, pub.ExternalID, pub.Payload, pub.ExternalVersion,
	)
	stored, err := scanImportPublication(row)
	if err != nil {
		return false, err
	}
	if stored == nil {
		return false, nil
	}
	*pub = *stored
	return true, nil
}

// AcquireLease makes the caller the owner of a resumable publication. It only
// succeeds for the supplied generation and while no other live lease exists.
func (r *ImportPublicationRepository) AcquireLease(ctx context.Context, id uuid.UUID, generation int, owner uuid.UUID, leasedUntil time.Time) (bool, error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE import_publications
		SET status = 'publishing',
		    lease_owner = $2,
		    leased_until = $3,
		    attempts = attempts + 1,
		    last_error = NULL,
		    updated_at = NOW()
		WHERE id = $1
		  AND generation = $4
		  AND (status <> 'publishing' OR leased_until IS NULL OR leased_until < NOW())
	`, id, owner, leasedUntil, generation)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// AttachAsset records the core asset created for this generation. It never
// overwrites an asset already attached.
func (r *ImportPublicationRepository) AttachAsset(ctx context.Context, id uuid.UUID, generation int, assetID uuid.UUID) (bool, error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE import_publications
		SET asset_id = $3, updated_at = NOW()
		WHERE id = $1 AND generation = $2 AND asset_id IS NULL
	`, id, generation, assetID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// AttachCoverObject records the uploaded storage object for the cover. The
// key is written before the Attachment row exists, so recovery can recreate
// just the record without re-downloading. An earlier key always wins.
func (r *ImportPublicationRepository) AttachCoverObject(ctx context.Context, id uuid.UUID, generation int, fileKey, fileName, contentType string, fileSize int64) (bool, error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE import_publications
		SET cover_file_key = $3,
		    cover_file_name = $4,
		    cover_content_type = $5,
		    cover_file_size = $6,
		    updated_at = NOW()
		WHERE id = $1 AND generation = $2 AND cover_file_key IS NULL
	`, id, generation, fileKey, fileName, contentType, fileSize)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// AttachCoverAttachment links the cover Attachment row, keeping an earlier
// attachment if the step already ran.
func (r *ImportPublicationRepository) AttachCoverAttachment(ctx context.Context, id uuid.UUID, generation int, attachmentID uuid.UUID) (bool, error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE import_publications
		SET cover_attachment_id = $3, updated_at = NOW()
		WHERE id = $1 AND generation = $2 AND cover_attachment_id IS NULL
	`, id, generation, attachmentID)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() > 0, nil
}

// MarkPublished completes the publication and releases the lease.
func (r *ImportPublicationRepository) MarkPublished(ctx context.Context, id uuid.UUID, generation int) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE import_publications
		SET status = 'published',
		    published_at = COALESCE(published_at, NOW()),
		    lease_owner = NULL,
		    leased_until = NULL,
		    last_error = NULL,
		    updated_at = NOW()
		WHERE id = $1 AND generation = $2
	`, id, generation)
	return err
}

// ReleaseLease returns a publication to the resumable pool after a failed or
// interrupted owner run.
func (r *ImportPublicationRepository) ReleaseLease(ctx context.Context, id uuid.UUID, generation int, cause string) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE import_publications
		SET status = 'pending',
		    lease_owner = NULL,
		    leased_until = NULL,
		    last_error = $3,
		    updated_at = NOW()
		WHERE id = $1 AND generation = $2 AND status = 'publishing'
	`, id, generation, cause)
	return err
}

// BumpGeneration starts a fresh publication for the same identity after the
// previous asset was deleted. The old snapshot and cover stage are abandoned
// and in-flight owners of the old generation are fenced off.
func (r *ImportPublicationRepository) BumpGeneration(ctx context.Context, id uuid.UUID, generation int, payload []byte, externalVersion string) (*domain.ImportPublication, bool, error) {
	row := r.pool.QueryRow(ctx, `
		UPDATE import_publications
		SET generation = generation + 1,
		    status = 'pending',
		    asset_id = NULL,
		    payload = $3,
		    external_version = $4,
		    cover_file_key = NULL,
		    cover_file_name = NULL,
		    cover_content_type = NULL,
		    cover_file_size = NULL,
		    cover_attachment_id = NULL,
		    lease_owner = NULL,
		    leased_until = NULL,
		    attempts = 0,
		    last_error = NULL,
		    published_at = NULL,
		    updated_at = NOW()
		WHERE id = $1 AND generation = $2
		RETURNING `+importPublicationColumns,
		id, generation, payload, externalVersion,
	)
	pub, err := scanImportPublication(row)
	if err != nil {
		return nil, false, err
	}
	return pub, pub != nil, nil
}

// ExpireStaleLeases reopens publications whose owner crashed or was restarted
// before releasing its lease.
func (r *ImportPublicationRepository) ExpireStaleLeases(ctx context.Context, now time.Time) (int64, error) {
	tag, err := r.pool.Exec(ctx, `
		UPDATE import_publications
		SET status = 'pending',
		    lease_owner = NULL,
		    leased_until = NULL,
		    last_error = COALESCE(last_error, 'owner lease expired before publication completed'),
		    updated_at = NOW()
		WHERE status = 'publishing' AND leased_until IS NOT NULL AND leased_until < $1
	`, now)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected(), nil
}

// ListRecoverable returns incomplete publications for an organization,
// oldest activity first.
func (r *ImportPublicationRepository) ListRecoverable(ctx context.Context, organizationID uuid.UUID, limit int) ([]domain.ImportPublication, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT `+importPublicationColumns+`
		FROM import_publications
		WHERE organization_id = $1 AND status = 'pending'
		ORDER BY updated_at ASC, id
		LIMIT $2
	`, organizationID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectImportPublications(rows)
}

// ListUnreferencedCoverObjects returns unfinished publications that uploaded
// a cover object without ever attaching it to a live core asset.
func (r *ImportPublicationRepository) ListUnreferencedCoverObjects(ctx context.Context, organizationID uuid.UUID) ([]domain.ImportPublication, error) {
	rows, err := r.pool.Query(ctx, `
		SELECT `+importPublicationColumns+`
		FROM import_publications
		WHERE organization_id = $1
		  AND status <> 'published'
		  AND asset_id IS NULL
		  AND cover_file_key IS NOT NULL
		ORDER BY updated_at ASC
	`, organizationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return collectImportPublications(rows)
}

// ClearCoverObject forgets a cover object after it has been removed from
// storage. Generation-guarded so a live owner never loses its tracked object.
func (r *ImportPublicationRepository) ClearCoverObject(ctx context.Context, id uuid.UUID, generation int) error {
	_, err := r.pool.Exec(ctx, `
		UPDATE import_publications
		SET cover_file_key = NULL,
		    cover_file_name = NULL,
		    cover_content_type = NULL,
		    cover_file_size = NULL,
		    cover_attachment_id = NULL,
		    updated_at = NOW()
		WHERE id = $1 AND generation = $2
	`, id, generation)
	return err
}

func collectImportPublications(rows pgx.Rows) ([]domain.ImportPublication, error) {
	publications := []domain.ImportPublication{}
	for rows.Next() {
		pub, err := scanImportPublication(rows)
		if err != nil {
			return nil, err
		}
		publications = append(publications, *pub)
	}
	return publications, rows.Err()
}
