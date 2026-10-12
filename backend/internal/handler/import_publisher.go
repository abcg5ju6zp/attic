package handler

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lmmendes/attic/internal/domain"
)

// ErrImportBusy is returned when another owner is mid-publication and did not
// finish within the wait window. The caller should suggest retrying.
var ErrImportBusy = errors.New("an import for this item is already in progress")

// ErrImportSuperseded is returned when the caller's generation was replaced by
// a newer delete-and-reimport before its writes could land.
var ErrImportSuperseded = errors.New("the import was superseded by a newer generation")

// ErrImportNeedsFetch is returned when an old asset was deleted and a new
// generation cannot start because the external Fetch was uncertain.
var ErrImportNeedsFetch = errors.New("a fresh external fetch is required to reimport this item")

// importPublicationStore is the durable state machine behind one import.
type importPublicationStore interface {
	GetByID(ctx context.Context, id uuid.UUID) (*domain.ImportPublication, error)
	GetByIdentity(ctx context.Context, organizationID uuid.UUID, pluginID, externalID string) (*domain.ImportPublication, error)
	CreateIfAbsent(ctx context.Context, pub *domain.ImportPublication) (bool, error)
	AcquireLease(ctx context.Context, id uuid.UUID, generation int, owner uuid.UUID, leasedUntil time.Time) (bool, error)
	AttachAsset(ctx context.Context, id uuid.UUID, generation int, assetID uuid.UUID) (bool, error)
	AttachCoverObject(ctx context.Context, id uuid.UUID, generation int, fileKey, fileName, contentType string, fileSize int64) (bool, error)
	AttachCoverAttachment(ctx context.Context, id uuid.UUID, generation int, attachmentID uuid.UUID) (bool, error)
	MarkPublished(ctx context.Context, id uuid.UUID, generation int) error
	ReleaseLease(ctx context.Context, id uuid.UUID, generation int, cause string) error
	BumpGeneration(ctx context.Context, id uuid.UUID, generation int, payload []byte, externalVersion string) (*domain.ImportPublication, bool, error)
	ExpireStaleLeases(ctx context.Context, now time.Time) (int64, error)
	ListRecoverable(ctx context.Context, organizationID uuid.UUID, limit int) ([]domain.ImportPublication, error)
	ListUnreferencedCoverObjects(ctx context.Context, organizationID uuid.UUID) ([]domain.ImportPublication, error)
	ClearCoverObject(ctx context.Context, id uuid.UUID, generation int) error
}

type importAssetStore interface {
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Asset, error)
	GetByIDFull(ctx context.Context, id uuid.UUID) (*domain.Asset, error)
	Create(ctx context.Context, asset *domain.Asset) error
	SetMainAttachmentIfAbsent(ctx context.Context, assetID, attachmentID uuid.UUID) (bool, error)
}

type importAttachmentStore interface {
	GetByID(ctx context.Context, id uuid.UUID) (*domain.Attachment, error)
	GetByAssetAndFileKey(ctx context.Context, assetID uuid.UUID, fileKey string) (*domain.Attachment, error)
	CreateOrGet(ctx context.Context, attachment *domain.Attachment) (bool, error)
}

// importResult is the single outcome of a successful publish request: the
// same asset is returned whether it was created now, returned, or recovered.
type importResult struct {
	asset   *domain.Asset
	created bool
}

const (
	importLeaseTTL     = 2 * time.Minute
	importAwaitTimeout = 10 * time.Second
	importAwaitTick    = 200 * time.Millisecond
	importImageLimit   = 10 * 1024 * 1024
)

// keyedMutex serializes work sharing a stable string identity inside one
// process. The deployment runs a single backend; the DB lease additionally
// covers crashes and restarts.
type keyedMutex struct {
	mu    sync.Mutex
	locks map[string]*keyLock
}

type keyLock struct {
	mu   sync.Mutex
	refs int
}

func newKeyedMutex() *keyedMutex {
	return &keyedMutex{locks: make(map[string]*keyLock)}
}

// Lock takes the per-key lock and returns its release function.
func (m *keyedMutex) Lock(key string) func() {
	m.mu.Lock()
	lock := m.locks[key]
	if lock == nil {
		lock = &keyLock{}
		m.locks[key] = lock
	}
	lock.refs++
	m.mu.Unlock()

	lock.mu.Lock()
	return func() {
		lock.mu.Unlock()
		m.mu.Lock()
		lock.refs--
		if lock.refs == 0 {
			delete(m.locks, key)
		}
		m.mu.Unlock()
	}
}

type importPublisher struct {
	orgID        uuid.UUID
	publications importPublicationStore
	assets       importAssetStore
	attachments  importAttachmentStore
	categories   pluginCategoryRepository
	attributes   pluginAttributeRepository
	storage      FileStorage
	locks        *keyedMutex
	now          func() time.Time
}

func newImportPublisher(
	orgID uuid.UUID,
	publications importPublicationStore,
	assets importAssetStore,
	attachments importAttachmentStore,
	categories pluginCategoryRepository,
	attributes pluginAttributeRepository,
	storage FileStorage,
) *importPublisher {
	return &importPublisher{
		orgID:        orgID,
		publications: publications,
		assets:       assets,
		attachments:  attachments,
		categories:   categories,
		attributes:   attributes,
		storage:      storage,
		locks:        newKeyedMutex(),
		now:          time.Now,
	}
}

func identityKey(pluginID, externalID string) string {
	return pluginID + "\x00" + externalID
}

// pinSnapshot freezes the external response for one publication generation.
// Recovery always reads this snapshot, never a later Fetch.
func pinSnapshot(requestedExternalID string, data *domain.ImportData) ([]byte, string, error) {
	snapshot := *data
	if snapshot.ExternalID == "" {
		snapshot.ExternalID = requestedExternalID
	}
	if snapshot.Attributes == nil {
		snapshot.Attributes = map[string]any{}
	}
	payload, err := json.Marshal(snapshot)
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(payload)
	return payload, hex.EncodeToString(sum[:]), nil
}

// publish is the entry point for an import HTTP request. fetched is nil when
// the external Fetch failed but an existing identity should be resumed.
func (p *importPublisher) publish(
	ctx context.Context,
	pl domain.ImportPlugin,
	externalID string,
	fetched *domain.ImportData,
	features *domain.OrganizationFeatures,
) (*importResult, error) {
	release := p.locks.Lock(identityKey(pl.ID(), externalID))
	defer release()

	pub, err := p.publications.GetByIdentity(ctx, p.orgID, pl.ID(), externalID)
	if err != nil {
		return nil, err
	}
	if pub == nil {
		if fetched == nil {
			return nil, ErrImportNeedsFetch
		}
		payload, version, err := pinSnapshot(externalID, fetched)
		if err != nil {
			return nil, err
		}
		candidate := &domain.ImportPublication{
			OrganizationID:  p.orgID,
			PluginID:        pl.ID(),
			ExternalID:      externalID,
			Payload:         payload,
			ExternalVersion: version,
		}
		created, err := p.publications.CreateIfAbsent(ctx, candidate)
		if err != nil {
			return nil, err
		}
		if !created {
			// A concurrent request won the identity race; adopt its row.
			pub, err = p.publications.GetByIdentity(ctx, p.orgID, pl.ID(), externalID)
			if err != nil {
				return nil, err
			}
			if pub == nil {
				return nil, errors.New("import publication vanished during creation")
			}
		} else {
			pub = candidate
		}
	}

	return p.adopt(ctx, pl, pub, fetched, features)
}

// adopt decides what an existing publication means for this request: a
// completed result, a resumable publication, or a brand-new generation after
// the previous asset was deleted.
func (p *importPublisher) adopt(
	ctx context.Context,
	pl domain.ImportPlugin,
	pub *domain.ImportPublication,
	fetched *domain.ImportData,
	features *domain.OrganizationFeatures,
) (*importResult, error) {
	if pub.AssetID != nil {
		asset, err := p.assets.GetByIDFull(ctx, *pub.AssetID)
		if err != nil {
			return nil, err
		}
		if asset != nil {
			// The core asset of this generation is live: return the same
			// result, or finish whatever stage the interrupted owner missed.
			if pub.Status == domain.ImportPublicationPublished {
				return &importResult{asset: asset, created: false}, nil
			}
			return p.runPublication(ctx, pl, pub, asset, false, features)
		}
	}

	// The previous asset no longer exists. Without fresh external data we
	// must not resurrect a deleted asset; leave the row for a later reimport.
	if pub.AssetID != nil {
		if fetched == nil {
			return nil, ErrImportNeedsFetch
		}
		p.abandonCoverObject(ctx, pub)
		payload, version, err := pinSnapshot(pub.ExternalID, fetched)
		if err != nil {
			return nil, err
		}
		next, ok, err := p.publications.BumpGeneration(ctx, pub.ID, pub.Generation, payload, version)
		if err != nil {
			return nil, err
		}
		if !ok {
			// Another request bumped first; reload and follow that generation.
			latest, err := p.publications.GetByID(ctx, pub.ID)
			if err != nil {
				return nil, err
			}
			if latest == nil {
				return nil, ErrImportSuperseded
			}
			return p.adopt(ctx, pl, latest, nil, features)
		}
		pub = next
	}

	// No core asset yet: continue this generation from its pinned snapshot.
	return p.runPublication(ctx, pl, pub, nil, true, features)
}

// runPublication leases the publication and executes its remaining stages.
func (p *importPublisher) runPublication(
	ctx context.Context,
	pl domain.ImportPlugin,
	pub *domain.ImportPublication,
	existing *domain.Asset,
	createsAsset bool,
	features *domain.OrganizationFeatures,
) (*importResult, error) {
	generation := pub.Generation
	owner := uuid.New()
	acquired, err := p.publications.AcquireLease(ctx, pub.ID, generation, owner, p.now().Add(importLeaseTTL))
	if err != nil {
		return nil, err
	}
	if !acquired {
		return p.awaitOtherOwner(ctx, pl, pub, features)
	}

	completed := false
	released := false
	release := func(cause string) {
		if completed || released {
			return
		}
		released = true
		releaseCtx := context.WithoutCancel(ctx)
		if err := p.publications.ReleaseLease(releaseCtx, pub.ID, generation, cause); err != nil {
			slog.Error("failed to release import publication lease",
				"plugin_id", pl.ID(), "external_id", pub.ExternalID, "generation", generation, "error", err)
		}
	}
	defer func() {
		if !completed {
			release("request ended before the publication completed")
		}
	}()

	var snapshot domain.ImportData
	if err := json.Unmarshal(pub.Payload, &snapshot); err != nil {
		return nil, fmt.Errorf("reading pinned import payload: %w", err)
	}

	// Stage 1: category and plugin attributes are prepared idempotently.
	var category *domain.Category
	if features.Categories {
		category, _, err = ensurePluginCategory(ctx, p.categories, p.attributes, p.orgID, pl)
		if err != nil {
			return nil, err
		}
	}

	// Stage 2: the core asset is created at most once per generation.
	asset := existing
	if asset == nil && pub.AssetID != nil {
		asset, err = p.assets.GetByIDFull(ctx, *pub.AssetID)
		if err != nil {
			return nil, err
		}
		if asset == nil {
			// The asset was deleted between adoption and this run. Do not
			// resurrect it from the old snapshot: re-adopt, which either bumps
			// to a new generation with fresh data or refuses (needs fetch).
			return p.adoptAfterReload(ctx, pl, pub, features)
		}
	}
	if asset == nil {
		importAttributes := map[string]any{}
		if features.Attributes {
			importAttributes = namespacedPluginAttributes(pl.ID(), snapshot.Attributes)
		}
		attrsJSON, err := json.Marshal(importAttributes)
		if err != nil {
			return nil, err
		}
		asset = &domain.Asset{
			OrganizationID:   p.orgID,
			Name:             snapshot.Name,
			Description:      snapshot.Description,
			Quantity:         1,
			Attributes:       attrsJSON,
			ImportPluginID:   &pub.PluginID,
			ImportExternalID: &snapshot.ExternalID,
		}
		if category != nil {
			asset.CategoryID = &category.ID
		}
		if err := p.assets.Create(ctx, asset); err != nil {
			return nil, err
		}
		attached, err := p.publications.AttachAsset(ctx, pub.ID, generation, asset.ID)
		if err != nil {
			return nil, err
		}
		if !attached {
			// Generation fenced the write: a delete-and-reimport won.
			latest, reloadErr := p.publications.GetByID(ctx, pub.ID)
			if reloadErr == nil && latest != nil && latest.Generation != generation {
				return nil, ErrImportSuperseded
			}
			// Asset exists but is tracked elsewhere; reload and re-adopt.
			return p.adoptAfterReload(ctx, pl, pub, features)
		}
		if category != nil {
			asset.Category = category
		}
	}

	// Stage 3: the cover (object -> attachment -> main image) is published as
	// its own recoverable unit. A cover failure never invalidates the unique
	// core asset; the publication stays pending for retry/restart recovery.
	imageURL := ""
	if snapshot.ImageURL != nil {
		imageURL = *snapshot.ImageURL
	}
	if imageURL != "" && p.storage != nil {
		if err := p.ensureCover(ctx, pub, generation, asset, imageURL); err != nil {
			if errors.Is(err, ErrImportSuperseded) {
				return nil, err
			}
			slog.Warn("import cover stage incomplete; publication remains recoverable",
				"plugin_id", pl.ID(), "external_id", pub.ExternalID,
				"generation", generation, "asset_id", asset.ID, "error", err)
			release(fmt.Sprintf("cover stage incomplete: %v", err))
			return &importResult{asset: asset, created: createsAsset}, nil
		}
	}

	// Stage 4: the publication is complete.
	if err := p.publications.MarkPublished(ctx, pub.ID, generation); err != nil {
		return nil, err
	}
	completed = true
	return &importResult{asset: asset, created: createsAsset}, nil
}

func (p *importPublisher) adoptAfterReload(ctx context.Context, pl domain.ImportPlugin, pub *domain.ImportPublication, features *domain.OrganizationFeatures) (*importResult, error) {
	latest, err := p.publications.GetByID(ctx, pub.ID)
	if err != nil {
		return nil, err
	}
	if latest == nil {
		return nil, ErrImportSuperseded
	}
	return p.adopt(ctx, pl, latest, nil, features)
}

// awaitOtherOwner waits briefly for a concurrent owner in another process and
// either returns its result or takes over an expired lease.
func (p *importPublisher) awaitOtherOwner(
	ctx context.Context,
	pl domain.ImportPlugin,
	pub *domain.ImportPublication,
	features *domain.OrganizationFeatures,
) (*importResult, error) {
	deadline := p.now().Add(importAwaitTimeout)
	ticker := time.NewTicker(importAwaitTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ticker.C:
		}
		latest, err := p.publications.GetByID(ctx, pub.ID)
		if err != nil {
			return nil, err
		}
		if latest == nil || latest.Generation != pub.Generation {
			return nil, ErrImportSuperseded
		}
		if latest.Status == domain.ImportPublicationPublished && latest.AssetID != nil {
			asset, err := p.assets.GetByIDFull(ctx, *latest.AssetID)
			if err != nil {
				return nil, err
			}
			if asset != nil {
				return &importResult{asset: asset, created: false}, nil
			}
		}
		if latest.Status == domain.ImportPublicationPending ||
			(latest.LeasedUntil != nil && latest.LeasedUntil.Before(p.now())) {
			return p.runPublication(ctx, pl, latest, nil, latest.AssetID == nil, features)
		}
		if !p.now().Before(deadline) {
			if latest.AssetID != nil {
				asset, err := p.assets.GetByIDFull(ctx, *latest.AssetID)
				if err == nil && asset != nil {
					return &importResult{asset: asset, created: false}, nil
				}
			}
			return nil, ErrImportBusy
		}
	}
}

// ensureCover publishes exactly one cover attachment for the generation,
// reusing any stage that already succeeded. It never overwrites a main image
// that the user or a newer generation chose later.
func (p *importPublisher) ensureCover(ctx context.Context, pub *domain.ImportPublication, generation int, asset *domain.Asset, imageURL string) error {
	attachment, err := p.existingCoverAttachment(ctx, pub, asset.ID)
	if err != nil {
		return err
	}

	if attachment == nil && pub.CoverFileKey != nil {
		// Object was uploaded previously; the Attachment row may still exist.
		attachment, err = p.attachments.GetByAssetAndFileKey(ctx, asset.ID, *pub.CoverFileKey)
		if err != nil {
			return err
		}
		if attachment == nil && pub.CoverFileName != nil && pub.CoverContentType != nil && pub.CoverFileSize != nil {
			// Only the record is missing: recreate it without re-downloading.
			description := "Imported cover image"
			attachment = &domain.Attachment{
				AssetID:     asset.ID,
				FileKey:     *pub.CoverFileKey,
				FileName:    *pub.CoverFileName,
				FileSize:    *pub.CoverFileSize,
				ContentType: pub.CoverContentType,
				Description: &description,
			}
			if _, err := p.attachments.CreateOrGet(ctx, attachment); err != nil {
				return fmt.Errorf("recreating cover attachment record: %w", err)
			}
		}
	}

	if attachment == nil {
		imageData, fileName, contentType, err := downloadImportImage(ctx, imageURL)
		if err != nil {
			return err
		}
		key, err := p.storage.Upload(ctx, fileName, contentType, bytes.NewReader(imageData))
		if err != nil {
			return fmt.Errorf("uploading cover to storage: %w", err)
		}
		tracked, err := p.publications.AttachCoverObject(ctx, pub.ID, generation, key, fileName, contentType, int64(len(imageData)))
		if err != nil {
			return err
		}
		if !tracked {
			// An earlier attempt already recorded an object: use that one and
			// discard the duplicate just uploaded.
			if err := p.storage.Delete(context.WithoutCancel(ctx), key); err != nil {
				slog.Warn("failed to delete duplicate cover object", "key", key, "error", err)
			}
			latest, err := p.publications.GetByID(ctx, pub.ID)
			if err != nil {
				return err
			}
			if latest == nil || latest.Generation != generation {
				return ErrImportSuperseded
			}
			return p.ensureCover(ctx, latest, generation, asset, imageURL)
		}
		description := "Imported cover image"
		attachment = &domain.Attachment{
			AssetID:     asset.ID,
			FileKey:     key,
			FileName:    fileName,
			FileSize:    int64(len(imageData)),
			ContentType: &contentType,
			Description: &description,
		}
		if _, err := p.attachments.CreateOrGet(ctx, attachment); err != nil {
			return fmt.Errorf("creating cover attachment record: %w", err)
		}
	}

	linked, err := p.publications.AttachCoverAttachment(ctx, pub.ID, generation, attachment.ID)
	if err != nil {
		return err
	}
	if !linked {
		latest, err := p.publications.GetByID(ctx, pub.ID)
		if err != nil {
			return err
		}
		if latest == nil || latest.Generation != generation {
			return ErrImportSuperseded
		}
	}

	if attachment.ContentType != nil && isImageContentType(*attachment.ContentType) {
		if _, err := p.assets.SetMainAttachmentIfAbsent(ctx, asset.ID, attachment.ID); err != nil {
			return fmt.Errorf("publishing main image: %w", err)
		}
	}
	return nil
}

func (p *importPublisher) existingCoverAttachment(ctx context.Context, pub *domain.ImportPublication, assetID uuid.UUID) (*domain.Attachment, error) {
	if pub.CoverAttachmentID == nil {
		return nil, nil
	}
	attachment, err := p.attachments.GetByID(ctx, *pub.CoverAttachmentID)
	if err != nil {
		return nil, err
	}
	if attachment == nil || attachment.AssetID != assetID {
		return nil, nil
	}
	return attachment, nil
}

// abandonCoverObject removes an old generation's tracked cover object when it
// was never referenced by an attachment (object uploaded, record never built).
func (p *importPublisher) abandonCoverObject(ctx context.Context, pub *domain.ImportPublication) {
	if p.storage == nil || pub.CoverFileKey == nil || pub.CoverAttachmentID != nil {
		return
	}
	referenced := false
	if pub.AssetID != nil {
		att, err := p.attachments.GetByAssetAndFileKey(ctx, *pub.AssetID, *pub.CoverFileKey)
		if err == nil && att != nil {
			referenced = true
		}
	}
	if !referenced {
		if err := p.storage.Delete(context.WithoutCancel(ctx), *pub.CoverFileKey); err != nil {
			slog.Warn("failed to delete unreferenced cover object during generation bump",
				"key", *pub.CoverFileKey, "error", err)
		}
	}
}

// RecoverPending resumes interrupted imports after a restart and removes
// cover objects that were uploaded for a publication which never produced a
// core asset. lookups returns the plugin for an id; missing/disabled plugins
// are skipped (they stay pending for the next restart/retry).
func (p *importPublisher) RecoverPending(ctx context.Context, lookup func(pluginID string) domain.ImportPlugin, features *domain.OrganizationFeatures) {
	expired, err := p.publications.ExpireStaleLeases(ctx, p.now())
	if err != nil {
		slog.Error("failed to expire stale import leases", "error", err)
	}
	if expired > 0 {
		slog.Info("reopened interrupted plugin imports", "count", expired)
	}

	orphans, err := p.publications.ListUnreferencedCoverObjects(ctx, p.orgID)
	if err != nil {
		slog.Error("failed to list unreferenced import cover objects", "error", err)
	}
	for _, pub := range orphans {
		key := ""
		if pub.CoverFileKey != nil {
			key = *pub.CoverFileKey
		}
		if p.storage != nil && key != "" {
			if err := p.storage.Delete(ctx, key); err != nil {
				slog.Warn("failed to delete orphaned import cover object", "key", key, "error", err)
				continue
			}
		}
		if err := p.publications.ClearCoverObject(ctx, pub.ID, pub.Generation); err != nil {
			slog.Warn("failed to clear orphaned cover tracking", "import_publication_id", pub.ID, "error", err)
		}
	}

	pending, err := p.publications.ListRecoverable(ctx, p.orgID, 200)
	if err != nil {
		slog.Error("failed to list recoverable plugin imports", "error", err)
		return
	}
	for _, pub := range pending {
		pl := lookup(pub.PluginID)
		if pl == nil || !pl.Enabled() {
			continue
		}
		release := p.locks.Lock(identityKey(pub.PluginID, pub.ExternalID))
		func(pub domain.ImportPublication) {
			defer release()
			if _, err := p.adopt(ctx, pl, &pub, nil, features); err != nil {
				if errors.Is(err, ErrImportNeedsFetch) {
					return
				}
				slog.Warn("pending plugin import remains unrecovered",
					"plugin_id", pub.PluginID, "external_id", pub.ExternalID,
					"generation", pub.Generation, "error", err)
			}
		}(pub)
	}
}

// downloadImportImage fetches and validates the cover image, returning its
// bytes, storage filename and detected content type.
func downloadImportImage(ctx context.Context, imageURL string) ([]byte, string, string, error) {
	client := &http.Client{Timeout: 30 * time.Second}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, imageURL, nil)
	if err != nil {
		return nil, "", "", fmt.Errorf("creating image request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", "", fmt.Errorf("downloading image: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, "", "", fmt.Errorf("image download returned status %d", resp.StatusCode)
	}

	limitedReader := io.LimitReader(resp.Body, importImageLimit)
	imageData, err := io.ReadAll(limitedReader)
	if err != nil {
		return nil, "", "", fmt.Errorf("reading image data: %w", err)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType == "" || !strings.HasPrefix(contentType, "image/") {
		contentType = http.DetectContentType(imageData)
	}
	if !strings.HasPrefix(contentType, "image/") {
		return nil, "", "", fmt.Errorf("invalid image content type: %s", contentType)
	}

	filename := "cover"
	if urlPath := path.Base(imageURL); urlPath != "" && urlPath != "/" && urlPath != "." {
		if idx := strings.Index(urlPath, "?"); idx > 0 {
			urlPath = urlPath[:idx]
		}
		if urlPath != "" {
			filename = urlPath
		}
	}
	ext := imageExtension(contentType)
	if ext != "" && !strings.Contains(filename, ".") {
		filename += ext
	}
	return imageData, filename, contentType, nil
}

func imageExtension(contentType string) string {
	switch contentType {
	case "image/jpeg":
		return ".jpg"
	case "image/png":
		return ".png"
	case "image/gif":
		return ".gif"
	case "image/webp":
		return ".webp"
	default:
		return ""
	}
}
