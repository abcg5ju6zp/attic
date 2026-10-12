package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"image"
	"image/color"
	"image/png"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/lmmendes/attic/internal/domain"
	"github.com/lmmendes/attic/internal/plugin"
	"github.com/lmmendes/attic/internal/repository"
	"github.com/lmmendes/attic/internal/storage"
	"github.com/lmmendes/attic/internal/testutil"
)

// flakyFileStorage wraps local storage and can fail uploads to simulate an
// interrupted cover stage.
type flakyFileStorage struct {
	inner       FileStorage
	failUploads atomic.Bool
}

func (f *flakyFileStorage) Upload(ctx context.Context, filename, contentType string, body io.Reader) (string, error) {
	if f.failUploads.Load() {
		return "", errors.New("simulated storage outage")
	}
	return f.inner.Upload(ctx, filename, contentType, body)
}

func (f *flakyFileStorage) GetPresignedURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	return f.inner.GetPresignedURL(ctx, key, expiry)
}

func (f *flakyFileStorage) Delete(ctx context.Context, key string) error {
	return f.inner.Delete(ctx, key)
}

type disabledMockPlugin struct {
	*mockPlugin
}

func (d *disabledMockPlugin) Enabled() bool { return false }
func (d *disabledMockPlugin) DisabledReason() string {
	return "simulated disabled plugin"
}

type importHTTPEnv struct {
	pool     *pgxpool.Pool
	repos    *Repositories
	local    *storage.LocalStorage
	store    *flakyFileStorage
	dir      string
	imageURL string
	pngBytes []byte
}

func newImportHTTPEnv(t *testing.T, ctx context.Context) *importHTTPEnv {
	t.Helper()
	db, err := testutil.NewTestDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(ctx) })

	img := image.NewRGBA(image.Rect(0, 0, 1, 1))
	img.Set(0, 0, color.RGBA{R: 1, G: 2, B: 3, A: 255})
	var imageBuffer bytes.Buffer
	if err := png.Encode(&imageBuffer, img); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(imageBuffer.Bytes())
	}))
	t.Cleanup(server.Close)

	dir := t.TempDir()
	local, err := storage.NewLocalStorage(storage.LocalConfig{BasePath: dir, BaseURL: "http://example.test/files"})
	if err != nil {
		t.Fatal(err)
	}
	flaky := &flakyFileStorage{inner: local}

	return &importHTTPEnv{
		pool:     db.Pool,
		dir:      dir,
		local:    local,
		store:    flaky,
		imageURL: server.URL + "/cover.png",
		pngBytes: imageBuffer.Bytes(),
		repos: &Repositories{
			Categories:   repository.NewCategoryRepository(db.Pool),
			Attributes:   repository.NewAttributeRepository(db.Pool),
			Assets:       repository.NewAssetRepository(db.Pool),
			Attachments:  repository.NewAttachmentRepository(db.Pool),
			Publications: repository.NewImportPublicationRepository(db.Pool),
		},
	}
}

func (e *importHTTPEnv) createOrg(ctx context.Context, t *testing.T, name string) uuid.UUID {
	t.Helper()
	org := &domain.Organization{ID: uuid.New(), Name: name}
	if _, err := e.pool.Exec(ctx,
		`INSERT INTO organizations (id, name, created_at, updated_at) VALUES ($1, $2, NOW(), NOW())`,
		org.ID, org.Name); err != nil {
		t.Fatal(err)
	}
	return org.ID
}

func (e *importHTTPEnv) newHandler(t *testing.T, orgID uuid.UUID, plugins ...domain.ImportPlugin) *PluginHandler {
	t.Helper()
	registry := plugin.NewRegistry()
	for _, p := range plugins {
		if err := registry.Register(p); err != nil {
			t.Fatal(err)
		}
	}
	return NewPluginHandler(registry, e.repos, e.store, orgID)
}

func importRequest(t *testing.T, h *PluginHandler, pluginID, externalID string, features *domain.OrganizationFeatures) *httptest.ResponseRecorder {
	t.Helper()
	body, _ := json.Marshal(ImportRequest{ExternalID: externalID})
	req := httptest.NewRequest(http.MethodPost, "/api/plugins/"+pluginID+"/import", bytes.NewReader(body))
	req = withPluginChiURLParam(req, "pluginId", pluginID)
	if features != nil {
		req = req.WithContext(context.WithValue(req.Context(), organizationFeaturesContextKey{}, features))
	}
	rec := httptest.NewRecorder()
	h.Import(rec, req)
	return rec
}

func decodeImportResponse(t *testing.T, rec *httptest.ResponseRecorder) ImportResponse {
	t.Helper()
	var response ImportResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decoding %q: %v", rec.Body.String(), err)
	}
	return response
}

func (e *importHTTPEnv) countLiveAssets(ctx context.Context, t *testing.T, orgID uuid.UUID, pluginID, externalID string) int {
	t.Helper()
	var count int
	if err := e.pool.QueryRow(ctx, `
		SELECT count(*) FROM assets
		WHERE organization_id=$1 AND import_plugin_id=$2 AND import_external_id=$3 AND deleted_at IS NULL
	`, orgID, pluginID, externalID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func (e *importHTTPEnv) countRows(ctx context.Context, t *testing.T, query string, args ...any) int {
	t.Helper()
	var count int
	if err := e.pool.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func (e *importHTTPEnv) countAttachments(ctx context.Context, t *testing.T, assetID uuid.UUID) int {
	return e.countRows(ctx, t, `SELECT count(*) FROM attachments WHERE asset_id=$1`, assetID)
}

func (e *importHTTPEnv) mainAttachment(ctx context.Context, t *testing.T, assetID uuid.UUID) *uuid.UUID {
	t.Helper()
	var mainID *uuid.UUID
	if err := e.pool.QueryRow(ctx,
		`SELECT main_attachment_id FROM assets WHERE id=$1`, assetID).Scan(&mainID); err != nil {
		t.Fatal(err)
	}
	return mainID
}

func (e *importHTTPEnv) publication(ctx context.Context, t *testing.T, orgID uuid.UUID, pluginID, externalID string) *domain.ImportPublication {
	t.Helper()
	pub, err := e.repos.Publications.GetByIdentity(ctx, orgID, pluginID, externalID)
	if err != nil {
		t.Fatal(err)
	}
	if pub == nil {
		t.Fatalf("expected publication for %s/%s", pluginID, externalID)
	}
	return pub
}

func (e *importHTTPEnv) countFiles(t *testing.T) int {
	t.Helper()
	files := 0
	err := filepath.WalkDir(e.dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files++
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return files
}

func imagePlugin(id, name string, data *domain.ImportData) *mockPlugin {
	return &mockPlugin{
		id:           id,
		name:         name,
		categoryName: name + " Category",
		categoryDesc: "integration test category",
		attributes: []domain.PluginAttribute{
			{Key: "books.author", Name: "Author", DataType: domain.AttributeTypeString},
		},
		fetchData: data,
	}
}

// TestPluginImportIdempotentPublication exercises the recoverable import
// publication end to end against a real database and local file storage.
func TestPluginImportIdempotentPublication(t *testing.T) {
	ctx := context.Background()
	env := newImportHTTPEnv(t, ctx)
	imageURL := env.imageURL

	t.Run("first import publishes one asset with cover and main image", func(t *testing.T) {
		orgID := env.createOrg(ctx, t, "First import")
		desc := "A book"
		pluginID := "books-first"
		externalID := "book-1"
		data := &domain.ImportData{
			ExternalID:  externalID,
			Name:        "Original Book",
			Description: &desc,
			ImageURL:    &imageURL,
			Attributes:  map[string]any{"books.author": "Ada"},
		}
		h := env.newHandler(t, orgID, imagePlugin(pluginID, "Books", data))

		rec := importRequest(t, h, pluginID, externalID, nil)
		if rec.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
		}
		asset := decodeImportResponse(t, rec).Asset
		if asset == nil || asset.Name != "Original Book" || asset.CategoryID == nil {
			t.Fatalf("unexpected asset: %#v", asset)
		}
		var attrs map[string]any
		if err := json.Unmarshal(asset.Attributes, &attrs); err != nil {
			t.Fatal(err)
		}
		if attrs["plugin."+pluginID+".books.author"] != "Ada" {
			t.Fatalf("namespaced attribute missing: %#v", attrs)
		}

		if got := env.countLiveAssets(ctx, t, orgID, pluginID, externalID); got != 1 {
			t.Fatalf("expected one live asset, got %d", got)
		}
		if got := env.countAttachments(ctx, t, asset.ID); got != 1 {
			t.Fatalf("expected one cover attachment, got %d", got)
		}
		if main := env.mainAttachment(ctx, t, asset.ID); main == nil || *main == uuid.Nil {
			t.Fatal("expected the cover to be published as main image")
		}
		if got := env.countRows(ctx, t,
			`SELECT count(*) FROM categories WHERE organization_id=$1 AND plugin_id=$2 AND deleted_at IS NULL`,
			orgID, pluginID); got != 1 {
			t.Fatalf("expected one plugin category, got %d", got)
		}
		pub := env.publication(ctx, t, orgID, pluginID, externalID)
		if pub.Status != domain.ImportPublicationPublished || pub.ExternalVersion == "" || pub.CoverAttachmentID == nil {
			t.Fatalf("publication not complete: %#v", pub)
		}
		if env.countFiles(t) != 1 {
			t.Fatalf("expected exactly one stored object, got %d", env.countFiles(t))
		}
	})

	t.Run("repeated clicks and uncertain fetches return the same pinned asset", func(t *testing.T) {
		orgID := env.createOrg(ctx, t, "Repeated import")
		pluginID := "books-repeat"
		externalID := "book-2"
		data := &domain.ImportData{
			ExternalID: externalID,
			Name:       "Pinned Book",
			ImageURL:   &imageURL,
			Attributes: map[string]any{"books.author": "Original"},
		}
		p := imagePlugin(pluginID, "Repeat Books", data)
		h := env.newHandler(t, orgID, p)

		first := importRequest(t, h, pluginID, externalID, nil)
		if first.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d: %s", first.Code, first.Body.String())
		}
		assetID := decodeImportResponse(t, first).Asset.ID

		// A later Fetch returns different fields; they must not enter the
		// existing publication.
		data.Name = "Mutated Book"
		second := importRequest(t, h, pluginID, externalID, nil)
		if second.Code != http.StatusOK {
			t.Fatalf("expected 200 on repeat, got %d: %s", second.Code, second.Body.String())
		}
		if decodeImportResponse(t, second).Asset.ID != assetID {
			t.Fatal("repeat import returned a different asset")
		}

		// Uncertain external fetch: resume from the pinned snapshot.
		p.fetchErr = errors.New("upstream timeout")
		third := importRequest(t, h, pluginID, externalID, nil)
		if third.Code != http.StatusOK {
			t.Fatalf("expected 200 while resuming after fetch failure, got %d: %s", third.Code, third.Body.String())
		}
		if decodeImportResponse(t, third).Asset.ID != assetID {
			t.Fatal("uncertain fetch must resume the original asset")
		}
		p.fetchErr = nil

		if got := env.countLiveAssets(ctx, t, orgID, pluginID, externalID); got != 1 {
			t.Fatalf("expected one live asset, got %d", got)
		}
		if got := env.countFiles(t); got < 1 {
			t.Fatal("expected stored objects to be reused")
		}
		latest, err := env.repos.Assets.GetByID(ctx, assetID)
		if err != nil || latest == nil {
			t.Fatal(err)
		}
		if latest.Name != "Pinned Book" {
			t.Fatalf("later fetch leaked into old publication: %q", latest.Name)
		}
	})

	t.Run("concurrent requests publish a single asset and category", func(t *testing.T) {
		orgID := env.createOrg(ctx, t, "Concurrent import")
		pluginID := "books-concurrent"
		externalID := "book-3"
		data := &domain.ImportData{ExternalID: externalID, Name: "Race Book", ImageURL: &imageURL}
		h := env.newHandler(t, orgID, imagePlugin(pluginID, "Race Books", data))

		const n = 10
		start := make(chan struct{})
		var wg sync.WaitGroup
		ids := make([]uuid.UUID, n)
		codes := make([]int, n)
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				<-start
				rec := importRequest(t, h, pluginID, externalID, nil)
				codes[i] = rec.Code
				if rec.Code == http.StatusCreated || rec.Code == http.StatusOK {
					ids[i] = decodeImportResponse(t, rec).Asset.ID
				}
			}(i)
		}
		close(start)
		wg.Wait()

		var created, okCount int
		var first uuid.UUID
		for i, code := range codes {
			if code != http.StatusCreated && code != http.StatusOK {
				t.Fatalf("request %d got unexpected status %d", i, code)
			}
			if code == http.StatusCreated {
				created++
			} else {
				okCount++
			}
			if first == uuid.Nil {
				first = ids[i]
			} else if ids[i] != first {
				t.Fatalf("concurrent imports diverged: %s vs %s", first, ids[i])
			}
		}
		if created != 1 || okCount != n-1 {
			t.Fatalf("expected exactly one 201 and %d 200, got created=%d ok=%d", n-1, created, okCount)
		}
		if got := env.countLiveAssets(ctx, t, orgID, pluginID, externalID); got != 1 {
			t.Fatalf("expected one live asset, got %d", got)
		}
		if got := env.countAttachments(ctx, t, first); got != 1 {
			t.Fatalf("expected one cover, got %d", got)
		}
		if got := env.countRows(ctx, t,
			`SELECT count(*) FROM categories WHERE organization_id=$1 AND plugin_id=$2 AND deleted_at IS NULL`,
			orgID, pluginID); got != 1 {
			t.Fatalf("expected one category under contention, got %d", got)
		}
	})

	t.Run("cover failure leaves a recoverable publication, retry finishes only the cover", func(t *testing.T) {
		orgID := env.createOrg(ctx, t, "Cover failure")
		pluginID := "books-flaky"
		externalID := "book-4"
		data := &domain.ImportData{ExternalID: externalID, Name: "Flaky Cover Book", ImageURL: &imageURL}
		h := env.newHandler(t, orgID, imagePlugin(pluginID, "Flaky Books", data))

		env.store.failUploads.Store(true)
		first := importRequest(t, h, pluginID, externalID, nil)
		env.store.failUploads.Store(false)
		if first.Code != http.StatusCreated {
			t.Fatalf("expected 201 with pending cover, got %d: %s", first.Code, first.Body.String())
		}
		assetID := decodeImportResponse(t, first).Asset.ID
		if got := env.countAttachments(ctx, t, assetID); got != 0 {
			t.Fatalf("expected no attachment yet, got %d", got)
		}
		if main := env.mainAttachment(ctx, t, assetID); main != nil {
			t.Fatal("expected no main image yet")
		}
		pub := env.publication(ctx, t, orgID, pluginID, externalID)
		if pub.Status != domain.ImportPublicationPending {
			t.Fatalf("expected pending publication, got %s", pub.Status)
		}

		second := importRequest(t, h, pluginID, externalID, nil)
		if second.Code != http.StatusOK {
			t.Fatalf("expected 200 on recovery, got %d: %s", second.Code, second.Body.String())
		}
		if decodeImportResponse(t, second).Asset.ID != assetID {
			t.Fatal("recovery must not create a second asset")
		}
		if got := env.countLiveAssets(ctx, t, orgID, pluginID, externalID); got != 1 {
			t.Fatalf("expected one live asset, got %d", got)
		}
		if got := env.countAttachments(ctx, t, assetID); got != 1 {
			t.Fatalf("expected recovered cover, got %d", got)
		}
		if main := env.mainAttachment(ctx, t, assetID); main == nil {
			t.Fatal("expected recovered main image")
		}
		if env.publication(ctx, t, orgID, pluginID, externalID).Status != domain.ImportPublicationPublished {
			t.Fatal("publication should be published after cover recovery")
		}
	})

	t.Run("restart resumes a pending cover without rebuilding the asset", func(t *testing.T) {
		orgID := env.createOrg(ctx, t, "Restart recovery")
		pluginID := "books-restart"
		externalID := "book-5"
		data := &domain.ImportData{ExternalID: externalID, Name: "Restart Book", ImageURL: &imageURL}
		p := imagePlugin(pluginID, "Restart Books", data)
		firstHandler := env.newHandler(t, orgID, p)

		env.store.failUploads.Store(true)
		rec := importRequest(t, firstHandler, pluginID, externalID, nil)
		env.store.failUploads.Store(false)
		if rec.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
		}
		assetID := decodeImportResponse(t, rec).Asset.ID

		// Object landed in storage but the attachment record was lost: the
		// publication only tracks the object metadata.
		key, err := env.local.Upload(ctx, "cover.png", "image/png", bytes.NewReader(env.pngBytes))
		if err != nil {
			t.Fatal(err)
		}
		pub := env.publication(ctx, t, orgID, pluginID, externalID)
		if _, err := env.repos.Publications.AttachCoverObject(ctx, pub.ID, pub.Generation, key, "cover.png", "image/png", int64(len(env.pngBytes))); err != nil {
			t.Fatal(err)
		}

		// A fresh process: fresh handler, background recovery with no HTTP.
		restarted := env.newHandler(t, orgID, p)
		restarted.RecoverPendingImports(ctx)

		if got := env.countLiveAssets(ctx, t, orgID, pluginID, externalID); got != 1 {
			t.Fatalf("recovery must keep one asset, got %d", got)
		}
		if got := env.countAttachments(ctx, t, assetID); got != 1 {
			t.Fatalf("expected the attachment record recreated, got %d", got)
		}
		att, err := env.repos.Attachments.GetByAssetAndFileKey(ctx, assetID, key)
		if err != nil || att == nil {
			t.Fatalf("expected attachment recreated for tracked object: %v %v", att, err)
		}
		if main := env.mainAttachment(ctx, t, assetID); main == nil || *main != att.ID {
			t.Fatalf("expected recovered main image %s, got %#v", att.ID, main)
		}
		if env.publication(ctx, t, orgID, pluginID, externalID).Status != domain.ImportPublicationPublished {
			t.Fatal("recovered publication should be published")
		}
	})

	t.Run("restart deletes a cover object with no publication or references", func(t *testing.T) {
		orgID := env.createOrg(ctx, t, "Orphan cleanup")
		pluginID := "books-ghost"
		externalID := "ghost-1"

		key, err := env.local.Upload(ctx, "ghost.png", "image/png", bytes.NewReader(env.pngBytes))
		if err != nil {
			t.Fatal(err)
		}
		pub := &domain.ImportPublication{
			OrganizationID:  orgID,
			PluginID:        pluginID,
			ExternalID:      externalID,
			Payload:         []byte(`{"external_id":"ghost-1","name":"Ghost"}`),
			ExternalVersion: "v1",
		}
		if _, err := env.repos.Publications.CreateIfAbsent(ctx, pub); err != nil {
			t.Fatal(err)
		}
		if _, err := env.repos.Publications.AcquireLease(ctx, pub.ID, 1, uuid.New(), time.Now().Add(-time.Minute)); err != nil {
			t.Fatal(err)
		}
		if _, err := env.repos.Publications.AttachCoverObject(ctx, pub.ID, 1, key, "ghost.png", "image/png", int64(len(env.pngBytes))); err != nil {
			t.Fatal(err)
		}
		// Plugin is intentionally not registered, so recovery can only sweep.
		h := env.newHandler(t, orgID)
		h.RecoverPendingImports(ctx)

		objectPath := filepath.Join(env.dir, key)
		if _, err := os.Stat(objectPath); !os.IsNotExist(err) {
			t.Fatalf("expected orphan object %s removed, stat err=%v", objectPath, err)
		}
		row, err := env.repos.Publications.GetByIdentity(ctx, orgID, pluginID, externalID)
		if err != nil {
			t.Fatal(err)
		}
		if row.CoverFileKey != nil || row.AssetID != nil {
			t.Fatalf("orphan tracking should be cleared: %#v", row)
		}
	})

	t.Run("delete and reimport creates a new generation without reviving the old asset", func(t *testing.T) {
		orgID := env.createOrg(ctx, t, "Reimport generation")
		pluginID := "books-reimport"
		externalID := "book-6"
		data := &domain.ImportData{ExternalID: externalID, Name: "First Edition", ImageURL: &imageURL}
		p := imagePlugin(pluginID, "Reimport Books", data)
		h := env.newHandler(t, orgID, p)

		first := importRequest(t, h, pluginID, externalID, nil)
		if first.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d: %s", first.Code, first.Body.String())
		}
		firstAsset := decodeImportResponse(t, first).Asset
		if err := env.repos.Assets.Delete(ctx, firstAsset.ID); err != nil {
			t.Fatal(err)
		}

		data.Name = "Second Edition"
		second := importRequest(t, h, pluginID, externalID, nil)
		if second.Code != http.StatusCreated {
			t.Fatalf("expected 201 for the new generation, got %d: %s", second.Code, second.Body.String())
		}
		secondAsset := decodeImportResponse(t, second).Asset
		if secondAsset.ID == firstAsset.ID || secondAsset.Name != "Second Edition" {
			t.Fatalf("expected a fresh second-generation asset, got %#v", secondAsset)
		}
		if got := env.countLiveAssets(ctx, t, orgID, pluginID, externalID); got != 1 {
			t.Fatalf("expected exactly one live asset after reimport, got %d", got)
		}
		pub := env.publication(ctx, t, orgID, pluginID, externalID)
		if pub.Generation != 2 || pub.AssetID == nil || *pub.AssetID != secondAsset.ID {
			t.Fatalf("publication should point at generation 2 asset: %#v", pub)
		}
		if got := env.countAttachments(ctx, t, secondAsset.ID); got != 1 {
			t.Fatalf("new generation should have its own cover, got %d", got)
		}
		if main := env.mainAttachment(ctx, t, secondAsset.ID); main == nil {
			t.Fatal("new generation should publish its main image")
		}
	})

	t.Run("recovery never overwrites a main image the user chose", func(t *testing.T) {
		orgID := env.createOrg(ctx, t, "User main image")
		pluginID := "books-userimage"
		externalID := "book-7"
		data := &domain.ImportData{ExternalID: externalID, Name: "User Choice Book", ImageURL: &imageURL}
		h := env.newHandler(t, orgID, imagePlugin(pluginID, "User Books", data))

		env.store.failUploads.Store(true)
		rec := importRequest(t, h, pluginID, externalID, nil)
		env.store.failUploads.Store(false)
		if rec.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
		}
		assetID := decodeImportResponse(t, rec).Asset.ID

		// Before the cover recovers, the user uploads their own image and sets
		// it as the main image.
		userKey, err := env.local.Upload(ctx, "user.jpg", "image/png", bytes.NewReader(env.pngBytes))
		if err != nil {
			t.Fatal(err)
		}
		contentType := "image/png"
		description := "user upload"
		userAttachment := &domain.Attachment{
			AssetID:     assetID,
			FileKey:     userKey,
			FileName:    "user.jpg",
			FileSize:    int64(len(env.pngBytes)),
			ContentType: &contentType,
			Description: &description,
		}
		if _, err := env.repos.Attachments.CreateOrGet(ctx, userAttachment); err != nil {
			t.Fatal(err)
		}
		if err := env.repos.Assets.SetMainAttachment(ctx, assetID, &userAttachment.ID); err != nil {
			t.Fatal(err)
		}

		retry := importRequest(t, h, pluginID, externalID, nil)
		if retry.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", retry.Code, retry.Body.String())
		}
		if got := env.countAttachments(ctx, t, assetID); got != 2 {
			t.Fatalf("expected user image plus recovered cover, got %d", got)
		}
		if main := env.mainAttachment(ctx, t, assetID); main == nil || *main != userAttachment.ID {
			t.Fatalf("user main image must be preserved, got %#v", main)
		}
		if env.publication(ctx, t, orgID, pluginID, externalID).Status != domain.ImportPublicationPublished {
			t.Fatal("publication should complete even when the main slot is taken")
		}
	})

	t.Run("no image and disabled categories/attributes stay compatible", func(t *testing.T) {
		orgID := env.createOrg(ctx, t, "Compatibility")

		noImagePluginID := "books-noimage"
		noImageData := &domain.ImportData{ExternalID: "n1", Name: "No Image Book", Attributes: map[string]any{"books.author": "X"}}
		h := env.newHandler(t, orgID, imagePlugin(noImagePluginID, "No Image Books", noImageData))
		rec := importRequest(t, h, noImagePluginID, "n1", nil)
		if rec.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
		}
		noImageAsset := decodeImportResponse(t, rec).Asset
		if got := env.countAttachments(ctx, t, noImageAsset.ID); got != 0 {
			t.Fatalf("expected no attachments, got %d", got)
		}
		if main := env.mainAttachment(ctx, t, noImageAsset.ID); main != nil {
			t.Fatal("expected no main image")
		}
		if env.publication(ctx, t, orgID, noImagePluginID, "n1").Status != domain.ImportPublicationPublished {
			t.Fatal("no-image publication should still be published")
		}

		disabledPluginID := "books-disabled-features"
		disabledData := &domain.ImportData{
			ExternalID: "n2",
			Name:       "Plain Book",
			ImageURL:   &imageURL,
			Attributes: map[string]any{"books.author": "Y"},
		}
		h = env.newHandler(t, orgID, imagePlugin(disabledPluginID, "Plain Books", disabledData))
		features := allFeaturesEnabled()
		features.Categories = false
		features.Attributes = false
		rec = importRequest(t, h, disabledPluginID, "n2", features)
		if rec.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
		}
		plainAsset := decodeImportResponse(t, rec).Asset
		if plainAsset.CategoryID != nil {
			t.Fatalf("expected no category, got %s", *plainAsset.CategoryID)
		}
		var attrs map[string]any
		if err := json.Unmarshal(plainAsset.Attributes, &attrs); err != nil {
			t.Fatal(err)
		}
		if len(attrs) != 0 {
			t.Fatalf("expected attributes stripped, got %#v", attrs)
		}
		if got := env.countRows(ctx, t,
			`SELECT count(*) FROM categories WHERE organization_id=$1 AND plugin_id=$2`,
			orgID, disabledPluginID); got != 0 {
			t.Fatal("disabled category feature must not create a plugin category")
		}
	})

	t.Run("unknown and disabled plugins keep their status codes", func(t *testing.T) {
		orgID := env.createOrg(ctx, t, "Disabled plugin")
		disabled := &disabledMockPlugin{mockPlugin: imagePlugin("books-off", "Off", &domain.ImportData{ExternalID: "x", Name: "X"})}
		h := env.newHandler(t, orgID, disabled)

		rec := importRequest(t, h, "books-off", "x", nil)
		if rec.Code != http.StatusServiceUnavailable {
			t.Fatalf("expected 503 for disabled plugin, got %d", rec.Code)
		}
		rec = importRequest(t, h, "books-missing", "x", nil)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404 for missing plugin, got %d", rec.Code)
		}
		if got := env.countRows(ctx, t, `SELECT count(*) FROM import_publications WHERE organization_id=$1`, orgID); got != 0 {
			t.Fatalf("rejected imports must not create publications, got %d", got)
		}
	})
}
