package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/lmmendes/attic/internal/deletion"
	"github.com/lmmendes/attic/internal/domain"
	"github.com/lmmendes/attic/internal/repository"
	"github.com/lmmendes/attic/internal/storage"
	"github.com/lmmendes/attic/internal/testutil"
)

type deletionHarness struct {
	t         *testing.T
	ctx       context.Context
	db        *testutil.TestDB
	f         *testutil.Fixtures
	h         *Handler
	router    http.Handler
	deletions *repository.DeletionRepository
	local     *storage.LocalStorage
}

func newDeletionHarness(t *testing.T, fileStorage FileStorage, local *storage.LocalStorage) *deletionHarness {
	t.Helper()
	ctx := context.Background()
	db, err := testutil.NewTestDB(ctx)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close(ctx) })

	fixtures := testutil.NewFixtures(db.Pool)
	assets := repository.NewAssetRepository(db.Pool)
	attachments := repository.NewAttachmentRepository(db.Pool)
	deletions := repository.NewDeletionRepository(db.Pool)
	h := New(nil, &Repositories{
		Assets:      assets,
		Attachments: attachments,
	}, fileStorage, uuid.Nil)
	h.SetDeletionService(deletion.New(deletions, attachments, fileStorage))

	r := chi.NewRouter()
	r.Delete("/api/assets/{id}", h.DeleteAsset)
	r.Get("/api/assets/{id}/attachments", h.ListAttachments)
	r.Get("/api/attachments/{attachmentId}", h.GetAttachment)
	r.Delete("/api/attachments/{attachmentId}", h.DeleteAttachment)

	return &deletionHarness{
		t: t, ctx: ctx, db: db, f: fixtures, h: h, router: r,
		deletions: deletions, local: local,
	}
}

func (h *deletionHarness) orgAsset(name string) *domain.Asset {
	h.t.Helper()
	org, err := h.f.CreateOrganization(h.ctx, "Org "+name)
	if err != nil {
		h.t.Fatal(err)
	}
	cat, err := h.f.CreateCategory(h.ctx, org.ID, "Cat", nil)
	if err != nil {
		h.t.Fatal(err)
	}
	asset, err := h.f.CreateAsset(h.ctx, org.ID, cat.ID, name)
	if err != nil {
		h.t.Fatal(err)
	}
	return asset
}

func (h *deletionHarness) request(method, path string, wantStatus int) []byte {
	h.t.Helper()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	h.router.ServeHTTP(rec, req)
	if rec.Code != wantStatus {
		h.t.Fatalf("%s %s: got %d, want %d: %s", method, path, rec.Code, wantStatus, rec.Body.String())
	}
	return rec.Body.Bytes()
}

func (h *deletionHarness) attach(assetID uuid.UUID, name, key string) *domain.Attachment {
	h.t.Helper()
	att := &domain.Attachment{
		AssetID:  assetID,
		FileKey:  key,
		FileName: name,
		FileSize: 4,
	}
	if err := h.h.repos.Attachments.Create(h.ctx, att); err != nil {
		h.t.Fatal(err)
	}
	return att
}

func (h *deletionHarness) uploadObject(key, content string) {
	h.t.Helper()
	fullPath := filepath.Join(h.local.BasePath(), key)
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		h.t.Fatal(err)
	}
	if err := os.WriteFile(fullPath, []byte(content), 0o644); err != nil {
		h.t.Fatal(err)
	}
}

func (h *deletionHarness) objectExists(key string) bool {
	h.t.Helper()
	_, err := os.Stat(filepath.Join(h.local.BasePath(), key))
	return !os.IsNotExist(err)
}

func (h *deletionHarness) soleJob(assetID uuid.UUID) repository.AssetDeletion {
	h.t.Helper()
	job, err := h.deletions.GetDeletionByAsset(h.ctx, assetID)
	if err != nil || job == nil {
		h.t.Fatalf("deletion job: %v err=%v", job, err)
	}
	return *job
}

func (h *deletionHarness) objectStatus(assetID uuid.UUID, key string) string {
	h.t.Helper()
	job := h.soleJob(assetID)
	objects, err := h.deletions.ListUnconfirmedObjects(h.ctx, job.ID)
	if err != nil {
		h.t.Fatal(err)
	}
	// Confirmed-deleted objects leave the unconfirmed view; check the table.
	for _, o := range objects {
		if o.FileKey == key {
			return o.Status
		}
	}
	var status string
	err = h.db.Pool.QueryRow(h.ctx,
		`SELECT status FROM asset_deletion_objects WHERE deletion_id=$1 AND file_key=$2`,
		job.ID, key).Scan(&status)
	if err != nil {
		h.t.Fatal(err)
	}
	return status
}

func newLocalTestStorage(t *testing.T) (*storage.LocalStorage, string) {
	t.Helper()
	dir := t.TempDir()
	s, err := storage.NewLocalStorage(storage.LocalConfig{BasePath: dir, BaseURL: "http://test/files"})
	if err != nil {
		t.Fatal(err)
	}
	return s, dir
}

func TestDeleteAsset_LocalStorageHappyPath(t *testing.T) {
	local, _ := newLocalTestStorage(t)
	h := newDeletionHarness(t, local, local)

	asset := h.orgAsset("Camera")
	att := h.attach(asset.ID, "pic.jpg", "u/pic.jpg")
	h.uploadObject("u/pic.jpg", "data")
	if updated, err := h.h.repos.Assets.SetMainAttachment(h.ctx, asset.ID, &att.ID); err != nil || !updated {
		t.Fatalf("set main: updated=%v err=%v", updated, err)
	}

	h.request(http.MethodDelete, "/api/assets/"+asset.ID.String(), http.StatusNoContent)

	if got, err := h.h.repos.Assets.GetByID(h.ctx, asset.ID); err != nil || got != nil {
		t.Errorf("asset should be hidden: %v err=%v", got, err)
	}
	if h.objectExists("u/pic.jpg") {
		t.Error("local object should have been removed")
	}
	if list, err := h.h.repos.Attachments.ListByAsset(h.ctx, asset.ID); err != nil || len(list) != 0 {
		t.Errorf("attachments should be hidden: %v err=%v", list, err)
	}
	job := h.soleJob(asset.ID)
	if job.Status != repository.DeletionStatusCompleted || job.FrozenMainAttachmentID == nil {
		t.Errorf("job=%+v", job)
	}
	if got := h.objectStatus(asset.ID, "u/pic.jpg"); got != repository.ObjectStatusDeleted {
		t.Errorf("object status=%s", got)
	}

	// Repeat delete keeps the same stable identity and the same outcome.
	h.request(http.MethodDelete, "/api/assets/"+asset.ID.String(), http.StatusNoContent)
	h.soleJob(asset.ID) // would fail if a second job existed... check count explicitly
	var count int
	if err := h.db.Pool.QueryRow(h.ctx,
		`SELECT COUNT(*) FROM asset_deletions WHERE asset_id=$1`, asset.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected one deletion identity, got %d", count)
	}
}

func TestDeleteAsset_MissingAndWithoutAttachmentsAreDeterministic(t *testing.T) {
	local, _ := newLocalTestStorage(t)
	h := newDeletionHarness(t, local, local)

	h.request(http.MethodDelete, "/api/assets/"+uuid.New().String(), http.StatusNoContent)

	asset := h.orgAsset("Empty")
	h.request(http.MethodDelete, "/api/assets/"+asset.ID.String(), http.StatusNoContent)
	job := h.soleJob(asset.ID)
	if job.Status != repository.DeletionStatusCompleted || job.AttachmentCount != 0 {
		t.Errorf("job=%+v", job)
	}
}

func TestDeleteAsset_SharedKeyProtectedThenReclaimedOnRetry(t *testing.T) {
	local, _ := newLocalTestStorage(t)
	h := newDeletionHarness(t, local, local)

	a := h.orgAsset("A")
	b := h.orgAsset("B")
	h.uploadObject("shared/k.bin", "data")
	h.attach(a.ID, "a.bin", "shared/k.bin")
	bAtt := h.attach(b.ID, "b.bin", "shared/k.bin")

	// Delete A: the object is still referenced by B and must survive.
	h.request(http.MethodDelete, "/api/assets/"+a.ID.String(), http.StatusNoContent)
	if !h.objectExists("shared/k.bin") {
		t.Fatal("shared object was deleted while B still references it")
	}
	if got := h.objectStatus(a.ID, "shared/k.bin"); got != repository.ObjectStatusRetained {
		t.Fatalf("object status=%s, want retained", got)
	}

	// B's attachment remains fully usable (presigned download compatibility).
	body := h.request(http.MethodGet, "/api/attachments/"+bAtt.ID.String(), http.StatusOK)
	var resp AttachmentResponse
	if err := json.Unmarshal(body, &resp); err != nil || resp.URL == "" {
		t.Fatalf("download response: %s err=%v", body, err)
	}

	// Explicitly delete B's attachment: the key is now exclusive and the
	// reference-aware single-attachment path removes the object.
	h.request(http.MethodDelete, "/api/attachments/"+bAtt.ID.String(), http.StatusNoContent)
	if h.objectExists("shared/k.bin") {
		t.Fatal("object should be gone once the last reference is removed")
	}

	// Repeat the asset deletion: the retained manifest item is rechecked from
	// the SAME frozen identity and reaches a confirmed outcome.
	h.request(http.MethodDelete, "/api/assets/"+a.ID.String(), http.StatusNoContent)
	if got := h.objectStatus(a.ID, "shared/k.bin"); got != repository.ObjectStatusDeleted {
		t.Fatalf("object status=%s, want deleted", got)
	}
	if job := h.soleJob(a.ID); job.Status != repository.DeletionStatusCompleted {
		t.Fatalf("job status=%s", job.Status)
	}
}

func TestExplicitDeleteAttachment_PresignThen204Then404(t *testing.T) {
	local, _ := newLocalTestStorage(t)
	h := newDeletionHarness(t, local, local)

	asset := h.orgAsset("Single")
	h.uploadObject("u/doc.pdf", "data")
	att := h.attach(asset.ID, "doc.pdf", "u/doc.pdf")

	h.request(http.MethodGet, "/api/attachments/"+att.ID.String(), http.StatusOK)
	h.request(http.MethodDelete, "/api/attachments/"+att.ID.String(), http.StatusNoContent)
	h.request(http.MethodDelete, "/api/attachments/"+att.ID.String(), http.StatusNotFound)
	if h.objectExists("u/doc.pdf") {
		t.Error("object should be removed")
	}
}

// flakyStorage wraps local storage and injects delete uncertainty: the first
// failures deletions leave the file in place and report a transport error.
type flakyStorage struct {
	inner       *storage.LocalStorage
	mu          sync.Mutex
	failUntil   map[string]int // remaining failures per key
	deleteCalls []string
}

func (s *flakyStorage) Upload(ctx context.Context, filename, contentType string, body io.Reader) (string, error) {
	return s.inner.Upload(ctx, filename, contentType, body)
}

func (s *flakyStorage) GetPresignedURL(ctx context.Context, key string, expiry time.Duration) (string, error) {
	return s.inner.GetPresignedURL(ctx, key, expiry)
}

func (s *flakyStorage) Delete(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteCalls = append(s.deleteCalls, key)
	if s.failUntil[key] > 0 {
		s.failUntil[key]--
		return fmt.Errorf("read tcp: i/o timeout")
	}
	return s.inner.Delete(ctx, key)
}

func (s *flakyStorage) Exists(ctx context.Context, key string) (bool, error) {
	return s.inner.Exists(ctx, key)
}

func (s *flakyStorage) deleteCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.deleteCalls)
}

func TestDeleteAsset_UncertainResponseStaysPendingAndRestartResumesManifest(t *testing.T) {
	local, _ := newLocalTestStorage(t)
	flaky := &flakyStorage{inner: local, failUntil: map[string]int{"u/flaky.bin": 10}}
	h := newDeletionHarness(t, flaky, local)

	asset := h.orgAsset("Flaky")
	h.uploadObject("u/flaky.bin", "data")
	h.attach(asset.ID, "flaky.bin", "u/flaky.bin")

	h.request(http.MethodDelete, "/api/assets/"+asset.ID.String(), http.StatusNoContent)
	if !h.objectExists("u/flaky.bin") {
		t.Fatal("object must remain when deletion could not be confirmed")
	}
	if got := h.objectStatus(asset.ID, "u/flaky.bin"); got != repository.ObjectStatusFailed {
		t.Fatalf("object status=%s, want failed", got)
	}
	if job := h.soleJob(asset.ID); job.Status == repository.DeletionStatusCompleted {
		t.Fatal("partial failure must keep the deletion queryable and open")
	}
	firstAttempts := flaky.deleteCount()

	// Simulate a service restart: a brand-new service over the same database
	// resumes from the frozen manifest instead of rescanning.
	flaky.failUntil["u/flaky.bin"] = 0
	restartAttachments := repository.NewAttachmentRepository(h.db.Pool)
	restartDeletions := repository.NewDeletionRepository(h.db.Pool)
	restartSvc := deletion.New(restartDeletions, restartAttachments, flaky)
	if err := restartSvc.ResumeAll(h.ctx); err != nil {
		t.Fatal(err)
	}

	if h.objectExists("u/flaky.bin") {
		t.Error("restart recovery should have removed the object")
	}
	if got := h.objectStatus(asset.ID, "u/flaky.bin"); got != repository.ObjectStatusDeleted {
		t.Fatalf("object status=%s, want deleted", got)
	}
	if job := h.soleJob(asset.ID); job.Status != repository.DeletionStatusCompleted {
		t.Fatalf("job status=%s", job.Status)
	}
	if flaky.deleteCount() != firstAttempts+1 {
		t.Errorf("restart should only retry the unconfirmed object: %d total calls", flaky.deleteCount())
	}

	// The restart path must use the stored manifest, not a rescan: the asset's
	// attachment rows remain claimed by the original deletion generation.
	var deletionID uuid.UUID
	if err := h.db.Pool.QueryRow(h.ctx,
		`SELECT deletion_id FROM attachments WHERE asset_id=$1`, asset.ID).Scan(&deletionID); err != nil {
		t.Fatal(err)
	}
	if deletionID != h.soleJob(asset.ID).ID {
		t.Error("restart changed the deletion generation")
	}
}
