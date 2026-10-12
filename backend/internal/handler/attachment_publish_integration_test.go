package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"io/fs"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/lmmendes/attic/internal/domain"
	"github.com/lmmendes/attic/internal/repository"
	"github.com/lmmendes/attic/internal/storage"
	"github.com/lmmendes/attic/internal/testutil"
)

// onePixelPNG is a minimal valid PNG used so image content detection works.
var onePixelPNG = []byte{
	0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, 0x00, 0x00, 0x00, 0x0D,
	0x49, 0x48, 0x44, 0x52, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
	0x08, 0x06, 0x00, 0x00, 0x00, 0x1F, 0x15, 0xC4, 0x89, 0x00, 0x00, 0x00,
	0x0D, 0x49, 0x44, 0x41, 0x54, 0x78, 0x9C, 0x62, 0x00, 0x01, 0x00, 0x00,
	0x05, 0x00, 0x01, 0x0D, 0x0A, 0x2D, 0xB4, 0x00, 0x00, 0x00, 0x00, 0x49,
	0x45, 0x4E, 0x44, 0xAE, 0x42, 0x60, 0x82,
}

type publishTestEnv struct {
	t        *testing.T
	ctx      context.Context
	db       *testutil.TestDB
	store    *storage.LocalStorage
	storeDir string
	pub      *repository.AttachmentPublisher
	router   http.Handler
	orgID    uuid.UUID
}

func setupPublishTest(t *testing.T) *publishTestEnv {
	t.Helper()
	ctx := context.Background()
	db, err := testutil.NewTestDB(ctx)
	if err != nil {
		t.Fatalf("starting test database: %v", err)
	}
	t.Cleanup(func() { db.Close(ctx) })

	fixtures := testutil.NewFixtures(db.Pool)
	org, err := fixtures.CreateOrganization(ctx, "Attachments")
	if err != nil {
		t.Fatal(err)
	}
	category, err := fixtures.CreateCategory(ctx, org.ID, "General", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixtures.CreateAsset(ctx, org.ID, category.ID, "Camera"); err != nil {
		t.Fatal(err)
	}

	storeDir := t.TempDir()
	store, err := storage.NewLocalStorage(storage.LocalConfig{
		BasePath: storeDir,
		BaseURL:  "http://localhost:8080/files",
	})
	if err != nil {
		t.Fatal(err)
	}

	assetRepo := repository.NewAssetRepository(db.Pool)
	attachmentRepo := repository.NewAttachmentRepository(db.Pool)
	repos := &Repositories{
		Assets:      assetRepo,
		Attachments: attachmentRepo,
	}
	h := New(nil, repos, store, org.ID)
	pub := repository.NewAttachmentPublisher(db.Pool, store)
	h.SetAttachmentPublisher(pub)

	r := chi.NewRouter()
	r.Route("/api/assets", func(r chi.Router) {
		r.Post("/{id}/attachments", h.UploadAttachment)
		r.Get("/{id}/attachments", h.ListAttachments)
		r.Get("/{id}/attachments/uploads/{uploadId}", h.GetUploadStatus)
		r.Put("/{id}/main-image/{attachmentId}", h.SetMainAttachment)
		r.Delete("/{id}/main-image", h.ClearMainAttachment)
	})
	r.Delete("/api/attachments/{attachmentId}", h.DeleteAttachment)

	return &publishTestEnv{
		t:        t,
		ctx:      ctx,
		db:       db,
		store:    store,
		storeDir: storeDir,
		pub:      pub,
		router:   r,
		orgID:    org.ID,
	}
}

func (e *publishTestEnv) assetID() uuid.UUID {
	e.t.Helper()
	rows, err := e.db.Pool.Query(e.ctx, `SELECT id FROM assets LIMIT 1`)
	if err != nil {
		e.t.Fatal(err)
	}
	defer rows.Close()
	if !rows.Next() {
		e.t.Fatal("no asset found")
	}
	var id uuid.UUID
	if err := rows.Scan(&id); err != nil {
		e.t.Fatal(err)
	}
	return id
}

func (e *publishTestEnv) mainID(assetID uuid.UUID) *uuid.UUID {
	e.t.Helper()
	var id *uuid.UUID
	if err := e.db.Pool.QueryRow(e.ctx,
		`SELECT main_attachment_id FROM assets WHERE id = $1`, assetID).Scan(&id); err != nil {
		e.t.Fatal(err)
	}
	return id
}

func (e *publishTestEnv) uploadStatus(uploadID uuid.UUID) string {
	e.t.Helper()
	var status string
	if err := e.db.Pool.QueryRow(e.ctx,
		`SELECT status FROM attachment_uploads WHERE id = $1`, uploadID).Scan(&status); err != nil {
		e.t.Fatal(err)
	}
	return status
}

func (e *publishTestEnv) countStoredObjects() int {
	e.t.Helper()
	count := 0
	filepath.WalkDir(e.storeDir, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d != nil && !d.IsDir() {
			count++
		}
		return nil
	})
	return count
}

func multipartUploadRequest(t *testing.T, filename, contentType string, content []byte, uploadID string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	hdr := make(map[string][]string)
	hdr["Content-Disposition"] = []string{`form-data; name="file"; filename="` + filename + `"`}
	hdr["Content-Type"] = []string{contentType}
	part, err := mw.CreatePart(hdr)
	if err != nil {
		t.Fatal(err)
	}
	part.Write(content)
	if uploadID != "" {
		mw.WriteField("upload_id", uploadID)
	}
	mw.Close()

	req := httptest.NewRequest(http.MethodPost, "/api/assets/x/attachments", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

func multipartResumeRequest(t *testing.T, uploadID string) *http.Request {
	t.Helper()
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	if err := mw.WriteField("upload_id", uploadID); err != nil {
		t.Fatal(err)
	}
	mw.Close()
	req := httptest.NewRequest(http.MethodPost, "/api/assets/x/attachments", &buf)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

func (e *publishTestEnv) do(req *http.Request, assetID uuid.UUID) *httptest.ResponseRecorder {
	e.t.Helper()
	if assetID != uuid.Nil {
		req.URL.Path = strings.Replace(req.URL.Path, "/x/", "/"+assetID.String()+"/", 1)
	}
	rec := httptest.NewRecorder()
	e.router.ServeHTTP(rec, req)
	return rec
}

func decodeAttachment(t *testing.T, body *bytes.Buffer) domain.Attachment {
	t.Helper()
	var att domain.Attachment
	if err := json.NewDecoder(body).Decode(&att); err != nil {
		t.Fatalf("decoding attachment: %v: %s", err, body.String())
	}
	return att
}

func TestAttachmentPublishImageUploadCommitsMainImage(t *testing.T) {
	e := setupPublishTest(t)
	assetID := e.assetID()
	uploadID := uuid.New()

	req := multipartUploadRequest(t, "pic.png", "image/png", onePixelPNG, uploadID.String())
	rec := e.do(req, assetID)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	if got := rec.Header().Get(uploadIDHeader); got != uploadID.String() {
		t.Fatalf("expected %s header, got %q", uploadIDHeader, got)
	}
	att := decodeAttachment(t, rec.Body)
	if att.ContentType == nil || *att.ContentType != "image/png" {
		t.Fatalf("unexpected content type: %+v", att.ContentType)
	}

	main := e.mainID(assetID)
	if main == nil || *main != att.ID {
		t.Fatalf("expected main image to be %s, got %v", att.ID, main)
	}
	if e.uploadStatus(uploadID) != domain.AttachmentUploadCompleted {
		t.Fatal("expected upload to be completed")
	}
	if _, err := os.Stat(filepath.Join(e.storeDir, att.FileKey)); err != nil {
		t.Fatalf("expected object on disk at %s: %v", att.FileKey, err)
	}
}

func TestAttachmentPublishIdempotentRetryReplaysSameResult(t *testing.T) {
	e := setupPublishTest(t)
	assetID := e.assetID()
	uploadID := uuid.New()

	first := e.do(multipartUploadRequest(t, "pic.png", "image/png", onePixelPNG, uploadID.String()), assetID)
	if first.Code != http.StatusCreated {
		t.Fatalf("first upload: expected 201, got %d: %s", first.Code, first.Body.String())
	}
	firstAtt := decodeAttachment(t, first.Body)
	objectsAfterFirst := e.countStoredObjects()

	// Full retry with the same identity must not create another object or row.
	second := e.do(multipartUploadRequest(t, "pic.png", "image/png", onePixelPNG, uploadID.String()), assetID)
	if second.Code != http.StatusCreated {
		t.Fatalf("retry: expected 201, got %d: %s", second.Code, second.Body.String())
	}
	secondAtt := decodeAttachment(t, second.Body)
	if secondAtt.ID != firstAtt.ID {
		t.Fatalf("retry created a new attachment %s instead of replaying %s", secondAtt.ID, firstAtt.ID)
	}
	if e.countStoredObjects() != objectsAfterFirst {
		t.Fatal("retry created a duplicate storage object")
	}

	// A body-less resume (lost response / cancelled request) replays too.
	third := e.do(multipartResumeRequest(t, uploadID.String()), assetID)
	if third.Code != http.StatusCreated {
		t.Fatalf("body-less resume: expected 201, got %d: %s", third.Code, third.Body.String())
	}
	if decodeAttachment(t, third.Body).ID != firstAtt.ID {
		t.Fatal("body-less resume did not return the original attachment")
	}

	// Status endpoint reports the published result.
	statusReq := httptest.NewRequest(http.MethodGet,
		"/api/assets/"+assetID.String()+"/attachments/uploads/"+uploadID.String(), nil)
	statusRec := e.do(statusReq, assetID)
	if statusRec.Code != http.StatusOK {
		t.Fatalf("status: expected 200, got %d: %s", statusRec.Code, statusRec.Body.String())
	}
	var status UploadStatusResponse
	if err := json.NewDecoder(statusRec.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if status.Status != domain.AttachmentUploadCompleted || status.Attachment == nil || status.Attachment.ID != firstAtt.ID {
		t.Fatalf("unexpected status response: %s", statusRec.Body.String())
	}

	// Exactly one attachment row for the asset.
	listReq := httptest.NewRequest(http.MethodGet, "/api/assets/"+assetID.String()+"/attachments", nil)
	listRec := e.do(listReq, assetID)
	var attachments []domain.Attachment
	if err := json.NewDecoder(listRec.Body).Decode(&attachments); err != nil {
		t.Fatal(err)
	}
	if len(attachments) != 1 {
		t.Fatalf("expected 1 attachment, got %d", len(attachments))
	}
}

func TestAttachmentPublishNonImageDoesNotTouchMainImage(t *testing.T) {
	e := setupPublishTest(t)
	assetID := e.assetID()

	rec := e.do(multipartUploadRequest(t, "doc.pdf", "application/pdf", []byte("%PDF-1.4"), ""), assetID)
	if rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
	}
	att := decodeAttachment(t, rec.Body)
	if main := e.mainID(assetID); main != nil {
		t.Fatalf("non-image upload changed main image to %v", main)
	}
	var setMain bool
	if err := e.db.Pool.QueryRow(e.ctx,
		`SELECT set_main FROM attachment_uploads WHERE attachment_id = $1`, att.ID).Scan(&setMain); err != nil {
		t.Fatal(err)
	}
	if setMain {
		t.Fatal("non-image upload must not promise a main-image update")
	}
}

func TestAttachmentPublishRecoveryFinishesInterruptedPublishes(t *testing.T) {
	e := setupPublishTest(t)
	assetID := e.assetID()

	// Simulate a process crash after the object was uploaded but before any
	// database commit beyond the upload record.
	uploadID := uuid.New()
	if _, err := e.pub.Register(e.ctx, repository.RegisterUploadInput{
		ID:       uploadID,
		AssetID:  assetID,
		FileName: "recovered.png",
		FileSize: int64(len(onePixelPNG)),
		ContentType: "image/png",
		SetMain:  true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pub.PutObject(e.ctx, uploadID, bytes.NewReader(onePixelPNG)); err != nil {
		t.Fatal(err)
	}

	// A plain retry (no body) drives the same publish to completion.
	u, att, err := e.pub.Commit(e.ctx, uploadID)
	if err != nil {
		t.Fatalf("commit after object stage: %v", err)
	}
	if u.Status != domain.AttachmentUploadCompleted || u.MainResult == nil ||
		*u.MainResult != domain.AttachmentUploadMainSet {
		t.Fatalf("unexpected publish outcome: %+v", u)
	}
	if main := e.mainID(assetID); main == nil || *main != att.ID {
		t.Fatalf("expected recovered attachment %s as main, got %v", att.ID, main)
	}

	// Simulate a crash after the attachment row existed but the main pointer
	// update did not commit, long enough ago for the sweeper to claim it.
	secondID := uuid.New()
	rec2 := e.do(multipartUploadRequest(t, "later.png", "image/png", onePixelPNG, secondID.String()), assetID)
	if rec2.Code != http.StatusCreated {
		t.Fatalf("second upload: %s", rec2.Body.String())
	}
	att2 := decodeAttachment(t, rec2.Body)
	if _, err := e.db.Pool.Exec(e.ctx, `
		UPDATE attachment_uploads
		SET status = 'attachment_created', main_result = NULL, updated_at = NOW() - INTERVAL '2 minutes'
		WHERE id = $1`, secondID); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.Pool.Exec(e.ctx,
		`UPDATE assets SET main_attachment_id = NULL WHERE id = $1`, assetID); err != nil {
		t.Fatal(err)
	}

	if err := e.pub.Recover(e.ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if e.uploadStatus(secondID) != domain.AttachmentUploadCompleted {
		t.Fatal("expected interrupted main-image stage to be completed by recovery")
	}
	if main := e.mainID(assetID); main == nil || *main != att2.ID {
		t.Fatalf("recovery did not restore main image %s, got %v", att2.ID, main)
	}
}

func TestAttachmentPublishOrphanPendingObjectIsAbortedAndCleaned(t *testing.T) {
	e := setupPublishTest(t)
	assetID := e.assetID()

	orphanID := uuid.New()
	u, err := e.pub.Register(e.ctx, repository.RegisterUploadInput{
		ID:       orphanID,
		AssetID:  assetID,
		FileName: "orphan.bin",
	})
	if err != nil {
		t.Fatal(err)
	}
	// Pretend an object exists even though the state never advanced.
	objectPath := filepath.Join(e.storeDir, u.FileKey)
	if err := os.MkdirAll(filepath.Dir(objectPath), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(objectPath, []byte("partial"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := e.db.Pool.Exec(e.ctx,
		`UPDATE attachment_uploads SET created_at = NOW() - INTERVAL '20 minutes', updated_at = NOW() - INTERVAL '20 minutes' WHERE id = $1`,
		orphanID); err != nil {
		t.Fatal(err)
	}

	if err := e.pub.Recover(e.ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if got := e.uploadStatus(orphanID); got != domain.AttachmentUploadAborted {
		t.Fatalf("expected orphan aborted, got %s", got)
	}
	if _, err := os.Stat(objectPath); !os.IsNotExist(err) {
		t.Fatalf("expected orphan object removed, stat err=%v", err)
	}
}

func TestAttachmentDeleteArbitratesAgainstUnfinishedPublish(t *testing.T) {
	e := setupPublishTest(t)
	assetID := e.assetID()
	uploadID := uuid.New()

	rec := e.do(multipartUploadRequest(t, "pic.png", "image/png", onePixelPNG, uploadID.String()), assetID)
	if rec.Code != http.StatusCreated {
		t.Fatalf("upload: %s", rec.Body.String())
	}
	att := decodeAttachment(t, rec.Body)
	objectPath := filepath.Join(e.storeDir, att.FileKey)

	// Rewind the publish to an unfinished state (crash between attachment row
	// and main pointer) with a dangling main pointer.
	if _, err := e.db.Pool.Exec(e.ctx, `
		UPDATE attachment_uploads
		SET status = 'attachment_created', main_result = NULL, updated_at = NOW() - INTERVAL '2 minutes'
		WHERE id = $1`, uploadID); err != nil {
		t.Fatal(err)
	}

	delReq := httptest.NewRequest(http.MethodDelete, "/api/attachments/"+att.ID.String(), nil)
	delRec := e.do(delReq, uuid.Nil)
	if delRec.Code != http.StatusNoContent {
		t.Fatalf("delete: expected 204, got %d: %s", delRec.Code, delRec.Body.String())
	}
	if main := e.mainID(assetID); main != nil {
		t.Fatalf("main image still points to deleted attachment %v", main)
	}
	if got := e.uploadStatus(uploadID); got != domain.AttachmentUploadAborted {
		t.Fatalf("expected upload aborted after delete, got %s", got)
	}
	if _, err := os.Stat(objectPath); !os.IsNotExist(err) {
		t.Fatalf("expected object removed, stat err=%v", err)
	}

	// Late recovery must not resurrect the pointer, row or object.
	if err := e.pub.Recover(e.ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if main := e.mainID(assetID); main != nil {
		t.Fatalf("recovery resurrected main pointer %v", main)
	}
	if e.countStoredObjects() != 0 {
		t.Fatal("recovery re-created a deleted object")
	}

	// Retrying the original identity is rejected instead of re-publishing.
	retry := e.do(multipartUploadRequest(t, "pic.png", "image/png", onePixelPNG, uploadID.String()), assetID)
	if retry.Code != http.StatusConflict {
		t.Fatalf("expected 409 on aborted retry, got %d", retry.Code)
	}

	// Deleting again is a 404.
	again := e.do(httptest.NewRequest(http.MethodDelete, "/api/attachments/"+att.ID.String(), nil), uuid.Nil)
	if again.Code != http.StatusNotFound {
		t.Fatalf("expected 404 on second delete, got %d", again.Code)
	}
}

func TestAttachmentManualMainChoiceIsNotOverriddenByInflightPublish(t *testing.T) {
	e := setupPublishTest(t)
	assetID := e.assetID()

	// Image A completes normally and becomes main.
	recA := e.do(multipartUploadRequest(t, "a.png", "image/png", onePixelPNG, ""), assetID)
	if recA.Code != http.StatusCreated {
		t.Fatalf("upload A: %s", recA.Body.String())
	}
	attA := decodeAttachment(t, recA.Body)

	// Image B is registered and its object is uploaded, but completion is
	// interrupted. The admin explicitly clears the main image.
	uploadB := uuid.New()
	if _, err := e.pub.Register(e.ctx, repository.RegisterUploadInput{
		ID:          uploadB,
		AssetID:     assetID,
		FileName:    "b.png",
		FileSize:    int64(len(onePixelPNG)),
		ContentType: "image/png",
		SetMain:     true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.pub.PutObject(e.ctx, uploadB, bytes.NewReader(onePixelPNG)); err != nil {
		t.Fatal(err)
	}
	clearReq := httptest.NewRequest(http.MethodDelete, "/api/assets/"+assetID.String()+"/main-image", nil)
	clearRec := e.do(clearReq, assetID)
	if clearRec.Code != http.StatusNoContent {
		t.Fatalf("clear main: %s", clearRec.Body.String())
	}

	u, attB, err := e.pub.Commit(e.ctx, uploadB)
	if err != nil {
		t.Fatalf("commit B: %v", err)
	}
	if u.MainResult == nil || *u.MainResult != domain.AttachmentUploadMainSkippedManual {
		t.Fatalf("expected manual clear to win, got %+v", u.MainResult)
	}
	if main := e.mainID(assetID); main != nil {
		t.Fatalf("in-flight publish overrode manual clear: %v", main)
	}

	// Recovery must also respect the manual choice.
	if err := e.pub.Recover(e.ctx); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if main := e.mainID(assetID); main != nil {
		t.Fatalf("recovery resurrected main %v after manual clear", main)
	}

	// A subsequent brand-new image upload becomes main again.
	recC := e.do(multipartUploadRequest(t, "c.png", "image/png", onePixelPNG, ""), assetID)
	if recC.Code != http.StatusCreated {
		t.Fatalf("upload C: %s", recC.Body.String())
	}
	attC := decodeAttachment(t, recC.Body)
	if main := e.mainID(assetID); main == nil || *main != attC.ID {
		t.Fatalf("expected new upload C as main, A=%s B=%s", attA.ID, attB.ID)
	}

	// Manual endpoint still validates ownership and image type.
	otherAsset := uuid.New()
	badOwner := httptest.NewRequest(http.MethodPut,
		"/api/assets/"+otherAsset.String()+"/main-image/"+attC.ID.String(), nil)
	badOwnerRec := e.do(badOwner, otherAsset)
	if badOwnerRec.Code != http.StatusNotFound {
		t.Fatalf("expected other-asset set to 404, got %d", badOwnerRec.Code)
	}
}

func TestAttachmentManualMainRejectsNonImage(t *testing.T) {
	e := setupPublishTest(t)
	assetID := e.assetID()

	rec := e.do(multipartUploadRequest(t, "notes.txt", "text/plain", []byte("hello"), ""), assetID)
	if rec.Code != http.StatusCreated {
		t.Fatalf("upload: %s", rec.Body.String())
	}
	att := decodeAttachment(t, rec.Body)

	setReq := httptest.NewRequest(http.MethodPut,
		"/api/assets/"+assetID.String()+"/main-image/"+att.ID.String(), nil)
	setRec := e.do(setReq, assetID)
	if setRec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-image main, got %d: %s", setRec.Code, setRec.Body.String())
	}
}

func TestAttachmentUploadWithoutStorageReturns503AndNoSideEffects(t *testing.T) {
	ctx := context.Background()
	db, err := testutil.NewTestDB(ctx)
	if err != nil {
		t.Fatalf("starting test database: %v", err)
	}
	defer db.Close(ctx)

	fixtures := testutil.NewFixtures(db.Pool)
	org, err := fixtures.CreateOrganization(ctx, "No Storage")
	if err != nil {
		t.Fatal(err)
	}
	category, err := fixtures.CreateCategory(ctx, org.ID, "General", nil)
	if err != nil {
		t.Fatal(err)
	}
	asset, err := fixtures.CreateAsset(ctx, org.ID, category.ID, "Lens")
	if err != nil {
		t.Fatal(err)
	}

	repos := &Repositories{
		Assets:      repository.NewAssetRepository(db.Pool),
		Attachments: repository.NewAttachmentRepository(db.Pool),
	}
	h := New(nil, repos, nil, org.ID)
	h.SetAttachmentPublisher(repository.NewAttachmentPublisher(db.Pool, nil))

	router := chi.NewRouter()
	router.Post("/api/assets/{id}/attachments", h.UploadAttachment)

	req := multipartUploadRequest(t, "pic.png", "image/png", onePixelPNG, uuid.NewString())
	rec := httptest.NewRecorder()
	req.URL.Path = "/api/assets/" + asset.ID.String() + "/attachments"
	router.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", rec.Code, rec.Body.String())
	}

	var count int
	if err := db.Pool.QueryRow(ctx, `SELECT COUNT(*) FROM attachment_uploads`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("expected no publish records without storage, got %d", count)
	}
}
