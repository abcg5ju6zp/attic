package deletion

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/lmmendes/attic/internal/repository"
)

type fakeStore struct {
	mu              sync.Mutex
	job             *repository.AssetDeletion
	objects         []repository.AssetDeletionObject
	refs            map[string]int
	beginErr        error
	beginCount      int
	objectsLoaded   int
	reclaimingCount int
	deleteCalls     []string

	failListObjects bool
	failOutcome     bool
}

func newFakeStore() *fakeStore {
	jobID := uuid.New()
	return &fakeStore{
		job: &repository.AssetDeletion{
			ID:     jobID,
			AssetID: uuid.New(),
			Status: repository.DeletionStatusPending,
		},
		refs: map[string]int{},
	}
}

func (s *fakeStore) addObject(key, status string) repository.AssetDeletionObject {
	o := repository.AssetDeletionObject{
		ID:         uuid.New(),
		DeletionID: s.job.ID,
		FileKey:    key,
		Status:     status,
	}
	s.objects = append(s.objects, o)
	return o
}

func (s *fakeStore) BeginDeletion(ctx context.Context, assetID uuid.UUID) (*repository.AssetDeletion, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.beginCount++
	if s.beginErr != nil {
		return nil, false, s.beginErr
	}
	return s.job, true, nil
}

func (s *fakeStore) MarkReclaiming(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reclaimingCount++
	if s.job.Status == repository.DeletionStatusPending {
		s.job.Status = repository.DeletionStatusReclaiming
	}
	return nil
}

func (s *fakeStore) ListActiveDeletions(ctx context.Context) ([]repository.AssetDeletion, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.job.Status != repository.DeletionStatusCompleted {
		return []repository.AssetDeletion{*s.job}, nil
	}
	return nil, nil
}

func (s *fakeStore) ListUnconfirmedObjects(ctx context.Context, deletionID uuid.UUID) ([]repository.AssetDeletionObject, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objectsLoaded++
	if s.failListObjects {
		return nil, errors.New("db unavailable")
	}
	var out []repository.AssetDeletionObject
	for _, o := range s.objects {
		if o.Status != repository.ObjectStatusDeleted {
			out = append(out, o)
		}
	}
	return out, nil
}

func (s *fakeStore) CountLiveReferences(ctx context.Context, fileKey string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refs[fileKey], nil
}

func (s *fakeStore) MarkObjectOutcome(ctx context.Context, id uuid.UUID, status string, lastError *string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.failOutcome {
		return errors.New("cannot write outcome")
	}
	for i := range s.objects {
		if s.objects[i].ID == id {
			s.objects[i].Status = status
			s.objects[i].LastError = lastError
			s.objects[i].Attempts++
		}
	}
	return nil
}

func (s *fakeStore) CompleteDeletionIfReady(ctx context.Context, id uuid.UUID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, o := range s.objects {
		if o.Status == repository.ObjectStatusPending || o.Status == repository.ObjectStatusFailed {
			return nil
		}
	}
	s.job.Status = repository.DeletionStatusCompleted
	return nil
}

func (s *fakeStore) statusOf(key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, o := range s.objects {
		if o.FileKey == key {
			return o.Status
		}
	}
	return ""
}

type fakeStorage struct {
	mu            sync.Mutex
	present       map[string]bool
	deleteErrors  map[string]error
	existsErrors  map[string]error
	deletedKeys   []string
	verifiedKeys  []string
}

func newFakeStorage(keys ...string) *fakeStorage {
	f := &fakeStorage{
		present:      map[string]bool{},
		deleteErrors: map[string]error{},
		existsErrors: map[string]error{},
	}
	for _, k := range keys {
		f.present[k] = true
	}
	return f
}

func (f *fakeStorage) Delete(ctx context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletedKeys = append(f.deletedKeys, key)
	if err := f.deleteErrors[key]; err != nil {
		return err
	}
	delete(f.present, key)
	return nil
}

func (f *fakeStorage) Exists(ctx context.Context, key string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verifiedKeys = append(f.verifiedKeys, key)
	if err := f.existsErrors[key]; err != nil {
		return false, err
	}
	return f.present[key], nil
}

func (f *fakeStorage) deleteCount(key string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	n := 0
	for _, k := range f.deletedKeys {
		if k == key {
			n++
		}
	}
	return n
}

func TestService_DeleteAsset_ExclusiveObjectsDeletedAndCompleted(t *testing.T) {
	store := newFakeStore()
	store.addObject("a/one.txt", repository.ObjectStatusPending)
	store.addObject("a/two.png", repository.ObjectStatusPending)
	st := newFakeStorage("a/one.txt", "a/two.png")
	svc := New(store, nil, st)

	found, err := svc.DeleteAsset(context.Background(), store.job.AssetID)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if got := store.statusOf("a/one.txt"); got != repository.ObjectStatusDeleted {
		t.Errorf("one.txt status=%s", got)
	}
	if got := store.statusOf("a/two.png"); got != repository.ObjectStatusDeleted {
		t.Errorf("two.png status=%s", got)
	}
	if store.job.Status != repository.DeletionStatusCompleted {
		t.Errorf("job status=%s", store.job.Status)
	}
	if len(st.deletedKeys) != 2 {
		t.Errorf("expected 2 delete calls, got %v", st.deletedKeys)
	}
}

func TestService_DeleteAsset_MissingAssetIsDeterministic(t *testing.T) {
	st := newFakeStorage()
	svc := New(&missingAssetStore{}, nil, st)
	found, err := svc.DeleteAsset(context.Background(), uuid.New())
	if err != nil || found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if len(st.deletedKeys) != 0 {
		t.Errorf("no object may be deleted for a missing asset: %v", st.deletedKeys)
	}
}

type missingAssetStore struct{ fakeStore }

func (s *missingAssetStore) BeginDeletion(ctx context.Context, assetID uuid.UUID) (*repository.AssetDeletion, bool, error) {
	return nil, false, nil
}

func TestService_ReferencedObjectRetainedAndNotDeleted(t *testing.T) {
	store := newFakeStore()
	store.addObject("shared/k.bin", repository.ObjectStatusPending)
	store.refs["shared/k.bin"] = 2 // another live attachment + main image
	st := newFakeStorage("shared/k.bin")
	svc := New(store, nil, st)

	if _, err := svc.DeleteAsset(context.Background(), store.job.AssetID); err != nil {
		t.Fatal(err)
	}
	if got := store.statusOf("shared/k.bin"); got != repository.ObjectStatusRetained {
		t.Fatalf("status=%s, want retained", got)
	}
	if len(st.deletedKeys) != 0 {
		t.Fatalf("referenced object must not be deleted: %v", st.deletedKeys)
	}
	if st.present["shared/k.bin"] != true {
		t.Error("remote object should still exist")
	}
	if store.job.Status != repository.DeletionStatusCompleted {
		t.Errorf("retained objects do not block completion, status=%s", store.job.Status)
	}
}

func TestService_UncertainDeleteResponseVerifiedByFact(t *testing.T) {
	store := newFakeStore()
	store.addObject("k1", repository.ObjectStatusPending)
	store.addObject("k2", repository.ObjectStatusPending)
	st := newFakeStorage("k1", "k2")
	// Both Delete calls report an ambiguous transport error; k1 is in fact
	// gone (server applied it), k2 is still present.
	st.deleteErrors["k1"] = errors.New("read tcp: connection reset")
	st.deleteErrors["k2"] = errors.New("read tcp: timeout")
	st.present["k1"] = true // force Exists to confirm? no: reset case server deleted it
	st.present["k1"] = false
	svc := New(store, nil, st)

	if _, err := svc.DeleteAsset(context.Background(), store.job.AssetID); err != nil {
		t.Fatal(err)
	}
	if got := store.statusOf("k1"); got != repository.ObjectStatusDeleted {
		t.Errorf("k1 verified absent despite error response: %s", got)
	}
	if got := store.statusOf("k2"); got != repository.ObjectStatusFailed {
		t.Errorf("k2 still exists, want failed: %s", got)
	}
	if store.job.Status == repository.DeletionStatusCompleted {
		t.Error("job with a failed object must stay reclaiming for retry")
	}
}

func TestService_ExistsCheckErrorKeepsObjectFailed(t *testing.T) {
	store := newFakeStore()
	store.addObject("k", repository.ObjectStatusPending)
	st := newFakeStorage("k")
	st.existsErrors["k"] = errors.New("head failed")
	svc := New(store, nil, st)

	if _, err := svc.DeleteAsset(context.Background(), store.job.AssetID); err != nil {
		t.Fatal(err)
	}
	if got := store.statusOf("k"); got != repository.ObjectStatusFailed {
		t.Fatalf("status=%s", got)
	}
}

func TestService_RetryOnlyRetriesUnconfirmedObjects(t *testing.T) {
	store := newFakeStore()
	store.addObject("ok", repository.ObjectStatusPending)
	store.addObject("flaky", repository.ObjectStatusPending)
	st := newFakeStorage("ok", "flaky")
	svc := New(store, nil, st)

	// First pass: "flaky" delete cannot be confirmed.
	st.deleteErrors["flaky"] = errors.New("temporary")
	if _, err := svc.DeleteAsset(context.Background(), store.job.AssetID); err != nil {
		t.Fatal(err)
	}
	if store.statusOf("ok") != repository.ObjectStatusDeleted || store.statusOf("flaky") != repository.ObjectStatusFailed {
		t.Fatalf("ok=%s flaky=%s", store.statusOf("ok"), store.statusOf("flaky"))
	}
	firstOkCalls := st.deleteCount("ok")

	// Second pass (repeat delete / restart) only walks unconfirmed objects.
	delete(st.deleteErrors, "flaky")
	if _, err := svc.DeleteAsset(context.Background(), store.job.AssetID); err != nil {
		t.Fatal(err)
	}
	if store.statusOf("flaky") != repository.ObjectStatusDeleted {
		t.Fatalf("flaky=%s", store.statusOf("flaky"))

	}
	if store.job.Status != repository.DeletionStatusCompleted {
		t.Errorf("job status=%s", store.job.Status)
	}
	if st.deleteCount("ok") != firstOkCalls {
		t.Error("confirmed object must not be retried")
	}
	if store.beginCount != 2 {
		t.Errorf("repeat delete must reuse the same identity, begin calls=%d", store.beginCount)
	}
}

func TestService_NilStorageKeepsManifestPending(t *testing.T) {
	store := newFakeStore()
	store.addObject("k", repository.ObjectStatusPending)
	svc := New(store, nil, nil)

	found, err := svc.DeleteAsset(context.Background(), store.job.AssetID)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if got := store.statusOf("k"); got != repository.ObjectStatusPending {
		t.Errorf("status=%s, want pending", got)
	}
	if store.job.Status != repository.DeletionStatusReclaiming {
		t.Errorf("job status=%s", store.job.Status)
	}
}

func TestService_ResumeAllContinuesFrozenManifest(t *testing.T) {
	store := newFakeStore()
	store.job.Status = repository.DeletionStatusReclaiming // crashed mid-reclaim
	store.addObject("k", repository.ObjectStatusFailed)
	st := newFakeStorage("k")
	svc := New(store, nil, st)

	if err := svc.ResumeAll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if store.statusOf("k") != repository.ObjectStatusDeleted {
		t.Fatalf("status=%s", store.statusOf("k"))
	}
	if store.job.Status != repository.DeletionStatusCompleted {
		t.Errorf("job status=%s", store.job.Status)
	}
}

func TestService_DeleteAttachmentReferenceAware(t *testing.T) {
	t.Run("exclusive key removed", func(t *testing.T) {
		store := newFakeStore()
		attID := uuid.New()
		attStore := &fakeAttachmentStore{key: "exclusive", found: true}
		st := newFakeStorage("exclusive")
		svc := New(store, attStore, st)

		found, err := svc.DeleteAttachment(context.Background(), attID)
		if err != nil || !found {
			t.Fatalf("found=%v err=%v", found, err)
		}
		if len(st.deletedKeys) != 1 || st.deletedKeys[0] != "exclusive" {
			t.Errorf("deleted=%v", st.deletedKeys)
		}
	})

	t.Run("shared key kept", func(t *testing.T) {
		store := newFakeStore()
		store.refs["shared"] = 1
		attStore := &fakeAttachmentStore{key: "shared", found: true}
		st := newFakeStorage("shared")
		svc := New(store, attStore, st)

		found, err := svc.DeleteAttachment(context.Background(), uuid.New())
		if err != nil || !found {
			t.Fatalf("found=%v err=%v", found, err)
		}
		if len(st.deletedKeys) != 0 {
			t.Errorf("shared key deleted: %v", st.deletedKeys)
		}
	})

	t.Run("missing attachment", func(t *testing.T) {
		store := newFakeStore()
		attStore := &fakeAttachmentStore{found: false}
		st := newFakeStorage()
		svc := New(store, attStore, st)

		found, err := svc.DeleteAttachment(context.Background(), uuid.New())
		if err != nil || found {
			t.Fatalf("found=%v err=%v", found, err)
		}
	})
}

type fakeAttachmentStore struct {
	key   string
	found bool
	err   error
}

func (f *fakeAttachmentStore) SoftDelete(ctx context.Context, id uuid.UUID) (string, bool, error) {
	return f.key, f.found, f.err
}
