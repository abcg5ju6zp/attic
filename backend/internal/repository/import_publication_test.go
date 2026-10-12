package repository

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lmmendes/attic/internal/domain"
)

func newImportTestOrg(ctx context.Context, t *testing.T) *domain.Organization {
	t.Helper()
	org := &domain.Organization{Name: "Import publications"}
	if err := NewOrganizationRepository(testDB.Pool).Create(ctx, org); err != nil {
		t.Fatal(err)
	}
	return org
}

func newImportTestAsset(ctx context.Context, t *testing.T, orgID uuid.UUID, name string) *domain.Asset {
	t.Helper()
	asset := &domain.Asset{
		OrganizationID: orgID,
		Name:           name,
		Quantity:       1,
		Attributes:     []byte("{}"),
	}
	if err := NewAssetRepository(testDB.Pool).Create(ctx, asset); err != nil {
		t.Fatal(err)
	}
	return asset
}

func TestImportPublicationIdentityCreateIfAbsent(t *testing.T) {
	ctx := context.Background()
	if err := testDB.TruncateAll(ctx); err != nil {
		t.Fatal(err)
	}
	org := newImportTestOrg(ctx, t)
	pubs := NewImportPublicationRepository(testDB.Pool)

	makeCandidate := func() *domain.ImportPublication {
		return &domain.ImportPublication{
			OrganizationID:  org.ID,
			PluginID:        "google_books",
			ExternalID:      "book-1",
			Payload:         []byte(`{"name":"Book"}`),
			ExternalVersion: "v1",
		}
	}

	created, err := pubs.CreateIfAbsent(ctx, makeCandidate())
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("first insert should create the publication")
	}

	competing, err := pubs.CreateIfAbsent(ctx, makeCandidate())
	if err != nil {
		t.Fatal(err)
	}
	if competing {
		t.Fatal("concurrent insert for the same identity must not create a second row")
	}

	got, err := pubs.GetByIdentity(ctx, org.ID, "google_books", "book-1")
	if err != nil {
		t.Fatal(err)
	}
	if got == nil || got.Status != domain.ImportPublicationPending || got.Generation != 1 {
		t.Fatalf("unexpected publication: %#v", got)
	}
}

func TestImportPublicationLeaseAndStagesAreGenerationFenced(t *testing.T) {
	ctx := context.Background()
	if err := testDB.TruncateAll(ctx); err != nil {
		t.Fatal(err)
	}
	org := newImportTestOrg(ctx, t)
	pubs := NewImportPublicationRepository(testDB.Pool)
	pub := &domain.ImportPublication{
		OrganizationID:  org.ID,
		PluginID:        "tmdb",
		ExternalID:      "movie-9",
		Payload:         []byte(`{"name":"Movie"}`),
		ExternalVersion: "v1",
	}
	if _, err := pubs.CreateIfAbsent(ctx, pub); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	ownerA := uuid.New()
	ownerB := uuid.New()

	acquired, err := pubs.AcquireLease(ctx, pub.ID, 1, ownerA, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if !acquired {
		t.Fatal("owner A should acquire the lease")
	}
	acquired, err = pubs.AcquireLease(ctx, pub.ID, 1, ownerB, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if acquired {
		t.Fatal("owner B must not take a live lease")
	}

	asset := newImportTestAsset(ctx, t, org.ID, "Movie")
	attached, err := pubs.AttachAsset(ctx, pub.ID, 1, asset.ID)
	if err != nil || !attached {
		t.Fatalf("attach asset: attached=%v err=%v", attached, err)
	}
	attached, err = pubs.AttachAsset(ctx, pub.ID, 1, uuid.New())
	if err != nil || attached {
		t.Fatalf("asset attachment must be one-shot: attached=%v err=%v", attached, err)
	}
	attached, err = pubs.AttachCoverObject(ctx, pub.ID, 1, "key-1", "cover.png", "image/png", 10)
	if err != nil || !attached {
		t.Fatalf("attach cover object: attached=%v err=%v", attached, err)
	}
	attached, err = pubs.AttachCoverObject(ctx, pub.ID, 1, "key-2", "cover.png", "image/png", 10)
	if err != nil || attached {
		t.Fatalf("first cover object must win: attached=%v err=%v", attached, err)
	}

	// A stale owner from generation 1 is fully fenced after a bump.
	next, ok, err := pubs.BumpGeneration(ctx, pub.ID, 1, []byte(`{"name":"Movie v2"}`), "v2")
	if err != nil || !ok {
		t.Fatalf("bump generation: ok=%v err=%v", ok, err)
	}
	if next.Generation != 2 || next.AssetID != nil || next.CoverFileKey != nil || next.Status != domain.ImportPublicationPending {
		t.Fatalf("new generation should start clean: %#v", next)
	}
	acquired, err = pubs.AcquireLease(ctx, pub.ID, 1, ownerA, now.Add(time.Minute))
	if err != nil || acquired {
		t.Fatalf("stale generation must not acquire a lease: acquired=%v err=%v", acquired, err)
	}
	attached, err = pubs.AttachAsset(ctx, pub.ID, 1, asset.ID)
	if err != nil || attached {
		t.Fatalf("stale generation must not attach assets: attached=%v err=%v", attached, err)
	}
	if err := pubs.ReleaseLease(ctx, pub.ID, 1, "late"); err != nil {
		t.Fatal(err)
	}
	latest, err := pubs.GetByID(ctx, pub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if latest.Status != domain.ImportPublicationPending {
		t.Fatalf("stale release must not touch generation 2: %#v", latest.Status)
	}

	// The new generation can lease and finish.
	acquired, err = pubs.AcquireLease(ctx, pub.ID, 2, ownerB, now.Add(time.Minute))
	if err != nil || !acquired {
		t.Fatalf("new generation should lease: acquired=%v err=%v", acquired, err)
	}
	if err := pubs.MarkPublished(ctx, pub.ID, 2); err != nil {
		t.Fatal(err)
	}
	latest, err = pubs.GetByID(ctx, pub.ID)
	if err != nil {
		t.Fatal(err)
	}
	if latest.Status != domain.ImportPublicationPublished || latest.PublishedAt == nil {
		t.Fatalf("publication should be published: %#v", latest)
	}
}

func TestImportPublicationExpireStaleLeasesAndOrphanSweep(t *testing.T) {
	ctx := context.Background()
	if err := testDB.TruncateAll(ctx); err != nil {
		t.Fatal(err)
	}
	org := newImportTestOrg(ctx, t)
	pubs := NewImportPublicationRepository(testDB.Pool)

	stale := &domain.ImportPublication{
		OrganizationID: org.ID, PluginID: "bgg", ExternalID: "game-1",
		Payload: []byte(`{}`), ExternalVersion: "v1",
	}
	if _, err := pubs.CreateIfAbsent(ctx, stale); err != nil {
		t.Fatal(err)
	}
	if _, err := pubs.AcquireLease(ctx, stale.ID, 1, uuid.New(), time.Now().Add(-time.Minute)); err != nil {
		t.Fatal(err)
	}
	live := &domain.ImportPublication{
		OrganizationID: org.ID, PluginID: "bgg", ExternalID: "game-2",
		Payload: []byte(`{}`), ExternalVersion: "v1",
	}
	if _, err := pubs.CreateIfAbsent(ctx, live); err != nil {
		t.Fatal(err)
	}
	if _, err := pubs.AcquireLease(ctx, live.ID, 1, uuid.New(), time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	expired, err := pubs.ExpireStaleLeases(ctx, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if expired != 1 {
		t.Fatalf("expected one expired lease, got %d", expired)
	}
	staleRow, _ := pubs.GetByID(ctx, stale.ID)
	liveRow, _ := pubs.GetByID(ctx, live.ID)
	if staleRow.Status != domain.ImportPublicationPending {
		t.Fatal("stale lease should reopen as pending")
	}
	if liveRow.Status != domain.ImportPublicationPublishing {
		t.Fatal("live lease must not be expired")
	}

	// An uploaded cover object on a publication that never reached asset
	// creation is reported for cleanup.
	orphan := &domain.ImportPublication{
		OrganizationID: org.ID, PluginID: "igdb", ExternalID: "ig-1",
		Payload: []byte(`{}`), ExternalVersion: "v1",
	}
	if _, err := pubs.CreateIfAbsent(ctx, orphan); err != nil {
		t.Fatal(err)
	}
	if _, err := pubs.AttachCoverObject(ctx, orphan.ID, 1, "orphan-key", "c.png", "image/png", 3); err != nil {
		t.Fatal(err)
	}
	list, err := pubs.ListUnreferencedCoverObjects(ctx, org.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].ID != orphan.ID || *list[0].CoverFileKey != "orphan-key" {
		t.Fatalf("unexpected orphan list: %#v", list)
	}
	if err := pubs.ClearCoverObject(ctx, orphan.ID, 1); err != nil {
		t.Fatal(err)
	}
	list, err = pubs.ListUnreferencedCoverObjects(ctx, org.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 0 {
		t.Fatalf("expected cleanup to clear orphans, got %#v", list)
	}

	recoverable, err := pubs.ListRecoverable(ctx, org.ID, 100)
	if err != nil {
		t.Fatal(err)
	}
	var ids []uuid.UUID
	for _, pub := range recoverable {
		ids = append(ids, pub.ID)
	}
	if len(ids) != 2 {
		t.Fatalf("expected stale and orphan publications pending, got %v", ids)
	}
}
