package repository

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/lmmendes/attic/internal/domain"
	"github.com/lmmendes/attic/internal/testutil"
)

func setupDeletionTest(t *testing.T) (context.Context, *testutil.Fixtures, *DeletionRepository, *AttachmentRepository, *AssetRepository) {
	t.Helper()
	ctx := context.Background()
	if err := testDB.TruncateAll(ctx); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	return ctx,
		testutil.NewFixtures(testDB.Pool),
		NewDeletionRepository(testDB.Pool),
		NewAttachmentRepository(testDB.Pool),
		NewAssetRepository(testDB.Pool)
}

func createAssetWithOrg(t *testing.T, ctx context.Context, f *testutil.Fixtures, name string) (*domain.Organization, *domain.Asset) {
	t.Helper()
	org, err := f.CreateOrganization(ctx, "Del Org "+name)
	if err != nil {
		t.Fatal(err)
	}
	cat, err := f.CreateCategory(ctx, org.ID, "Cat "+name, nil)
	if err != nil {
		t.Fatal(err)
	}
	asset, err := f.CreateAsset(ctx, org.ID, cat.ID, name)
	if err != nil {
		t.Fatal(err)
	}
	return org, asset
}

func Test_Deletion_Begin_FreezesManifestAndCommitsLifecycle(t *testing.T) {
	ctx, f, del, atts, _ := setupDeletionTest(t)
	_, asset := createAssetWithOrg(t, ctx, f, "Camera")

	a1, err := f.CreateAttachment(ctx, asset.ID, "a.jpg", "u1/a.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.CreateAttachment(ctx, asset.ID, "a-dup.jpg", "u1/a.jpg"); err != nil {
		t.Fatal(err)
	}
	if _, err := f.CreateAttachment(ctx, asset.ID, "doc.pdf", "u2/doc.pdf"); err != nil {
		t.Fatal(err)
	}
	if _, err := testDB.Pool.Exec(ctx,
		`UPDATE assets SET main_attachment_id=$2 WHERE id=$1`, asset.ID, a1.ID); err != nil {
		t.Fatal(err)
	}

	job, found, err := del.BeginDeletion(ctx, asset.ID)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if job.ID == uuid.Nil || job.Status != DeletionStatusPending {
		t.Fatalf("unexpected job: %+v", job)
	}
	if job.AttachmentCount != 3 {
		t.Errorf("attachment count=%d, want 3", job.AttachmentCount)
	}
	if job.FrozenMainAttachmentID == nil || *job.FrozenMainAttachmentID != a1.ID {
		t.Errorf("frozen main attachment mismatch: %v", job.FrozenMainAttachmentID)
	}

	// Distinct object keys, not attachment rows, define the reclaim manifest.
	objects, err := del.ListUnconfirmedObjects(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]bool{}
	for _, o := range objects {
		keys[o.FileKey] = true
		if o.Status != ObjectStatusPending {
			t.Errorf("object %s status=%s", o.FileKey, o.Status)
		}
	}
	if len(keys) != 2 || !keys["u1/a.jpg"] || !keys["u2/doc.pdf"] {
		t.Errorf("manifest keys=%v", keys)
	}

	// Asset lifecycle committed and main image detached.
	var deletedAt *time.Time
	var mainID *uuid.UUID
	if err := testDB.Pool.QueryRow(ctx,
		`SELECT deleted_at, main_attachment_id FROM assets WHERE id=$1`, asset.ID,
	).Scan(&deletedAt, &mainID); err != nil {
		t.Fatal(err)
	}
	if deletedAt == nil {
		t.Error("asset should be soft-deleted")
	}
	if mainID != nil {
		t.Error("main attachment should be detached")
	}

	// Attachments claimed by the deletion generation and hidden.
	var claimed int
	var nullClaims int
	if err := testDB.Pool.QueryRow(ctx, `
		SELECT COUNT(*), COUNT(*) FILTER (WHERE deleted_at IS NULL OR deletion_id <> $2)
		FROM attachments WHERE asset_id=$1`, asset.ID, job.ID,
	).Scan(&claimed, &nullClaims); err != nil {
		t.Fatal(err)
	}
	if claimed != 3 || nullClaims != 0 {
		t.Errorf("claimed=%d unclaimed=%d, want 3/0", claimed, nullClaims)
	}
	if got, err := atts.GetByID(ctx, a1.ID); err != nil || got != nil {
		t.Errorf("claimed attachment must be hidden from GetByID: got=%v err=%v", got, err)
	}

	// Live reference counting sees no exclusive references anymore.
	if refs, err := del.CountLiveReferences(ctx, "u2/doc.pdf"); err != nil || refs != 0 {
		t.Errorf("doc.pdf refs=%d err=%v, want 0", refs, err)
	}
}

func Test_Deletion_Begin_MissingAssetIsDeterministic(t *testing.T) {
	ctx, _, del, _, _ := setupDeletionTest(t)
	job, found, err := del.BeginDeletion(ctx, uuid.New())
	if err != nil || found || job != nil {
		t.Fatalf("job=%v found=%v err=%v", job, found, err)
	}
}

func Test_Deletion_Begin_NoAttachmentsCompletes(t *testing.T) {
	ctx, f, del, _, _ := setupDeletionTest(t)
	_, asset := createAssetWithOrg(t, ctx, f, "Empty")

	job, found, err := del.BeginDeletion(ctx, asset.ID)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	objects, err := del.ListUnconfirmedObjects(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(objects) != 0 {
		t.Errorf("expected empty manifest, got %d", len(objects))
	}
	if err := del.MarkReclaiming(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := del.CompleteDeletionIfReady(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	completed, err := del.GetDeletion(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != DeletionStatusCompleted || completed.CompletedAt == nil {
		t.Errorf("completed=%+v", completed)
	}
}

func Test_Deletion_RepeatUsesSameIdentity(t *testing.T) {
	ctx, f, del, _, _ := setupDeletionTest(t)
	_, asset := createAssetWithOrg(t, ctx, f, "Repeat")
	if _, err := f.CreateAttachment(ctx, asset.ID, "x.txt", "u/x.txt"); err != nil {
		t.Fatal(err)
	}

	first, found, err := del.BeginDeletion(ctx, asset.ID)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	second, found, err := del.BeginDeletion(ctx, asset.ID)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if first.ID != second.ID {
		t.Fatalf("repeat delete created a new identity: %s vs %s", first.ID, second.ID)
	}
}

func Test_Deletion_ConcurrentBeginSharesIdentity(t *testing.T) {
	ctx, f, del, _, _ := setupDeletionTest(t)
	_, asset := createAssetWithOrg(t, ctx, f, "Concurrent")
	if _, err := f.CreateAttachment(ctx, asset.ID, "x.txt", "u/x.txt"); err != nil {
		t.Fatal(err)
	}

	const n = 8
	var wg sync.WaitGroup
	ids := make([]uuid.UUID, n)
	errs := make([]error, n)
	start := make(chan struct{})
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			<-start
			job, _, err := del.BeginDeletion(ctx, asset.ID)
			if err != nil {
				errs[i] = err
				return
			}
			ids[i] = job.ID
		}(i)
	}
	close(start)
	wg.Wait()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 1; i < n; i++ {
		if ids[i] != ids[0] {
			t.Fatalf("deletion identities diverged: %s vs %s", ids[0], ids[i])
		}
	}
	var count int
	if err := testDB.Pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM asset_deletions WHERE asset_id=$1`, asset.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("expected exactly one deletion, got %d", count)
	}
}

func Test_Deletion_LegacySoftDeletedAssetReconciled(t *testing.T) {
	ctx, f, del, atts, _ := setupDeletionTest(t)
	_, asset := createAssetWithOrg(t, ctx, f, "Legacy")
	att, err := f.CreateAttachment(ctx, asset.ID, "old.txt", "u/old.txt")
	if err != nil {
		t.Fatal(err)
	}
	// Simulate an old-version deletion: asset hidden, attachment rows left live.
	if _, err := testDB.Pool.Exec(ctx, `UPDATE assets SET deleted_at=NOW() WHERE id=$1`, asset.ID); err != nil {
		t.Fatal(err)
	}

	job, found, err := del.BeginDeletion(ctx, asset.ID)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if job.AttachmentCount != 1 {
		t.Errorf("attachment count=%d", job.AttachmentCount)
	}
	objects, err := del.ListUnconfirmedObjects(ctx, job.ID)
	if err != nil || len(objects) != 1 || objects[0].FileKey != "u/old.txt" {
		t.Fatalf("objects=%v err=%v", objects, err)
	}
	if hidden, err := atts.GetByID(ctx, att.ID); err != nil || hidden != nil {
		t.Errorf("legacy attachment should be claimed, got=%v err=%v", hidden, err)
	}
}

func Test_Deletion_ReferenceCounting(t *testing.T) {
	ctx, f, del, atts, assets := setupDeletionTest(t)
	_, assetA := createAssetWithOrg(t, ctx, f, "A")
	_, assetB := createAssetWithOrg(t, ctx, f, "B")

	aOnA, err := f.CreateAttachment(ctx, assetA.ID, "a.bin", "shared/key.bin")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.CreateAttachment(ctx, assetB.ID, "b.bin", "shared/key.bin"); err != nil {
		t.Fatal(err)
	}
	bMain, err := f.CreateAttachment(ctx, assetB.ID, "m.png", "u/main.png")
	if err != nil {
		t.Fatal(err)
	}
	if updated, err := assets.SetMainAttachment(ctx, assetB.ID, &bMain.ID); err != nil || !updated {
		t.Fatalf("set main: updated=%v err=%v", updated, err)
	}

	// Freeze A: its attachment is claimed, but the key is shared with live B.
	job, found, err := del.BeginDeletion(ctx, assetA.ID)
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	if refs, err := del.CountLiveReferences(ctx, "shared/key.bin"); err != nil || refs != 1 {
		t.Fatalf("shared refs=%d err=%v, want 1", refs, err)
	}
	if refs, err := del.CountLiveReferences(ctx, "u/main.png"); err != nil || refs != 2 {
		t.Fatalf("main image refs=%d err=%v, want 2 (attachment + main slot)", refs, err)
	}

	// The claimed attachment on A cannot be hidden again by the explicit
	// single-attachment path.
	if _, found, err := atts.SoftDelete(ctx, aOnA.ID); err != nil || found {
		t.Fatalf("claimed attachment must not be re-deletable: found=%v err=%v", found, err)
	}

	// B removes its attachment: the shared key is now exclusively A's frozen
	// object even though A's own row still physically exists.
	if key, found, err := atts.SoftDelete(ctx, mustAttachmentID(ctx, t, assetB.ID, "b.bin")); err != nil || !found || key != "shared/key.bin" {
		t.Fatalf("soft delete B attachment: key=%s found=%v err=%v", key, found, err)
	}
	if refs, err := del.CountLiveReferences(ctx, "shared/key.bin"); err != nil || refs != 0 {
		t.Fatalf("shared refs=%d err=%v, want 0", refs, err)
	}
	if job.Status == DeletionStatusCompleted {
		t.Error("job should not auto-complete without reclaim pass")
	}
}

func mustAttachmentID(ctx context.Context, t *testing.T, assetID uuid.UUID, fileName string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := testDB.Pool.QueryRow(ctx,
		`SELECT id FROM attachments WHERE asset_id=$1 AND file_name=$2 AND deleted_at IS NULL`,
		assetID, fileName).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func Test_Deletion_UploadInterleaveArbitratedByGeneration(t *testing.T) {
	ctx, f, del, atts, _ := setupDeletionTest(t)

	// Regardless of scheduling, the arbitration must leave one consistent
	// generation: either the upload joins the frozen manifest or it is
	// rejected. A live attachment must never outlive the freeze with its key
	// missing from the manifest.
	for i := 0; i < 12; i++ {
		if err := testDB.TruncateAll(ctx); err != nil {
			t.Fatal(err)
		}
		org, err := f.CreateOrganization(ctx, "Interleave")
		if err != nil {
			t.Fatal(err)
		}
		cat, err := f.CreateCategory(ctx, org.ID, "Cat", nil)
		if err != nil {
			t.Fatal(err)
		}
		asset, err := f.CreateAsset(ctx, org.ID, cat.ID, "Item")
		if err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		var uploadErr error
		var job *AssetDeletion
		var found bool
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			job, found, uploadErr = del.BeginDeletion(ctx, asset.ID)
		}()
		go func() {
			defer wg.Done()
			<-start
			uploadErr = atts.Create(ctx, &domain.Attachment{
				AssetID:  asset.ID,
				FileKey:  "u/concurrent.txt",
				FileName: "concurrent.txt",
				FileSize: 3,
			})
		}()
		close(start)
		wg.Wait()
		if !found || job == nil {
			t.Fatalf("iteration %d: deletion did not run: %v", i, uploadErr)
		}

		var liveRows int
		var claimedRows int
		var wrongGeneration int
		if err := testDB.Pool.QueryRow(ctx, `
			SELECT
				COUNT(*) FILTER (WHERE deleted_at IS NULL),
				COUNT(*) FILTER (WHERE deleted_at IS NOT NULL AND deletion_id = $2),
				COUNT(*) FILTER (WHERE deletion_id IS DISTINCT FROM $2)
			FROM attachments WHERE asset_id = $1`, asset.ID, job.ID,
		).Scan(&liveRows, &claimedRows, &wrongGeneration); err != nil {
			t.Fatal(err)
		}
		if liveRows != 0 || wrongGeneration != 0 || claimedRows > 1 {
			t.Fatalf("iteration %d: live=%d claimed=%d wrongGen=%d", i, liveRows, claimedRows, wrongGeneration)
		}

		// If the upload joined the manifest, its key must be present exactly
		// once in the frozen object set; if rejected, the manifest is empty.
		var manifestKeys int
		if err := testDB.Pool.QueryRow(ctx,
			`SELECT COUNT(*) FROM asset_deletion_objects WHERE deletion_id=$1 AND file_key=$2`,
			job.ID, "u/concurrent.txt").Scan(&manifestKeys); err != nil {
			t.Fatal(err)
		}
		if claimedRows == 1 && manifestKeys != 1 {
			t.Fatalf("iteration %d: claimed upload missing from manifest (%d)", i, manifestKeys)
		}
		if claimedRows == 0 && manifestKeys != 0 {
			t.Fatalf("iteration %d: manifest contains key of rejected upload", i)
		}
	}
}

func Test_Deletion_MainImageSwitchInterleaveArbitrated(t *testing.T) {
	ctx, f, del, _, assets := setupDeletionTest(t)

	for i := 0; i < 12; i++ {
		if err := testDB.TruncateAll(ctx); err != nil {
			t.Fatal(err)
		}
		_, asset := createAssetWithOrg(t, ctx, f, "Switch")
		att, err := f.CreateAttachment(ctx, asset.ID, "m.png", "u/m.png")
		if err != nil {
			t.Fatal(err)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		var updated bool
		var job *AssetDeletion
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			job, _, _ = del.BeginDeletion(ctx, asset.ID)
		}()
		go func() {
			defer wg.Done()
			<-start
			attID := att.ID
			updated, _ = assets.SetMainAttachment(ctx, asset.ID, &attID)
		}()
		close(start)
		wg.Wait()

		var deletedAt *time.Time
		var mainID *uuid.UUID
		if err := testDB.Pool.QueryRow(ctx,
			`SELECT deleted_at, main_attachment_id FROM assets WHERE id=$1`, asset.ID).Scan(&deletedAt, &mainID); err != nil {
			t.Fatal(err)
		}
		if deletedAt == nil || mainID != nil {
			t.Fatalf("iteration %d: deleted=%v main=%v", i, deletedAt, mainID)
		}
		// Switch-before-freeze: frozen main = att, update landed.
		// Freeze-before-switch: frozen main = nil, update affected no rows.
		if updated != (job.FrozenMainAttachmentID != nil && *job.FrozenMainAttachmentID == att.ID) {
			t.Fatalf("iteration %d: updated=%v frozenMain=%v", i, updated, job.FrozenMainAttachmentID)
		}
	}
}

func Test_Attachment_Create_RejectedOnDeletedAsset(t *testing.T) {
	ctx, f, _, atts, _ := setupDeletionTest(t)
	_, asset := createAssetWithOrg(t, ctx, f, "Deleting")

	if err := atts.Create(ctx, &domain.Attachment{
		AssetID:  asset.ID,
		FileKey:  "u/ok.txt",
		FileName: "ok.txt",
		FileSize: 1,
	}); err != nil {
		t.Fatalf("create on live asset: %v", err)
	}

	if _, err := testDB.Pool.Exec(ctx, `UPDATE assets SET deleted_at=NOW() WHERE id=$1`, asset.ID); err != nil {
		t.Fatal(err)
	}
	err := atts.Create(ctx, &domain.Attachment{
		AssetID:  asset.ID,
		FileKey:  "u/new.txt",
		FileName: "new.txt",
		FileSize: 1,
	})
	if !errors.Is(err, ErrAssetNotLive) {
		t.Fatalf("want ErrAssetNotLive, got %v", err)
	}
}

func Test_Attachment_SoftDelete_HidesAndDetachesMain(t *testing.T) {
	ctx, f, _, atts, assets := setupDeletionTest(t)
	_, asset := createAssetWithOrg(t, ctx, f, "Single")
	att, err := f.CreateAttachment(ctx, asset.ID, "m.jpg", "u/m.jpg")
	if err != nil {
		t.Fatal(err)
	}
	if updated, err := assets.SetMainAttachment(ctx, asset.ID, &att.ID); err != nil || !updated {
		t.Fatalf("set main: updated=%v err=%v", updated, err)
	}

	key, found, err := atts.SoftDelete(ctx, att.ID)
	if err != nil || !found || key != "u/m.jpg" {
		t.Fatalf("key=%s found=%v err=%v", key, found, err)
	}
	if got, err := atts.GetByID(ctx, att.ID); err != nil || got != nil {
		t.Errorf("soft-deleted attachment should be hidden: got=%v err=%v", got, err)
	}
	mainID, err := assets.GetMainAttachmentID(ctx, asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	if mainID != nil {
		t.Errorf("main pointer should be cleared, got %s", *mainID)
	}
	if _, found, err := atts.SoftDelete(ctx, att.ID); err != nil || found {
		t.Fatalf("repeat soft delete: found=%v err=%v", found, err)
	}
}

func Test_Deletion_ObjectOutcomeLifecycle(t *testing.T) {
	ctx, f, del, _, _ := setupDeletionTest(t)
	_, asset := createAssetWithOrg(t, ctx, f, "Outcomes")
	if _, err := f.CreateAttachment(ctx, asset.ID, "x", "u/x"); err != nil {
		t.Fatal(err)
	}
	job, _, err := del.BeginDeletion(ctx, asset.ID)
	if err != nil {
		t.Fatal(err)
	}
	objects, err := del.ListUnconfirmedObjects(ctx, job.ID)
	if err != nil || len(objects) != 1 {
		t.Fatalf("objects=%v err=%v", objects, err)
	}
	obj := objects[0]

	// Failure keeps the item queryable and the job open.
	detail := "boom"
	if err := del.MarkObjectOutcome(ctx, obj.ID, ObjectStatusFailed, &detail); err != nil {
		t.Fatal(err)
	}
	if err := del.MarkReclaiming(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if err := del.CompleteDeletionIfReady(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if d, _ := del.GetDeletion(ctx, job.ID); d.Status == DeletionStatusCompleted {
		t.Fatal("failed object must keep the deletion open")
	}
	pending, err := del.ListUnconfirmedObjects(ctx, job.ID)
	if err != nil || len(pending) != 1 || pending[0].Attempts != 1 || pending[0].LastError == nil {
		t.Fatalf("pending=%+v err=%v", pending, err)
	}

	// Confirmation finalizes.
	if err := del.MarkObjectOutcome(ctx, obj.ID, ObjectStatusDeleted, nil); err != nil {
		t.Fatal(err)
	}
	if err := del.CompleteDeletionIfReady(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	if d, _ := del.GetDeletion(ctx, job.ID); d.Status != DeletionStatusCompleted {
		t.Fatalf("status=%s", d.Status)
	}
	if more, err := del.ListUnconfirmedObjects(ctx, job.ID); err != nil || len(more) != 0 {
		t.Fatalf("confirmed objects are not retried: %v err=%v", more, err)
	}
}
