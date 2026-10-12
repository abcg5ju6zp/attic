// Package deletion orchestrates reference-aware asset deletion and storage
// object reclamation.
//
// The database lifecycle (soft-deleting the asset, claiming its attachments
// and freezing the object key manifest under a stable deletion identity)
// commits before any remote object is touched. Object reclamation then walks
// only the frozen manifest, skips keys still referenced by live records,
// verifies uncertain delete responses against the storage backend, and keeps
// every per-item outcome durable so partial failures are queryable and only
// unconfirmed objects are retried - including after a service restart.
package deletion

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/lmmendes/attic/internal/repository"
)

// DeletionStore persists deletion identities, frozen manifests and per-object
// reclaim state.
type DeletionStore interface {
	BeginDeletion(ctx context.Context, assetID uuid.UUID) (*repository.AssetDeletion, bool, error)
	MarkReclaiming(ctx context.Context, id uuid.UUID) error
	ListActiveDeletions(ctx context.Context) ([]repository.AssetDeletion, error)
	ListUnconfirmedObjects(ctx context.Context, deletionID uuid.UUID) ([]repository.AssetDeletionObject, error)
	CountLiveReferences(ctx context.Context, fileKey string) (int, error)
	MarkObjectOutcome(ctx context.Context, id uuid.UUID, status string, lastError *string) error
	CompleteDeletionIfReady(ctx context.Context, id uuid.UUID) error
}

// AttachmentStore hides attachment rows as part of their lifecycle.
type AttachmentStore interface {
	SoftDelete(ctx context.Context, id uuid.UUID) (fileKey string, found bool, err error)
}

// ObjectStorage deletes objects and reports factual object existence.
type ObjectStorage interface {
	Delete(ctx context.Context, key string) error
	Exists(ctx context.Context, key string) (bool, error)
}

// reclaimInterval is how often the background worker re-attempts frozen
// objects that could not be confirmed deleted.
const reclaimInterval = time.Minute

// reclaimJobTimeout bounds a single background reclaim pass.
const reclaimJobTimeout = 5 * time.Minute

// Service coordinates asset soft-delete and object reclamation.
type Service struct {
	deletions   DeletionStore
	attachments AttachmentStore
	storage     ObjectStorage // nil when no storage backend is configured
	logger      *slog.Logger

	// inFlight prevents concurrent reclaim passes for the same deletion
	// identity within this process; durable state stays the source of truth.
	mu       sync.Mutex
	inFlight map[uuid.UUID]struct{}
}

// New creates a deletion service. storage may be nil when file storage is not
// configured; in that case database deletion still proceeds and object
// reclamation stays pending until storage becomes available.
func New(deletions DeletionStore, attachments AttachmentStore, storage ObjectStorage) *Service {
	return &Service{
		deletions:   deletions,
		attachments: attachments,
		storage:     storage,
		logger:      slog.Default(),
		inFlight:    make(map[uuid.UUID]struct{}),
	}
}

// DeleteAsset freezes the deletion manifest, commits the database lifecycle
// and then reclaims exclusively-owned objects. A missing asset returns
// found=false; a repeat delete rejoins the existing deletion identity. Any
// unconfirmed object remains durably pending for later retry and never
// changes the committed database outcome.
func (s *Service) DeleteAsset(ctx context.Context, assetID uuid.UUID) (found bool, err error) {
	job, found, err := s.deletions.BeginDeletion(ctx, assetID)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	s.reclaim(ctx, job.ID)
	return true, nil
}

// DeleteAttachment hides one explicit attachment and, after the database
// commit, removes its object when nothing live references the key. Storage
// uncertainty is verified against backend facts; storage problems are logged
// and never resurrect the committed attachment lifecycle.
func (s *Service) DeleteAttachment(ctx context.Context, attachmentID uuid.UUID) (found bool, err error) {
	key, found, err := s.attachments.SoftDelete(ctx, attachmentID)
	if err != nil || !found {
		return found, err
	}
	if s.storage != nil {
		s.removeExclusiveObject(ctx, key, "explicit attachment delete")
	}
	return true, nil
}

// ResumeAll continues every unfinished deletion from its frozen manifest. It
// never rescans assets or attachments, so a restart targets exactly the
// objects captured at deletion time.
func (s *Service) ResumeAll(ctx context.Context) error {
	jobs, err := s.deletions.ListActiveDeletions(ctx)
	if err != nil {
		return err
	}
	for _, job := range jobs {
		reclaimCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reclaimJobTimeout)
		s.reclaim(reclaimCtx, job.ID)
		cancel()
	}
	return nil
}

// Run periodically resumes unfinished deletions until ctx is canceled.
func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(reclaimInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.ResumeAll(ctx); err != nil {
				s.logger.Error("failed to resume pending asset deletions", "error", err)
			}
		}
	}
}

// reclaim walks the frozen manifest of one deletion and records each outcome.
func (s *Service) reclaim(ctx context.Context, deletionID uuid.UUID) {
	if !s.enter(deletionID) {
		return
	}
	defer s.leave(deletionID)

	if err := s.deletions.MarkReclaiming(ctx, deletionID); err != nil {
		s.logger.Error("failed to mark deletion reclaiming", "deletion_id", deletionID, "error", err)
		return
	}

	objects, err := s.deletions.ListUnconfirmedObjects(ctx, deletionID)
	if err != nil {
		s.logger.Error("failed to load frozen deletion objects", "deletion_id", deletionID, "error", err)
		return
	}

	for _, object := range objects {
		if ctx.Err() != nil {
			return
		}
		s.reclaimObject(ctx, object)
	}

	if err := s.deletions.CompleteDeletionIfReady(ctx, deletionID); err != nil {
		s.logger.Error("failed to finalize asset deletion", "deletion_id", deletionID, "error", err)
	}
}

// reclaimObject processes a single frozen object.
func (s *Service) reclaimObject(ctx context.Context, object repository.AssetDeletionObject) {
	// No storage backend: keep the item pending so a later restart with
	// storage configured can reclaim the exact same manifest.
	if s.storage == nil {
		return
	}

	refs, err := s.deletions.CountLiveReferences(ctx, object.FileKey)
	if err != nil {
		detail := fmt.Sprintf("reference check failed: %v", err)
		s.recordOutcome(ctx, object, repository.ObjectStatusFailed, &detail)
		s.logger.Warn("object reclaim not confirmed",
			"deletion_id", object.DeletionID, "file_key", object.FileKey, "detail", detail)
		return
	}
	if refs > 0 {
		detail := fmt.Sprintf("retained: %d live reference(s) still use this object key", refs)
		if err := s.deletions.MarkObjectOutcome(ctx, object.ID, repository.ObjectStatusRetained, &detail); err != nil {
			s.logger.Error("failed to record retained object",
				"deletion_id", object.DeletionID, "file_key", object.FileKey, "error", err)
		}
		return
	}

	status, detail := s.deleteAndVerify(ctx, object.FileKey, fmt.Sprintf("asset deletion %s", object.DeletionID))
	s.recordOutcome(ctx, object, status, detail)
}

// removeExclusiveObject deletes an object that no longer has any live
// reference and logs the verified result. It is used by the explicit single
// attachment path, whose durable lifecycle is already committed.
func (s *Service) removeExclusiveObject(ctx context.Context, key, scope string) {
	refs, err := s.deletions.CountLiveReferences(ctx, key)
	if err != nil {
		s.logger.Error("object reference check failed", "scope", scope, "file_key", key, "error", err)
		return
	}
	if refs > 0 {
		s.logger.Info("keeping object referenced by another record", "scope", scope, "file_key", key, "references", refs)
		return
	}
	s.deleteAndVerify(ctx, key, scope)
}

// deleteAndVerify issues the delete but trusts the existence fact rather than
// the delete response. Returns the per-item status and an optional detail.
func (s *Service) deleteAndVerify(ctx context.Context, key, scope string) (string, *string) {
	deleteErr := s.storage.Delete(ctx, key)
	exists, existsErr := s.storage.Exists(ctx, key)
	switch {
	case existsErr != nil:
		detail := fmt.Sprintf("fact check failed: %v", existsErr)
		if deleteErr != nil {
			detail = fmt.Sprintf("delete error: %v; %s", deleteErr, detail)
		}
		s.logger.Warn("object fact check failed after delete", "scope", scope, "file_key", key, "detail", detail)
		return repository.ObjectStatusFailed, &detail
	case exists:
		detail := "delete unconfirmed: object still exists"
		if deleteErr != nil {
			detail = fmt.Sprintf("delete error: %v; %s", deleteErr, detail)
		}
		s.logger.Warn("object still exists after delete", "scope", scope, "file_key", key, "detail", detail)
		return repository.ObjectStatusFailed, &detail
	default:
		if deleteErr != nil {
			s.logger.Warn("delete response was uncertain but object is gone",
				"scope", scope, "file_key", key, "delete_error", deleteErr)
		}
		return repository.ObjectStatusDeleted, nil
	}
}

func (s *Service) recordOutcome(ctx context.Context, object repository.AssetDeletionObject, status string, detail *string) {
	if err := s.deletions.MarkObjectOutcome(ctx, object.ID, status, detail); err != nil {
		s.logger.Error("failed to record object reclaim outcome",
			"deletion_id", object.DeletionID, "file_key", object.FileKey, "status", status, "error", err)
	}
}

func (s *Service) enter(id uuid.UUID) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inFlight[id]; ok {
		return false
	}
	s.inFlight[id] = struct{}{}
	return true
}

func (s *Service) leave(id uuid.UUID) {
	s.mu.Lock()
	delete(s.inFlight, id)
	s.mu.Unlock()
}
