package handler

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/lmmendes/attic/internal/database"
	"github.com/lmmendes/attic/internal/domain"
	"github.com/lmmendes/attic/internal/repository"
)

// FileStorage defines the interface for file storage backends
type FileStorage interface {
	Upload(ctx context.Context, filename string, contentType string, body io.Reader) (string, error)
	UploadWithKey(ctx context.Context, key string, contentType string, body io.Reader) error
	GetPresignedURL(ctx context.Context, key string, expiry time.Duration) (string, error)
	Delete(ctx context.Context, key string) error
}

// AttachmentPublisher drives recoverable attachment upload publishes.
type AttachmentPublisher interface {
	Register(ctx context.Context, in repository.RegisterUploadInput) (*domain.AttachmentUpload, error)
	PutObject(ctx context.Context, id uuid.UUID, body io.Reader) (*domain.AttachmentUpload, error)
	Commit(ctx context.Context, id uuid.UUID) (*domain.AttachmentUpload, *domain.Attachment, error)
	Get(ctx context.Context, id uuid.UUID) (*domain.AttachmentUpload, error)
	AbortAttachment(ctx context.Context, attachmentID uuid.UUID) (assetID uuid.UUID, fileKey string, err error)
}

// Repositories holds all repository implementations
type Repositories struct {
	Organizations domain.OrganizationRepository
	Users         *repository.UserRepository
	Categories    *repository.CategoryRepository
	Collections   *repository.CollectionRepository
	Locations     *repository.LocationRepository
	Conditions    *repository.ConditionRepository
	Assets        *repository.AssetRepository
	Warranties    *repository.WarrantyRepository
	Attachments   *repository.AttachmentRepository
	AssetEvents   *repository.AssetEventRepository
	Attributes    *repository.AttributeRepository
	SavedFilters  *repository.SavedFilterRepository
	Tags          *repository.TagRepository
}

// Handler holds dependencies for HTTP handlers
type Handler struct {
	db        *database.DB
	repos     *Repositories
	storage   FileStorage
	publisher AttachmentPublisher
	orgID     uuid.UUID // Default organization ID
}

// New creates a new Handler
func New(db *database.DB, repos *Repositories, storage FileStorage, defaultOrgID uuid.UUID) *Handler {
	return &Handler{
		db:      db,
		repos:   repos,
		storage: storage,
		orgID:   defaultOrgID,
	}
}

// SetAttachmentPublisher wires the recoverable upload publisher. It must be
// configured for attachment upload/deletion endpoints to be available.
func (h *Handler) SetAttachmentPublisher(p AttachmentPublisher) {
	h.publisher = p
}

// Health returns server health status
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// Ready returns database readiness status
func (h *Handler) Ready(w http.ResponseWriter, r *http.Request) {
	if err := h.db.Health(r.Context()); err != nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "not ready", "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ready"})
}

// Helper functions

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func parseUUID(r *http.Request, param string) (uuid.UUID, error) {
	return uuid.Parse(chi.URLParam(r, param))
}

func decodeJSON(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}
