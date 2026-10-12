package handler

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/lmmendes/attic/internal/domain"
	"github.com/lmmendes/attic/internal/repository"
)

const maxUploadSize = 50 * 1024 * 1024 // 50MB

// uploadIDHeader carries the stable publish identity of an upload. Retrying a
// request with the same identity resumes the original publish instead of
// creating a duplicate object.
const uploadIDHeader = "X-Upload-ID"

type AttachmentResponse struct {
	domain.Attachment
	URL string `json:"url,omitempty"`
}

// UploadStatusResponse describes the current publish state of one upload
// identity, allowing clients to decide whether the original upload published
// after a lost response, cancelled request or process restart.
type UploadStatusResponse struct {
	*domain.AttachmentUpload
	Attachment *domain.Attachment `json:"attachment,omitempty"`
}

func (h *Handler) ListAttachments(w http.ResponseWriter, r *http.Request) {
	assetID, err := parseUUID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid asset ID")
		return
	}

	attachments, err := h.repos.Attachments.ListByAsset(r.Context(), assetID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to list attachments")
		return
	}

	if attachments == nil {
		attachments = []domain.Attachment{}
	}

	writeJSON(w, http.StatusOK, attachments)
}

func (h *Handler) UploadAttachment(w http.ResponseWriter, r *http.Request) {
	assetID, err := parseUUID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid asset ID")
		return
	}

	// Check if asset exists
	asset, err := h.repos.Assets.GetByID(r.Context(), assetID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to check asset")
		return
	}
	if asset == nil {
		writeError(w, http.StatusNotFound, "asset not found")
		return
	}

	// Limit upload size
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize)

	// Parse multipart form
	if err := r.ParseMultipartForm(maxUploadSize); err != nil {
		writeError(w, http.StatusBadRequest, "file too large or invalid form")
		return
	}

	// Storage and the publisher must be configured before any publish state is
	// created so misconfigured deployments keep returning 503 with no side
	// effects.
	if h.storage == nil {
		writeError(w, http.StatusServiceUnavailable, "storage not configured")
		return
	}
	if h.publisher == nil {
		writeError(w, http.StatusServiceUnavailable, "attachment publishing not configured")
		return
	}

	uploadID, clientProvidedID, err := resolveUploadID(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid upload ID")
		return
	}

	// A request body is optional when resuming an existing publish (the
	// response was lost or the request was cancelled). A brand new upload
	// always requires a file.
	file, header, fileErr := r.FormFile("file")
	hasFile := fileErr == nil
	if hasFile {
		defer file.Close()
	} else if !clientProvidedID {
		writeError(w, http.StatusBadRequest, "missing file in request")
		return
	}

	// Determine content type before registering so the publish metadata is
	// complete even if recovery finishes the publish without the body.
	var contentType string
	var fileName string
	var fileSize int64
	if hasFile {
		contentType = header.Header.Get("Content-Type")
		if contentType == "" {
			// Try to detect from file content
			buf := make([]byte, 512)
			n, _ := file.Read(buf)
			contentType = http.DetectContentType(buf[:n])
			file.Seek(0, io.SeekStart)
		}
		fileName = header.Filename
		fileSize = header.Size
	}

	description := r.FormValue("description")
	var desc *string
	if description != "" {
		desc = &description
	}

	// Register the stable publish identity. An existing record means the
	// client is resuming/retrying; we replay or resume instead of starting a
	// second publish.
	registered, err := h.publisher.Register(r.Context(), repository.RegisterUploadInput{
		ID:          uploadID,
		AssetID:     assetID,
		FileName:    fileName,
		FileSize:    fileSize,
		ContentType: contentType,
		Description: desc,
		SetMain:     isImageContentType(contentType),
	})
	if err != nil {
		slog.Error("failed to register attachment upload", "error", err, "upload_id", uploadID)
		writeError(w, http.StatusInternalServerError, "failed to start attachment upload")
		return
	}
	if registered.AssetID != assetID {
		writeError(w, http.StatusBadRequest, "upload ID belongs to a different asset")
		return
	}

	w.Header().Set(uploadIDHeader, uploadID.String())

	if registered.Status == domain.AttachmentUploadAborted {
		writeError(w, http.StatusConflict, "upload was aborted because the attachment was deleted; start a new upload")
		return
	}

	if registered.Status == domain.AttachmentUploadCompleted {
		h.replayCompletedUpload(w, r, registered)
		return
	}

	// From here the publish is non-terminal. Resume through the object stage
	// (a no-op when the object is already recorded); a body-less resume that
	// still needs the object is reported as a conflict asking for the file.
	if hasFile {
		if _, err := h.publisher.PutObject(r.Context(), uploadID, file); err != nil {
			if errors.Is(err, repository.ErrUploadAborted) {
				writeError(w, http.StatusConflict, "upload was aborted because the attachment was deleted; start a new upload")
				return
			}
			slog.Error("failed to upload object", "error", err, "upload_id", uploadID)
			writeError(w, http.StatusInternalServerError, "failed to upload file; retry with the same upload ID")
			return
		}
	}

	_, attachment, err := h.publisher.Commit(r.Context(), uploadID)
	switch {
	case err == nil:
		// Published successfully.
	case errors.Is(err, repository.ErrUploadAborted):
		writeError(w, http.StatusConflict, "upload was aborted because the attachment was deleted; start a new upload")
		return
	case errors.Is(err, repository.ErrObjectNotUploaded):
		writeError(w, http.StatusConflict, "file upload did not complete; resend the file with the same upload ID")
		return
	default:
		slog.Error("failed to commit attachment publish", "error", err, "upload_id", uploadID)
		writeError(w, http.StatusInternalServerError, "failed to publish attachment; retry with the same upload ID")
		return
	}

	writeJSON(w, http.StatusCreated, attachment)
}

// replayCompletedUpload returns the original result of a finished publish,
// making response loss and client retries idempotent.
func (h *Handler) replayCompletedUpload(w http.ResponseWriter, r *http.Request, upload *domain.AttachmentUpload) {
	if upload.AttachmentID == nil {
		// Defensive: completed without an attachment row should never happen.
		writeError(w, http.StatusConflict, "upload was aborted; start a new upload")
		return
	}
	attachment, err := h.repos.Attachments.GetByID(r.Context(), *upload.AttachmentID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to load published attachment")
		return
	}
	if attachment == nil {
		writeError(w, http.StatusConflict, "published attachment was deleted; start a new upload")
		return
	}
	writeJSON(w, http.StatusCreated, attachment)
}

// GetUploadStatus reports whether an upload identity has published. It is the
// stable observation point after response loss, cancellation or restart.
func (h *Handler) GetUploadStatus(w http.ResponseWriter, r *http.Request) {
	assetID, err := parseUUID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid asset ID")
		return
	}
	uploadID, err := parseUUID(r, "uploadId")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid upload ID")
		return
	}
	if h.publisher == nil {
		writeError(w, http.StatusServiceUnavailable, "attachment publishing not configured")
		return
	}

	upload, err := h.publisher.Get(r.Context(), uploadID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get upload status")
		return
	}
	if upload == nil {
		writeError(w, http.StatusNotFound, "upload not found")
		return
	}
	if upload.AssetID != assetID {
		writeError(w, http.StatusNotFound, "upload not found")
		return
	}

	response := UploadStatusResponse{AttachmentUpload: upload}
	if upload.AttachmentID != nil {
		attachment, err := h.repos.Attachments.GetByID(r.Context(), *upload.AttachmentID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to get attachment")
			return
		}
		response.Attachment = attachment
	}
	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) GetAttachment(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(r, "attachmentId")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid attachment ID")
		return
	}

	attachment, err := h.repos.Attachments.GetByID(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get attachment")
		return
	}
	if attachment == nil {
		writeError(w, http.StatusNotFound, "attachment not found")
		return
	}

	// Generate presigned URL
	if h.storage == nil {
		writeError(w, http.StatusServiceUnavailable, "storage not configured")
		return
	}

	url, err := h.storage.GetPresignedURL(r.Context(), attachment.FileKey, 15*time.Minute)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to generate download URL")
		return
	}

	response := AttachmentResponse{
		Attachment: *attachment,
		URL:        url,
	}

	writeJSON(w, http.StatusOK, response)
}

func (h *Handler) DeleteAttachment(w http.ResponseWriter, r *http.Request) {
	id, err := parseUUID(r, "attachmentId")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid attachment ID")
		return
	}

	if h.publisher == nil {
		writeError(w, http.StatusServiceUnavailable, "attachment publishing not configured")
		return
	}

	// One transaction aborts unfinished publishes, clears any main-image
	// pointer to this attachment and deletes the row. A late publish recovery
	// therefore observes the aborted state and can neither re-set the pointer
	// nor re-create the row/object reference.
	_, fileKey, err := h.publisher.AbortAttachment(r.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrUploadNotFound) {
			writeError(w, http.StatusNotFound, "attachment not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to delete attachment")
		return
	}

	// Remove the storage object only after the database decision is committed.
	if h.storage != nil {
		if err := h.storage.Delete(r.Context(), fileKey); err != nil {
			// Log but continue - the database is the source of truth and
			// best-effort cleanup can remove orphaned objects later.
			slog.Warn("failed to delete attachment object from storage", "key", fileKey, "error", err)
		}
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) SetMainAttachment(w http.ResponseWriter, r *http.Request) {
	assetID, err := parseUUID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid asset ID")
		return
	}

	attachmentID, err := parseUUID(r, "attachmentId")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid attachment ID")
		return
	}

	// Verify asset exists
	asset, err := h.repos.Assets.GetByID(r.Context(), assetID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to check asset")
		return
	}
	if asset == nil {
		writeError(w, http.StatusNotFound, "asset not found")
		return
	}

	// Verify attachment exists and belongs to this asset
	attachment, err := h.repos.Attachments.GetByID(r.Context(), attachmentID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to check attachment")
		return
	}
	if attachment == nil {
		writeError(w, http.StatusNotFound, "attachment not found")
		return
	}
	if attachment.AssetID != assetID {
		writeError(w, http.StatusBadRequest, "attachment does not belong to this asset")
		return
	}

	// Verify it's an image
	if attachment.ContentType == nil || !isImageContentType(*attachment.ContentType) {
		writeError(w, http.StatusBadRequest, "only image attachments can be set as main image")
		return
	}

	// Stamp the manual choice so an unfinished automatic publish cannot
	// overwrite it, while later uploads still become main automatically.
	if err := h.repos.Assets.SetMainAttachmentManual(r.Context(), assetID, &attachmentID); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to set main attachment")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) ClearMainAttachment(w http.ResponseWriter, r *http.Request) {
	assetID, err := parseUUID(r, "id")
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid asset ID")
		return
	}

	// Verify asset exists
	asset, err := h.repos.Assets.GetByID(r.Context(), assetID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to check asset")
		return
	}
	if asset == nil {
		writeError(w, http.StatusNotFound, "asset not found")
		return
	}

	// An explicit clear is a manual choice: stamp it so an in-flight automatic
	// publish cannot resurrect the previous main image.
	if err := h.repos.Assets.SetMainAttachmentManual(r.Context(), assetID, nil); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to clear main attachment")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

// resolveUploadID reads the stable publish identity from an explicit form
// field or header, generating one for ordinary single-shot uploads.
func resolveUploadID(r *http.Request) (uuid.UUID, bool, error) {
	raw := r.FormValue("upload_id")
	if raw == "" {
		raw = r.Header.Get(uploadIDHeader)
	}
	if raw == "" {
		return uuid.New(), false, nil
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.Nil, true, err
	}
	return id, true, nil
}

func isImageContentType(contentType string) bool {
	switch contentType {
	case "image/jpeg", "image/png", "image/gif", "image/webp", "image/svg+xml":
		return true
	default:
		return false
	}
}
