package handler

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/lmmendes/attic/internal/domain"
	"github.com/lmmendes/attic/internal/repository"
)

const maxUploadSize = 50 * 1024 * 1024 // 50MB

type AttachmentResponse struct {
	domain.Attachment
	URL string `json:"url,omitempty"`
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

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, "missing file in request")
		return
	}
	defer file.Close()

	// Determine content type
	contentType := header.Header.Get("Content-Type")
	if contentType == "" {
		// Try to detect from file content
		buf := make([]byte, 512)
		n, _ := file.Read(buf)
		contentType = http.DetectContentType(buf[:n])
		file.Seek(0, io.SeekStart)
	}

	// Upload to S3
	if h.storage == nil {
		writeError(w, http.StatusServiceUnavailable, "storage not configured")
		return
	}

	key, err := h.storage.Upload(r.Context(), header.Filename, contentType, file)
	if err != nil {
		slog.Error("failed to upload file to storage", "error", err, "filename", header.Filename)
		writeError(w, http.StatusInternalServerError, "failed to upload file")
		return
	}

	// Get description from form
	description := r.FormValue("description")
	var desc *string
	if description != "" {
		desc = &description
	}

	// Create attachment record
	attachment := &domain.Attachment{
		AssetID:     assetID,
		FileKey:     key,
		FileName:    header.Filename,
		FileSize:    header.Size,
		ContentType: &contentType,
		Description: desc,
	}

	if err := h.repos.Attachments.Create(r.Context(), attachment); err != nil {
		// Try to clean up the uploaded file
		h.storage.Delete(r.Context(), key)
		// The asset was hidden by a concurrent deletion generation: the new
		// upload must not attach to that generation, report it deterministically.
		if errors.Is(err, repository.ErrAssetNotLive) {
			writeError(w, http.StatusNotFound, "asset not found")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to save attachment record")
		return
	}

	// Auto-set as main image for any image upload
	if isImageContentType(contentType) {
		h.repos.Assets.SetMainAttachment(r.Context(), assetID, &attachment.ID)
	}

	writeJSON(w, http.StatusCreated, attachment)
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

	attachment, err := h.repos.Attachments.GetByID(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to get attachment")
		return
	}
	if attachment == nil {
		writeError(w, http.StatusNotFound, "attachment not found")
		return
	}

	if h.deletions != nil {
		// Lifecycle commits first; the object is removed only when no live
		// record references its key, and uncertain responses are verified.
		found, err := h.deletions.DeleteAttachment(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "failed to delete attachment")
			return
		}
		if !found {
			writeError(w, http.StatusNotFound, "attachment not found")
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}

	// Delete from S3
	if h.storage != nil {
		if err := h.storage.Delete(r.Context(), attachment.FileKey); err != nil {
			// Log but continue - we still want to delete the DB record
			slog.Warn("failed to delete attachment object", "error", err)
		}
	}

	// Delete from database
	if err := h.repos.Attachments.Delete(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to delete attachment")
		return
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

	// Set as main attachment. A zero-row update means the asset was hidden by
	// a deletion generation in the meantime; an old relationship must not be
	// revived on a deleted asset.
	updated, err := h.repos.Assets.SetMainAttachment(r.Context(), assetID, &attachmentID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to set main attachment")
		return
	}
	if !updated {
		writeError(w, http.StatusNotFound, "asset not found")
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

	// Clear main attachment
	updated, err := h.repos.Assets.SetMainAttachment(r.Context(), assetID, nil)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to clear main attachment")
		return
	}
	if !updated {
		writeError(w, http.StatusNotFound, "asset not found")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func isImageContentType(contentType string) bool {
	switch contentType {
	case "image/jpeg", "image/png", "image/gif", "image/webp", "image/svg+xml":
		return true
	default:
		return false
	}
}
