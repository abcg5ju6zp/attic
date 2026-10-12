package domain

import (
	"time"

	"github.com/google/uuid"
)

// Attachment upload publish statuses. The publish pipeline advances through
// these states and never moves backwards:
//
//	pending_object -> object_uploaded -> attachment_created -> completed
//
// aborted is the terminal failure state. Every non-terminal state is
// resumable using the same upload ID.
const (
	AttachmentUploadPendingObject     = "pending_object"
	AttachmentUploadObjectUploaded    = "object_uploaded"
	AttachmentUploadAttachmentCreated = "attachment_created"
	AttachmentUploadCompleted         = "completed"
	AttachmentUploadAborted           = "aborted"
)

// Outcome of the promised main-image update for an image upload.
const (
	AttachmentUploadMainSet            = "set"
	AttachmentUploadMainSkippedManual  = "skipped_manual"
)

// AttachmentUpload is the stable publish identity of one attachment upload.
// It links the storage object key, the attachments row, the asset main-image
// pointer and the HTTP response so that retries, lost responses and process
// restarts all observe the same publish state.
type AttachmentUpload struct {
	ID           uuid.UUID  `json:"id"`
	AssetID      uuid.UUID  `json:"asset_id"`
	AttachmentID *uuid.UUID `json:"attachment_id,omitempty"`
	FileKey      string     `json:"file_key"`
	FileName     string     `json:"file_name"`
	FileSize     int64      `json:"file_size"`
	ContentType  *string    `json:"content_type,omitempty"`
	Description  *string    `json:"description,omitempty"`
	IsImage      bool       `json:"is_image"`
	SetMain      bool       `json:"set_main"`
	Status       string     `json:"status"`
	MainResult   *string    `json:"main_result,omitempty"`
	Error        *string    `json:"error,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	CompletedAt  *time.Time `json:"completed_at,omitempty"`
}
