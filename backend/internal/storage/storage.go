package storage

import (
	"context"
	"io"
	"time"
)

// FileStorage defines the interface for file storage backends
type FileStorage interface {
	// Upload uploads a file and returns the storage key
	Upload(ctx context.Context, filename string, contentType string, body io.Reader) (string, error)

	// UploadWithKey stores a file under a caller-chosen, stable key. Repeating
	// the call with the same key overwrites the same object instead of
	// producing a duplicate, which makes retries idempotent.
	UploadWithKey(ctx context.Context, key string, contentType string, body io.Reader) error

	// GetPresignedURL returns a URL for downloading the file
	// For S3, this is a presigned URL. For local storage, this is a direct path.
	GetPresignedURL(ctx context.Context, key string, expiry time.Duration) (string, error)

	// Delete removes a file from storage
	Delete(ctx context.Context, key string) error
}
