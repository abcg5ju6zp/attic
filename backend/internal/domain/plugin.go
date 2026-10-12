package domain

import (
	"context"
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// ImportPlugin defines the interface for all import plugins
type ImportPlugin interface {
	// Metadata
	ID() string          // Unique identifier, e.g., "google_books"
	Name() string        // Display name, e.g., "Google Books"
	Description() string // Brief description of the plugin

	// Configuration status
	Enabled() bool          // Returns true if the plugin is properly configured
	DisabledReason() string // Returns reason if disabled (e.g., "Missing API key: ATTIC_TMDB_API_KEY")

	// Category management
	CategoryName() string        // Category this plugin manages
	CategoryDescription() string // Description for the category
	Attributes() []PluginAttribute

	// Search capabilities
	SearchFields() []SearchField
	Search(ctx context.Context, field, query string, limit int) ([]SearchResult, error)

	// Data fetching
	Fetch(ctx context.Context, externalID string) (*ImportData, error)
}

// PluginAttribute defines an attribute managed by a plugin
type PluginAttribute struct {
	Key      string            `json:"key"`       // Namespaced key, e.g., "books.isbn"
	Name     string            `json:"name"`      // Display name, e.g., "ISBN"
	DataType AttributeDataType `json:"data_type"` // string, number, boolean, date, text
	Required bool              `json:"required"`  // Is this attribute required?
}

// SearchField defines a searchable field
type SearchField struct {
	Key   string `json:"key"`   // Field identifier, e.g., "title", "isbn"
	Label string `json:"label"` // Display label, e.g., "Title", "ISBN"
}

// SearchResult represents a search result from a plugin
type SearchResult struct {
	ExternalID string  `json:"external_id"` // External identifier for fetching full data
	Title      string  `json:"title"`       // Primary display title
	Subtitle   string  `json:"subtitle"`    // Secondary info (e.g., author, year)
	ImageURL   *string `json:"image_url"`   // Thumbnail URL if available
}

// ImportData contains the full data for an import
type ImportData struct {
	Name        string         `json:"name"`                   // Asset name
	Description *string        `json:"description,omitempty"`  // Asset description
	ImageURL    *string        `json:"image_url,omitempty"`    // Primary image URL
	Attributes  map[string]any `json:"attributes"`             // Attribute values (keyed by attribute key)
	ExternalID  string         `json:"external_id"`            // External ID for source tracking
}

// ImportPublicationStatus tracks how far a single plugin import has progressed.
type ImportPublicationStatus string

const (
	// ImportPublicationPending means the publication is incomplete and may be
	// (re)claimed by any request with the same identity.
	ImportPublicationPending ImportPublicationStatus = "pending"
	// ImportPublicationPublishing means an owner currently holds the lease.
	ImportPublicationPublishing ImportPublicationStatus = "publishing"
	// ImportPublicationPublished means the asset and, when the source had one,
	// its cover attachment/main image are complete.
	ImportPublicationPublished ImportPublicationStatus = "published"
)

// ImportPublication is the durable, recoverable record of one plugin import.
// The stable identity is (OrganizationID, PluginID, ExternalID). Each
// delete-and-reimport bumps Generation so a stale recovery task can detect
// that it no longer owns the result.
type ImportPublication struct {
	ID                uuid.UUID               `json:"id"`
	OrganizationID    uuid.UUID               `json:"organization_id"`
	PluginID          string                  `json:"plugin_id"`
	ExternalID        string                  `json:"external_id"`
	Generation        int                     `json:"generation"`
	Status            ImportPublicationStatus `json:"status"`
	AssetID           *uuid.UUID              `json:"asset_id,omitempty"`
	Payload           json.RawMessage         `json:"payload"`
	ExternalVersion   string                  `json:"external_version"`
	CoverFileKey      *string                 `json:"cover_file_key,omitempty"`
	CoverFileName     *string                 `json:"cover_file_name,omitempty"`
	CoverContentType  *string                 `json:"cover_content_type,omitempty"`
	CoverFileSize     *int64                  `json:"cover_file_size,omitempty"`
	CoverAttachmentID *uuid.UUID              `json:"cover_attachment_id,omitempty"`
	LeaseOwner        *uuid.UUID              `json:"lease_owner,omitempty"`
	LeasedUntil       *time.Time              `json:"leased_until,omitempty"`
	Attempts          int                     `json:"attempts"`
	LastError         *string                 `json:"last_error,omitempty"`
	CreatedAt         time.Time               `json:"created_at"`
	UpdatedAt         time.Time               `json:"updated_at"`
	PublishedAt       *time.Time              `json:"published_at,omitempty"`
}

// PluginInfo represents plugin metadata for API responses
type PluginInfo struct {
	ID                  string            `json:"id"`
	Name                string            `json:"name"`
	Description         string            `json:"description"`
	Enabled             bool              `json:"enabled"`
	DisabledReason      string            `json:"disabled_reason,omitempty"`
	CategoryName        string            `json:"category_name"`
	CategoryDescription string            `json:"category_description"`
	SearchFields        []SearchField     `json:"search_fields"`
	Attributes          []PluginAttribute `json:"attributes"`
}

// ToInfo converts an ImportPlugin to PluginInfo for API responses
func PluginToInfo(p ImportPlugin) PluginInfo {
	return PluginInfo{
		ID:                  p.ID(),
		Name:                p.Name(),
		Description:         p.Description(),
		Enabled:             p.Enabled(),
		DisabledReason:      p.DisabledReason(),
		CategoryName:        p.CategoryName(),
		CategoryDescription: p.CategoryDescription(),
		SearchFields:        p.SearchFields(),
		Attributes:          p.Attributes(),
	}
}
