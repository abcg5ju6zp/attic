package handler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/lmmendes/attic/internal/domain"
	"github.com/lmmendes/attic/internal/plugin"
	"github.com/lmmendes/attic/internal/repository"
)

// PluginHandler handles plugin-related HTTP requests
type PluginHandler struct {
	registry  *plugin.Registry
	repos     *Repositories
	storage   FileStorage
	orgID     uuid.UUID
	publisher *importPublisher
}

func namespacedPluginAttributes(pluginID string, attributes map[string]any) map[string]any {
	result := make(map[string]any, len(attributes))
	for key, value := range attributes {
		if strings.HasPrefix(key, "plugin.") {
			result[key] = value
		} else {
			result["plugin."+pluginID+"."+key] = value
		}
	}
	return result
}

func namespacedPluginAttributeDefinitions(pluginID string, attributes []domain.PluginAttribute) []domain.PluginAttribute {
	result := make([]domain.PluginAttribute, len(attributes))
	copy(result, attributes)
	for i := range result {
		if !strings.HasPrefix(result[i].Key, "plugin.") {
			result[i].Key = "plugin." + pluginID + "." + result[i].Key
		}
	}
	return result
}

type pluginCategoryRepository interface {
	GetByPluginID(context.Context, uuid.UUID, string) (*domain.Category, error)
	GetByIDWithAttributes(context.Context, uuid.UUID) (*domain.Category, error)
	Create(context.Context, *domain.Category) error
	SetAttributes(context.Context, uuid.UUID, []domain.CategoryAttributeAssignment) error
}

type pluginAttributeRepository interface {
	GetByKey(context.Context, uuid.UUID, string) (*domain.Attribute, error)
	Create(context.Context, *domain.Attribute) error
}

// NewPluginHandler creates a new PluginHandler
func NewPluginHandler(registry *plugin.Registry, repos *Repositories, storage FileStorage, defaultOrgID uuid.UUID) *PluginHandler {
	h := &PluginHandler{
		registry: registry,
		repos:    repos,
		storage:  storage,
		orgID:    defaultOrgID,
	}
	if repos != nil && repos.Publications != nil && repos.Assets != nil && repos.Attachments != nil {
		h.publisher = newImportPublisher(
			defaultOrgID,
			repos.Publications,
			repos.Assets,
			repos.Attachments,
			repos.Categories,
			repos.Attributes,
			storage,
		)
	}
	return h
}

func allFeaturesEnabled() *domain.OrganizationFeatures {
	return &domain.OrganizationFeatures{Locations: true, Collections: true, Tags: true, Categories: true, Attributes: true, Conditions: true, Warranties: true, Plugins: true}
}

func (h *PluginHandler) organizationFeatures(ctx context.Context) (*domain.OrganizationFeatures, error) {
	if features, ok := featuresFromContext(ctx); ok {
		return normalizeFeatures(features), nil
	}
	if h.repos.Organizations == nil {
		return allFeaturesEnabled(), nil
	}
	return h.repos.Organizations.GetFeatures(ctx, h.orgID)
}

// InitializeEnabledPluginCategories creates the category schema for every usable
// import plugin before the server begins accepting requests.
func (h *PluginHandler) InitializeEnabledPluginCategories(ctx context.Context) error {
	features, err := h.organizationFeatures(ctx)
	if err != nil {
		return fmt.Errorf("reading organization features: %w", err)
	}
	if !features.Plugins {
		return nil
	}
	if !features.Categories {
		return nil
	}
	var initializationErrors []error

	for _, p := range h.registry.List() {
		if !p.Enabled() {
			continue
		}

		category, created, err := h.ensurePluginCategory(ctx, p)
		if err != nil {
			slog.Error("failed to initialize plugin category",
				"plugin_id", p.ID(),
				"plugin_name", p.Name(),
				"error", err)
			initializationErrors = append(initializationErrors, fmt.Errorf("%s: %w", p.ID(), err))
			continue
		}

		slog.Info("initialized plugin category",
			"plugin_id", p.ID(),
			"plugin_name", p.Name(),
			"category_id", category.ID,
			"category_name", category.Name,
			"created", created)
	}

	return errors.Join(initializationErrors...)
}

// PluginListResponse represents the response for listing plugins
type PluginListResponse struct {
	Plugins []PluginResponse `json:"plugins"`
}

// PluginResponse represents a plugin in API responses
type PluginResponse struct {
	ID                  string                   `json:"id"`
	Name                string                   `json:"name"`
	Description         string                   `json:"description"`
	Enabled             bool                     `json:"enabled"`
	DisabledReason      string                   `json:"disabled_reason,omitempty"`
	CategoryName        string                   `json:"category_name"`
	CategoryDescription string                   `json:"category_description"`
	SearchFields        []domain.SearchField     `json:"search_fields"`
	Attributes          []domain.PluginAttribute `json:"attributes"`
	CategoryID          *uuid.UUID               `json:"category_id,omitempty"`
}

// SearchResponse represents the response for plugin search
type SearchResponse struct {
	Results []domain.SearchResult `json:"results"`
}

func newPluginResponse(p domain.ImportPlugin, includeAttributes bool) PluginResponse {
	attributes := []domain.PluginAttribute{}
	if includeAttributes {
		attributes = namespacedPluginAttributeDefinitions(p.ID(), p.Attributes())
	}
	return PluginResponse{
		ID:                  p.ID(),
		Name:                p.Name(),
		Description:         p.Description(),
		Enabled:             p.Enabled(),
		DisabledReason:      p.DisabledReason(),
		CategoryName:        p.CategoryName(),
		CategoryDescription: p.CategoryDescription(),
		SearchFields:        p.SearchFields(),
		Attributes:          attributes,
	}
}

// ImportRequest represents the request body for importing
type ImportRequest struct {
	ExternalID string `json:"external_id"`
}

// ImportResponse represents the response for importing
type ImportResponse struct {
	Asset *domain.Asset `json:"asset"`
}

// ListPlugins returns all available plugins
func (h *PluginHandler) ListPlugins(w http.ResponseWriter, r *http.Request) {
	features, err := h.organizationFeatures(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read organization features")
		return
	}
	plugins := h.registry.List()

	response := PluginListResponse{
		Plugins: make([]PluginResponse, 0, len(plugins)),
	}

	for _, p := range plugins {
		pr := newPluginResponse(p, features.Attributes)

		// Check if category exists for this plugin
		cat, _ := h.repos.Categories.GetByPluginID(r.Context(), h.orgID, p.ID())
		if cat != nil {
			pr.CategoryID = &cat.ID
		}

		response.Plugins = append(response.Plugins, pr)
	}

	writeJSON(w, http.StatusOK, response)
}

// GetPlugin returns a specific plugin's info
func (h *PluginHandler) GetPlugin(w http.ResponseWriter, r *http.Request) {
	pluginID := chi.URLParam(r, "pluginId")

	p, exists := h.registry.Get(pluginID)
	if !exists {
		writeError(w, http.StatusNotFound, "plugin not found")
		return
	}
	features, err := h.organizationFeatures(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read organization features")
		return
	}

	pr := newPluginResponse(p, features.Attributes)

	// Check if category exists for this plugin
	cat, _ := h.repos.Categories.GetByPluginID(r.Context(), h.orgID, p.ID())
	if cat != nil {
		pr.CategoryID = &cat.ID
	}

	writeJSON(w, http.StatusOK, pr)
}

// Search performs a search using a plugin
func (h *PluginHandler) Search(w http.ResponseWriter, r *http.Request) {
	pluginID := chi.URLParam(r, "pluginId")

	p, exists := h.registry.Get(pluginID)
	if !exists {
		writeError(w, http.StatusNotFound, fmt.Sprintf("plugin '%s' not found", pluginID))
		return
	}

	if !p.Enabled() {
		writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("plugin '%s' is disabled: %s", pluginID, p.DisabledReason()))
		return
	}

	q := r.URL.Query()
	field := q.Get("field")
	query := q.Get("q")
	limit, _ := strconv.Atoi(q.Get("limit"))

	if query == "" {
		writeError(w, http.StatusBadRequest, "query parameter 'q' is required")
		return
	}

	if len(query) < 2 {
		writeError(w, http.StatusBadRequest, "search query must be at least 2 characters")
		return
	}

	// Validate search field
	searchFields := p.SearchFields()
	if field == "" {
		// Default to first search field
		if len(searchFields) > 0 {
			field = searchFields[0].Key
		}
	} else {
		// Validate field exists
		validField := false
		for _, f := range searchFields {
			if f.Key == field {
				validField = true
				break
			}
		}
		if !validField {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid search field '%s'", field))
			return
		}
	}

	if limit <= 0 || limit > 20 {
		limit = 10
	}

	results, err := p.Search(r.Context(), field, query, limit)
	if err != nil {
		slog.Error("plugin search failed",
			"plugin_id", pluginID,
			"field", field,
			"query", query,
			"error", err)

		// Check for context cancellation (user navigated away)
		if r.Context().Err() != nil {
			return
		}

		writeError(w, http.StatusBadGateway, "search service temporarily unavailable")
		return
	}

	if results == nil {
		results = []domain.SearchResult{}
	}

	writeJSON(w, http.StatusOK, SearchResponse{Results: results})
}

// Import fetches data from a plugin and publishes a single, recoverable
// asset for the stable (organization, plugin, external id) identity.
func (h *PluginHandler) Import(w http.ResponseWriter, r *http.Request) {
	pluginID := chi.URLParam(r, "pluginId")

	p, exists := h.registry.Get(pluginID)
	if !exists {
		writeError(w, http.StatusNotFound, fmt.Sprintf("plugin '%s' not found", pluginID))
		return
	}

	if !p.Enabled() {
		writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("plugin '%s' is disabled: %s", pluginID, p.DisabledReason()))
		return
	}
	features, err := h.organizationFeatures(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, "failed to read organization features")
		return
	}

	var req ImportRequest
	if err := decodeJSON(r, &req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body: expected JSON with 'external_id' field")
		return
	}

	if req.ExternalID == "" {
		writeError(w, http.StatusBadRequest, "external_id is required")
		return
	}

	slog.Info("importing item from plugin",
		"plugin_id", pluginID,
		"external_id", req.ExternalID)

	// Fetch data from plugin. An uncertain fetch must not produce a duplicate:
	// when the identity already exists the request resumes that publication
	// from its pinned snapshot instead of failing.
	fetched, fetchErr := p.Fetch(r.Context(), req.ExternalID)
	if fetchErr != nil {
		if r.Context().Err() != nil {
			return
		}
		existing, lookupErr := h.repos.Publications.GetByIdentity(r.Context(), h.orgID, pluginID, req.ExternalID)
		if lookupErr != nil {
			slog.Error("failed to look up existing import publication",
				"plugin_id", pluginID, "external_id", req.ExternalID, "error", lookupErr)
			writeError(w, http.StatusInternalServerError, "failed to resume import")
			return
		}
		if existing != nil {
			slog.Warn("external fetch uncertain; resuming existing import publication",
				"plugin_id", pluginID, "external_id", req.ExternalID,
				"generation", existing.Generation, "fetch_error", fetchErr)
			h.publishImport(w, r, p, req.ExternalID, nil, features)
			return
		}

		slog.Error("failed to fetch import data",
			"plugin_id", pluginID,
			"external_id", req.ExternalID,
			"error", fetchErr)
		if strings.Contains(fetchErr.Error(), "not found") {
			writeError(w, http.StatusNotFound, "item not found in external source")
			return
		}
		writeError(w, http.StatusBadGateway, "failed to fetch data from external source")
		return
	}

	// Validate import data
	if fetched.Name == "" {
		writeError(w, http.StatusBadGateway, "external source returned invalid data (missing name)")
		return
	}

	h.publishImport(w, r, p, req.ExternalID, fetched, features)
}

// publishImport runs the recoverable publication and maps its outcome to HTTP.
func (h *PluginHandler) publishImport(w http.ResponseWriter, r *http.Request, p domain.ImportPlugin, externalID string, fetched *domain.ImportData, features *domain.OrganizationFeatures) {
	result, err := h.publisher.publish(r.Context(), p, externalID, fetched, features)
	if err != nil {
		switch {
		case errors.Is(err, ErrImportSuperseded):
			writeError(w, http.StatusConflict, "a newer import of this item superseded this request")
			return
		case errors.Is(err, ErrImportBusy):
			writeError(w, http.StatusServiceUnavailable, "an import for this item is already finishing; please retry")
			return
		case errors.Is(err, ErrImportNeedsFetch):
			writeError(w, http.StatusBadGateway, "failed to fetch data from external source")
			return
		}
		var attributeErr *repository.AttributeError
		if errors.As(err, &attributeErr) {
			writeAttributeError(w, err)
			return
		}
		slog.Error("failed to publish imported item",
			"plugin_id", p.ID(), "external_id", externalID, "error", err)
		writeError(w, http.StatusInternalServerError, "failed to save imported item")
		return
	}

	slog.Info("completed plugin import",
		"plugin_id", p.ID(),
		"external_id", externalID,
		"asset_id", result.asset.ID,
		"created", result.created)

	status := http.StatusCreated
	if !result.created {
		status = http.StatusOK
	}
	writeJSON(w, status, ImportResponse{Asset: result.asset})
}

// RecoverPendingImports resumes imports interrupted by a restart, using each
// publication's pinned snapshot and finishing only the missing stages.
func (h *PluginHandler) RecoverPendingImports(ctx context.Context) {
	if h.publisher == nil || h.repos == nil || h.repos.Publications == nil {
		return
	}
	features, err := h.organizationFeatures(ctx)
	if err != nil {
		slog.Error("failed to read organization features during import recovery", "error", err)
		return
	}
	h.publisher.RecoverPending(ctx, func(pluginID string) domain.ImportPlugin {
		p, ok := h.registry.Get(pluginID)
		if !ok {
			return nil
		}
		return p
	}, features)
}

// ensurePluginCategory ensures the plugin's category and attributes exist
func (h *PluginHandler) ensurePluginCategory(ctx context.Context, p domain.ImportPlugin) (*domain.Category, bool, error) {
	return ensurePluginCategory(ctx, h.repos.Categories, h.repos.Attributes, h.orgID, p)
}

func ensurePluginCategory(
	ctx context.Context,
	categories pluginCategoryRepository,
	attributes pluginAttributeRepository,
	organizationID uuid.UUID,
	p domain.ImportPlugin,
) (*domain.Category, bool, error) {
	pluginID := p.ID()

	// Check if category already exists
	cat, err := categories.GetByPluginID(ctx, organizationID, pluginID)
	if err != nil {
		return nil, false, err
	}

	created := cat == nil
	if created {
		cat = &domain.Category{
			OrganizationID: organizationID,
			PluginID:       &pluginID,
			Name:           p.CategoryName(),
			Description:    strPtr(p.CategoryDescription()),
		}

		if err := categories.Create(ctx, cat); err != nil {
			if !isUniqueViolation(err) {
				return nil, false, err
			}
			// A concurrent first import won the category race; adopt it.
			cat, err = categories.GetByPluginID(ctx, organizationID, pluginID)
			if err != nil {
				return nil, false, err
			}
			if cat == nil {
				return nil, false, errors.New("plugin category vanished after unique conflict")
			}
			created = false
		}
	}

	categoryWithAttributes, err := categories.GetByIDWithAttributes(ctx, cat.ID)
	if err != nil {
		return nil, false, err
	}
	if categoryWithAttributes == nil {
		return nil, false, fmt.Errorf("category %s disappeared during initialization", cat.ID)
	}
	cat = categoryWithAttributes

	// Preserve user-defined fields and add any plugin fields that are missing.
	pluginAttrs := p.Attributes()
	assignments := make([]domain.CategoryAttributeAssignment, 0, len(cat.Attributes)+len(pluginAttrs))
	assignedAttributeIDs := make(map[uuid.UUID]struct{}, len(cat.Attributes))
	maxSortOrder := -1
	for _, categoryAttribute := range cat.Attributes {
		assignments = append(assignments, domain.CategoryAttributeAssignment{
			AttributeID: categoryAttribute.AttributeID,
			Required:    categoryAttribute.Required,
			SortOrder:   categoryAttribute.SortOrder,
		})
		assignedAttributeIDs[categoryAttribute.AttributeID] = struct{}{}
		if categoryAttribute.SortOrder > maxSortOrder {
			maxSortOrder = categoryAttribute.SortOrder
		}
	}
	assignmentsChanged := false

	for _, pa := range pluginAttrs {
		if !strings.HasPrefix(pa.Key, "plugin.") {
			pa.Key = "plugin." + pluginID + "." + pa.Key
		}
		// Check if attribute already exists
		attr, err := attributes.GetByKey(ctx, organizationID, pa.Key)
		if err != nil {
			return nil, false, err
		}

		if attr == nil {
			// Create the attribute
			attr = &domain.Attribute{
				OrganizationID: organizationID,
				PluginID:       &pluginID,
				Name:           pa.Name,
				Key:            pa.Key,
				DataType:       pa.DataType,
			}
			if err := attributes.Create(ctx, attr); err != nil {
				if !isUniqueViolation(err) {
					return nil, false, err
				}
				// A concurrent first import won the attribute race; adopt it.
				attr, err = attributes.GetByKey(ctx, organizationID, pa.Key)
				if err != nil {
					return nil, false, err
				}
				if attr == nil {
					return nil, false, fmt.Errorf("plugin attribute %s vanished after unique conflict", pa.Key)
				}
			}
		}

		if _, assigned := assignedAttributeIDs[attr.ID]; assigned {
			continue
		}

		maxSortOrder++
		assignments = append(assignments, domain.CategoryAttributeAssignment{
			AttributeID: attr.ID,
			Required:    pa.Required,
			SortOrder:   maxSortOrder,
		})
		assignedAttributeIDs[attr.ID] = struct{}{}
		assignmentsChanged = true
	}

	if assignmentsChanged {
		if err := categories.SetAttributes(ctx, cat.ID, assignments); err != nil {
			return nil, false, err
		}
	}

	return cat, created, nil
}

func strPtr(s string) *string {
	return &s
}

// isUniqueViolation reports whether err is a PostgreSQL unique constraint
// failure, used to turn concurrent first-time category/attribute creation into
// an idempotent read.
func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}
