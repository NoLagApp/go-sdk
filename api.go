package nolag

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

const (
	DefaultAPIBaseURL = "https://api.nolag.app/v1"
	DefaultAPITimeout = 30 * time.Second
)

// APIOptions configures the NoLag API client.
type APIOptions struct {
	BaseURL string
	Timeout time.Duration
	Headers map[string]string
}

// DefaultAPIOptions returns the default API options.
func DefaultAPIOptions() APIOptions {
	return APIOptions{
		BaseURL: DefaultAPIBaseURL,
		Timeout: DefaultAPITimeout,
		Headers: make(map[string]string),
	}
}

// APIError represents an error from the NoLag API.
type APIError struct {
	StatusCode int    `json:"statusCode"`
	Message    string `json:"message"`
	Error      string `json:"error,omitempty"`
}

func (e *APIError) String() string {
	return fmt.Sprintf("NoLag API Error [%d]: %s", e.StatusCode, e.Message)
}

// NoLagAPIError wraps an API error with context.
type NoLagAPIError struct {
	Err     error
	Status  int
	Details *APIError
}

func (e *NoLagAPIError) Error() string {
	if e.Details != nil {
		return e.Details.String()
	}
	return e.Err.Error()
}

// API is the NoLag REST API client.
// API keys are project-scoped, so you don't need to pass organization or project IDs.
type API struct {
	apiKey  string
	baseURL string
	timeout time.Duration
	headers map[string]string
	client  *http.Client

	Apps   *AppsAPI
	Rooms  *RoomsAPI
	Actors *ActorsAPI
	Scopes *ScopesAPI
}

// NewAPI creates a new NoLag API client.
//
// Example:
//
//	api := nolag.NewAPI("nlg_live_xxx.secret")
//
//	// List apps
//	apps, err := api.Apps.List(ctx, nil)
//
//	// Create an actor
//	actor, err := api.Actors.Create(ctx, ActorCreate{
//	    Name:      "web-client",
//	    ActorType: ActorTypeDevice,
//	})
//	fmt.Println("Save this token:", actor.AccessToken)
func NewAPI(apiKey string, opts ...APIOptions) *API {
	options := DefaultAPIOptions()
	if len(opts) > 0 {
		if opts[0].BaseURL != "" {
			options.BaseURL = opts[0].BaseURL
		}
		if opts[0].Timeout > 0 {
			options.Timeout = opts[0].Timeout
		}
		for k, v := range opts[0].Headers {
			options.Headers[k] = v
		}
	}

	api := &API{
		apiKey:  apiKey,
		baseURL: options.BaseURL,
		timeout: options.Timeout,
		headers: options.Headers,
		client: &http.Client{
			Timeout: options.Timeout,
		},
	}

	api.Apps = &AppsAPI{api: api}
	api.Rooms = &RoomsAPI{api: api}
	api.Actors = &ActorsAPI{api: api}
	api.Scopes = &ScopesAPI{api: api}

	return api
}

func (a *API) request(ctx context.Context, method, path string, body, result any) error {
	u, err := url.Parse(a.baseURL + path)
	if err != nil {
		return err
	}

	var bodyReader io.Reader
	if body != nil {
		jsonBody, err := json.Marshal(body)
		if err != nil {
			return err
		}
		bodyReader = bytes.NewReader(jsonBody)
	}

	req, err := http.NewRequestWithContext(ctx, method, u.String(), bodyReader)
	if err != nil {
		return err
	}

	req.Header.Set("Authorization", "Bearer "+a.apiKey)
	req.Header.Set("Content-Type", "application/json")
	for k, v := range a.headers {
		req.Header.Set(k, v)
	}

	resp, err := a.client.Do(req)
	if err != nil {
		return &NoLagAPIError{Err: err, Status: 0}
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode >= 400 {
		var apiErr APIError
		if err := json.Unmarshal(respBody, &apiErr); err != nil {
			apiErr = APIError{
				StatusCode: resp.StatusCode,
				Message:    resp.Status,
			}
		}
		return &NoLagAPIError{
			Status:  resp.StatusCode,
			Details: &apiErr,
		}
	}

	// Handle 204 No Content
	if resp.StatusCode == 204 || len(respBody) == 0 {
		return nil
	}

	if result != nil {
		return json.Unmarshal(respBody, result)
	}

	return nil
}

// ============ Types ============

// App represents an app resource.
type AppResource struct {
	AppID       string         `json:"appId"`
	ProjectID   string         `json:"projectId"`
	Name        string         `json:"name"`
	Slug        string         `json:"slug,omitempty"`
	Description string         `json:"description,omitempty"`
	BlueprintID string         `json:"blueprintId,omitempty"`
	Config      map[string]any `json:"config,omitempty"`
	CreatedAt   string         `json:"createdAt,omitempty"`
	UpdatedAt   string         `json:"updatedAt,omitempty"`
	DeletedAt   string         `json:"deletedAt,omitempty"`
}

// AppCreate is the request to create an app.
type AppCreate struct {
	Name        string         `json:"name"`
	Slug        string         `json:"slug,omitempty"`
	Description string         `json:"description,omitempty"`
	BlueprintID string         `json:"blueprintId,omitempty"`
	Config      map[string]any `json:"config,omitempty"`
}

// AppUpdate is the request to update an app.
type AppUpdate struct {
	Name        *string        `json:"name,omitempty"`
	Slug        *string        `json:"slug,omitempty"`
	Description *string        `json:"description,omitempty"`
	Config      map[string]any `json:"config,omitempty"`
}

// RoomResource represents a room resource.
type RoomResource struct {
	RoomID      string         `json:"roomId"`
	AppID       string         `json:"appId"`
	Name        string         `json:"name"`
	Slug        string         `json:"slug"`
	Description string         `json:"description,omitempty"`
	RoomType    string         `json:"roomType,omitempty"`
	IsEnabled   bool           `json:"isEnabled"`
	Topics      []string       `json:"topics,omitempty"`
	Config      map[string]any `json:"config,omitempty"`
	CreatedAt   string         `json:"createdAt,omitempty"`
	UpdatedAt   string         `json:"updatedAt,omitempty"`
}

// RoomCreate is the request to create a room.
type RoomCreate struct {
	Name        string         `json:"name"`
	Slug        string         `json:"slug,omitempty"`
	Description string         `json:"description,omitempty"`
	Topics      []string       `json:"topics,omitempty"`
	Config      map[string]any `json:"config,omitempty"`
}

// RoomUpdate is the request to update a room.
type RoomUpdate struct {
	Name        *string        `json:"name,omitempty"`
	Description *string        `json:"description,omitempty"`
	IsEnabled   *bool          `json:"isEnabled,omitempty"`
	Topics      []string       `json:"topics,omitempty"`
	Config      map[string]any `json:"config,omitempty"`
}

// ActorResource represents an actor resource.
type ActorResource struct {
	ActorTokenID    string         `json:"actorTokenId"`
	ProjectID       string         `json:"projectId"`
	Name            string         `json:"name"`
	ActorType       ActorType      `json:"actorType"`
	Description     string         `json:"description,omitempty"`
	ExternalID      string         `json:"externalId,omitempty"`
	Metadata        map[string]any `json:"metadata,omitempty"`
	IsActive        bool           `json:"isActive"`
	AccessScopeID   *string        `json:"accessScopeId,omitempty"`
	LastConnectedAt string         `json:"lastConnectedAt,omitempty"`
	CreatedAt       string         `json:"createdAt,omitempty"`
	UpdatedAt       string         `json:"updatedAt,omitempty"`
}

// ActorWithToken is an actor with its access token (returned on creation only).
type ActorWithToken struct {
	ActorResource
	AccessToken string `json:"accessToken"`
}

// ActorCreate is the request to create an actor.
type ActorCreate struct {
	Name        string         `json:"name"`
	ActorType   ActorType      `json:"actorType"`
	Description string         `json:"description,omitempty"`
	ExternalID  string         `json:"externalId,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

// ActorUpdate is the request to update an actor.
type ActorUpdate struct {
	Name          *string        `json:"name,omitempty"`
	Description   *string        `json:"description,omitempty"`
	ExternalID    *string        `json:"externalId,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	IsActive      *bool          `json:"isActive,omitempty"`
	AccessScopeID *string        `json:"accessScopeId,omitempty"`
}

// ScopeResource represents an access scope resource.
type ScopeResource struct {
	AccessScopeID string         `json:"accessScopeId"`
	ProjectID     string         `json:"projectId"`
	Slug          string         `json:"slug"`
	Name          string         `json:"name"`
	Description   *string        `json:"description,omitempty"`
	Metadata      map[string]any `json:"metadata,omitempty"`
	IsActive      bool           `json:"isActive"`
	CreatedAt     string         `json:"createdAt,omitempty"`
	UpdatedAt     string         `json:"updatedAt,omitempty"`
}

// ScopeCreate is the request to create an access scope.
type ScopeCreate struct {
	Slug        string         `json:"slug"`
	Name        string         `json:"name"`
	Description string         `json:"description,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
}

// ScopeUpdate is the request to update an access scope.
type ScopeUpdate struct {
	Name        *string        `json:"name,omitempty"`
	Description *string        `json:"description,omitempty"`
	Metadata    map[string]any `json:"metadata,omitempty"`
	IsActive    *bool          `json:"isActive,omitempty"`
}

// PaginatedResult is a paginated API response.
type PaginatedApps struct {
	Data       []AppResource `json:"data"`
	Total      int           `json:"total"`
	Page       int           `json:"page"`
	Limit      int           `json:"limit"`
	TotalPages int           `json:"totalPages"`
}

// ListOptions are options for list endpoints.
type ListOptions struct {
	Page      int
	Limit     int
	SortBy    string
	SortOrder string
	Search    string
}

func (o *ListOptions) toQuery() string {
	if o == nil {
		return ""
	}
	q := url.Values{}
	if o.Page > 0 {
		q.Set("page", fmt.Sprintf("%d", o.Page))
	}
	if o.Limit > 0 {
		q.Set("limit", fmt.Sprintf("%d", o.Limit))
	}
	if o.SortBy != "" {
		q.Set("sortBy", o.SortBy)
	}
	if o.SortOrder != "" {
		q.Set("sortOrder", o.SortOrder)
	}
	if o.Search != "" {
		q.Set("search", o.Search)
	}
	if len(q) == 0 {
		return ""
	}
	return "?" + q.Encode()
}

// ============ Apps API ============

// AppsAPI handles app management.
type AppsAPI struct {
	api *API
}

// List returns all apps in the project.
func (a *AppsAPI) List(ctx context.Context, opts *ListOptions) (*PaginatedApps, error) {
	var result PaginatedApps
	path := "/apps" + opts.toQuery()
	if err := a.api.request(ctx, "GET", path, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Get returns an app by ID.
func (a *AppsAPI) Get(ctx context.Context, appID string) (*AppResource, error) {
	var result AppResource
	if err := a.api.request(ctx, "GET", "/apps/"+appID, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Create creates a new app.
func (a *AppsAPI) Create(ctx context.Context, data AppCreate) (*AppResource, error) {
	var result AppResource
	if err := a.api.request(ctx, "POST", "/apps", data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Update updates an app.
func (a *AppsAPI) Update(ctx context.Context, appID string, data AppUpdate) (*AppResource, error) {
	var result AppResource
	if err := a.api.request(ctx, "PATCH", "/apps/"+appID, data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Delete soft deletes an app.
func (a *AppsAPI) Delete(ctx context.Context, appID string) error {
	return a.api.request(ctx, "DELETE", "/apps/"+appID, nil, nil)
}

// ResetToBlueprint resets an app to its blueprint configuration.
func (a *AppsAPI) ResetToBlueprint(ctx context.Context, appID string) (*AppResource, error) {
	var result AppResource
	if err := a.api.request(ctx, "POST", "/apps/"+appID+"/reset-to-blueprint", nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// ============ Rooms API ============

// RoomsAPI handles room management.
type RoomsAPI struct {
	api *API
}

// List returns all rooms in an app.
func (r *RoomsAPI) List(ctx context.Context, appID string) ([]RoomResource, error) {
	var result []RoomResource
	if err := r.api.request(ctx, "GET", "/apps/"+appID+"/rooms", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// Get returns a room by ID.
func (r *RoomsAPI) Get(ctx context.Context, appID, roomID string) (*RoomResource, error) {
	var result RoomResource
	if err := r.api.request(ctx, "GET", "/apps/"+appID+"/rooms/"+roomID, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Create creates a new dynamic room.
func (r *RoomsAPI) Create(ctx context.Context, appID string, data RoomCreate) (*RoomResource, error) {
	var result RoomResource
	if err := r.api.request(ctx, "POST", "/apps/"+appID+"/rooms", data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Update updates a room.
func (r *RoomsAPI) Update(ctx context.Context, appID, roomID string, data RoomUpdate) (*RoomResource, error) {
	var result RoomResource
	if err := r.api.request(ctx, "PATCH", "/apps/"+appID+"/rooms/"+roomID, data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Delete deletes a dynamic room (static rooms cannot be deleted).
func (r *RoomsAPI) Delete(ctx context.Context, appID, roomID string) error {
	return r.api.request(ctx, "DELETE", "/apps/"+appID+"/rooms/"+roomID, nil, nil)
}

// ============ Actors API ============

// ActorsAPI handles actor management.
type ActorsAPI struct {
	api *API
}

// List returns all actors in the project.
func (a *ActorsAPI) List(ctx context.Context) ([]ActorResource, error) {
	var result []ActorResource
	if err := a.api.request(ctx, "GET", "/actors", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// Get returns an actor by ID.
func (a *ActorsAPI) Get(ctx context.Context, actorID string) (*ActorResource, error) {
	var result ActorResource
	if err := a.api.request(ctx, "GET", "/actors/"+actorID, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Create creates a new actor.
// IMPORTANT: The access token is only returned once! Save it immediately.
func (a *ActorsAPI) Create(ctx context.Context, data ActorCreate) (*ActorWithToken, error) {
	var result ActorWithToken
	if err := a.api.request(ctx, "POST", "/actors", data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Update updates an actor.
func (a *ActorsAPI) Update(ctx context.Context, actorID string, data ActorUpdate) (*ActorResource, error) {
	var result ActorResource
	if err := a.api.request(ctx, "PATCH", "/actors/"+actorID, data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Delete deletes an actor.
func (a *ActorsAPI) Delete(ctx context.Context, actorID string) error {
	return a.api.request(ctx, "DELETE", "/actors/"+actorID, nil, nil)
}

// ============ Scopes API ============

// ScopesAPI handles access scope management.
type ScopesAPI struct {
	api *API
}

// List returns all access scopes in the project.
func (s *ScopesAPI) List(ctx context.Context) ([]ScopeResource, error) {
	var result []ScopeResource
	if err := s.api.request(ctx, "GET", "/scopes", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// Get returns an access scope by ID.
func (s *ScopesAPI) Get(ctx context.Context, scopeID string) (*ScopeResource, error) {
	var result ScopeResource
	if err := s.api.request(ctx, "GET", "/scopes/"+scopeID, nil, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Create creates a new access scope.
func (s *ScopesAPI) Create(ctx context.Context, data ScopeCreate) (*ScopeResource, error) {
	var result ScopeResource
	if err := s.api.request(ctx, "POST", "/scopes", data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Update updates an access scope.
func (s *ScopesAPI) Update(ctx context.Context, scopeID string, data ScopeUpdate) (*ScopeResource, error) {
	var result ScopeResource
	if err := s.api.request(ctx, "PATCH", "/scopes/"+scopeID, data, &result); err != nil {
		return nil, err
	}
	return &result, nil
}

// Delete deletes an access scope.
func (s *ScopesAPI) Delete(ctx context.Context, scopeID string) error {
	return s.api.request(ctx, "DELETE", "/scopes/"+scopeID, nil, nil)
}

// ListActors returns all actors assigned to an access scope.
func (s *ScopesAPI) ListActors(ctx context.Context, scopeID string) ([]ActorResource, error) {
	var result []ActorResource
	if err := s.api.request(ctx, "GET", "/scopes/"+scopeID+"/actors", nil, &result); err != nil {
		return nil, err
	}
	return result, nil
}

// AddActor assigns an actor to this scope.
func (s *ScopesAPI) AddActor(ctx context.Context, scopeID, actorID string) (*ActorResource, error) {
	return s.api.Actors.Update(ctx, actorID, ActorUpdate{AccessScopeID: &scopeID})
}

// RemoveActor removes an actor from its scope (unscopes it).
// Sends {"accessScopeId": null} to the API.
func (s *ScopesAPI) RemoveActor(ctx context.Context, actorID string) (*ActorResource, error) {
	var result ActorResource
	body := map[string]any{"accessScopeId": nil}
	if err := s.api.request(ctx, "PATCH", "/actors/"+actorID, body, &result); err != nil {
		return nil, err
	}
	return &result, nil
}
