package store

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/dcm-project/control-plane/internal/placement/store/model"
	"github.com/dcm-project/control-plane/internal/placement/types"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrResourceNotFound = errors.New("resource not found")
	ErrResourceIdExist  = errors.New("resource with id already exists")
)

type CleanupIntent string

const (
	CleanupIntentRollback CleanupIntent = "ROLLBACK"
	CleanupIntentExplicit CleanupIntent = "EXPLICIT_DELETE"
)

// ResourceListOptions contains optional fields for listing runs.
type ResourceListOptions struct {
	AgentName *string
	PageSize  int
	PageToken *string
}

// ResourceListResult contains resources for a page of runs (complete sets per run_id).
// PageSize is the number of runs; Resources may contain more rows than PageSize.
type ResourceListResult struct {
	Resources     model.ResourceList
	NextPageToken *string
}

// Resource defines the repository interface for Resource operations
type Resource interface { //nolint:interfacebloat
	ListRun(ctx context.Context, opts *ResourceListOptions) (*ResourceListResult, error)
	Create(ctx context.Context, request model.Resource) (*model.Resource, error)
	CreateBatch(ctx context.Context, resources []model.Resource) ([]model.Resource, error)
	Delete(ctx context.Context, id string) error
	Get(ctx context.Context, id string) (*model.Resource, error)
	ListByRunID(ctx context.Context, runID string) (model.ResourceList, error)
	DeleteByRunID(ctx context.Context, runID string) error
	UpdateRunID(ctx context.Context, oldRunID, newRunID string) error
	UpdateStatus(ctx context.Context, id, status string) error
	// UpdateStatusFrom atomically transitions status only when the current
	// status is one of fromStatuses. Returns whether the update applied.
	UpdateStatusFrom(ctx context.Context, id string, fromStatuses []string, toStatus string) (bool, error)
	UpdateStatusByRunID(ctx context.Context, runID, status string) error
	PrepareRunDeletion(ctx context.Context, runID string, intent CleanupIntent) error
	UpdateAgentName(ctx context.Context, id string, agentName string) error
	UpdateAgentError(ctx context.Context, id, agentName string, streamSequence uint64, details types.AgentErrorDetails) (bool, error)
	UpdatePlacementDecision(ctx context.Context, id, agentName, approval string) error
}

type ResourceStore struct {
	db *gorm.DB
}

var _ Resource = (*ResourceStore)(nil)

// NewResource creates a new Resource repository
func NewResource(db *gorm.DB) Resource {
	return &ResourceStore{db: db}
}

// ListRun paginates by distinct run_id, then loads the full resource set for each
// run on the page. PageSize is the number of runs, not resource rows.
func (s *ResourceStore) ListRun(ctx context.Context, opts *ResourceListOptions) (*ResourceListResult, error) {
	// Default page size
	pageSize := 100
	if opts != nil && opts.PageSize > 0 {
		pageSize = opts.PageSize
	}

	// Decode page token to get offset
	offset := 0
	if opts != nil {
		offset = decodePageToken(opts.PageToken)
	}

	query := s.db.WithContext(ctx).Model(&model.Resource{})

	// Apply filters
	if opts != nil && opts.AgentName != nil && strings.TrimSpace(*opts.AgentName) != "" {
		query = query.Where("agent_name = ?", *opts.AgentName)
	}

	// Page distinct run_ids (limit+1 to detect if there are more results).
	var runIDs []string
	if err := query.
		// Session: allows reuse of the original query when loading resources
		Session(&gorm.Session{}).
		Distinct("run_id").
		Order("run_id ASC").
		Limit(pageSize+1).
		Offset(offset).
		// Pluck: select run_id column into []string
		Pluck("run_id", &runIDs).Error; err != nil {
		return nil, err
	}

	// Build next page token before trimming to page size
	nextToken := generateNextPageToken(len(runIDs), pageSize, offset)
	if len(runIDs) > pageSize {
		runIDs = runIDs[:pageSize]
	}

	var resources model.ResourceList
	if len(runIDs) > 0 {
		if err := query.
			Where("run_id IN ?", runIDs).
			Order("run_id ASC, dag_level ASC, name ASC, id ASC").
			Find(&resources).Error; err != nil {
			return nil, err
		}
	}

	return &ResourceListResult{
		Resources:     resources,
		NextPageToken: nextToken,
	}, nil
}

func (s *ResourceStore) Create(ctx context.Context, request model.Resource) (*model.Resource, error) {
	if err := s.db.WithContext(ctx).Clauses(clause.Returning{}).Create(&request).Error; err != nil {
		return nil, mapResourceCreateError(err)
	}
	return &request, nil
}

func (s *ResourceStore) CreateBatch(ctx context.Context, resources []model.Resource) ([]model.Resource, error) {
	if len(resources) == 0 {
		return nil, nil
	}
	if err := s.db.WithContext(ctx).Clauses(clause.Returning{}).Create(&resources).Error; err != nil {
		return nil, mapResourceCreateError(err)
	}
	return resources, nil
}

func mapResourceCreateError(err error) error {
	errMsg := err.Error()
	if errors.Is(err, gorm.ErrDuplicatedKey) ||
		strings.Contains(errMsg, "UNIQUE constraint") ||
		strings.Contains(errMsg, "duplicate key") {
		return ErrResourceIdExist
	}
	return err
}

func (s *ResourceStore) Delete(ctx context.Context, id string) error {
	result := s.db.WithContext(ctx).Where("id = ?", id).Delete(&model.Resource{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrResourceNotFound
	}
	return nil
}

func (s *ResourceStore) Get(ctx context.Context, id string) (*model.Resource, error) {
	var request model.Resource
	if err := s.db.WithContext(ctx).First(&request, "id = ?", id).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrResourceNotFound
		}
		return nil, err
	}
	return &request, nil
}

func (s *ResourceStore) ListByRunID(ctx context.Context, runID string) (model.ResourceList, error) {
	var resources model.ResourceList
	if err := s.db.WithContext(ctx).
		Where("run_id = ?", runID).
		Order("dag_level ASC, name ASC, id ASC").
		Find(&resources).Error; err != nil {
		return nil, err
	}
	return resources, nil
}

func (s *ResourceStore) DeleteByRunID(ctx context.Context, runID string) error {
	result := s.db.WithContext(ctx).Where("run_id = ?", runID).Delete(&model.Resource{})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrResourceNotFound
	}
	return nil
}

func (s *ResourceStore) UpdateRunID(ctx context.Context, oldRunID, newRunID string) error {
	result := s.db.WithContext(ctx).Model(&model.Resource{}).
		Where("run_id = ?", oldRunID).
		Update("run_id", newRunID)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrResourceNotFound
	}
	return nil
}

func (s *ResourceStore) UpdateStatus(ctx context.Context, id, status string) error {
	result := s.db.WithContext(ctx).Model(&model.Resource{}).
		Where("id = ?", id).
		Update("status", status)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrResourceNotFound
	}
	return nil
}

func (s *ResourceStore) UpdateStatusFrom(ctx context.Context, id string, fromStatuses []string, toStatus string) (bool, error) {
	// Returns whether the update applied (false when status was not in fromStatuses).
	if len(fromStatuses) == 0 {
		return false, nil
	}
	result := s.db.WithContext(ctx).Model(&model.Resource{}).
		Where("id = ? AND status IN ?", id, fromStatuses).
		Update("status", toStatus)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

func (s *ResourceStore) UpdateStatusByRunID(ctx context.Context, runID, status string) error {
	result := s.db.WithContext(ctx).Model(&model.Resource{}).
		Where("run_id = ?", runID).
		Update("status", status)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrResourceNotFound
	}
	return nil
}

// PrepareRunDeletion atomically records cleanup intent and schedules pending rows.
func (s *ResourceStore) PrepareRunDeletion(ctx context.Context, runID string, intent CleanupIntent) error {
	if intent != CleanupIntentRollback && intent != CleanupIntentExplicit {
		return fmt.Errorf("invalid cleanup intent %q", intent)
	}
	result := s.db.WithContext(ctx).Model(&model.Resource{}).
		Where("run_id = ?", runID).
		Updates(map[string]any{
			"status": gorm.Expr("CASE WHEN status IN (?, ?) THEN status ELSE ? END",
				types.ResourceStatusDeleting, types.ResourceStatusDeleted, types.ResourceStatusPendingDeletion),
			"cleanup_intent": gorm.Expr("CASE WHEN ? = ? THEN ? WHEN cleanup_intent = ? THEN ? ELSE ? END",
				intent, CleanupIntentExplicit, CleanupIntentExplicit,
				CleanupIntentExplicit, CleanupIntentExplicit, CleanupIntentRollback),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrResourceNotFound
	}
	return nil
}

// UpdateAgentName updates the agent_name column for observability after the
// self-healing loop re-routes a resource to a different agent.
func (s *ResourceStore) UpdateAgentName(ctx context.Context, id string, agentName string) error {
	result := s.db.WithContext(ctx).Model(&model.Resource{}).Where("id = ?", id).Update("agent_name", agentName)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrResourceNotFound
	}
	return nil
}

// UpdateAgentError stores the newest assigned-agent error and accepts equal-sequence retries.
func (s *ResourceStore) UpdateAgentError(ctx context.Context, id, agentName string, streamSequence uint64, details types.AgentErrorDetails) (bool, error) {
	result := s.db.WithContext(ctx).Model(&model.Resource{}).Where(
		"id = ? AND agent_name = ? AND agent_error_stream_sequence < ?",
		id, agentName, streamSequence,
	).Select("AgentErrorDetails", "AgentErrorStreamSequence").Updates(model.Resource{
		AgentErrorDetails:        &details,
		AgentErrorStreamSequence: streamSequence,
	})
	if result.Error != nil {
		return false, result.Error
	}
	if result.RowsAffected > 0 {
		return true, nil
	}

	var count int64
	err := s.db.WithContext(ctx).Model(&model.Resource{}).Where(
		"id = ? AND agent_name = ? AND agent_error_stream_sequence = ?",
		id, agentName, streamSequence,
	).Count(&count).Error
	return count > 0, err
}

func (s *ResourceStore) UpdatePlacementDecision(ctx context.Context, id, agentName, approval string) error {
	result := s.db.WithContext(ctx).Model(&model.Resource{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"agent_name":      agentName,
			"approval_status": approval,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrResourceNotFound
	}
	return nil
}
