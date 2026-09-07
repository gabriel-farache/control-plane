// Package store provides database access for service type instance operations.
package store

import (
	"context"
	"encoding/base64"
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v5"
	"github.com/dcm-project/control-plane/internal/sp/store/model"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	ErrInstanceNotFound = errors.New("service type instance not found")
	// ErrInstanceNotEligible is returned by ReassignAndReset when the
	// instance exists but is mid-deletion (status "deleting" or
	// "pending_deletion"), so a self-heal/reassignment can't resurrect it
	// into "pending" out from under an in-flight delete.
	ErrInstanceNotEligible = errors.New("instance is not eligible for reassignment")
	// ErrInstanceConflict is returned when Create hits a unique-constraint
	// violation (typically a duplicate primary key).
	ErrInstanceConflict = errors.New("service type instance already exists")
)

func isUniqueViolation(err error) bool {
	if errors.Is(err, gorm.ErrDuplicatedKey) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unique constraint") || strings.Contains(msg, "duplicate key")
}

// ServiceTypeInstanceListOptions contains optional fields for listing instances.
type ServiceTypeInstanceListOptions struct {
	ServiceType *string
	AgentName   *string
	ShowDeleted bool
	PageSize    int
	PageToken   *string
}

// ServiceTypeInstanceListResult contains the result of a List operation.
type ServiceTypeInstanceListResult struct {
	Instances     model.ServiceTypeInstanceList
	NextPageToken *string
}

type ServiceTypeInstance interface { //nolint:interfacebloat
	List(ctx context.Context, opts *ServiceTypeInstanceListOptions) (*ServiceTypeInstanceListResult, error)
	Create(ctx context.Context, instance model.ServiceTypeInstance) (*model.ServiceTypeInstance, error)
	Get(ctx context.Context, id string, showDeleted bool) (*model.ServiceTypeInstance, error)
	ExistsByID(ctx context.Context, id string) (bool, error)
	UpdateStatus(ctx context.Context, instanceID string, status string, statusMessage string, outputSpec map[string]any) error
	UpdateStatusFrom(ctx context.Context, instanceID string, fromStatuses []string, agentName string, status string, statusMessage string) (bool, error)
	MarkQueued(ctx context.Context, id string, agentName string) error
	ReassignAndReset(ctx context.Context, id string, agentName string, expectedCurrentAgent string) error
	// MarkForDeletion enrolls id for deletion (deletion_status=SCHEDULED),
	// persisting hardDelete (see model.ServiceTypeInstance.HardDelete) in
	// the same update.
	MarkForDeletion(ctx context.Context, id string, hardDelete bool) error
	ListPendingDeletions(ctx context.Context) ([]model.ServiceTypeInstance, error)
	IncrementDeletionRetry(ctx context.Context, id string) error
	MarkDeletionFailed(ctx context.Context, id string) error
	MarkDeletionComplete(ctx context.Context, id string) error
	HardDelete(ctx context.Context, id string) error
	// HardDeleteFromAgent is HardDelete gated by the currently-assigned
	// agent_name, for CE-event-driven callers that must reject a stale event
	// from a superseded agent. Internal callers with no event to validate
	// against (create rollback, non-deferred delete-on-agent-not-found) keep
	// using HardDelete directly.
	HardDeleteFromAgent(ctx context.Context, id string, agentName string) error
	// MarkDeletionCompleteFromAgent is MarkDeletionComplete gated by the
	// currently-assigned agent_name; see HardDeleteFromAgent.
	MarkDeletionCompleteFromAgent(ctx context.Context, id string, agentName string) error
	// ResetRetryCount re-enrolls an already-SCHEDULED id without losing
	// deletion_requested_at, re-persisting hardDelete (see MarkForDeletion).
	ResetRetryCount(ctx context.Context, id string, hardDelete bool) error
	// FinalizeDeletionAcknowledged atomically checks still-pending
	// (SCHEDULED/FAILED/pending_deletion) + agent_name match, then acts on
	// the current hard_delete, all in one transaction (fixes a TOCTOU race
	// against concurrent ResetRetryCount). finalized=false, nil error means
	// "ack, don't retry" (not found, agent mismatch, or already resolved).
	// wasFailed reports if it was recovering a FAILED row.
	FinalizeDeletionAcknowledged(ctx context.Context, id, agentName string) (finalized, hardDeleted, wasFailed bool, err error)
	// FinalizeAuditGiveUp is FinalizeDeletionAcknowledged for the scheduler's
	// give-up path: same atomic check-and-act, gated on still SCHEDULED, no
	// agent_name gate.
	FinalizeAuditGiveUp(ctx context.Context, id string) (finalized, hardDeleted bool, err error)
}

type ServiceTypeInstanceStore struct {
	db            *gorm.DB
	retryOptsFunc func() []backoff.RetryOption
}

var _ ServiceTypeInstance = (*ServiceTypeInstanceStore)(nil)

// NewServiceTypeInstance constructs the store. If no retry options are passed,
// production backoff is used for Create and HardDelete, built fresh on every
// call (retryOptsFunc) rather than shared across calls: *backoff.ExponentialBackOff
// carries mutable state (currentInterval) that Retry() mutates in place, so
// sharing one instance across concurrent Create/HardDelete calls would be a
// data race. Tests may pass custom options (e.g. sub-second intervals) to
// avoid slow retry exhaustion; those are reused as-is since tests don't
// exercise concurrent retry timing.
func NewServiceTypeInstance(db *gorm.DB, retryOpts ...backoff.RetryOption) ServiceTypeInstance {
	if len(retryOpts) == 0 {
		return &ServiceTypeInstanceStore{db: db, retryOptsFunc: getRetryOptions}
	}
	fixed := append([]backoff.RetryOption(nil), retryOpts...)
	return &ServiceTypeInstanceStore{db: db, retryOptsFunc: func() []backoff.RetryOption { return fixed }}
}

func (s *ServiceTypeInstanceStore) List(ctx context.Context, opts *ServiceTypeInstanceListOptions) (*ServiceTypeInstanceListResult, error) {
	var instances model.ServiceTypeInstanceList
	query := s.db.WithContext(ctx)

	// Default page size
	pageSize := 50
	if opts != nil && opts.PageSize > 0 {
		pageSize = opts.PageSize
	}

	// Decode page token to get offset
	offset := 0
	if opts != nil && opts.PageToken != nil && *opts.PageToken != "" {
		decoded, err := base64.StdEncoding.DecodeString(*opts.PageToken)
		if err == nil {
			if parsedOffset, err := strconv.Atoi(string(decoded)); err == nil {
				offset = parsedOffset
			}
		}
	}

	// Apply filters
	if opts != nil && opts.ServiceType != nil && strings.TrimSpace(*opts.ServiceType) != "" {
		query = query.Where("service_type = ?", *opts.ServiceType)
	}

	if opts != nil && opts.AgentName != nil && strings.TrimSpace(*opts.AgentName) != "" {
		query = query.Where("agent_name = ?", *opts.AgentName)
	}

	// By default, exclude soft-deleted instances; show_deleted includes them
	if opts == nil || !opts.ShowDeleted {
		query = query.Where("deletion_status IS NULL")
	}

	// Apply consistent ordering for pagination
	query = query.Order("create_time ASC, id ASC")

	// Query with limit+1 to detect if there are more results
	query = query.Limit(pageSize + 1).Offset(offset)

	if err := query.Find(&instances).Error; err != nil {
		return nil, err
	}

	// Generate next page token if there are more results
	result := &ServiceTypeInstanceListResult{
		Instances: instances,
	}

	if len(instances) > pageSize {
		// Trim to requested page size
		result.Instances = instances[:pageSize]
		// Encode next offset as page token
		nextOffset := offset + pageSize
		encodedNextPageToken := base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(nextOffset)))
		result.NextPageToken = &encodedNextPageToken
	}

	return result, nil
}

func (s *ServiceTypeInstanceStore) Create(ctx context.Context, instance model.ServiceTypeInstance) (*model.ServiceTypeInstance, error) {
	operation := func() (*model.ServiceTypeInstance, error) {
		if err := s.db.WithContext(ctx).Clauses(clause.Returning{}).Create(&instance).Error; err != nil {
			if isUniqueViolation(err) {
				return nil, backoff.Permanent(ErrInstanceConflict)
			}
			return nil, err
		}
		return &instance, nil
	}

	return backoff.Retry(ctx, operation, s.retryOptsFunc()...)
}

func (s *ServiceTypeInstanceStore) Get(ctx context.Context, id string, showDeleted bool) (*model.ServiceTypeInstance, error) {
	var instance model.ServiceTypeInstance
	query := s.db.WithContext(ctx).Where("id = ?", id)
	if !showDeleted {
		query = query.Where("deletion_status IS NULL")
	}
	if err := query.First(&instance).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, ErrInstanceNotFound
		}
		return nil, err
	}
	return &instance, nil
}

func (s *ServiceTypeInstanceStore) UpdateStatus(ctx context.Context, instanceID string, status string, statusMessage string, outputSpec map[string]any) error {
	update := model.ServiceTypeInstance{
		Status:        status,
		StatusMessage: statusMessage,
	}
	query := s.db.WithContext(ctx).Model(&model.ServiceTypeInstance{}).Where("id = ?", instanceID).Select("status", "status_message")
	if outputSpec != nil {
		update.OutputSpec = outputSpec
		query = query.Select("status", "status_message", "output_spec")
	}
	result := query.Updates(&update)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrInstanceNotFound
	}
	return nil
}

// UpdateStatusFrom atomically transitions status only if the instance's
// current status is one of fromStatuses AND its currently-assigned
// agent_name matches agentName, gated in the same WHERE clause (single
// atomic UPDATE) so a late event from an agent superseded by self-healing
// is rejected even if status cycled back into an allowed fromStatus under
// the new agent. Returns whether the update applied.
func (s *ServiceTypeInstanceStore) UpdateStatusFrom(ctx context.Context, instanceID string, fromStatuses []string, agentName string, status string, statusMessage string) (bool, error) {
	result := s.db.WithContext(ctx).
		Model(&model.ServiceTypeInstance{}).
		Where("id = ? AND status IN ? AND agent_name = ?", instanceID, fromStatuses, agentName).
		Updates(map[string]any{
			"status":         status,
			"status_message": statusMessage,
		})
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// MarkQueued transitions an instance to "queued" and resets pending_started_at
// so the queued-timeout sweep measures from the moment the agent queued the
// request. Gated on agent_name in the same WHERE clause, same as UpdateStatusFrom.
func (s *ServiceTypeInstanceStore) MarkQueued(ctx context.Context, id string, agentName string) error {
	now := time.Now()
	result := s.db.WithContext(ctx).
		Model(&model.ServiceTypeInstance{}).
		Where("id = ? AND status = ? AND agent_name = ?", id, model.StatusPending, agentName).
		Updates(map[string]any{
			"status":             model.StatusQueued,
			"pending_started_at": now,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrInstanceNotFound
	}
	return nil
}

// ReassignAndReset re-points an instance at a new agent and resets it to a
// fresh "pending" state. retry_count is deliberately NOT reset: it's the
// cumulative count of self-heal attempts across every agent tried, so
// maxRetries is enforced globally even if a different agent is found each time.
//
// CAS-guarded to an ALLOW-list of exactly {pending, cancelled} - the two
// statuses the self-healing call sites call this from - rather than a
// deny-list of just {deleting, pending_deletion}: a deny-list would still
// let "provisioning" be reassigned, silently duplicating provisioning if the
// response consumer applies a creation-acknowledged in the same narrow
// window. Also requires deletion_status IS NULL, so an instance with a
// delete already scheduled can't be resurrected out from under the cleanup
// scheduler. Anything outside the allow-list returns ErrInstanceNotEligible.
//
// expectedCurrentAgent additionally CASes on agent_name: status alone stays
// "pending" across a successful reassignment, so two concurrent callers
// racing to reassign the SAME instance (e.g. two control-plane replicas, one
// via its own sweep claim and one via a sibling self-heal from a different
// resource in the same run) would otherwise both pass a status-only check
// and both publish a create to a different agent. Requiring the caller's
// observed agent_name to still match makes the second racer's update affect
// zero rows and fail closed with ErrInstanceNotEligible instead.
func (s *ServiceTypeInstanceStore) ReassignAndReset(ctx context.Context, id string, agentName string, expectedCurrentAgent string) error {
	now := time.Now()
	result := s.db.WithContext(ctx).
		Model(&model.ServiceTypeInstance{}).
		Where("id = ? AND status IN ? AND deletion_status IS NULL AND agent_name = ?", id, []string{model.StatusPending, model.StatusCancelled}, expectedCurrentAgent).
		Updates(map[string]any{
			"agent_name":         agentName,
			"status":             model.StatusPending,
			"status_message":     "",
			"pending_started_at": now,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected > 0 {
		return nil
	}

	exists, err := s.ExistsByID(ctx, id)
	if err != nil {
		return err
	}
	if !exists {
		return ErrInstanceNotFound
	}
	return ErrInstanceNotEligible
}

func (s *ServiceTypeInstanceStore) ExistsByID(ctx context.Context, id string) (bool, error) {
	var instance model.ServiceTypeInstance
	err := s.db.WithContext(ctx).Select("id").Where("id = ?", id).Take(&instance).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

const (
	DeletionStatusScheduled = "SCHEDULED"
	DeletionStatusFailed    = "FAILED"
	DeletionStatusDeleted   = "DELETED"
)

func (s *ServiceTypeInstanceStore) MarkForDeletion(ctx context.Context, id string, hardDelete bool) error {
	now := time.Now()
	result := s.db.WithContext(ctx).
		Model(&model.ServiceTypeInstance{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"deletion_status":       DeletionStatusScheduled,
			"deletion_requested_at": now,
			"retry_count":           0,
			"last_deletion_attempt": nil,
			"hard_delete":           hardDelete,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrInstanceNotFound
	}
	return nil
}

func (s *ServiceTypeInstanceStore) ListPendingDeletions(ctx context.Context) ([]model.ServiceTypeInstance, error) {
	var instances []model.ServiceTypeInstance
	if err := s.db.WithContext(ctx).
		Where("deletion_status = ?", DeletionStatusScheduled).
		Order("deletion_requested_at ASC").
		Find(&instances).Error; err != nil {
		return nil, err
	}
	return instances, nil
}

func (s *ServiceTypeInstanceStore) IncrementDeletionRetry(ctx context.Context, id string) error {
	now := time.Now()
	result := s.db.WithContext(ctx).
		Model(&model.ServiceTypeInstance{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"retry_count":           gorm.Expr("retry_count + 1"),
			"last_deletion_attempt": now,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrInstanceNotFound
	}
	return nil
}

func (s *ServiceTypeInstanceStore) MarkDeletionFailed(ctx context.Context, id string) error {
	result := s.db.WithContext(ctx).
		Model(&model.ServiceTypeInstance{}).
		Where("id = ? AND deletion_status <> ?", id, DeletionStatusDeleted).
		Update("deletion_status", DeletionStatusFailed)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		var existing model.ServiceTypeInstance
		err := s.db.WithContext(ctx).Select("id", "deletion_status").Where("id = ?", id).First(&existing).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return ErrInstanceNotFound
		}
		if err != nil {
			return err
		}
		// Already DELETED (or otherwise not eligible): do not regress status.
		return nil
	}
	return nil
}

func (s *ServiceTypeInstanceStore) MarkDeletionComplete(ctx context.Context, id string) error {
	result := s.db.WithContext(ctx).
		Model(&model.ServiceTypeInstance{}).
		Where("id = ?", id).
		Update("deletion_status", DeletionStatusDeleted)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrInstanceNotFound
	}
	return nil
}

// MarkDeletionCompleteFromAgent is MarkDeletionComplete additionally gated on
// agentName matching the instance's currently-assigned agent_name, for
// CE-event-driven callers (handleDeletionAcknowledged) that must not act on a
// stale event from a superseded agent. Returns ErrInstanceNotFound (matching
// MarkDeletionComplete's existing sentinel) both when the instance genuinely
// doesn't exist and when it exists but the agent doesn't match - callers
// already treat that sentinel as "ack, don't retry" either way.
func (s *ServiceTypeInstanceStore) MarkDeletionCompleteFromAgent(ctx context.Context, id string, agentName string) error {
	result := s.db.WithContext(ctx).
		Model(&model.ServiceTypeInstance{}).
		Where("id = ? AND agent_name = ?", id, agentName).
		Update("deletion_status", DeletionStatusDeleted)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrInstanceNotFound
	}
	return nil
}

func (s *ServiceTypeInstanceStore) HardDelete(ctx context.Context, id string) error {
	operation := func() (any, error) {
		result := s.db.WithContext(ctx).Unscoped().Where("id = ?", id).Delete(&model.ServiceTypeInstance{})
		if result.Error != nil {
			return nil, result.Error
		}
		if result.RowsAffected == 0 {
			return nil, backoff.Permanent(ErrInstanceNotFound)
		}
		return nil, nil
	}

	_, err := backoff.Retry(ctx, operation, s.retryOptsFunc()...)
	return err
}

// HardDeleteFromAgent is HardDelete additionally gated on agentName matching
// the instance's currently-assigned agent_name; see
// MarkDeletionCompleteFromAgent for the rationale and error-sentinel
// behavior.
func (s *ServiceTypeInstanceStore) HardDeleteFromAgent(ctx context.Context, id string, agentName string) error {
	operation := func() (any, error) {
		result := s.db.WithContext(ctx).Unscoped().Where("id = ? AND agent_name = ?", id, agentName).Delete(&model.ServiceTypeInstance{})
		if result.Error != nil {
			return nil, result.Error
		}
		if result.RowsAffected == 0 {
			return nil, backoff.Permanent(ErrInstanceNotFound)
		}
		return nil, nil
	}

	_, err := backoff.Retry(ctx, operation, s.retryOptsFunc()...)
	return err
}

func (s *ServiceTypeInstanceStore) ResetRetryCount(ctx context.Context, id string, hardDelete bool) error {
	result := s.db.WithContext(ctx).
		Model(&model.ServiceTypeInstance{}).
		Where("id = ?", id).
		Updates(map[string]any{
			"deletion_status":       DeletionStatusScheduled,
			"retry_count":           0,
			"last_deletion_attempt": nil,
			"hard_delete":           hardDelete,
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return ErrInstanceNotFound
	}
	return nil
}

// lockForUpdate row-locks query on Postgres only: SQLite's driver doesn't
// support "FOR UPDATE", but its single-writer serialization already gives
// the same guarantee for tests.
func lockForUpdate(query *gorm.DB) *gorm.DB {
	if query.Dialector.Name() == "postgres" {
		return query.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	return query
}

// stillPendingDeletion is true for deletion_status SCHEDULED or FAILED
// (FAILED isn't terminal - a late ack still resolves it), or
// status=pending_deletion with no deletion_status set yet.
func stillPendingDeletion(instance *model.ServiceTypeInstance) bool {
	if instance.Status == model.StatusPendingDeletion {
		return true
	}
	if instance.DeletionStatus == nil {
		return false
	}
	return *instance.DeletionStatus == DeletionStatusScheduled || *instance.DeletionStatus == DeletionStatusFailed
}

func (s *ServiceTypeInstanceStore) FinalizeDeletionAcknowledged(ctx context.Context, id, agentName string) (finalized, hardDeleted, wasFailed bool, err error) {
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var instance model.ServiceTypeInstance
		query := lockForUpdate(tx.Model(&model.ServiceTypeInstance{}).Where("id = ?", id))
		if txErr := query.First(&instance).Error; txErr != nil {
			if errors.Is(txErr, gorm.ErrRecordNotFound) {
				return nil
			}
			return txErr
		}
		if instance.AgentName == nil || *instance.AgentName != agentName {
			return nil
		}
		if !stillPendingDeletion(&instance) {
			return nil
		}

		wasFailed = instance.DeletionStatus != nil && *instance.DeletionStatus == DeletionStatusFailed
		hardDeleted = instance.HardDelete
		if hardDeleted {
			if txErr := tx.Unscoped().Where("id = ? AND agent_name = ?", id, agentName).Delete(&model.ServiceTypeInstance{}).Error; txErr != nil {
				return txErr
			}
		} else {
			if txErr := tx.Model(&model.ServiceTypeInstance{}).Where("id = ? AND agent_name = ?", id, agentName).Update("deletion_status", DeletionStatusDeleted).Error; txErr != nil {
				return txErr
			}
		}
		finalized = true
		return nil
	})
	if err != nil {
		return false, false, false, err
	}
	return finalized, hardDeleted, wasFailed, nil
}

func (s *ServiceTypeInstanceStore) FinalizeAuditGiveUp(ctx context.Context, id string) (finalized, hardDeleted bool, err error) {
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var instance model.ServiceTypeInstance
		query := lockForUpdate(tx.Model(&model.ServiceTypeInstance{}).Where("id = ?", id))
		if txErr := query.First(&instance).Error; txErr != nil {
			if errors.Is(txErr, gorm.ErrRecordNotFound) {
				return nil
			}
			return txErr
		}
		if instance.DeletionStatus == nil || *instance.DeletionStatus != DeletionStatusScheduled {
			return nil
		}

		hardDeleted = instance.HardDelete
		if hardDeleted {
			if txErr := tx.Unscoped().Where("id = ?", id).Delete(&model.ServiceTypeInstance{}).Error; txErr != nil {
				return txErr
			}
		} else {
			if txErr := tx.Model(&model.ServiceTypeInstance{}).Where("id = ?", id).Update("deletion_status", DeletionStatusDeleted).Error; txErr != nil {
				return txErr
			}
		}
		finalized = true
		return nil
	})
	if err != nil {
		return false, false, err
	}
	return finalized, hardDeleted, nil
}

// getRetryOptions returns common retry configuration for database operations
func getRetryOptions() []backoff.RetryOption {
	b := backoff.NewExponentialBackOff()
	b.InitialInterval = 1 * time.Second // Wait 1 second before first retry
	b.MaxInterval = 4 * time.Second     // Cap maximum wait time at 4 seconds
	b.Multiplier = 2.0                  // Double the wait time after each retry (1s, 2s, 4s)

	return []backoff.RetryOption{
		backoff.WithBackOff(b),
		backoff.WithMaxTries(4), // 1 initial attempt + 3 retries = 4 max tries
	}
}
