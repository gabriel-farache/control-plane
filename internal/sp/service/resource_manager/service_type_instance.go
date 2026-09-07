// Package resource_manager implements service type instance management.
package resource_manager

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dcm-project/control-plane/api/sp/v1alpha1/resource_manager"
	agentstore "github.com/dcm-project/control-plane/internal/agent/store/agent"
	agentmodel "github.com/dcm-project/control-plane/internal/agent/store/model"
	"github.com/dcm-project/control-plane/internal/sp/logging"
	"github.com/dcm-project/control-plane/internal/sp/messaging"
	"github.com/dcm-project/control-plane/internal/sp/service"
	"github.com/dcm-project/control-plane/internal/sp/store"
	"github.com/dcm-project/control-plane/internal/sp/store/model"
	rmstore "github.com/dcm-project/control-plane/internal/sp/store/resource_manager"
	"github.com/google/uuid"
)

type InstanceService struct {
	store      store.Store
	publisher  *messaging.Publisher
	agentStore agentstore.Agent
}

// NewInstanceService constructs InstanceService. publisher may be nil (e.g. in
// tests); when nil, NATS publishing is skipped.
func NewInstanceService(store store.Store, publisher *messaging.Publisher, agentSt agentstore.Agent) *InstanceService {
	return &InstanceService{
		store:      store,
		publisher:  publisher,
		agentStore: agentSt,
	}
}

func (s *InstanceService) CreateInstance(ctx context.Context, request *resource_manager.ServiceTypeInstance, queryID *string, agentName string) (*resource_manager.ServiceTypeInstance, error) {
	log := logging.FromContext(ctx)
	log.Debug("Creating instance", "agent_name", agentName)

	serviceType, ok := request.Spec["service_type"].(string)
	if !ok {
		return nil, service.NewValidationError("spec.service_type is required and must be a string")
	}
	if strings.TrimSpace(serviceType) == "" {
		return nil, service.NewValidationError("spec.service_type must not be empty")
	}

	// An empty agentName would create a permanently orphaned Pending row:
	// sweepPending's query filters on `agent_name IS NOT NULL`.
	if strings.TrimSpace(agentName) == "" {
		return nil, service.NewValidationError("agent_name is required and must not be empty")
	}

	if s.publisher == nil {
		return nil, service.NewUnavailableError("nats publisher unavailable, cannot dispatch to agent")
	}
	if err := s.validateAgent(ctx, agentName, serviceType); err != nil {
		return nil, err
	}

	instanceID, err := s.resolveInstanceID(ctx, queryID)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	instance := model.ServiceTypeInstance{
		ID:               *instanceID,
		ServiceType:      serviceType,
		Status:           model.StatusPending,
		Spec:             request.Spec,
		AgentName:        &agentName,
		PendingStartedAt: &now,
	}

	created, err := s.store.ServiceTypeInstance().Create(ctx, instance)
	if err != nil {
		if errors.Is(err, rmstore.ErrInstanceConflict) {
			return nil, service.NewConflictError(fmt.Sprintf("instance with ID '%s' already exists", *instanceID))
		}
		log.Error("Failed to create instance in store", "instance_id", *instanceID, "error", err)
		return nil, service.NewInternalError(fmt.Sprintf("failed to create database record for instance %s: %v", *instanceID, err))
	}

	subject, pubErr := s.resolveAgentSubject(ctx, agentName)
	if pubErr != nil {
		log.Error("Failed to resolve agent topic, rolling back instance", "agent_name", agentName, "error", pubErr)
		_ = s.store.ServiceTypeInstance().HardDelete(ctx, created.ID)
		return nil, service.NewProvisioningError(fmt.Sprintf("agent '%s' topic resolution failed: %v", agentName, pubErr))
	}

	pubErr = s.publisher.PublishCreate(ctx, subject, messaging.CreatePayload{
		ResourceID:  created.ID,
		ServiceType: serviceType,
		Spec:        request.Spec,
	})
	if pubErr != nil {
		log.Error("Failed to publish create event, rolling back instance", "instance_id", created.ID, "error", pubErr)
		_ = s.store.ServiceTypeInstance().HardDelete(ctx, created.ID)
		return nil, service.NewProvisioningError(fmt.Sprintf("failed to publish create request for instance %s: %v", created.ID, pubErr))
	}

	log.Info("Instance created", "instance_id", created.ID, "status", created.Status, "agent_name", agentName)
	return ModelToAPI(created), nil
}

// ReassignAgent re-points an existing instance at a new agent and re-triggers
// provisioning from scratch (fresh "pending" state, retry count reset). Used
// by the self-healing loop when the originally assigned agent fails, times
// out, or is excluded from re-evaluation.
//
// expectedCurrentAgent is CASed against agent_name in the same update as the
// status check, and must be the caller's own observation of the instance's
// agent (e.g. the excluded agent), not derived from a fresh read here: a
// fresh read would just reflect whatever the most recent writer set,
// silently turning the CAS into an unconditional overwrite and defeating the
// cross-replica/sibling-heal race it exists to catch.
func (s *InstanceService) ReassignAgent(ctx context.Context, instanceID string, agentName string, expectedCurrentAgent string) error {
	log := logging.FromContext(ctx)

	instance, err := s.store.ServiceTypeInstance().Get(ctx, instanceID, false)
	if err != nil {
		if errors.Is(err, rmstore.ErrInstanceNotFound) {
			return service.NewNotFoundError(fmt.Sprintf("instance %s not found", instanceID))
		}
		return service.NewInternalError(fmt.Sprintf("failed to retrieve instance: %v", err))
	}

	if err := s.validateAgent(ctx, agentName, instance.ServiceType); err != nil {
		return err
	}

	if s.publisher == nil {
		return service.NewUnavailableError("nats publisher unavailable, cannot reassign instance")
	}

	subject, err := s.resolveAgentSubject(ctx, agentName)
	if err != nil {
		return service.NewProvisioningError(fmt.Sprintf("agent '%s' topic resolution failed: %v", agentName, err))
	}

	if err := s.store.ServiceTypeInstance().ReassignAndReset(ctx, instanceID, agentName, expectedCurrentAgent); err != nil {
		if errors.Is(err, rmstore.ErrInstanceNotFound) {
			return service.NewNotFoundError(fmt.Sprintf("instance %s not found", instanceID))
		}
		if errors.Is(err, rmstore.ErrInstanceNotEligible) {
			return service.NewConflictError(fmt.Sprintf("instance %s is being deleted and cannot be reassigned", instanceID))
		}
		return service.NewInternalError(fmt.Sprintf("failed to reassign instance %s: %v", instanceID, err))
	}

	if pubErr := s.publisher.PublishCreate(ctx, subject, messaging.CreatePayload{
		ResourceID:  instanceID,
		ServiceType: instance.ServiceType,
		Spec:        instance.Spec,
	}); pubErr != nil {
		log.Error("Failed to publish create after reassignment", "instance_id", instanceID, "agent_name", agentName, "error", pubErr)
		return service.NewProvisioningError(fmt.Sprintf("failed to publish create for reassigned instance %s: %v", instanceID, pubErr))
	}

	log.Info("Instance reassigned to new agent", "instance_id", instanceID, "agent_name", agentName)
	return nil
}

func (s *InstanceService) resolveAgentSubject(ctx context.Context, agentName string) (string, error) {
	agent, err := s.agentStore.GetByName(ctx, agentName)
	if err != nil {
		return "", err
	}
	return agent.TopicName, nil
}

func (s *InstanceService) validateAgent(ctx context.Context, agentName string, serviceType string) error {
	if strings.TrimSpace(agentName) == "" {
		return service.NewValidationError("agent_name is required and must not be empty")
	}
	if s.agentStore == nil {
		return service.NewUnavailableError("agent store unavailable, cannot validate or dispatch to agent")
	}
	agent, err := s.agentStore.GetByName(ctx, agentName)
	if err != nil {
		if errors.Is(err, agentstore.ErrAgentNotFound) {
			return service.NewNotFoundError(fmt.Sprintf("agent '%s' not found", agentName))
		}
		return service.NewInternalError(fmt.Sprintf("failed to look up agent '%s': %v", agentName, err))
	}

	if agent.HealthStatus != agentmodel.AgentHealthStatusReady {
		return service.NewUnavailableError(fmt.Sprintf("agent '%s' is %s", agentName, agent.HealthStatus))
	}

	for _, st := range agent.ServiceTypes {
		if st == serviceType {
			return nil
		}
	}
	return service.NewValidationError(fmt.Sprintf("agent '%s' does not serve service type '%s'", agentName, serviceType))
}

func (s *InstanceService) GetInstance(ctx context.Context, instanceID string, showDeleted bool) (*resource_manager.ServiceTypeInstance, error) {
	log := logging.FromContext(ctx)
	log.Debug("Getting instance", "instance_id", instanceID, "show_deleted", showDeleted)

	instance, err := s.getInstanceModel(ctx, instanceID, showDeleted)
	if err != nil {
		return nil, err
	}
	return ModelToAPI(instance), nil
}

func (s *InstanceService) getInstanceModel(ctx context.Context, instanceID string, showDeleted bool) (*model.ServiceTypeInstance, error) {
	log := logging.FromContext(ctx)
	instance, err := s.store.ServiceTypeInstance().Get(ctx, instanceID, showDeleted)
	if err != nil {
		if errors.Is(err, rmstore.ErrInstanceNotFound) {
			return nil, service.NewNotFoundError(fmt.Sprintf("instance %s not found", instanceID))
		}
		log.Error("Failed to get instance from store", "instance_id", instanceID, "error", err)
		return nil, service.NewInternalError(fmt.Sprintf("failed to retrieve instance: %v", err))
	}
	return instance, nil
}

// GetOutputSpec returns the persisted output_spec for an instance.
func (s *InstanceService) GetOutputSpec(ctx context.Context, instanceID string) (map[string]any, error) {
	log := logging.FromContext(ctx)
	log.Debug("Getting instance output_spec", "instance_id", instanceID)

	instance, err := s.getInstanceModel(ctx, instanceID, false)
	if err != nil {
		return nil, err
	}
	return instance.OutputSpec, nil
}

func (s *InstanceService) ListInstances(ctx context.Context, serviceType, agentName *string, showDeleted bool, maxPageSize *int, pageToken *string) (*resource_manager.ServiceTypeInstanceList, error) {
	log := logging.FromContext(ctx)
	log.Debug("Listing instances",
		"service_type", serviceType,
		"agent_name", agentName,
		"show_deleted", showDeleted,
		"max_page_size", maxPageSize,
		"has_page_token", pageToken != nil && *pageToken != "",
	)

	opts := &rmstore.ServiceTypeInstanceListOptions{
		ServiceType: serviceType,
		AgentName:   agentName,
		ShowDeleted: showDeleted,
	}

	if maxPageSize != nil {
		if *maxPageSize > 0 && *maxPageSize <= 100 {
			opts.PageSize = *maxPageSize
		} else {
			return nil, service.NewValidationError("page size must be between 1 and 100")
		}
	}

	if pageToken != nil && *pageToken != "" {
		opts.PageToken = pageToken
	}

	result, err := s.store.ServiceTypeInstance().List(ctx, opts)
	if err != nil {
		log.Error("Failed to list instances from store", "error", err)
		return nil, service.NewInternalError(fmt.Sprintf("failed to list instances: %v", err))
	}

	apiInstances := make([]resource_manager.ServiceTypeInstance, len(result.Instances))
	for i, inst := range result.Instances {
		apiInstances[i] = *ModelToAPI(&inst)
	}

	log.Debug("Instances listed",
		"count", len(apiInstances),
		"has_next_page", result.NextPageToken != nil,
	)
	return &resource_manager.ServiceTypeInstanceList{
		Instances:     &apiInstances,
		NextPageToken: result.NextPageToken,
	}, nil
}

// DeleteInstance removes an instance. For an agent-routed instance (has
// agent_name and a configured publisher), both deferred and non-deferred
// deletes enroll it for deletion (deletion_status=SCHEDULED, or a retry
// reset if already enrolled) before attempting to publish a delete request
// to the agent - persisting HardDelete=!deferred in that same enroll step,
// so the mode survives a fast ack, a publish failure, or a scheduler retry.
// A publish failure other than agent-not-found is logged and never
// returned to the caller: the enrollment guarantees
// internal/sp/cleanup/scheduler.go's ProcessPendingDeletions retries it.
// Agent-not-found is resolved immediately for a non-deferred delete (purge
// now) but left SCHEDULED for a deferred delete, resolved later by the
// cleanup scheduler's audit-give-up logic. HardDelete decides hard-delete
// (no tombstone) vs. soft-complete (deletion_status=DELETED tombstone,
// visible via show_deleted=true) once the deletion completes.
func (s *InstanceService) DeleteInstance(ctx context.Context, instanceID string, deferred bool) error {
	log := logging.FromContext(ctx)
	log.Debug("Deleting instance", "instance_id", instanceID, "deferred", deferred)

	instance, err := s.store.ServiceTypeInstance().Get(ctx, instanceID, true)
	if err != nil {
		if errors.Is(err, rmstore.ErrInstanceNotFound) {
			return service.NewNotFoundError(fmt.Sprintf("instance %s not found", instanceID))
		}
		log.Error("Failed to get instance for deletion", "instance_id", instanceID, "error", err)
		return service.NewInternalError(fmt.Sprintf("failed to retrieve instance: %v", err))
	}

	if deferred {
		if err := s.enrollForDeletion(ctx, instance, deferred); err != nil {
			return err
		}
		if err := s.publishDeleteToAgent(ctx, instance); err != nil {
			log.Warn("Failed to publish deferred delete to agent, cleanup scheduler will retry",
				"instance_id", instanceID, "error", err)
		}
		log.Info("Scheduled deferred deletion", "instance_id", instanceID)
		return nil
	}

	// Nothing to wait on: never agent-routed, or no publisher configured to
	// dispatch a delete request in the first place. Delete now, matching the
	// cleanup scheduler's own handling of the same case.
	if instance.AgentName == nil || s.publisher == nil {
		if err := s.store.ServiceTypeInstance().HardDelete(ctx, instanceID); err != nil {
			return service.NewInternalError(fmt.Sprintf("failed to delete instance %s: %v", instanceID, err))
		}
		log.Info("Instance deleted (no agent to notify)", "instance_id", instance.ID)
		return nil
	}

	if err := s.enrollForDeletion(ctx, instance, deferred); err != nil {
		return err
	}

	if err := s.publishDeleteToAgent(ctx, instance); err != nil {
		if errors.Is(err, agentstore.ErrAgentNotFound) {
			// The agent is gone, so no "deletion-acknowledged" will ever
			// arrive: purge now instead of stranding the instance in
			// SCHEDULED forever, matching the cleanup scheduler's own
			// audit-giveup behavior for the deferred path.
			log.Warn("Agent not found for non-deferred delete, deleting locally without confirmation",
				"instance_id", instanceID, "agent_name", *instance.AgentName)
			if hardErr := s.store.ServiceTypeInstance().HardDelete(ctx, instanceID); hardErr != nil {
				return service.NewInternalError(fmt.Sprintf("failed to delete instance %s: %v", instanceID, hardErr))
			}
			return nil
		}
		// Any other publish failure (NATS unavailable, topic resolution,
		// etc.) is never surfaced to the caller: the instance stays
		// enrolled and the cleanup scheduler retries the publish.
		log.Warn("Failed to publish delete to agent, cleanup scheduler will retry",
			"instance_id", instanceID, "error", err)
		return nil
	}

	// Best-effort observability only (surfaces "deleting" via GetInstance);
	// HardDelete is already durably persisted, so a failure here must not
	// fail the call.
	if err := s.store.ServiceTypeInstance().UpdateStatus(ctx, instanceID, model.StatusDeleting, "", nil); err != nil {
		log.Warn("Failed to mark instance deleting (best-effort)", "instance_id", instanceID, "error", err)
	}

	log.Info("Delete requested, awaiting agent acknowledgement", "instance_id", instance.ID)
	return nil
}

// enrollForDeletion marks instance for deletion (deletion_status=SCHEDULED),
// or resets its retry count without losing the original
// deletion_requested_at if it's already enrolled. Either branch also
// (re-)persists HardDelete=!deferred in the same update, so a re-delete
// with a different `deferred` value corrects the stored mode. An
// enroll-step DB failure always returns an error to the caller,
// symmetrically for both deletion modes (REQ-DEL-04).
func (s *InstanceService) enrollForDeletion(ctx context.Context, instance *model.ServiceTypeInstance, deferred bool) error {
	log := logging.FromContext(ctx)
	hardDelete := !deferred
	if instance.DeletionStatus != nil {
		if err := s.store.ServiceTypeInstance().ResetRetryCount(ctx, instance.ID, hardDelete); err != nil {
			log.Error("Failed to reset retry count", "instance_id", instance.ID, "error", err)
			return service.NewInternalError(fmt.Sprintf("failed to reset retry count for instance %s: %v", instance.ID, err))
		}
		return nil
	}
	if err := s.store.ServiceTypeInstance().MarkForDeletion(ctx, instance.ID, hardDelete); err != nil {
		return service.NewInternalError(fmt.Sprintf("failed to mark instance %s for deletion: %v", instance.ID, err))
	}
	return nil
}

// publishDeleteToAgent publishes a delete request to the instance's agent.
// Returns nil (no-op) if there is no publisher configured or the instance
// has no agent_name. Callers decide what an agentstore.ErrAgentNotFound
// error means for their delete path (see DeleteInstance).
func (s *InstanceService) publishDeleteToAgent(ctx context.Context, instance *model.ServiceTypeInstance) error {
	if s.publisher == nil || instance.AgentName == nil {
		return nil
	}
	subject, err := s.resolveAgentSubject(ctx, *instance.AgentName)
	if err != nil {
		return err
	}
	return s.publisher.PublishDelete(ctx, subject, messaging.DeletePayload{
		ResourceID:  instance.ID,
		ServiceType: instance.ServiceType,
	})
}

func (s *InstanceService) resolveInstanceID(ctx context.Context, queryID *string) (*string, error) {
	log := logging.FromContext(ctx)

	if queryID == nil || *queryID == "" {
		generatedID := uuid.New().String()
		log.Debug("Generated instance ID", "instance_id", generatedID)
		return &generatedID, nil
	}

	requestedID := *queryID

	exists, err := s.store.ServiceTypeInstance().ExistsByID(ctx, requestedID)
	if err != nil {
		log.Error("Failed to check instance ID existence", "instance_id", requestedID, "error", err)
		return nil, service.NewInternalError(fmt.Sprintf("failed to check instance existence: %v", err))
	}
	if exists {
		log.Warn("Duplicate instance ID", "instance_id", requestedID)
		return nil, service.NewConflictError(fmt.Sprintf("instance with ID '%s' already exists", requestedID))
	}

	return &requestedID, nil
}
