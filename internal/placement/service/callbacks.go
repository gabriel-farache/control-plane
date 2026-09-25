package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/dcm-project/control-plane/internal/cel"
	"github.com/dcm-project/control-plane/internal/placement/logging"
	"github.com/dcm-project/control-plane/internal/placement/sprm"
	"github.com/dcm-project/control-plane/internal/placement/store"
	"github.com/dcm-project/control-plane/internal/placement/types"
)

// OnResourceRunning progresses create orchestration for a run after a resource
// reaches RUNNING state.
func (s *PlacementService) OnResourceRunning(ctx context.Context, event types.ResourceStatusEvent) error {
	log := logging.FromContext(ctx)
	resourceID := event.ResourceID

	// Step 1: Load the resource that emitted RUNNING and persist its status.
	resource, err := s.store.Resource().Get(ctx, resourceID)
	if err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return NewNotFoundError(fmt.Sprintf("resource %s not found", resourceID))
		}
		return NewInternalError(fmt.Sprintf("failed to load resource %s: %v", resourceID, err))
	}
	if resourceStatusBlocksCreateProgression(resource.Status) {
		log.Debug("Ignoring RUNNING callback during teardown or terminal state",
			"run_id", resource.RunID,
			"resource_id", resourceID,
			"status", resource.Status,
		)
		return nil
	}

	// set the resource to RUNNING only if PENDING or PROVISIONING state.
	applied, err := s.store.Resource().UpdateStatusFrom(ctx, resourceID,
		[]string{types.ResourceStatusPending, types.ResourceStatusProvisioning},
		types.ResourceStatusRunning,
	)
	if err != nil {
		return NewInternalError(fmt.Sprintf("failed to set RUNNING status for resource %s: %v", resourceID, err))
	}
	if !applied {
		resource, err = s.store.Resource().Get(ctx, resourceID)
		if err != nil {
			return NewInternalError(fmt.Sprintf("failed to reload resource %s after CAS: %v", resourceID, err))
		}
		if resource.Status != types.ResourceStatusRunning {
			if resourceStatusBlocksCreateProgression(resource.Status) {
				return nil
			}
			log.Debug("Ignoring RUNNING callback for resource in unexpected state",
				"run_id", resource.RunID,
				"resource_id", resourceID,
				"status", resource.Status,
			)
			return nil
		}
	}

	// Step 2: Reload the full run
	resources, err := s.store.Resource().ListByRunID(ctx, resource.RunID)
	if err != nil {
		return NewInternalError(fmt.Sprintf("failed to list run %s resources: %v", resource.RunID, err))
	}
	for i := range resources {
		if resources[i].ID == resourceID {
			resources[i].Status = types.ResourceStatusRunning
			break
		}
	}

	// Step 3: Find PENDING resources whose dependencies are all RUNNING.
	ready := pendingResourcesReadyAtLowestLevel(resources)
	if len(ready) == 0 {
		return nil
	}

	// Step 4: Provision only the next DAG level resource(s)
	outputsByName, err := runningResourceOutputsByName(ctx, s.sprm, resources)
	if err != nil {
		return handleSPRMError(err)
	}
	for _, r := range ready {
		refs, err := cel.CollectReferences(r.Spec)
		if err != nil {
			return NewValidationError(fmt.Sprintf("resource %s: %v", r.Name, err))
		}
		for _, ref := range refs {
			log.Info("audit: CEL output field consumed",
				"run_id", r.RunID,
				"resource_id", r.ID,
				"resource_name", r.Name,
				"source_resource", ref.ResourceName,
				"output_field", ref.OutputField,
			)
		}

		// Step 5: Bind apply-time CEL references from RUNNING dependency outputs.
		boundSpec, err := cel.BindReferences(r.Spec, outputsByName)
		if err != nil {
			return NewValidationError(fmt.Sprintf("resource %s: %v", r.Name, err))
		}

		// Claim after validation so a terminal CEL error leaves the resource PENDING,
		// matching pre-CAS behavior. PROVISIONING is a placement-side CAS lock only.
		claimed, err := s.store.Resource().UpdateStatusFrom(ctx, r.ID,
			[]string{types.ResourceStatusPending},
			types.ResourceStatusProvisioning,
		)
		if err != nil {
			return NewInternalError(fmt.Sprintf("failed to claim resource %s for provisioning: %v", r.ID, err))
		}
		if !claimed {
			log.Debug("Skipping duplicate DAG progression for resource already claimed",
				"run_id", r.RunID,
				"resource_id", r.ID,
				"name", r.Name,
			)
			continue
		}

		availableAgents, err := s.listAvailableAgents(ctx)
		if err != nil {
			if rbErr := releaseProvisioningClaim(ctx, s.store.Resource(), r.ID); rbErr != nil {
				logClaimRollbackFailure(log, "provisioning", r.ID, rbErr)
			}
			return err
		}

		// Step 6: Re-evaluate policy at provision time so agent selection reflects
		// current policy state and the bound spec.
		evaluated, err := s.evaluateResourcePolicy(ctx, r.ID, boundSpec, availableAgents)
		if err != nil {
			if rbErr := releaseProvisioningClaim(ctx, s.store.Resource(), r.ID); rbErr != nil {
				logClaimRollbackFailure(log, "provisioning", r.ID, rbErr)
			}
			return err
		}
		if err := s.store.Resource().UpdatePlacementDecision(ctx, r.ID, evaluated.SelectedAgent, evaluated.Status); err != nil {
			if rbErr := releaseProvisioningClaim(ctx, s.store.Resource(), r.ID); rbErr != nil {
				logClaimRollbackFailure(log, "provisioning", r.ID, rbErr)
			}
			return NewInternalError(fmt.Sprintf("failed to update placement decision for resource %s: %v", r.ID, err))
		}

		// Step 7: Provision via SPRM using the evaluated spec (not the raw stored spec).
		log.Debug("Provisioning next DAG level resource via SPRM",
			"run_id", r.RunID,
			"resource_id", r.ID,
			"name", r.Name,
			"dag_level", r.DagLevel,
			"agent", evaluated.SelectedAgent,
		)
		if _, err := s.sprm.CreateResource(ctx, sprm.CreateResourceRequest{
			ID:        r.ID,
			Spec:      evaluated.EvaluatedSpec,
			AgentName: evaluated.SelectedAgent,
		}); err != nil {
			log.Error("Failed to progress DAG create after RUNNING",
				"run_id", r.RunID,
				"resource_id", r.ID,
				"dag_level", r.DagLevel,
				"error", err,
			)
			if rbErr := releaseProvisioningClaim(ctx, s.store.Resource(), r.ID); rbErr != nil {
				logClaimRollbackFailure(log, "provisioning", r.ID, rbErr)
			}
			return handleSPRMError(err)
		}
	}
	return nil
}

// OnResourceDeleted progresses reverse-DAG deletion after a resource reaches DELETED.
func (s *PlacementService) OnResourceDeleted(ctx context.Context, resourceID string) error {
	resource, err := s.store.Resource().Get(ctx, resourceID)
	if err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return NewNotFoundError(fmt.Sprintf("resource %s not found", resourceID))
		}
		return NewInternalError(fmt.Sprintf("failed to load resource %s: %v", resourceID, err))
	}

	// Set status to DELETED only if the current status is one of these five.
	applied, err := s.store.Resource().UpdateStatusFrom(ctx, resourceID,
		[]string{
			types.ResourceStatusDeleting,
			types.ResourceStatusPendingDeletion,
			types.ResourceStatusPending,
			types.ResourceStatusProvisioning,
			types.ResourceStatusRunning,
		},
		types.ResourceStatusDeleted,
	)
	if err != nil {
		return NewInternalError(fmt.Sprintf("failed to set DELETED status for resource %s: %v", resourceID, err))
	}
	if !applied {
		resource, err = s.store.Resource().Get(ctx, resourceID)
		if err != nil {
			return NewInternalError(fmt.Sprintf("failed to reload resource %s after CAS: %v", resourceID, err))
		}
		if resource.Status != types.ResourceStatusDeleted {
			return nil
		}
	}
	return s.progressRunDeletion(ctx, resource.RunID)
}

func (s *PlacementService) progressRunDeletion(ctx context.Context, runID string) error {
	log := logging.FromContext(ctx)
	for {
		resources, err := s.store.Resource().ListByRunID(ctx, runID)
		if err != nil {
			return NewInternalError(fmt.Sprintf("failed to list run %s resources: %v", runID, err))
		}
		if len(resources) == 0 {
			return nil
		}

		nextLevel := highestPendingDeletionLevel(resources)
		if nextLevel < 0 {
			for _, r := range resources {
				if r.Status == types.ResourceStatusDeleting {
					return nil
				}
			}
			if allResourcesDeleted(resources) {
				retainForRollback := true
				for _, resource := range resources {
					if resource.CleanupIntent != string(store.CleanupIntentRollback) {
						retainForRollback = false
						break
					}
				}
				if !retainForRollback {
					if err := s.store.Resource().DeleteByRunID(ctx, runID); err != nil {
						if errors.Is(err, store.ErrResourceNotFound) {
							return nil
						}
						return NewInternalError(fmt.Sprintf("failed to clean up completed run %s: %v", runID, err))
					}
				}
			}
			return nil
		}

		// Continue dispatching pending siblings at this level, but do not
		// advance below an in-flight delete at a higher level.
		for _, r := range resources {
			if r.Status == types.ResourceStatusDeleting && r.DagLevel > nextLevel {
				return nil
			}
		}

		anyDeleting := false
		for _, r := range resourcesAtDeletionLevel(resources, nextLevel) {
			applied, err := s.store.Resource().UpdateStatusFrom(ctx, r.ID,
				[]string{types.ResourceStatusPendingDeletion},
				types.ResourceStatusDeleting,
			)
			if err != nil {
				return NewInternalError(fmt.Sprintf("failed to set DELETING status for resource %s: %v", r.ID, err))
			}
			if !applied {
				current, getErr := s.store.Resource().Get(ctx, r.ID)
				if getErr != nil {
					return NewInternalError(fmt.Sprintf("failed to reload resource %s after CAS: %v", r.ID, getErr))
				}
				switch current.Status {
				case types.ResourceStatusDeleting:
					anyDeleting = true
					continue
				case types.ResourceStatusDeleted:
					continue
				default:
					log.Debug("Skipping deletion progression for resource in unexpected state",
						"run_id", runID,
						"resource_id", r.ID,
						"status", current.Status,
					)
					continue
				}
			}

			if err := s.sprm.DeleteResource(ctx, r.ID); err != nil {
				var httpErr *sprm.HTTPError
				if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
					log.Warn("SPRM resource already absent during reverse-DAG delete progression",
						"run_id", runID,
						"resource_id", r.ID,
						"dag_level", r.DagLevel,
					)
					if _, casErr := s.store.Resource().UpdateStatusFrom(ctx, r.ID,
						[]string{types.ResourceStatusDeleting, types.ResourceStatusPendingDeletion},
						types.ResourceStatusDeleted,
					); casErr != nil {
						return NewInternalError(fmt.Sprintf("failed to set DELETED status for resource %s: %v", r.ID, casErr))
					}
				} else {
					svcErr := handleSPRMError(err)
					if rbErr := releaseDeletionDispatch(ctx, s.store.Resource(), r.ID); rbErr != nil {
						logClaimRollbackFailure(log, "deletion", r.ID, rbErr)
					}
					return svcErr
				}
				continue
			}
			anyDeleting = true
		}
		if anyDeleting {
			return nil
			// else: entire level was already absent in SPRM, keep progressing now.
		}
	}
}

// OnAgentError persists the newest assigned-agent error and starts rollback.
func (s *PlacementService) OnAgentError(ctx context.Context, resourceID, agentName string, streamSequence uint64, details types.AgentErrorDetails) error {
	accepted, err := s.store.Resource().UpdateAgentError(ctx, resourceID, agentName, streamSequence, details)
	if err != nil {
		return NewInternalError(fmt.Sprintf("failed to persist agent error for resource %s: %v", resourceID, err))
	}
	if !accepted {
		return nil
	}

	resource, err := s.store.Resource().Get(ctx, resourceID)
	if err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return NewNotFoundError(fmt.Sprintf("resource %s not found", resourceID))
		}
		return NewInternalError(fmt.Sprintf("failed to load resource %s: %v", resourceID, err))
	}
	switch resource.Status {
	case types.ResourceStatusPendingDeletion, types.ResourceStatusDeleting, types.ResourceStatusDeleted:
		return s.progressRunDeletion(ctx, resource.RunID)
	}
	if resource.Status != types.ResourceStatusFailed {
		applied, err := s.store.Resource().UpdateStatusFrom(ctx, resourceID,
			[]string{
				types.ResourceStatusPending,
				types.ResourceStatusProvisioning,
				types.ResourceStatusRunning,
			},
			types.ResourceStatusFailed,
		)
		if err != nil {
			return NewInternalError(fmt.Sprintf("failed to set FAILED status for resource %s: %v", resourceID, err))
		}
		if !applied {
			resource, err = s.store.Resource().Get(ctx, resourceID)
			if err != nil {
				return NewInternalError(fmt.Sprintf("failed to reload resource %s after failure status CAS: %v", resourceID, err))
			}
			switch resource.Status {
			case types.ResourceStatusFailed:
				return s.beginRunDeletion(ctx, resource.RunID, store.CleanupIntentRollback)
			case types.ResourceStatusPendingDeletion, types.ResourceStatusDeleting, types.ResourceStatusDeleted:
				return s.progressRunDeletion(ctx, resource.RunID)
			default:
				return nil
			}
		}
	}
	return s.beginRunDeletion(ctx, resource.RunID, store.CleanupIntentRollback)
}

// OnResourceFailed halts progression and starts run teardown.
func (s *PlacementService) OnResourceFailed(ctx context.Context, resourceID string) error {
	resource, err := s.store.Resource().Get(ctx, resourceID)
	if err != nil {
		if errors.Is(err, store.ErrResourceNotFound) {
			return NewNotFoundError(fmt.Sprintf("resource %s not found", resourceID))
		}
		return NewInternalError(fmt.Sprintf("failed to load resource %s: %v", resourceID, err))
	}

	// Set status to FAILED only if PENDING, PROVISIONING or RUNNING
	applied, err := s.store.Resource().UpdateStatusFrom(ctx, resourceID,
		[]string{
			types.ResourceStatusPending,
			types.ResourceStatusProvisioning,
			types.ResourceStatusRunning,
		},
		types.ResourceStatusFailed,
	)
	if err != nil {
		return NewInternalError(fmt.Sprintf("failed to set FAILED status for resource %s: %v", resourceID, err))
	}
	if !applied {
		resource, err = s.store.Resource().Get(ctx, resourceID)
		if err != nil {
			return NewInternalError(fmt.Sprintf("failed to reload resource %s after CAS: %v", resourceID, err))
		}
		switch resource.Status {
		case types.ResourceStatusFailed:
			return s.DeleteRun(ctx, resource.RunID)
		case types.ResourceStatusPendingDeletion, types.ResourceStatusDeleting:
			return s.progressRunDeletion(ctx, resource.RunID)
		case types.ResourceStatusDeleted:
			return nil
		default:
			return nil
		}
	}
	return s.DeleteRun(ctx, resource.RunID)
}
