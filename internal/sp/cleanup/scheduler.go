// Package cleanup implements stale instance cleanup scheduling.
package cleanup

import (
	"context"
	"errors"
	"sync"
	"time"

	agentstore "github.com/dcm-project/control-plane/internal/agent/store/agent"
	"github.com/dcm-project/control-plane/internal/sp/config"
	"github.com/dcm-project/control-plane/internal/sp/logging"
	"github.com/dcm-project/control-plane/internal/sp/messaging"
	"github.com/dcm-project/control-plane/internal/sp/store"
	"github.com/dcm-project/control-plane/internal/sp/store/model"
)

type Scheduler struct {
	store      store.Store
	publisher  *messaging.Publisher
	agentStore agentstore.Agent
	interval   time.Duration
	timeout    time.Duration
	maxRetries int
	stopCh     chan struct{}
	stopOnce   sync.Once
	wg         sync.WaitGroup
}

func NewScheduler(store store.Store, publisher *messaging.Publisher, agentSt agentstore.Agent, cfg *config.CleanupConfig) *Scheduler {
	return &Scheduler{
		store:      store,
		publisher:  publisher,
		agentStore: agentSt,
		interval:   cfg.Interval,
		timeout:    cfg.Timeout,
		maxRetries: cfg.MaxRetries,
		stopCh:     make(chan struct{}),
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	s.wg.Add(1)
	go s.run(ctx)
}

func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
	s.wg.Wait()
}

func (s *Scheduler) run(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.runCycle(ctx)
		}
	}
}

// runCycle bounds a single sweep of pending deletions to the configured
// timeout so a slow DB or agent lookup can't stall the next tick indefinitely.
func (s *Scheduler) runCycle(ctx context.Context) {
	if s.timeout <= 0 {
		s.ProcessPendingDeletions(ctx)
		return
	}
	cycleCtx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()
	s.ProcessPendingDeletions(cycleCtx)
}

func (s *Scheduler) ProcessPendingDeletions(ctx context.Context) {
	log := logging.FromContext(ctx)
	pending, err := s.store.ServiceTypeInstance().ListPendingDeletions(ctx)
	if err != nil {
		log.Error("Error listing pending deletions", "error", err)
		return
	}

	for _, instance := range pending {
		select {
		case <-ctx.Done():
			return
		default:
			s.processOne(ctx, instance)
		}
	}
}

// processOne advances one deferred deletion by one step. An agent-routed
// instance is only marked DELETED once its "deletion-acknowledged" event
// arrives (see consumer.ResponseConsumer), except for the audited give-up
// cases below.
func (s *Scheduler) processOne(ctx context.Context, instance model.ServiceTypeInstance) {
	log := logging.FromContext(ctx)

	if instance.AgentName == nil {
		// Never agent-routed: no physical resource to wait for, so this is
		// a normal completion. Always a tombstone: a non-deferred no-agent
		// delete resolves immediately in DeleteInstance and never reaches here.
		log.Info("cleanup: no agent, marking DELETED", "instance_id", instance.ID)
		if err := s.store.ServiceTypeInstance().MarkDeletionComplete(ctx, instance.ID); err != nil {
			log.Error("Failed to mark instance as DELETED", "instance_id", instance.ID, "error", err)
		}
		return
	}

	if s.publisher == nil || s.agentStore == nil {
		s.auditGiveUp(ctx, instance, "publisher_or_agent_store_unavailable")
		return
	}

	agent, err := s.agentStore.GetByName(ctx, *instance.AgentName)
	if err != nil {
		if errors.Is(err, agentstore.ErrAgentNotFound) {
			s.auditGiveUp(ctx, instance, "agent_not_found")
			return
		}
		log.Error("cleanup: agent lookup failed, will retry next cycle", "instance_id", instance.ID, "error", err)
		return
	}

	if s.maxRetries > 0 && instance.RetryCount >= s.maxRetries {
		log.Warn("cleanup audit: deletion retries exhausted, marking FAILED for manual intervention",
			"instance_id", instance.ID, "agent_name", *instance.AgentName, "retry_count", instance.RetryCount, "reason", "retries_exhausted")
		if err := s.store.ServiceTypeInstance().MarkDeletionFailed(ctx, instance.ID); err != nil {
			log.Error("Failed to mark instance deletion as FAILED", "instance_id", instance.ID, "error", err)
		}
		return
	}

	pubErr := s.publisher.PublishDelete(ctx, agent.TopicName, messaging.DeletePayload{
		ResourceID:  instance.ID,
		ServiceType: instance.ServiceType,
	})
	if pubErr != nil {
		log.Warn("cleanup: delete publish failed, will retry next cycle", "instance_id", instance.ID, "error", pubErr)
	} else {
		log.Info("cleanup: delete published, awaiting agent acknowledgement", "instance_id", instance.ID)
	}

	// Every attempt counts toward maxRetries whether or not the publish
	// itself succeeded, so a permanently unreachable NATS/agent eventually
	// trips the retries-exhausted branch above instead of retrying forever.
	if err := s.store.ServiceTypeInstance().IncrementDeletionRetry(ctx, instance.ID); err != nil {
		log.Error("Failed to record deletion retry attempt", "instance_id", instance.ID, "error", err)
	}
}

// auditGiveUp resolves an instance without ever confirming the physical
// resource was removed, because the CP has lost its only path to ask the
// agent (agent deregistered, or no NATS/agent store wired up). Delegates to
// FinalizeAuditGiveUp, which re-reads and acts on state atomically instead
// of the possibly-stale `instance` snapshot from ListPendingDeletions.
// Logged at Warn with a structured reason so operators can find instances
// whose backing resource may be orphaned (REQ-CLEANUP-AUDIT).
func (s *Scheduler) auditGiveUp(ctx context.Context, instance model.ServiceTypeInstance, reason string) {
	log := logging.FromContext(ctx)
	finalized, hardDeleted, err := s.store.ServiceTypeInstance().FinalizeAuditGiveUp(ctx, instance.ID)
	if err != nil {
		log.Error("Failed to finalize audit give-up", "instance_id", instance.ID, "error", err)
		return
	}
	if !finalized {
		// Raced with a concurrent finalize (ack arrived, or another cycle already gave up).
		log.Info("cleanup audit: instance no longer pending deletion, skipping give-up",
			"instance_id", instance.ID, "agent_name", *instance.AgentName, "reason", reason)
		return
	}
	if hardDeleted {
		log.Warn("cleanup audit: hard-deleting without confirmed physical deletion",
			"instance_id", instance.ID, "agent_name", *instance.AgentName, "reason", reason)
		return
	}
	log.Warn("cleanup audit: marking DELETED without confirmed physical deletion",
		"instance_id", instance.ID, "agent_name", *instance.AgentName, "reason", reason)
}
