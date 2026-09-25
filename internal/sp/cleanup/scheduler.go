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
		// Never agent-routed: there is no physical resource on an agent to
		// wait for, so this is a normal (non-audited) completion.
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

	if s.interval > 0 && instance.LastDeletionAttempt != nil && time.Since(*instance.LastDeletionAttempt) < s.interval {
		log.Debug("cleanup: deletion attempt is not due yet", "instance_id", instance.ID)
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

	claimed, err := s.store.ServiceTypeInstance().ClaimDeletionAttempt(ctx, instance.ID, instance.LastDeletionAttempt)
	if err != nil {
		log.Error("Failed to claim deletion retry attempt", "instance_id", instance.ID, "error", err)
		return
	}
	if !claimed {
		log.Debug("cleanup: deletion attempt was claimed since the pending list was read", "instance_id", instance.ID)
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
}

// auditGiveUp marks an instance DELETED without ever confirming the physical
// resource was removed, because the CP has lost its only path to ask the
// agent (agent deregistered, or no NATS/agent store wired up). This is
// intentionally logged at Warn with a structured reason so operators can
// find instances whose backing resource may be orphaned (REQ-CLEANUP-AUDIT).
func (s *Scheduler) auditGiveUp(ctx context.Context, instance model.ServiceTypeInstance, reason string) {
	log := logging.FromContext(ctx)
	log.Warn("cleanup audit: marking DELETED without confirmed physical deletion",
		"instance_id", instance.ID, "agent_name", *instance.AgentName, "reason", reason)
	if err := s.store.ServiceTypeInstance().MarkDeletionComplete(ctx, instance.ID); err != nil {
		log.Error("Failed to mark instance as DELETED", "instance_id", instance.ID, "error", err)
	}
}
