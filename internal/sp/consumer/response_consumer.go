package consumer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	agentstore "github.com/dcm-project/control-plane/internal/agent/store/agent"
	"github.com/dcm-project/control-plane/internal/sp/messaging"
	"github.com/dcm-project/control-plane/internal/sp/store"
	"github.com/dcm-project/control-plane/internal/sp/store/model"
	rmstore "github.com/dcm-project/control-plane/internal/sp/store/resource_manager"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	consumerName = "control-plane-response-consumer"
)

// defaultMaxDeliver/defaultAckWait are used when the caller passes <= 0,
// keeping existing callers (and tests) working without having to plumb
// config through everywhere.
const (
	defaultMaxDeliver = 10
	defaultAckWait    = 30 * time.Second
)

type cloudEvent struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

type eventData struct {
	ResourceID string `json:"resource_id"`
	// AgentName is checked against the instance's currently assigned
	// agent_name before any status transition, rejecting late events from
	// an agent superseded by self-healing.
	AgentName string `json:"agent_name"`
}

type ResponseConsumer struct {
	js         jetstream.JetStream
	store      store.Store
	publisher  *messaging.Publisher
	agentStore agentstore.Agent
	onDeleted  func(context.Context, string) error
	maxDeliver int
	ackWait    time.Duration
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
}

// ResponseOption configures ResponseConsumer behavior.
type ResponseOption func(*ResponseConsumer)

// SetPlacementDeletionHandler registers a callback invoked when an agent
// deletion-acknowledged event finalizes deletion for an instance.
func SetPlacementDeletionHandler(onDeleted func(context.Context, string) error) ResponseOption {
	return func(c *ResponseConsumer) {
		c.onDeleted = onDeleted
	}
}

// NewResponseConsumer constructs a ResponseConsumer. maxDeliver bounds
// redeliveries of a message that keeps getting Nak'd; ackWait is how long
// JetStream waits for an ack before redelivering. Pass <= 0 for either to
// use the package defaults.
func NewResponseConsumer(js jetstream.JetStream, st store.Store, agentSt agentstore.Agent, maxDeliver int, ackWait time.Duration, opts ...ResponseOption) *ResponseConsumer {
	if maxDeliver <= 0 {
		maxDeliver = defaultMaxDeliver
	}
	if ackWait <= 0 {
		ackWait = defaultAckWait
	}
	c := &ResponseConsumer{
		js:         js,
		store:      st,
		publisher:  messaging.NewPublisher(js),
		agentStore: agentSt,
		maxDeliver: maxDeliver,
		ackWait:    ackWait,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *ResponseConsumer) Start(ctx context.Context) error {
	c.ctx, c.cancel = context.WithCancel(ctx)

	// WorkQueuePolicy: this is a point-to-point work queue (one durable
	// consumer, each message consumed and acked exactly once), so messages
	// are removed once acked instead of retained forever.
	stream, err := c.js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      messaging.ResponseStreamName,
		Subjects:  []string{messaging.ResponseSubject},
		Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		return fmt.Errorf("create response stream: %w", err)
	}

	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:    consumerName,
		AckPolicy:  jetstream.AckExplicitPolicy,
		AckWait:    c.ackWait,
		MaxDeliver: c.maxDeliver,
	})
	if err != nil {
		return fmt.Errorf("create response consumer: %w", err)
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.consumeLoop(cons)
	}()
	return nil
}

func (c *ResponseConsumer) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}

// consumeLoop uses only c.ctx for shutdown, so Stop() cancelling it is
// always sufficient to exit the loop, even if a caller cancels the ctx
// passed to Start() directly.
func (c *ResponseConsumer) consumeLoop(cons jetstream.Consumer) {
	for {
		select {
		case <-c.ctx.Done():
			return
		default:
		}

		msgs, err := cons.Fetch(1, jetstream.FetchMaxWait(500*time.Millisecond))
		if err != nil {
			select {
			case <-c.ctx.Done():
				return
			case <-time.After(time.Second):
			}
			continue
		}
		for msg := range msgs.Messages() {
			c.handleMessage(msg)
		}
	}
}

func (c *ResponseConsumer) handleMessage(msg jetstream.Msg) {
	var ce cloudEvent
	if err := json.Unmarshal(msg.Data(), &ce); err != nil {
		slog.Error("malformed cloud event, acking to discard", "error", err)
		_ = msg.Ack()
		return
	}

	var data eventData
	if err := json.Unmarshal(ce.Data, &data); err != nil {
		slog.Error("malformed event data, acking to discard", "error", err)
		_ = msg.Ack()
		return
	}

	if data.ResourceID == "" {
		slog.Error("event missing resource_id, acking to discard")
		_ = msg.Ack()
		return
	}

	// agent_name is required on every response CE payload; never fall back
	// to "trust it anyway" for a missing value.
	if data.AgentName == "" {
		slog.Error("event missing agent_name, acking to discard", "instance_id", data.ResourceID)
		_ = msg.Ack()
		return
	}

	ctx := c.ctx

	switch ce.Type {
	case messaging.CETypeCancelRejected:
		c.handleCancelRejected(ctx, data, msg)
		return
	case messaging.CETypeDeletionAcknowledged:
		c.handleDeletionAcknowledged(ctx, data, msg)
		return
	case messaging.CETypeRequestQueued:
		c.handleRequestQueued(ctx, data, msg)
		return
	default:
	}

	// fromStatuses CAS-guards each transition in addition to (not instead
	// of) UpdateStatusFrom's agent_name identity check below: status alone
	// wouldn't catch a late ack from a superseded agent if the instance's
	// status has since cycled back into an allowed fromStatus under a new one.
	var newStatus string
	var fromStatuses []string
	switch ce.Type {
	case messaging.CETypeCreationAcknowledged:
		newStatus = model.StatusProvisioning
		fromStatuses = []string{model.StatusPending, model.StatusQueued}
	case messaging.CETypeError:
		newStatus = model.StatusFailed
		fromStatuses = []string{model.StatusPending, model.StatusQueued, model.StatusProvisioning}
	case messaging.CETypeCancelAcknowledged:
		newStatus = model.StatusCancelled
		fromStatuses = []string{model.StatusQueued}
	default:
		slog.Warn("unknown event type, acking", "event_type", ce.Type)
		_ = msg.Ack()
		return
	}

	stiStore := c.store.ServiceTypeInstance()
	applied, err := stiStore.UpdateStatusFrom(ctx, data.ResourceID, fromStatuses, data.AgentName, newStatus, "")
	if err != nil {
		slog.Error("failed to update status, nacking", "instance_id", data.ResourceID, "event_type", ce.Type, "agent_name", data.AgentName, "error", err)
		_ = msg.NakWithDelay(5 * time.Second)
		return
	}
	if !applied {
		// A second read would tell us status- vs agent-mismatch but
		// reintroduce a TOCTOU window purely for logging; include agent_name
		// so operators can cross-reference it against the DB instead.
		slog.Info("stale or duplicate status event, instance already moved on, agent mismatch, or not found, acking",
			"instance_id", data.ResourceID, "event_type", ce.Type, "agent_name", data.AgentName)
	} else {
		slog.Info("status transition applied", "instance_id", data.ResourceID, "event_type", ce.Type, "agent_name", data.AgentName, "status", newStatus)
	}

	_ = msg.Ack()
}

// handleRequestQueued transitions status to "queued" and resets
// pending_started_at, so the queued-timeout sweep measures from the moment
// the agent queued the request rather than from the original pending
// timestamp (a long-pending instance would otherwise be cancelled
// immediately upon queueing).
func (c *ResponseConsumer) handleRequestQueued(ctx context.Context, data eventData, msg jetstream.Msg) {
	if err := c.store.ServiceTypeInstance().MarkQueued(ctx, data.ResourceID, data.AgentName); err != nil {
		if errors.Is(err, rmstore.ErrInstanceNotFound) {
			slog.Warn("instance not found, status mismatch, or agent mismatch for request-queued, acking to discard poison message",
				"instance_id", data.ResourceID, "event_type", messaging.CETypeRequestQueued, "agent_name", data.AgentName)
			_ = msg.Ack()
			return
		}
		slog.Error("failed to mark instance queued, nacking", "instance_id", data.ResourceID, "event_type", messaging.CETypeRequestQueued, "agent_name", data.AgentName, "error", err)
		_ = msg.NakWithDelay(5 * time.Second)
		return
	}
	slog.Info("status transition applied", "instance_id", data.ResourceID, "event_type", messaging.CETypeRequestQueued, "agent_name", data.AgentName, "status", model.StatusQueued)
	_ = msg.Ack()
}

// handleDeletionAcknowledged finalizes a delete once the agent confirms the
// physical resource is gone. FinalizeDeletionAcknowledged does the
// still-pending check (including FAILED, REQ-DEL-10), agent-identity gate,
// hard-delete-vs-soft-complete choice, and the write atomically (REQ-DEL-11).
func (c *ResponseConsumer) handleDeletionAcknowledged(ctx context.Context, data eventData, msg jetstream.Msg) {
	stiStore := c.store.ServiceTypeInstance()

	finalized, hardDeleted, wasFailed, err := stiStore.FinalizeDeletionAcknowledged(ctx, data.ResourceID, data.AgentName)
	if err != nil {
		slog.Error("deletion-acknowledged: failed to finalize instance, nacking", "instance_id", data.ResourceID, "event_type", messaging.CETypeDeletionAcknowledged, "agent_name", data.AgentName, "error", err)
		_ = msg.NakWithDelay(5 * time.Second)
		return
	}

	if !finalized {
		// Gone, agent mismatch, already finalized, or never scheduled: all "ack, don't retry".
		slog.Info("deletion-acknowledged: instance already gone, already finalized, agent mismatch, or never scheduled for deletion, ignoring stale/duplicate ack",
			"instance_id", data.ResourceID, "event_type", messaging.CETypeDeletionAcknowledged, "agent_name", data.AgentName)
		_ = msg.Ack()
		return
	}

	if wasFailed {
		// Cleanup scheduler had already given up on this; let operators know it self-resolved.
		slog.Warn("deletion-acknowledged: received after cleanup scheduler had already given up (FAILED), finalizing anyway",
			"instance_id", data.ResourceID, "event_type", messaging.CETypeDeletionAcknowledged, "agent_name", data.AgentName)
	}

	if hardDeleted {
		slog.Info("deletion-acknowledged: instance hard-deleted", "instance_id", data.ResourceID, "event_type", messaging.CETypeDeletionAcknowledged, "agent_name", data.AgentName, "status", "DELETED")
	} else {
		slog.Info("deletion-acknowledged: deferred deletion marked complete", "instance_id", data.ResourceID, "event_type", messaging.CETypeDeletionAcknowledged, "agent_name", data.AgentName, "status", "DELETED")
	}

	if c.onDeleted != nil {
		if err := c.onDeleted(ctx, data.ResourceID); err != nil {
			slog.Error("deletion-acknowledged: placement OnResourceDeleted callback failed",
				"instance_id", data.ResourceID,
				"event_type", messaging.CETypeDeletionAcknowledged,
				"error", err,
			)
		}
	}
	_ = msg.Ack()
}

// handleCancelRejected transitions a queued/cancelled instance back to
// "pending_deletion" so the delete can be retried, then re-publishes the
// delete. cancellableStatuses is deliberately an ALLOW-list of just
// {queued, cancelled} - the only statuses a genuine ack of
// sweep.cancelQueuedInstance's cancel request can arrive during - not a
// broader "any non-terminal status": a wider list could match a LATER,
// unrelated reassignment cycle and delete a freshly re-provisioned instance
// out from under its new agent.
func (c *ResponseConsumer) handleCancelRejected(ctx context.Context, data eventData, msg jetstream.Msg) {
	stiStore := c.store.ServiceTypeInstance()

	cancellableStatuses := []string{model.StatusQueued, model.StatusCancelled}
	applied, err := stiStore.UpdateStatusFrom(ctx, data.ResourceID, cancellableStatuses, data.AgentName, model.StatusPendingDeletion, "")
	if err != nil {
		slog.Error("cancel-rejected: failed to update status, nacking", "instance_id", data.ResourceID, "event_type", messaging.CETypeCancelRejected, "agent_name", data.AgentName, "error", err)
		_ = msg.NakWithDelay(5 * time.Second)
		return
	}
	if !applied {
		slog.Info("cancel-rejected: instance already moved to a terminal state or agent mismatch, skipping redundant delete",
			"instance_id", data.ResourceID, "event_type", messaging.CETypeCancelRejected, "agent_name", data.AgentName)
		_ = msg.Ack()
		return
	}
	slog.Info("status transition applied", "instance_id", data.ResourceID, "event_type", messaging.CETypeCancelRejected, "agent_name", data.AgentName, "status", model.StatusPendingDeletion)

	// Enroll in cleanup's retry/timeout tracking, not just the best-effort
	// republish below: otherwise a failed republish is never retried.
	// Always HardDelete=false: keeps this internal re-enrollment's pre-existing tombstone behavior.
	if err := stiStore.MarkForDeletion(ctx, data.ResourceID, false); err != nil {
		slog.Error("cancel-rejected: failed to enroll instance in deletion retry tracking", "instance_id", data.ResourceID, "event_type", messaging.CETypeCancelRejected, "agent_name", data.AgentName, "error", err)
	}

	instance, err := stiStore.Get(ctx, data.ResourceID, true)
	if err == nil && instance.AgentName != nil {
		subject, ok := c.resolveAgentTopic(ctx, *instance.AgentName)
		if ok {
			payload := messaging.DeletePayload{
				ResourceID:  data.ResourceID,
				ServiceType: instance.ServiceType,
			}
			if pubErr := c.publisher.PublishDelete(ctx, subject, payload); pubErr != nil {
				slog.Warn("cancel-rejected: publish delete failed, sweep will retry", "instance_id", data.ResourceID, "error", pubErr)
			}
		}
	}

	_ = msg.Ack()
}

func (c *ResponseConsumer) resolveAgentTopic(ctx context.Context, agentName string) (string, bool) {
	if c.agentStore == nil {
		slog.Error("resolveAgentTopic: agent store not configured, cannot resolve topic", "agent_name", agentName)
		return "", false
	}
	agent, err := c.agentStore.GetByName(ctx, agentName)
	if err != nil {
		slog.Warn("resolveAgentTopic: agent not found, skipping publish", "agent_name", agentName, "error", err)
		return "", false
	}
	return agent.TopicName, true
}
