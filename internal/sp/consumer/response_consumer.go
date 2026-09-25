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
	placementservice "github.com/dcm-project/control-plane/internal/placement/service"
	placementtypes "github.com/dcm-project/control-plane/internal/placement/types"
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

type agentErrorEventData struct {
	ResourceID string             `json:"resource_id"`
	AgentName  string             `json:"agent_name"`
	Error      string             `json:"error"`
	Details    *agentErrorDetails `json:"details"`
}

type agentErrorDetails struct {
	Message       string               `json:"message"`
	ProviderError *providerErrorDetail `json:"provider_error"`
}

type providerErrorDetail struct {
	StatusCode *int   `json:"status_code"`
	Message    string `json:"message"`
}

func (d agentErrorEventData) statusMessage() string {
	if d.Details != nil {
		if d.Details.ProviderError != nil && d.Details.ProviderError.Message != "" {
			return d.Details.ProviderError.Message
		}
		if d.Details.Message != "" {
			return d.Details.Message
		}
	}
	return d.Error
}

func (d agentErrorEventData) placementDetails() placementtypes.AgentErrorDetails {
	details := placementtypes.AgentErrorDetails{Error: d.Error}
	if d.Details != nil {
		details.Message = d.Details.Message
		if providerError := d.Details.ProviderError; providerError != nil {
			details.ProviderError = &placementtypes.AgentProviderError{
				StatusCode: providerError.StatusCode,
				Message:    providerError.Message,
			}
		}
	}
	return details
}

func (d agentErrorEventData) safeLogAttrs() []any {
	detailsMessage := ""
	var statusCode *int
	if d.Details != nil {
		detailsMessage = d.Details.Message
		if d.Details.ProviderError != nil {
			statusCode = d.Details.ProviderError.StatusCode
		}
	}
	attrs := []any{"error", d.Error, "details_message", detailsMessage}
	if statusCode != nil {
		attrs = append(attrs, "provider_status_code", *statusCode)
	}
	return attrs
}

type ResponseConsumer struct {
	js           jetstream.JetStream
	store        store.Store
	publisher    *messaging.Publisher
	agentStore   agentstore.Agent
	onDeleted    func(context.Context, string) error
	onAgentError func(context.Context, string, string, uint64, placementtypes.AgentErrorDetails) error
	maxDeliver   int
	ackWait      time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
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

// SetPlacementAgentErrorHandler registers the callback for accepted agent errors.
func SetPlacementAgentErrorHandler(onAgentError func(context.Context, string, string, uint64, placementtypes.AgentErrorDetails) error) ResponseOption {
	return func(c *ResponseConsumer) {
		c.onAgentError = onAgentError
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
	var agentError *agentErrorEventData
	if ce.Type == messaging.CETypeError {
		var errorData agentErrorEventData
		if err := json.Unmarshal(ce.Data, &errorData); err != nil {
			slog.Error("malformed event data, acking to discard", "error", err)
			_ = msg.Ack()
			return
		}
		data = eventData{ResourceID: errorData.ResourceID, AgentName: errorData.AgentName}
		agentError = &errorData
	} else if err := json.Unmarshal(ce.Data, &data); err != nil {
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
		fromStatuses = []string{model.StatusPending, model.StatusQueued, model.StatusProvisioning, model.StatusFailed}
	case messaging.CETypeCancelAcknowledged:
		newStatus = model.StatusCancelled
		fromStatuses = []string{model.StatusQueued}
	default:
		slog.Warn("unknown event type, acking", "event_type", ce.Type)
		_ = msg.Ack()
		return
	}

	statusMessage := ""
	if agentError != nil {
		statusMessage = agentError.statusMessage()
	}
	stiStore := c.store.ServiceTypeInstance()
	var applied bool
	var streamSequence uint64
	if agentError != nil {
		metadata, err := msg.Metadata()
		if err != nil {
			slog.Error("failed to read agent error stream sequence, nacking", "instance_id", data.ResourceID, "error", err)
			_ = msg.NakWithDelay(5 * time.Second)
			return
		}
		streamSequence = metadata.Sequence.Stream
		applied, err = stiStore.UpdateAgentErrorFrom(ctx, data.ResourceID, fromStatuses, data.AgentName, streamSequence, newStatus, statusMessage)
		if err != nil {
			slog.Error("failed to update agent error, nacking", "instance_id", data.ResourceID, "event_type", ce.Type, "agent_name", data.AgentName, "stream_sequence", streamSequence, "error", err)
			_ = msg.NakWithDelay(5 * time.Second)
			return
		}
	} else {
		var err error
		applied, err = stiStore.UpdateStatusFrom(ctx, data.ResourceID, fromStatuses, data.AgentName, newStatus, statusMessage)
		if err != nil {
			slog.Error("failed to update status, nacking", "instance_id", data.ResourceID, "event_type", ce.Type, "agent_name", data.AgentName, "error", err)
			_ = msg.NakWithDelay(5 * time.Second)
			return
		}
	}
	logAttrs := []any{"instance_id", data.ResourceID, "event_type", ce.Type, "agent_name", data.AgentName}
	if agentError != nil {
		logAttrs = append(logAttrs, agentError.safeLogAttrs()...)
		logAttrs = append(logAttrs, "stream_sequence", streamSequence)
	}
	if !applied {
		slog.Info("stale or duplicate status event, instance already moved on, agent mismatch, or not found, acking", logAttrs...)
	} else {
		slog.Info("status transition applied", append(logAttrs, "status", newStatus)...)
		if agentError != nil && c.onAgentError != nil {
			if err := c.onAgentError(ctx, data.ResourceID, data.AgentName, streamSequence, agentError.placementDetails()); err != nil {
				slog.Error("failed to notify Placement of agent error, nacking", "instance_id", data.ResourceID, "agent_name", data.AgentName, "stream_sequence", streamSequence, "error", err)
				_ = msg.NakWithDelay(5 * time.Second)
				return
			}
		}
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
// physical resource is gone. It branches on deletion_status/status because
// the same event serves both delete paths: non-deferred deletes are
// hard-deleted now (nothing else is keeping them around); deferred deletes
// are soft-completed to keep their tombstone. Any other combination is a
// late/duplicate redelivery and a no-op for SP, but placement progression
// is still invoked so a prior placement callback failure can retry.
func (c *ResponseConsumer) handleDeletionAcknowledged(ctx context.Context, data eventData, msg jetstream.Msg) {
	stiStore := c.store.ServiceTypeInstance()

	instance, err := stiStore.Get(ctx, data.ResourceID, true)
	if err != nil {
		if errors.Is(err, rmstore.ErrInstanceNotFound) {
			slog.Info("deletion-acknowledged: instance already gone, continuing to placement",
				"instance_id", data.ResourceID,
				"event_type", messaging.CETypeDeletionAcknowledged,
				"agent_name", data.AgentName,
			)
		} else {
			slog.Error("deletion-acknowledged: failed to look up instance, nacking", "instance_id", data.ResourceID, "event_type", messaging.CETypeDeletionAcknowledged, "agent_name", data.AgentName, "error", err)
			_ = msg.NakWithDelay(5 * time.Second)
			return
		}
	} else {
		switch {
		case instance.Status == model.StatusDeleting:
			// Checked ahead of DeletionStatus: a non-deferred delete must
			// always be fully removed once acknowledged, regardless of whether
			// its best-effort MarkForDeletion enrollment also set SCHEDULED.
			if err := stiStore.HardDeleteFromAgent(ctx, data.ResourceID, data.AgentName); err != nil {
				if !errors.Is(err, rmstore.ErrInstanceNotFound) {
					slog.Error("deletion-acknowledged: failed to hard-delete instance, nacking", "instance_id", data.ResourceID, "event_type", messaging.CETypeDeletionAcknowledged, "agent_name", data.AgentName, "error", err)
					_ = msg.NakWithDelay(5 * time.Second)
					return
				}
				slog.Info("deletion-acknowledged: instance already gone or agent mismatch, continuing to placement",
					"instance_id", data.ResourceID, "event_type", messaging.CETypeDeletionAcknowledged, "agent_name", data.AgentName)
			} else {
				slog.Info("deletion-acknowledged: instance hard-deleted", "instance_id", data.ResourceID, "event_type", messaging.CETypeDeletionAcknowledged, "agent_name", data.AgentName, "status", "DELETED")
			}
		case instance.Status == model.StatusPendingDeletion,
			instance.DeletionStatus != nil && *instance.DeletionStatus == rmstore.DeletionStatusScheduled:
			// Status=pending_deletion is matched even without deletion_status
			// set, so a cancel-rejected retry whose own MarkForDeletion
			// enrollment failed doesn't fall through to the default case and
			// get stranded as "stale".
			if err := stiStore.MarkDeletionCompleteFromAgent(ctx, data.ResourceID, data.AgentName); err != nil {
				if errors.Is(err, rmstore.ErrInstanceNotFound) {
					slog.Info("deletion-acknowledged: instance already gone or agent mismatch, continuing to placement",
						"instance_id", data.ResourceID, "event_type", messaging.CETypeDeletionAcknowledged, "agent_name", data.AgentName)
				} else {
					slog.Error("deletion-acknowledged: failed to mark deferred deletion complete, nacking", "instance_id", data.ResourceID, "event_type", messaging.CETypeDeletionAcknowledged, "agent_name", data.AgentName, "error", err)
					_ = msg.NakWithDelay(5 * time.Second)
					return
				}
			} else {
				slog.Info("deletion-acknowledged: deferred deletion marked complete", "instance_id", data.ResourceID, "event_type", messaging.CETypeDeletionAcknowledged, "agent_name", data.AgentName, "status", "DELETED")
			}
		default:
			slog.Info("deletion-acknowledged: SP deletion already finalized or instance was never deleting, continuing to placement",
				"instance_id", data.ResourceID, "event_type", messaging.CETypeDeletionAcknowledged, "agent_name", data.AgentName, "status", instance.Status, "deletion_status", instance.DeletionStatus)
		}
	}

	if c.onDeleted != nil {
		if err := c.onDeleted(ctx, data.ResourceID); err != nil {
			if placementservice.IsCallbackRetryable(err) {
				slog.Warn("deletion-acknowledged: placement OnResourceDeleted callback failed with retryable error, nacking",
					"instance_id", data.ResourceID,
					"event_type", messaging.CETypeDeletionAcknowledged,
					"error", err,
				)
				_ = msg.NakWithDelay(5 * time.Second)
				return
			}
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
	canPublish := true
	if err := stiStore.MarkForDeletion(ctx, data.ResourceID); err != nil {
		slog.Error("cancel-rejected: failed to enroll instance in deletion retry tracking", "instance_id", data.ResourceID, "event_type", messaging.CETypeCancelRejected, "agent_name", data.AgentName, "error", err)
	} else {
		claimed, claimErr := stiStore.ClaimDeletionAttempt(ctx, data.ResourceID, nil)
		if claimErr != nil {
			slog.Error("cancel-rejected: failed to claim delete publication, sweep will retry", "instance_id", data.ResourceID, "event_type", messaging.CETypeCancelRejected, "agent_name", data.AgentName, "error", claimErr)
			canPublish = false
		} else if !claimed {
			slog.Info("cancel-rejected: cleanup scheduler already claimed delete publication", "instance_id", data.ResourceID, "event_type", messaging.CETypeCancelRejected, "agent_name", data.AgentName)
			canPublish = false
		}
	}

	if canPublish {
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
