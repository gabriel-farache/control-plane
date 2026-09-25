package consumer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"sync"
	"time"

	agentstore "github.com/dcm-project/control-plane/internal/agent/store/agent"
	agentmodel "github.com/dcm-project/control-plane/internal/agent/store/model"
	placementtypes "github.com/dcm-project/control-plane/internal/placement/types"
	"github.com/dcm-project/control-plane/internal/sp/cleanup"
	"github.com/dcm-project/control-plane/internal/sp/config"
	"github.com/dcm-project/control-plane/internal/sp/consumer"
	"github.com/dcm-project/control-plane/internal/sp/messaging"
	"github.com/dcm-project/control-plane/internal/sp/store"
	"github.com/dcm-project/control-plane/internal/sp/store/model"
	rmstore "github.com/dcm-project/control-plane/internal/sp/store/resource_manager"
	"github.com/google/uuid"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type recordingJetStream struct {
	jetstream.JetStream
	mu              sync.Mutex
	deletePublishes int
}

func (s *recordingJetStream) Publish(ctx context.Context, subject string, data []byte, opts ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if subject == "dcm.agent.test-agent" {
		s.mu.Lock()
		s.deletePublishes++
		s.mu.Unlock()
	}
	return s.JetStream.Publish(ctx, subject, data, opts...)
}

func (s *recordingJetStream) deletePublishCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deletePublishes
}

type responseStoreOverride struct {
	store.Store
	instance rmstore.ServiceTypeInstance
}

func (s *responseStoreOverride) ServiceTypeInstance() rmstore.ServiceTypeInstance {
	return s.instance
}

type runSchedulerAfterMarkForDeletion struct {
	rmstore.ServiceTypeInstance
	scheduler *cleanup.Scheduler
}

func (s *runSchedulerAfterMarkForDeletion) MarkForDeletion(ctx context.Context, id string) error {
	if err := s.ServiceTypeInstance.MarkForDeletion(ctx, id); err != nil {
		return err
	}
	s.scheduler.ProcessPendingDeletions(ctx)
	return nil
}

var _ = Describe("ResponseConsumer", func() {
	var (
		ns        *natsserver.Server
		nc        *nats.Conn
		js        jetstream.JetStream
		dataStore store.Store
		db        *gorm.DB
		rc        *consumer.ResponseConsumer
		ctx       context.Context
	)

	BeforeEach(func() {
		opts := &natsserver.Options{
			Host:      "127.0.0.1",
			Port:      -1,
			JetStream: true,
			StoreDir:  GinkgoT().TempDir(),
		}
		var err error
		ns, err = natsserver.NewServer(opts)
		Expect(err).NotTo(HaveOccurred())
		ns.Start()
		Expect(ns.ReadyForConnections(2 * time.Second)).To(BeTrue())

		nc, err = nats.Connect(ns.ClientURL())
		Expect(err).NotTo(HaveOccurred())

		js, err = jetstream.New(nc)
		Expect(err).NotTo(HaveOccurred())

		db, err = gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		Expect(err).NotTo(HaveOccurred())
		// sqlite's ":memory:" DSN gives each new physical connection its own
		// empty database, so once the consumer's background goroutine and the
		// test's own Eventually assertions query concurrently, a second
		// pooled connection would see "no such table" instead of the
		// migrated schema.
		sqlDB, err := db.DB()
		Expect(err).NotTo(HaveOccurred())
		sqlDB.SetMaxOpenConns(1)
		Expect(db.AutoMigrate(&agentmodel.Agent{}, &model.ServiceTypeInstance{})).To(Succeed())
		Expect(db.Create(&agentmodel.Agent{ID: uuid.New().String(), Name: testAgentName, TopicName: "dcm.agent.test-agent"}).Error).NotTo(HaveOccurred())

		dataStore = store.NewStore(db)
		rc = consumer.NewResponseConsumer(js, dataStore, nil, 0, 0)
		ctx = context.Background()
	})

	AfterEach(func() {
		rc.Stop()
		if nc != nil {
			nc.Close()
		}
		if ns != nil {
			ns.Shutdown()
		}
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
	})

	It("stops promptly once the parent context is cancelled directly, without a separate stopCh", func() {
		// consumeLoop's select only watches c.ctx.Done(), so Stop()
		// cancelling that same ctx is the single canonical shutdown signal.
		startCtx, cancel := context.WithCancel(context.Background())
		Expect(rc.Start(startCtx)).To(Succeed())

		cancel()

		stopped := make(chan struct{})
		go func() {
			rc.Stop()
			close(stopped)
		}()
		// consumeLoop's Fetch call waits up to 500ms and its error-retry
		// path waits up to 1s, so 2s comfortably bounds a healthy exit
		// while still catching a hang if ctx.Done() were ignored.
		Eventually(stopped, 2*time.Second).Should(BeClosed())
	})

	It("configures the response stream and consumer with WorkQueuePolicy, MaxDeliver and AckWait", func() {
		rc2 := consumer.NewResponseConsumer(js, dataStore, nil, 5, 15*time.Second)
		Expect(rc2.Start(ctx)).To(Succeed())
		defer rc2.Stop()

		stream, err := js.Stream(ctx, "dcm-agent-responses")
		Expect(err).NotTo(HaveOccurred())
		info, err := stream.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(info.Config.Retention).To(Equal(jetstream.WorkQueuePolicy))

		cons, err := stream.Consumer(ctx, "control-plane-response-consumer")
		Expect(err).NotTo(HaveOccurred())
		consInfo, err := cons.Info(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(consInfo.Config.MaxDeliver).To(Equal(5))
		Expect(consInfo.Config.AckWait).To(Equal(15 * time.Second))
	})

	It("transitions PENDING to PROVISIONING on creation-acknowledged", func() {
		instance := createPendingInstance(ctx, db)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.creation-acknowledged", instance.ID, testAgentName)

		Eventually(func() string {
			return currentStatus(db, instance.ID)
		}, 2*time.Second, 20*time.Millisecond).Should(Equal("provisioning"))
	})

	It("logs the status transition on a successful creation-acknowledged", func() {
		instance := createPendingInstance(ctx, db)

		var buf syncBuffer
		prevLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		defer slog.SetDefault(prevLogger)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.creation-acknowledged", instance.ID, testAgentName)

		Eventually(buf.String, 2*time.Second, 20*time.Millisecond).Should(SatisfyAll(
			ContainSubstring("status transition applied"),
			ContainSubstring("instance_id="+instance.ID),
			ContainSubstring("event_type=dcm.agent.creation-acknowledged"),
			ContainSubstring("agent_name="+testAgentName),
			ContainSubstring("status=provisioning"),
		))
	})

	// A late creation-acknowledged from a superseded agent must not apply
	// even though the instance is still "pending" - a status a genuine ack
	// from the new agent could also legitimately arrive during.
	It("ignores a creation-acknowledged from a superseded agent even though status still matches (identity check)", func() {
		instance := createPendingInstance(ctx, db)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.creation-acknowledged", instance.ID, staleAgentName)

		Consistently(func() string {
			return currentStatus(db, instance.ID)
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("pending"))
	})

	// A mismatch is logged with agent_name included, so operators can
	// cross-reference "stale agent" against "instance already moved on".
	It("includes agent_name in the log line when a creation-acknowledged is rejected for an agent mismatch", func() {
		instance := createPendingInstance(ctx, db)

		var buf syncBuffer
		prevLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		defer slog.SetDefault(prevLogger)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.creation-acknowledged", instance.ID, staleAgentName)

		Eventually(buf.String, 2*time.Second, 20*time.Millisecond).Should(SatisfyAll(
			ContainSubstring("stale or duplicate status event"),
			ContainSubstring("agent_name="+staleAgentName),
		))
	})

	It("transitions to FAILED on error event", func() {
		instance := createPendingInstance(ctx, db)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.error", instance.ID, testAgentName)

		Eventually(func() string {
			return currentStatus(db, instance.ID)
		}, 2*time.Second, 20*time.Millisecond).Should(Equal("failed"))
	})

	It("stores the provider error message and logs safe error fields", func() {
		instance := createPendingInstance(ctx, db)
		var buf syncBuffer
		prevLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		defer slog.SetDefault(prevLogger)

		Expect(rc.Start(ctx)).To(Succeed())

		const providerBody = "SENSITIVE_PROVIDER_RESPONSE_BODY"
		publishAgentEventWithData(js, "dcm.agent.error", map[string]any{
			"resource_id": instance.ID,
			"agent_name":  testAgentName,
			"error":       "provider request failed",
			"details": map[string]any{
				"message": "agent failure summary",
				"provider_error": map[string]any{
					"status_code": 503,
					"message":     providerBody,
				},
			},
		})

		Eventually(func() string { return currentStatus(db, instance.ID) }, 2*time.Second, 20*time.Millisecond).Should(Equal("failed"))
		Eventually(func() string { return currentStatusMessage(db, instance.ID) }, 2*time.Second, 20*time.Millisecond).Should(Equal(providerBody))
		Eventually(buf.String, 2*time.Second, 20*time.Millisecond).Should(SatisfyAll(
			ContainSubstring(`="provider request failed"`),
			ContainSubstring(`="agent failure summary"`),
			ContainSubstring("=503"),
		))
		Expect(buf.String()).NotTo(ContainSubstring(providerBody))
	})

	It("falls back from provider details to details.message, top-level error, and an empty message", func() {
		Expect(rc.Start(ctx)).To(Succeed())

		for _, tc := range []struct {
			name    string
			data    map[string]any
			message string
		}{
			{
				name: "details.message",
				data: map[string]any{
					"error":   "top-level fallback",
					"details": map[string]any{"message": "details fallback"},
				},
				message: "details fallback",
			},
			{
				name:    "top-level error",
				data:    map[string]any{"error": "top-level fallback"},
				message: "top-level fallback",
			},
			{
				name: "empty",
				data: map[string]any{},
			},
		} {
			instance := createPendingInstance(ctx, db)
			tc.data["resource_id"] = instance.ID
			tc.data["agent_name"] = testAgentName
			By(tc.name)
			publishAgentEventWithData(js, "dcm.agent.error", tc.data)

			Eventually(func() string { return currentStatus(db, instance.ID) }, 2*time.Second, 20*time.Millisecond).Should(Equal("failed"))
			Expect(currentStatusMessage(db, instance.ID)).To(Equal(tc.message))
		}
	})

	It("updates the failure message on a repeated error from the assigned agent", func() {
		instance := createPendingInstance(ctx, db)
		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEventWithData(js, "dcm.agent.error", map[string]any{
			"resource_id": instance.ID,
			"agent_name":  testAgentName,
			"details":     map[string]any{"message": "first failure"},
		})
		Eventually(func() string { return currentStatusMessage(db, instance.ID) }, 2*time.Second, 20*time.Millisecond).Should(Equal("first failure"))

		publishAgentEventWithData(js, "dcm.agent.error", map[string]any{
			"resource_id": instance.ID,
			"agent_name":  testAgentName,
			"details":     map[string]any{"message": "latest failure"},
		})
		Eventually(func() string { return currentStatusMessage(db, instance.ID) }, 2*time.Second, 20*time.Millisecond).Should(Equal("latest failure"))
		Expect(currentStatus(db, instance.ID)).To(Equal("failed"))
	})

	It("calls Placement with structured details only for the assigned agent", func() {
		rc.Stop()
		type placementCall struct {
			resourceID string
			agentName  string
			sequence   uint64
			details    placementtypes.AgentErrorDetails
		}
		var mu sync.Mutex
		calls := make([]placementCall, 0, 1)
		rc = consumer.NewResponseConsumer(
			js,
			dataStore,
			nil,
			0,
			0,
			consumer.SetPlacementAgentErrorHandler(func(_ context.Context, resourceID, agentName string, sequence uint64, details placementtypes.AgentErrorDetails) error {
				mu.Lock()
				defer mu.Unlock()
				calls = append(calls, placementCall{resourceID: resourceID, agentName: agentName, sequence: sequence, details: details})
				return nil
			}),
		)
		instance := createPendingInstance(ctx, db)
		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEventWithData(js, "dcm.agent.error", map[string]any{
			"resource_id": instance.ID,
			"agent_name":  staleAgentName,
			"error":       "stale failure",
			"details":     map[string]any{"message": "stale details"},
		})
		Consistently(func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(calls)
		}, 200*time.Millisecond, 20*time.Millisecond).Should(BeZero())

		statusCode := 503
		publishAgentEventWithData(js, "dcm.agent.error", map[string]any{
			"resource_id": instance.ID,
			"agent_name":  testAgentName,
			"error":       "provider request failed",
			"details": map[string]any{
				"message":        "agent summary",
				"provider_error": map[string]any{"status_code": statusCode, "message": "provider body"},
			},
		})
		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(calls)
		}, 2*time.Second, 20*time.Millisecond).Should(Equal(1))
		mu.Lock()
		call := calls[0]
		mu.Unlock()
		Expect(call.resourceID).To(Equal(instance.ID))
		Expect(call.agentName).To(Equal(testAgentName))
		Expect(call.sequence).To(BeNumerically(">", 0))
		Expect(call.details).To(Equal(placementtypes.AgentErrorDetails{
			Error:   "provider request failed",
			Message: "agent summary",
			ProviderError: &placementtypes.AgentProviderError{
				StatusCode: &statusCode,
				Message:    "provider body",
			},
		}))
	})

	It("retries an equal-sequence Placement notification after a partial failure", func() {
		rc.Stop()
		var mu sync.Mutex
		sequences := make([]uint64, 0, 2)
		var calls int
		rc = consumer.NewResponseConsumer(
			js,
			dataStore,
			nil,
			0,
			0,
			consumer.SetPlacementAgentErrorHandler(func(_ context.Context, _, _ string, sequence uint64, _ placementtypes.AgentErrorDetails) error {
				mu.Lock()
				defer mu.Unlock()
				calls++
				sequences = append(sequences, sequence)
				if calls == 1 {
					return errors.New("temporary placement failure")
				}
				return nil
			}),
		)
		instance := createPendingInstance(ctx, db)
		Expect(rc.Start(ctx)).To(Succeed())
		publishAgentEventWithData(js, "dcm.agent.error", map[string]any{
			"resource_id": instance.ID,
			"agent_name":  testAgentName,
			"error":       "failure",
			"details":     map[string]any{"message": "summary"},
		})

		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return calls
		}, 2*time.Second, 20*time.Millisecond).Should(Equal(1))
		mu.Lock()
		firstSequence := sequences[0]
		mu.Unlock()
		Expect(currentStatus(db, instance.ID)).To(Equal("failed"))
		Expect(currentAgentErrorStreamSequence(db, instance.ID)).To(Equal(firstSequence))
		var persisted model.ServiceTypeInstance
		Expect(db.First(&persisted, "id = ?", instance.ID).Error).NotTo(HaveOccurred())
		Expect(persisted.AgentName).NotTo(BeNil())
		Expect(*persisted.AgentName).To(Equal(testAgentName))
		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return calls
		}, 7*time.Second, 20*time.Millisecond).Should(Equal(2))
		mu.Lock()
		defer mu.Unlock()
		Expect(sequences[0]).To(BeNumerically(">", 0))
		Expect(sequences[1]).To(Equal(sequences[0]))
	})

	It("does not let an older redelivery overwrite a newer accepted error", func() {
		rc.Stop()
		var mu sync.Mutex
		var calls int
		var placedDetails placementtypes.AgentErrorDetails
		var placedSequence uint64
		rc = consumer.NewResponseConsumer(
			js,
			dataStore,
			nil,
			0,
			0,
			consumer.SetPlacementAgentErrorHandler(func(_ context.Context, _, _ string, sequence uint64, details placementtypes.AgentErrorDetails) error {
				mu.Lock()
				defer mu.Unlock()
				calls++
				if details.Error == "E1" {
					return errors.New("temporary failure for first event")
				}
				placedDetails = details
				placedSequence = sequence
				return nil
			}),
		)
		instance := createPendingInstance(ctx, db)
		Expect(rc.Start(ctx)).To(Succeed())
		publishAgentEventWithData(js, "dcm.agent.error", map[string]any{
			"resource_id": instance.ID,
			"agent_name":  testAgentName,
			"error":       "E1",
			"details":     map[string]any{"message": "first"},
		})
		Eventually(func() int {
			mu.Lock()
			defer mu.Unlock()
			return calls
		}, 2*time.Second, 20*time.Millisecond).Should(Equal(1))

		publishAgentEventWithData(js, "dcm.agent.error", map[string]any{
			"resource_id": instance.ID,
			"agent_name":  testAgentName,
			"error":       "E2",
			"details":     map[string]any{"message": "second"},
		})
		Eventually(func() string { return currentStatusMessage(db, instance.ID) }, 2*time.Second, 20*time.Millisecond).Should(Equal("second"))
		mu.Lock()
		Expect(placedDetails.Error).To(Equal("E2"))
		Expect(placedDetails.Message).To(Equal("second"))
		Expect(placedSequence).To(BeNumerically(">", 0))
		Expect(calls).To(Equal(2))
		mu.Unlock()

		Consistently(func() string { return currentStatusMessage(db, instance.ID) }, 6*time.Second, 50*time.Millisecond).Should(Equal("second"))
		mu.Lock()
		Expect(calls).To(Equal(2))
		mu.Unlock()
	})

	// Same mismatch treatment as creation-acknowledged, for the error event.
	It("ignores an error event from a superseded agent even though status still matches (identity check)", func() {
		instance := createPendingInstance(ctx, db)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEventWithData(js, "dcm.agent.error", map[string]any{
			"resource_id": instance.ID,
			"agent_name":  staleAgentName,
			"error":       "stale failure",
			"details": map[string]any{
				"message": "stale details",
				"provider_error": map[string]any{
					"status_code": 503,
					"message":     "stale provider body",
				},
			},
		})

		Consistently(func() string {
			return currentStatus(db, instance.ID)
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("pending"))
		Expect(currentStatusMessage(db, instance.ID)).To(BeEmpty())
	})

	It("transitions to QUEUED and resets the pending timer on request-queued", func() {
		instance := createPendingInstance(ctx, db)
		// Simulate the instance having been pending for a while already, so a
		// stale pending_started_at would make the queued-timeout sweep see it
		// as immediately overdue.
		staleTime := time.Now().Add(-1 * time.Hour)
		Expect(db.Model(&instance).Update("pending_started_at", staleTime).Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.request-queued", instance.ID, testAgentName)

		Eventually(func() string {
			return currentStatus(db, instance.ID)
		}, 2*time.Second, 20*time.Millisecond).Should(Equal("queued"))

		var updated model.ServiceTypeInstance
		Expect(db.First(&updated, "id = ?", instance.ID).Error).NotTo(HaveOccurred())
		Expect(updated.PendingStartedAt).NotTo(BeNil())
		Expect(*updated.PendingStartedAt).To(BeTemporally(">", staleTime))
	})

	It("logs the status transition on a successful request-queued", func() {
		instance := createPendingInstance(ctx, db)

		var buf syncBuffer
		prevLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		defer slog.SetDefault(prevLogger)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.request-queued", instance.ID, testAgentName)

		Eventually(buf.String, 2*time.Second, 20*time.Millisecond).Should(SatisfyAll(
			ContainSubstring("status transition applied"),
			ContainSubstring("instance_id="+instance.ID),
			ContainSubstring("event_type=dcm.agent.request-queued"),
			ContainSubstring("agent_name="+testAgentName),
			ContainSubstring("status=queued"),
		))
	})

	// A stale request-queued from a superseded agent must not mark the
	// instance queued or reset its pending timer.
	It("ignores a request-queued from a superseded agent even though status still matches (identity check)", func() {
		instance := createPendingInstance(ctx, db)
		staleTime := time.Now().Add(-1 * time.Hour)
		Expect(db.Model(&instance).Update("pending_started_at", staleTime).Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.request-queued", instance.ID, staleAgentName)

		Consistently(func() string {
			return currentStatus(db, instance.ID)
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("pending"))

		var updated model.ServiceTypeInstance
		Expect(db.First(&updated, "id = ?", instance.ID).Error).NotTo(HaveOccurred())
		Expect(updated.PendingStartedAt).NotTo(BeNil())
		Expect(*updated.PendingStartedAt).To(BeTemporally("==", staleTime))
	})

	It("hard-deletes a non-deferred instance on deletion-acknowledged", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("status", "deleting").Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, testAgentName)

		Eventually(func() error {
			return db.First(&model.ServiceTypeInstance{}, "id = ?", instance.ID).Error
		}, 2*time.Second, 100*time.Millisecond).Should(MatchError(gorm.ErrRecordNotFound))
	})

	It("invokes placement deletion callback when deletion is finalized", func() {
		rc.Stop()
		calls := 0
		lastID := ""
		rc = consumer.NewResponseConsumer(
			js,
			dataStore,
			nil,
			0,
			0,
			consumer.SetPlacementDeletionHandler(func(_ context.Context, resourceID string) error {
				calls++
				lastID = resourceID
				return nil
			}),
		)

		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("status", "deleting").Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())
		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, testAgentName)

		Eventually(func() int { return calls }, 2*time.Second, 20*time.Millisecond).Should(Equal(1))
		Expect(lastID).To(Equal(instance.ID))
	})

	It("logs the hard-delete with event_type on a successful deletion-acknowledged (non-deferred)", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("status", "deleting").Error).NotTo(HaveOccurred())

		var buf syncBuffer
		prevLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		defer slog.SetDefault(prevLogger)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, testAgentName)

		Eventually(buf.String, 2*time.Second, 20*time.Millisecond).Should(SatisfyAll(
			ContainSubstring("instance hard-deleted"),
			ContainSubstring("instance_id="+instance.ID),
			ContainSubstring("event_type=dcm.agent.deletion-acknowledged"),
			ContainSubstring("agent_name="+testAgentName),
			ContainSubstring("status=DELETED"),
		))
	})

	// A stale deletion-acknowledged from a superseded agent must not
	// hard-delete the row, for the non-deferred branch.
	It("ignores a deletion-acknowledged (non-deferred branch) from a superseded agent", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("status", "deleting").Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, staleAgentName)

		Consistently(func() error {
			return db.First(&model.ServiceTypeInstance{}, "id = ?", instance.ID).Error
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Succeed())
	})

	It("marks a deferred instance DELETED (soft) on deletion-acknowledged", func() {
		// A deferred DeleteInstance never touches Status (only
		// deletion_status) - "deleting" is exclusively the non-deferred
		// marker, so this leaves Status at its original "pending" to match
		// production reality.
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("deletion_status", "SCHEDULED").Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, testAgentName)

		Eventually(func() string {
			var updated model.ServiceTypeInstance
			if err := db.First(&updated, "id = ?", instance.ID).Error; err != nil {
				return ""
			}
			if updated.DeletionStatus == nil {
				return ""
			}
			return *updated.DeletionStatus
		}, 2*time.Second, 100*time.Millisecond).Should(Equal("DELETED"))

		// The row must still exist (soft-delete tombstone), unlike the
		// non-deferred case above.
		Expect(db.First(&model.ServiceTypeInstance{}, "id = ?", instance.ID).Error).NotTo(HaveOccurred())
	})

	It("logs the soft-complete with event_type on a successful deferred deletion-acknowledged", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("deletion_status", "SCHEDULED").Error).NotTo(HaveOccurred())

		var buf syncBuffer
		prevLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		defer slog.SetDefault(prevLogger)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, testAgentName)

		Eventually(buf.String, 2*time.Second, 20*time.Millisecond).Should(SatisfyAll(
			ContainSubstring("deferred deletion marked complete"),
			ContainSubstring("instance_id="+instance.ID),
			ContainSubstring("event_type=dcm.agent.deletion-acknowledged"),
			ContainSubstring("agent_name="+testAgentName),
			ContainSubstring("status=DELETED"),
		))
	})

	// Same mismatch treatment, for the deferred (soft-complete) branch.
	It("ignores a deletion-acknowledged (deferred branch) from a superseded agent", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("deletion_status", "SCHEDULED").Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, staleAgentName)

		Consistently(func() string {
			var updated model.ServiceTypeInstance
			Expect(db.First(&updated, "id = ?", instance.ID).Error).NotTo(HaveOccurred())
			if updated.DeletionStatus == nil {
				return ""
			}
			return *updated.DeletionStatus
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("SCHEDULED"))
	})

	It("finalizes a pending_deletion instance on deletion-acknowledged even if its MarkForDeletion enrollment failed (R2 S1/S2: finding #2/#3)", func() {
		// handleCancelRejected best-effort calls MarkForDeletion after
		// transitioning to pending_deletion; if that call fails,
		// deletion_status stays nil while Status is pending_deletion. The
		// switch must still key off Status==pending_deletion in that case -
		// otherwise this ack falls through to the default "stale" branch and
		// strands the instance in pending_deletion forever.
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("status", model.StatusPendingDeletion).Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, testAgentName)

		Eventually(func() string {
			var updated model.ServiceTypeInstance
			if err := db.First(&updated, "id = ?", instance.ID).Error; err != nil {
				return ""
			}
			if updated.DeletionStatus == nil {
				return ""
			}
			return *updated.DeletionStatus
		}, 2*time.Second, 100*time.Millisecond).Should(Equal("DELETED"))

		Expect(db.First(&model.ServiceTypeInstance{}, "id = ?", instance.ID).Error).NotTo(HaveOccurred())
	})

	It("ignores a late/duplicate deletion-acknowledged instead of erasing an existing DELETED tombstone (A)", func() {
		// A DELETED tombstone only ever arises via the deferred path (a
		// non-deferred delete is hard-deleted, not soft-marked), which never
		// sets Status to "deleting" - leave it at "pending" to match.
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("deletion_status", "DELETED").Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, testAgentName)

		// Asserting a no-op: Consistently (not a fixed sleep-then-assert)
		// gives the consumer the same window to WRONGLY erase the tombstone
		// while actively re-checking throughout it, rather than a single
		// read after one fixed delay.
		Consistently(func() string {
			var updated model.ServiceTypeInstance
			Expect(db.First(&updated, "id = ?", instance.ID).Error).NotTo(HaveOccurred())
			if updated.DeletionStatus == nil {
				return ""
			}
			return *updated.DeletionStatus
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("DELETED"))
	})

	It("ignores a late/duplicate deletion-acknowledged instead of erasing an existing FAILED audit record (A)", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("deletion_status", "FAILED").Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, testAgentName)

		Consistently(func() string {
			var updated model.ServiceTypeInstance
			Expect(db.First(&updated, "id = ?", instance.ID).Error).NotTo(HaveOccurred())
			if updated.DeletionStatus == nil {
				return ""
			}
			return *updated.DeletionStatus
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("FAILED"))
	})

	It("publishes deletion on cancel-rejected and enrolls it in cleanup retry tracking", func() {
		// cancel-rejected is only ever a genuine ack of the sweep's
		// queued-timeout PublishCancel, which fires exclusively from
		// "queued" - so that (or the "cancelled" it transitions to) is the
		// only realistic precondition, not "pending".
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("status", model.StatusQueued).Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.cancel-rejected", instance.ID, testAgentName)

		Eventually(func() string {
			return currentStatus(db, instance.ID)
		}, 2*time.Second, 20*time.Millisecond).Should(Equal("pending_deletion"))

		var updated model.ServiceTypeInstance
		Expect(db.First(&updated, "id = ?", instance.ID).Error).NotTo(HaveOccurred())
		// D: enrolled via MarkForDeletion so the cleanup scheduler will
		// retry/time-out/audit-giveup this delete even if the immediate
		// republish above fails or the ack never arrives.
		Expect(updated.DeletionStatus).NotTo(BeNil())
		Expect(*updated.DeletionStatus).To(Equal("SCHEDULED"))
	})

	It("does not duplicate cancel-rejected deletion when the cleanup scheduler claims first", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("status", model.StatusQueued).Error).NotTo(HaveOccurred())

		sharedJS := &recordingJetStream{JetStream: js}
		requestPublisher := messaging.NewPublisher(sharedJS)
		Expect(requestPublisher.EnsureStream(ctx)).To(Succeed())
		scheduler := cleanup.NewScheduler(
			dataStore, requestPublisher, agentstore.NewAgent(db),
			&config.CleanupConfig{Interval: time.Hour, MaxRetries: 3},
		)
		wrappedStore := &responseStoreOverride{
			Store: dataStore,
			instance: &runSchedulerAfterMarkForDeletion{
				ServiceTypeInstance: dataStore.ServiceTypeInstance(),
				scheduler:           scheduler,
			},
		}
		rc = consumer.NewResponseConsumer(sharedJS, wrappedStore, agentstore.NewAgent(db), 0, 0)
		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.cancel-rejected", instance.ID, testAgentName)

		Eventually(func() string {
			return currentStatus(db, instance.ID)
		}, 2*time.Second, 20*time.Millisecond).Should(Equal(model.StatusPendingDeletion))
		Eventually(func() uint64 {
			responseStream, err := js.Stream(ctx, messaging.ResponseStreamName)
			if err != nil {
				return 0
			}
			responseConsumer, err := responseStream.Consumer(ctx, "control-plane-response-consumer")
			if err != nil {
				return 0
			}
			info, err := responseConsumer.Info(ctx)
			if err != nil {
				return 0
			}
			return info.AckFloor.Stream
		}, 2*time.Second, 20*time.Millisecond).Should(BeNumerically(">=", 1))
		Expect(sharedJS.deletePublishCount()).To(Equal(1))
	})

	It("logs the status transition on a successful cancel-rejected", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("status", model.StatusQueued).Error).NotTo(HaveOccurred())

		var buf syncBuffer
		prevLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		defer slog.SetDefault(prevLogger)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.cancel-rejected", instance.ID, testAgentName)

		Eventually(buf.String, 2*time.Second, 20*time.Millisecond).Should(SatisfyAll(
			ContainSubstring("status transition applied"),
			ContainSubstring("instance_id="+instance.ID),
			ContainSubstring("event_type=dcm.agent.cancel-rejected"),
			ContainSubstring("agent_name="+testAgentName),
			ContainSubstring("status=pending_deletion"),
		))
	})

	It("does not clobber a terminal state on cancel-rejected (CAS guard)", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("status", "failed").Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.cancel-rejected", instance.ID, testAgentName)

		Consistently(func() string {
			return currentStatus(db, instance.ID)
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("failed"))
	})

	It("ignores a late cancel-rejected for an instance the self-healing loop already reassigned (R2 S3: finding #1)", func() {
		// Instance has been reassigned to a new agent (status "pending") by
		// the time a cancel-rejected for the original request arrives late;
		// status alone (not agent identity) must already reject it, so
		// agent_name is kept matching to isolate that guard.
		instance := createPendingInstance(ctx, db)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.cancel-rejected", instance.ID, testAgentName)

		Consistently(func() string {
			return currentStatus(db, instance.ID)
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("pending"))

		var updated model.ServiceTypeInstance
		Expect(db.First(&updated, "id = ?", instance.ID).Error).NotTo(HaveOccurred())
		Expect(updated.DeletionStatus).To(BeNil())
	})

	// A second, independent guard, this time closed by identity rather than
	// status: even while the instance is still in an allowed
	// cancellableStatus, an agent_name mismatch must reject the event.
	It("ignores a cancel-rejected from a superseded agent even though status still matches (identity check)", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("status", model.StatusQueued).Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.cancel-rejected", instance.ID, staleAgentName)

		Consistently(func() string {
			return currentStatus(db, instance.ID)
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("queued"))

		var updated model.ServiceTypeInstance
		Expect(db.First(&updated, "id = ?", instance.ID).Error).NotTo(HaveOccurred())
		Expect(updated.DeletionStatus).To(BeNil())
	})

	It("NakWithDelay on transient failures", func() {
		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.creation-acknowledged", "nonexistent-id", testAgentName)
		time.Sleep(500 * time.Millisecond)
	})

	It("Ack + log on permanent failures (malformed)", func() {
		Expect(rc.Start(ctx)).To(Succeed())

		ctx := context.Background()
		_, err := js.Publish(ctx, "dcm.agents.responses", []byte("not-json"))
		Expect(err).NotTo(HaveOccurred())
		time.Sleep(500 * time.Millisecond)
	})

	// A malformed event with no agent_name is acked and discarded without
	// touching the instance, same as an empty/missing resource_id.
	It("acks and discards an event missing agent_name without touching the instance", func() {
		instance := createPendingInstance(ctx, db)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEventNoAgentName(js, "dcm.agent.creation-acknowledged", instance.ID)

		Consistently(func() string {
			return currentStatus(db, instance.ID)
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("pending"))
	})

	// The blank-check in handleMessage must reject a missing agent_name on
	// its own merits, not merely benefit from the store's WHERE clause never
	// matching blank against a non-blank stored value - so the instance's
	// own agent_name is blank here too.
	It("acks and discards an event missing agent_name even when the instance's own agent_name is also blank", func() {
		blank := ""
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("agent_name", blank).Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEventNoAgentName(js, "dcm.agent.creation-acknowledged", instance.ID)

		Consistently(func() string {
			return currentStatus(db, instance.ID)
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("pending"))
	})

	It("transitions QUEUED to CANCELLED on cancel-acknowledged", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("status", model.StatusQueued).Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.cancel-acknowledged", instance.ID, testAgentName)

		Eventually(func() string {
			return currentStatus(db, instance.ID)
		}, 2*time.Second, 20*time.Millisecond).Should(Equal("cancelled"))
	})

	It("ignores a stale cancel-acknowledged for an instance the self-healing loop already reassigned (F: split-brain)", func() {
		// Simulate: the queued-timeout sweep already cancelled+reassigned
		// this instance to a new agent (status is back to "pending"), and
		// the OLD agent's cancel-acknowledged for the original cancel
		// request arrives late. It must not clobber the fresh "pending"
		// state set up for the new agent. Status alone already guards this
		// (pending is not in fromStatuses={queued}) - kept as testAgentName
		// to isolate the status-CAS behavior from the identity check
		// exercised below.
		instance := createPendingInstance(ctx, db)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.cancel-acknowledged", instance.ID, testAgentName)

		Consistently(func() string {
			return currentStatus(db, instance.ID)
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("pending"))
	})

	// Same mismatch treatment as creation-acknowledged/error, for
	// cancel-acknowledged.
	It("ignores a cancel-acknowledged from a superseded agent even though status still matches (identity check)", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("status", model.StatusQueued).Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.cancel-acknowledged", instance.ID, staleAgentName)

		Consistently(func() string {
			return currentStatus(db, instance.ID)
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("queued"))
	})

	It("ignores a stale error event for an instance that already moved past provisioning", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("status", model.StatusCancelled).Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.error", instance.ID, testAgentName)

		Consistently(func() string {
			return currentStatus(db, instance.ID)
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("cancelled"))
	})
})

// syncBuffer wraps bytes.Buffer with a mutex: the consumer writes logs from
// its own background goroutine while these tests concurrently poll the
// buffer's contents via Eventually, which a plain bytes.Buffer doesn't
// support safely.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// currentStatus polls an instance's current status for use with
// Eventually/Consistently, instead of a fixed time.Sleep before one read.
func currentStatus(db *gorm.DB, id string) string {
	var updated model.ServiceTypeInstance
	Expect(db.First(&updated, "id = ?", id).Error).NotTo(HaveOccurred())
	return updated.Status
}

func currentStatusMessage(db *gorm.DB, id string) string {
	var updated model.ServiceTypeInstance
	Expect(db.First(&updated, "id = ?", id).Error).NotTo(HaveOccurred())
	return updated.StatusMessage
}

func currentAgentErrorStreamSequence(db *gorm.DB, id string) uint64 {
	var updated model.ServiceTypeInstance
	Expect(db.First(&updated, "id = ?", id).Error).NotTo(HaveOccurred())
	return updated.AgentErrorStreamSequence
}

func createPendingInstance(_ context.Context, db *gorm.DB) model.ServiceTypeInstance {
	agentName := testAgentName
	now := time.Now()
	instance := model.ServiceTypeInstance{
		ID:               uuid.New().String(),
		ServiceType:      "vm",
		Status:           "pending",
		InstanceName:     "test-instance",
		Spec:             map[string]any{"cpu": 4},
		AgentName:        &agentName,
		PendingStartedAt: &now,
	}
	Expect(db.Create(&instance).Error).NotTo(HaveOccurred())
	return instance
}

// testAgentName is the agent_name used by createPendingInstance's fixture
// and by publishAgentEvent's default matching-agent call sites, so a
// "genuine ack" scenario is the default and mismatch scenarios explicitly
// opt into a different, stale agent name.
const testAgentName = "test-agent"

// staleAgentName simulates a superseded agent's late event: an agent that
// no longer owns the instance (self-healing has since reassigned it to
// testAgentName) sending a delayed ack for its original assignment.
const staleAgentName = "stale-agent"

func publishAgentEvent(js jetstream.JetStream, eventType string, resourceID string, agentName string) {
	publishAgentEventWithData(js, eventType, map[string]any{"resource_id": resourceID, "agent_name": agentName})
}

func publishAgentEventWithData(js jetstream.JetStream, eventType string, eventData map[string]any) {
	data, err := json.Marshal(map[string]any{
		"specversion": "1.0",
		"type":        eventType,
		"source":      "test",
		"id":          uuid.New().String(),
		"data":        eventData,
	})
	Expect(err).NotTo(HaveOccurred())
	ctx := context.Background()
	_, err = js.Publish(ctx, "dcm.agents.responses", data)
	Expect(err).NotTo(HaveOccurred())
}

// publishAgentEventNoAgentName publishes a response event with resource_id
// but no agent_name field at all.
func publishAgentEventNoAgentName(js jetstream.JetStream, eventType string, resourceID string) {
	data, err := json.Marshal(map[string]any{
		"specversion": "1.0",
		"type":        eventType,
		"source":      "test",
		"id":          uuid.New().String(),
		"data":        map[string]any{"resource_id": resourceID},
	})
	Expect(err).NotTo(HaveOccurred())
	ctx := context.Background()
	_, err = js.Publish(ctx, "dcm.agents.responses", data)
	Expect(err).NotTo(HaveOccurred())
}
