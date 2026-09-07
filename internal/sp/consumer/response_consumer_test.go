package consumer_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	agentmodel "github.com/dcm-project/control-plane/internal/agent/store/model"
	"github.com/dcm-project/control-plane/internal/sp/consumer"
	"github.com/dcm-project/control-plane/internal/sp/store"
	"github.com/dcm-project/control-plane/internal/sp/store/model"
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

	// Same mismatch treatment as creation-acknowledged, for the error event.
	It("ignores an error event from a superseded agent even though status still matches (identity check)", func() {
		instance := createPendingInstance(ctx, db)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.error", instance.ID, staleAgentName)

		Consistently(func() string {
			return currentStatus(db, instance.ID)
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Equal("pending"))
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
		Expect(db.Model(&instance).Updates(map[string]any{
			"deletion_status": "SCHEDULED",
			"hard_delete":     true,
		}).Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, testAgentName)

		Eventually(func() error {
			return db.First(&model.ServiceTypeInstance{}, "id = ?", instance.ID).Error
		}, 2*time.Second, 100*time.Millisecond).Should(MatchError(gorm.ErrRecordNotFound))
	})

	// A "fast ack" arriving before status is ever set to deleting must
	// still hard-delete, not soft-complete into a tombstone.
	It("hard-deletes on a fast ack that arrives before status is ever set to deleting", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Updates(map[string]any{
			"deletion_status": "SCHEDULED",
			"hard_delete":     true,
		}).Error).NotTo(HaveOccurred())
		Expect(currentStatus(db, instance.ID)).To(Equal("pending"))

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, testAgentName)

		Eventually(func() error {
			return db.First(&model.ServiceTypeInstance{}, "id = ?", instance.ID).Error
		}, 2*time.Second, 100*time.Millisecond).Should(MatchError(gorm.ErrRecordNotFound))
	})

	// Scheduler retries never set status=deleting either; the ack must
	// still hard-delete once it finally arrives.
	It("hard-deletes after simulated publish-failure retries, still without status ever set to deleting", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Updates(map[string]any{
			"deletion_status": "SCHEDULED",
			"hard_delete":     true,
			"retry_count":     3,
		}).Error).NotTo(HaveOccurred())
		Expect(currentStatus(db, instance.ID)).To(Equal("pending"))

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
		Expect(db.Model(&instance).Updates(map[string]any{
			"deletion_status": "SCHEDULED",
			"hard_delete":     true,
		}).Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())
		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, testAgentName)

		Eventually(func() int { return calls }, 2*time.Second, 20*time.Millisecond).Should(Equal(1))
		Expect(lastID).To(Equal(instance.ID))
	})

	It("logs the hard-delete with event_type on a successful deletion-acknowledged (non-deferred)", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Updates(map[string]any{
			"deletion_status": "SCHEDULED",
			"hard_delete":     true,
		}).Error).NotTo(HaveOccurred())

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
		Expect(db.Model(&instance).Updates(map[string]any{
			"deletion_status": "SCHEDULED",
			"hard_delete":     true,
		}).Error).NotTo(HaveOccurred())

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

	// TC-DEL-08 (REQ-DEL-10): a late ack from the assigned agent for a
	// FAILED row still hard-deletes it.
	It("hard-deletes a FAILED (retries-exhausted) instance on a late deletion-acknowledged from the assigned agent", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Updates(map[string]any{
			"deletion_status": "FAILED",
			"hard_delete":     true,
		}).Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, testAgentName)

		Eventually(func() error {
			return db.First(&model.ServiceTypeInstance{}, "id = ?", instance.ID).Error
		}, 2*time.Second, 100*time.Millisecond).Should(MatchError(gorm.ErrRecordNotFound))
	})

	// TC-DEL-09 (REQ-DEL-10): same, but hard_delete=false soft-completes to a tombstone.
	It("soft-completes a FAILED (retries-exhausted) instance on a late deletion-acknowledged from the assigned agent", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Update("deletion_status", "FAILED").Error).NotTo(HaveOccurred())

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

	// TC-DEL-10 (REQ-DEL-10 AC-2): a FAILED row's ack from a superseded agent is still ignored.
	It("ignores a deletion-acknowledged for a FAILED instance from a superseded agent", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Updates(map[string]any{
			"deletion_status": "FAILED",
			"hard_delete":     true,
		}).Error).NotTo(HaveOccurred())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, staleAgentName)

		Consistently(func() error {
			return db.First(&model.ServiceTypeInstance{}, "id = ?", instance.ID).Error
		}, 300*time.Millisecond, 20*time.Millisecond).Should(Succeed())
	})

	// TC-DEL-11 (REQ-DEL-10 AC-3): finalizing a FAILED row via a late ack logs a distinct Warn.
	It("logs a distinct Warn line when finalizing a FAILED instance via a late ack", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Updates(map[string]any{
			"deletion_status": "FAILED",
			"hard_delete":     true,
		}).Error).NotTo(HaveOccurred())

		var buf syncBuffer
		prevLogger := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
		defer slog.SetDefault(prevLogger)

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, testAgentName)

		Eventually(buf.String, 2*time.Second, 20*time.Millisecond).Should(SatisfyAll(
			ContainSubstring("already given up (FAILED), finalizing anyway"),
			ContainSubstring("instance_id="+instance.ID),
			ContainSubstring("agent_name="+testAgentName),
		))
	})

	// TC-DEL-12 (REQ-DEL-11): must act on the CURRENT hard_delete value, not
	// one read before a concurrent re-delete (ResetRetryCount) changed it.
	It("resolves according to the current hard_delete value, not one changed by a concurrent re-delete after the original enrollment", func() {
		instance := createPendingInstance(ctx, db)
		Expect(db.Model(&instance).Updates(map[string]any{
			"deletion_status": "SCHEDULED",
			"hard_delete":     true,
		}).Error).NotTo(HaveOccurred())

		// Simulates a concurrent re-delete to deferred mode landing before the ack is processed.
		Expect(dataStore.ServiceTypeInstance().ResetRetryCount(ctx, instance.ID, false)).To(Succeed())

		Expect(rc.Start(ctx)).To(Succeed())

		publishAgentEvent(js, "dcm.agent.deletion-acknowledged", instance.ID, testAgentName)

		// Must soft-complete (tombstone), not hard-delete.
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
	data, err := json.Marshal(map[string]any{
		"specversion": "1.0",
		"type":        eventType,
		"source":      "test",
		"id":          uuid.New().String(),
		"data":        map[string]any{"resource_id": resourceID, "agent_name": agentName},
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
