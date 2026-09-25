package resource_manager_test

import (
	"context"
	"errors"
	"time"

	"github.com/dcm-project/control-plane/api/sp/v1alpha1/resource_manager"
	agentStoreImpl "github.com/dcm-project/control-plane/internal/agent/store/agent"
	agentmodel "github.com/dcm-project/control-plane/internal/agent/store/model"
	"github.com/dcm-project/control-plane/internal/sp/cleanup"
	"github.com/dcm-project/control-plane/internal/sp/config"
	"github.com/dcm-project/control-plane/internal/sp/messaging"
	"github.com/dcm-project/control-plane/internal/sp/service"
	rmsvc "github.com/dcm-project/control-plane/internal/sp/service/resource_manager"
	"github.com/dcm-project/control-plane/internal/sp/store"
	"github.com/dcm-project/control-plane/internal/sp/store/model"
	rmstore "github.com/dcm-project/control-plane/internal/sp/store/resource_manager"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// stubJetStream acknowledges every publish so tests can exercise the
// agent-routed CreateInstance/ReassignAgent paths without a real NATS server.
type stubJetStream struct {
	jetstream.JetStream
	onPublish  func(context.Context, string, []byte)
	publishErr error
}

func (s *stubJetStream) Publish(ctx context.Context, subject string, data []byte, _ ...jetstream.PublishOpt) (*jetstream.PubAck, error) {
	if s.onPublish != nil {
		s.onPublish(ctx, subject, data)
	}
	return &jetstream.PubAck{}, s.publishErr
}

type serviceInstanceStoreOverride struct {
	store.Store
	instance rmstore.ServiceTypeInstance
}

func (s *serviceInstanceStoreOverride) ServiceTypeInstance() rmstore.ServiceTypeInstance {
	return s.instance
}

type capturePendingDeletionSnapshot struct {
	rmstore.ServiceTypeInstance
	snapshot *[]model.ServiceTypeInstance
}

func (s *capturePendingDeletionSnapshot) MarkForDeletion(ctx context.Context, id string) error {
	if err := s.ServiceTypeInstance.MarkForDeletion(ctx, id); err != nil {
		return err
	}
	pending, err := s.ServiceTypeInstance.ListPendingDeletions(ctx)
	if err == nil {
		*s.snapshot = pending
	}
	return err
}

type stalePendingDeletionSnapshot struct {
	rmstore.ServiceTypeInstance
	snapshot []model.ServiceTypeInstance
}

func (s *stalePendingDeletionSnapshot) ListPendingDeletions(context.Context) ([]model.ServiceTypeInstance, error) {
	return s.snapshot, nil
}

func ptrString(s string) *string { return &s }

var _ = Describe("InstanceService", func() {
	var (
		db              *gorm.DB
		dataStore       store.Store
		instanceService *rmsvc.InstanceService
		publishStub     *stubJetStream
		ctx             context.Context
	)

	BeforeEach(func() {
		var err error
		db, err = gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(db.AutoMigrate(&agentmodel.Agent{}, &model.ServiceTypeInstance{})).To(Succeed())
		Expect(db.Create(&agentmodel.Agent{ID: uuid.New().String(), Name: "test-agent", TopicName: "dcm.agent.test-agent", HealthStatus: agentmodel.AgentHealthStatusReady, ServiceTypes: []string{"vm", "container"}}).Error).NotTo(HaveOccurred())

		dataStore = store.NewStore(db)
		publishStub = &stubJetStream{}
		pub := messaging.NewPublisher(publishStub)
		instanceService = rmsvc.NewInstanceService(dataStore, pub, agentStoreImpl.NewAgent(db))
		ctx = context.Background()
	})

	AfterEach(func() {
		_ = dataStore.Close()
	})

	Describe("CreateInstance (agent-routed provisioning)", func() {
		It("creates instance with pending status via agent NATS", func() {
			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 2, "memory": "4GB", "service_type": "vm"},
			}

			result, err := instanceService.CreateInstance(ctx, req, nil, "test-agent")

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.Id).NotTo(BeNil())

			var stored model.ServiceTypeInstance
			Expect(db.First(&stored, "id = ?", *result.Id).Error).NotTo(HaveOccurred())
			Expect(stored.Status).To(Equal("pending"))
		})

		It("sets service_type from spec", func() {
			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 2, "service_type": "vm"},
			}

			result, err := instanceService.CreateInstance(ctx, req, nil, "test-agent")

			Expect(err).NotTo(HaveOccurred())
			var dbInstance model.ServiceTypeInstance
			Expect(db.Where("id = ?", *result.Id).First(&dbInstance).Error).NotTo(HaveOccurred())
			Expect(dbInstance.ServiceType).To(Equal("vm"))
		})

		It("creates instance with specified ID", func() {
			specifiedID := uuid.New().String()
			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 1, "service_type": "vm"},
			}

			result, err := instanceService.CreateInstance(ctx, req, &specifiedID, "test-agent")

			Expect(err).NotTo(HaveOccurred())
			Expect(*result.Id).To(Equal(specifiedID))
		})

		It("returns conflict error for duplicate ID", func() {
			specifiedID := uuid.New().String()
			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 1, "service_type": "vm"},
			}

			_, err := instanceService.CreateInstance(ctx, req, &specifiedID, "test-agent")
			Expect(err).NotTo(HaveOccurred())

			_, err = instanceService.CreateInstance(ctx, req, &specifiedID, "test-agent")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			errors.As(err, &svcErr)
			Expect(svcErr.Code).To(Equal(service.ErrCodeConflict))
		})

		It("returns validation error when spec is missing service_type", func() {
			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 2},
			}

			_, err := instanceService.CreateInstance(ctx, req, nil, "test-agent")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			errors.As(err, &svcErr)
			Expect(svcErr.Code).To(Equal(service.ErrCodeValidation))
			Expect(svcErr.Message).To(ContainSubstring("spec.service_type is required"))
		})

		It("returns validation error when spec.service_type is not a string", func() {
			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 2, "service_type": 42},
			}

			_, err := instanceService.CreateInstance(ctx, req, nil, "test-agent")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			errors.As(err, &svcErr)
			Expect(svcErr.Code).To(Equal(service.ErrCodeValidation))
		})

		It("returns validation error when spec.service_type is empty", func() {
			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 2, "service_type": ""},
			}

			_, err := instanceService.CreateInstance(ctx, req, nil, "test-agent")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			errors.As(err, &svcErr)
			Expect(svcErr.Code).To(Equal(service.ErrCodeValidation))
			Expect(svcErr.Message).To(ContainSubstring("must not be empty"))
		})

		It("returns validation error when spec.service_type is whitespace only", func() {
			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 2, "service_type": " "},
			}

			_, err := instanceService.CreateInstance(ctx, req, nil, "test-agent")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			errors.As(err, &svcErr)
			Expect(svcErr.Code).To(Equal(service.ErrCodeValidation))
			Expect(svcErr.Message).To(ContainSubstring("must not be empty"))
		})

		It("returns validation error when agentName is empty instead of creating an orphan pending row", func() {
			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 2, "service_type": "vm"},
			}

			_, err := instanceService.CreateInstance(ctx, req, nil, "")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			errors.As(err, &svcErr)
			Expect(svcErr.Code).To(Equal(service.ErrCodeValidation))
			Expect(svcErr.Message).To(ContainSubstring("agent_name"))

			var count int64
			Expect(db.Model(&model.ServiceTypeInstance{}).Count(&count).Error).NotTo(HaveOccurred())
			Expect(count).To(BeZero(), "no instance row should have been created")
		})

		It("returns validation error when agentName is whitespace only", func() {
			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 2, "service_type": "vm"},
			}

			_, err := instanceService.CreateInstance(ctx, req, nil, "   ")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			errors.As(err, &svcErr)
			Expect(svcErr.Code).To(Equal(service.ErrCodeValidation))
		})
	})

	Describe("GetInstance", func() {
		It("returns an instance", func() {
			inst := model.ServiceTypeInstance{
				ID:           uuid.New().String(),
				ServiceType:  "vm",
				Status:       "pending",
				InstanceName: "get-inst",
				Spec:         map[string]any{"cpu": 2},
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())

			result, err := instanceService.GetInstance(ctx, inst.ID, false)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(*result.Id).To(Equal(inst.ID))
		})

		It("returns not found error for non-existent instance", func() {
			_, err := instanceService.GetInstance(ctx, uuid.New().String(), false)

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			errors.As(err, &svcErr)
			Expect(svcErr.Code).To(Equal(service.ErrCodeNotFound))
		})
	})

	Describe("GetOutputSpec", func() {
		It("returns persisted output_spec", func() {
			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 2, "service_type": "vm"},
			}
			created, err := instanceService.CreateInstance(ctx, req, nil, "test-agent")
			Expect(err).NotTo(HaveOccurred())

			outputSpec := map[string]any{"connection_string": "postgres://db:5432/orders"}
			err = dataStore.ServiceTypeInstance().UpdateStatus(ctx, *created.Id, "RUNNING", "ready", outputSpec)
			Expect(err).NotTo(HaveOccurred())

			got, err := instanceService.GetOutputSpec(ctx, *created.Id)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(outputSpec))
		})

		It("returns not found error for non-existent instance", func() {
			_, err := instanceService.GetOutputSpec(ctx, uuid.New().String())

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			errors.As(err, &svcErr)
			Expect(svcErr.Code).To(Equal(service.ErrCodeNotFound))
		})
	})

	Describe("ListInstances", func() {
		It("returns empty list when no instances exist", func() {
			result, err := instanceService.ListInstances(ctx, nil, nil, false, nil, nil)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(*result.Instances).To(BeEmpty())
		})

		It("returns all instances", func() {
			for i := 0; i < 3; i++ {
				inst := model.ServiceTypeInstance{
					ID:           uuid.New().String(),
					ServiceType:  "vm",
					Status:       "pending",
					InstanceName: uuid.New().String(),
					Spec:         map[string]any{"cpu": i + 1},
				}
				Expect(db.Create(&inst).Error).NotTo(HaveOccurred())
			}

			result, err := instanceService.ListInstances(ctx, nil, nil, false, nil, nil)

			Expect(err).NotTo(HaveOccurred())
			Expect(*result.Instances).To(HaveLen(3))
		})

		It("filters instances by service type", func() {
			for i := 0; i < 2; i++ {
				inst := model.ServiceTypeInstance{
					ID:           uuid.New().String(),
					ServiceType:  "vm",
					Status:       "pending",
					InstanceName: uuid.New().String(),
					Spec:         map[string]any{"cpu": i + 1},
				}
				Expect(db.Create(&inst).Error).NotTo(HaveOccurred())
			}
			for i := 0; i < 3; i++ {
				inst := model.ServiceTypeInstance{
					ID:           uuid.New().String(),
					ServiceType:  "container",
					Status:       "pending",
					InstanceName: uuid.New().String(),
					Spec:         map[string]any{"image": "nginx"},
				}
				Expect(db.Create(&inst).Error).NotTo(HaveOccurred())
			}

			vmType := "vm"
			result, err := instanceService.ListInstances(ctx, &vmType, nil, false, nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(*result.Instances).To(HaveLen(2))

			containerType := "container"
			result, err = instanceService.ListInstances(ctx, &containerType, nil, false, nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(*result.Instances).To(HaveLen(3))
		})

		It("filters instances by agent name", func() {
			agentA, agentB := "agent-a", "agent-b"
			for i := 0; i < 2; i++ {
				inst := model.ServiceTypeInstance{
					ID:           uuid.New().String(),
					ServiceType:  "vm",
					Status:       "pending",
					InstanceName: uuid.New().String(),
					Spec:         map[string]any{"cpu": i + 1},
					AgentName:    &agentA,
				}
				Expect(db.Create(&inst).Error).NotTo(HaveOccurred())
			}
			inst := model.ServiceTypeInstance{
				ID:           uuid.New().String(),
				ServiceType:  "vm",
				Status:       "pending",
				InstanceName: uuid.New().String(),
				Spec:         map[string]any{"cpu": 3},
				AgentName:    &agentB,
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())

			result, err := instanceService.ListInstances(ctx, nil, &agentA, false, nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(*result.Instances).To(HaveLen(2))

			result, err = instanceService.ListInstances(ctx, nil, &agentB, false, nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(*result.Instances).To(HaveLen(1))
		})
	})

	Describe("DeleteInstance (agent-routed)", func() {
		It("persists deleting status and cleanup enrollment before publishing", func() {
			agentName := "test-agent"
			inst := model.ServiceTypeInstance{
				ID: uuid.New().String(), ServiceType: "vm", Status: "running",
				InstanceName: "ordered-delete", Spec: map[string]any{"cpu": 2}, AgentName: &agentName,
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())

			published := false
			publishStub.onPublish = func(_ context.Context, _ string, _ []byte) {
				var stored model.ServiceTypeInstance
				Expect(db.First(&stored, "id = ?", inst.ID).Error).To(Succeed())
				Expect(stored.Status).To(Equal(model.StatusDeleting))
				Expect(stored.DeletionStatus).NotTo(BeNil())
				Expect(*stored.DeletionStatus).To(Equal("SCHEDULED"))
				published = true
			}

			Expect(instanceService.DeleteInstance(ctx, inst.ID, false)).To(Succeed())
			Expect(published).To(BeTrue())
		})

		It("does not publish when persisting deleting status fails", func() {
			agentName := "test-agent"
			inst := model.ServiceTypeInstance{
				ID: uuid.New().String(), ServiceType: "vm", Status: "running",
				InstanceName: "failed-status-delete", Spec: map[string]any{"cpu": 2}, AgentName: &agentName,
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())
			Expect(db.Exec("CREATE TRIGGER fail_deleting_status BEFORE UPDATE OF status ON service_type_instances WHEN NEW.status = 'deleting' BEGIN SELECT RAISE(ABORT, 'forced status failure'); END").Error).NotTo(HaveOccurred())

			publishCount := 0
			publishStub.onPublish = func(context.Context, string, []byte) { publishCount++ }
			err := instanceService.DeleteInstance(ctx, inst.ID, false)

			Expect(err).To(HaveOccurred())
			Expect(publishCount).To(BeZero())
			var stored model.ServiceTypeInstance
			Expect(db.First(&stored, "id = ?", inst.ID).Error).To(Succeed())
			Expect(stored.Status).To(Equal("running"))
			Expect(stored.DeletionStatus).To(BeNil())
		})

		It("does not publish when cleanup enrollment fails", func() {
			agentName := "test-agent"
			inst := model.ServiceTypeInstance{
				ID: uuid.New().String(), ServiceType: "vm", Status: "running",
				InstanceName: "failed-enrollment-delete", Spec: map[string]any{"cpu": 2}, AgentName: &agentName,
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())
			Expect(db.Exec("CREATE TRIGGER fail_deletion_enrollment BEFORE UPDATE OF deletion_status ON service_type_instances WHEN NEW.deletion_status = 'SCHEDULED' BEGIN SELECT RAISE(ABORT, 'forced enrollment failure'); END").Error).NotTo(HaveOccurred())

			publishCount := 0
			publishStub.onPublish = func(context.Context, string, []byte) { publishCount++ }
			Expect(instanceService.DeleteInstance(ctx, inst.ID, false)).To(HaveOccurred())
			Expect(publishCount).To(BeZero())
			var stored model.ServiceTypeInstance
			Expect(db.First(&stored, "id = ?", inst.ID).Error).To(Succeed())
			Expect(stored.Status).To(Equal(model.StatusDeleting))
			Expect(stored.DeletionStatus).To(BeNil())
		})

		It("leaves a failed publication enrolled for cleanup retry", func() {
			agentName := "test-agent"
			inst := model.ServiceTypeInstance{
				ID: uuid.New().String(), ServiceType: "vm", Status: "running",
				InstanceName: "retry-publish-delete", Spec: map[string]any{"cpu": 2}, AgentName: &agentName,
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())
			publishStub.publishErr = errors.New("temporary publish failure")

			Expect(instanceService.DeleteInstance(ctx, inst.ID, false)).To(HaveOccurred())
			var stored model.ServiceTypeInstance
			Expect(db.First(&stored, "id = ?", inst.ID).Error).To(Succeed())
			Expect(stored.Status).To(Equal(model.StatusDeleting))
			Expect(stored.DeletionStatus).NotTo(BeNil())
			Expect(*stored.DeletionStatus).To(Equal("SCHEDULED"))
		})
		It("does not republish an already scheduled non-deferred delete on caller retry", func() {
			agentName := "test-agent"
			inst := model.ServiceTypeInstance{
				ID: uuid.New().String(), ServiceType: "vm", Status: "running",
				InstanceName: "retry-already-scheduled-delete", Spec: map[string]any{"cpu": 2}, AgentName: &agentName,
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())
			publishCount := 0
			publishStub.onPublish = func(context.Context, string, []byte) { publishCount++ }
			publishStub.publishErr = errors.New("ambiguous publish failure")

			Expect(instanceService.DeleteInstance(ctx, inst.ID, false)).To(HaveOccurred())
			attemptsAfterFirstCall := publishCount
			Expect(attemptsAfterFirstCall).To(BeNumerically(">", 0))
			publishStub.publishErr = nil

			Expect(instanceService.DeleteInstance(ctx, inst.ID, false)).To(Succeed())
			Expect(publishCount).To(Equal(attemptsAfterFirstCall))
		})

		It("does not republish an already scheduled deferred delete", func() {
			agentName := "test-agent"
			inst := model.ServiceTypeInstance{
				ID: uuid.New().String(), ServiceType: "vm", Status: "running",
				InstanceName: "retry-already-scheduled-deferred-delete", Spec: map[string]any{"cpu": 2}, AgentName: &agentName,
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())
			publishCount := 0
			publishStub.onPublish = func(context.Context, string, []byte) { publishCount++ }

			Expect(instanceService.DeleteInstance(ctx, inst.ID, true)).To(Succeed())
			Expect(publishCount).To(Equal(1))
			Expect(instanceService.DeleteInstance(ctx, inst.ID, true)).To(Succeed())
			Expect(publishCount).To(Equal(1))
		})

		It("does not let a stale cleanup snapshot duplicate the direct delete publish", func() {
			agentName := "test-agent"
			inst := model.ServiceTypeInstance{
				ID: uuid.New().String(), ServiceType: "vm", Status: "running",
				InstanceName: "direct-cleanup-race", Spec: map[string]any{"cpu": 2}, AgentName: &agentName,
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())
			var staleSnapshot []model.ServiceTypeInstance
			capturingStore := &capturePendingDeletionSnapshot{
				ServiceTypeInstance: dataStore.ServiceTypeInstance(),
				snapshot:            &staleSnapshot,
			}
			directService := rmsvc.NewInstanceService(
				&serviceInstanceStoreOverride{Store: dataStore, instance: capturingStore},
				messaging.NewPublisher(publishStub), agentStoreImpl.NewAgent(db),
			)
			publishCount := 0
			publishStub.onPublish = func(context.Context, string, []byte) { publishCount++ }

			Expect(directService.DeleteInstance(ctx, inst.ID, false)).To(Succeed())
			Expect(staleSnapshot).To(HaveLen(1))
			Expect(staleSnapshot[0].LastDeletionAttempt).To(BeNil())

			schedulerStore := &serviceInstanceStoreOverride{
				Store: dataStore,
				instance: &stalePendingDeletionSnapshot{
					ServiceTypeInstance: dataStore.ServiceTypeInstance(),
					snapshot:            staleSnapshot,
				},
			}
			scheduler := cleanup.NewScheduler(
				schedulerStore, messaging.NewPublisher(publishStub), agentStoreImpl.NewAgent(db),
				&config.CleanupConfig{MaxRetries: 3},
			)
			scheduler.ProcessPendingDeletions(ctx)
			scheduler = cleanup.NewScheduler(
				dataStore, messaging.NewPublisher(publishStub), agentStoreImpl.NewAgent(db),
				&config.CleanupConfig{Interval: time.Hour, MaxRetries: 3},
			)
			scheduler.ProcessPendingDeletions(ctx)

			Expect(publishCount).To(Equal(1))
			found, err := dataStore.ServiceTypeInstance().Get(ctx, inst.ID, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.RetryCount).To(Equal(1))
		})

		It("publishes delete event and marks deleting, awaiting agent acknowledgement, for non-deferred deletion", func() {
			agentName := "test-agent"
			inst := model.ServiceTypeInstance{
				ID:           uuid.New().String(),
				ServiceType:  "vm",
				Status:       "running",
				InstanceName: "del-inst",
				Spec:         map[string]any{"cpu": 2},
				AgentName:    &agentName,
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())

			err := instanceService.DeleteInstance(ctx, inst.ID, false)
			Expect(err).NotTo(HaveOccurred())

			// The record must still exist as "deleting" until the agent's
			// deletion-acknowledged event confirms the physical resource is
			// gone, and be enrolled in retry tracking like a deferred delete.
			got, getErr := instanceService.GetInstance(ctx, inst.ID, true)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(got.Status).NotTo(BeNil())
			Expect(*got.Status).To(Equal("deleting"))

			_, hiddenErr := instanceService.GetInstance(ctx, inst.ID, false)
			var svcErr *service.ServiceError
			Expect(hiddenErr).To(BeAssignableToTypeOf(svcErr))
			errors.As(hiddenErr, &svcErr)
			Expect(svcErr.Code).To(Equal(service.ErrCodeNotFound))
		})

		It("hard-deletes immediately when the instance has no agent", func() {
			inst := model.ServiceTypeInstance{
				ID:           uuid.New().String(),
				ServiceType:  "vm",
				Status:       "running",
				InstanceName: "del-inst-no-agent",
				Spec:         map[string]any{"cpu": 2},
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())

			err := instanceService.DeleteInstance(ctx, inst.ID, false)
			Expect(err).NotTo(HaveOccurred())

			_, getErr := instanceService.GetInstance(ctx, inst.ID, false)
			var svcErr *service.ServiceError
			Expect(getErr).To(BeAssignableToTypeOf(svcErr))
			errors.As(getErr, &svcErr)
			Expect(svcErr.Code).To(Equal(service.ErrCodeNotFound))
		})

		It("hard-deletes immediately when the assigned agent no longer exists (C)", func() {
			// Without this, publishDeleteToAgent's old behavior (silently
			// treating ErrAgentNotFound as a successful publish) would leave
			// this instance stuck in "deleting" forever: no agent will ever
			// send a "deletion-acknowledged" for a nonexistent agent.
			gone := "nonexistent-agent"
			inst := model.ServiceTypeInstance{
				ID:           uuid.New().String(),
				ServiceType:  "vm",
				Status:       "running",
				InstanceName: "del-inst-gone-agent",
				Spec:         map[string]any{"cpu": 2},
				AgentName:    &gone,
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())

			err := instanceService.DeleteInstance(ctx, inst.ID, false)
			Expect(err).NotTo(HaveOccurred())

			_, getErr := instanceService.GetInstance(ctx, inst.ID, true)
			var svcErr *service.ServiceError
			Expect(getErr).To(BeAssignableToTypeOf(svcErr))
			errors.As(getErr, &svcErr)
			Expect(svcErr.Code).To(Equal(service.ErrCodeNotFound))
		})

		It("returns not found error for non-existent instance", func() {
			err := instanceService.DeleteInstance(ctx, uuid.New().String(), false)

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			errors.As(err, &svcErr)
			Expect(svcErr.Code).To(Equal(service.ErrCodeNotFound))
		})

		It("defers deletion without contacting provider", func() {
			agentName := "test-agent"
			inst := model.ServiceTypeInstance{
				ID:           uuid.New().String(),
				ServiceType:  "vm",
				Status:       "running",
				InstanceName: "defer-del-inst",
				Spec:         map[string]any{"cpu": 2},
				AgentName:    &agentName,
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())

			err := instanceService.DeleteInstance(ctx, inst.ID, true)
			Expect(err).NotTo(HaveOccurred())

			result, err := instanceService.ListInstances(ctx, nil, nil, false, nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(*result.Instances).To(BeEmpty())

			result, err = instanceService.ListInstances(ctx, nil, nil, true, nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(*result.Instances).To(HaveLen(1))
			Expect(string(*(*result.Instances)[0].DeletionStatus)).To(Equal("SCHEDULED"))
		})

		It("surfaces deletion_status DELETED on the API struct after cleanup completes", func() {
			agentName := "test-agent"
			inst := model.ServiceTypeInstance{
				ID:           uuid.New().String(),
				ServiceType:  "vm",
				Status:       "running",
				InstanceName: "deleted-api-inst",
				Spec:         map[string]any{"cpu": 2},
				AgentName:    &agentName,
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())
			Expect(dataStore.ServiceTypeInstance().MarkForDeletion(ctx, inst.ID)).To(Succeed())
			Expect(dataStore.ServiceTypeInstance().MarkDeletionComplete(ctx, inst.ID)).To(Succeed())

			got, err := instanceService.GetInstance(ctx, inst.ID, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(got.DeletionStatus).NotTo(BeNil())
			Expect(string(*got.DeletionStatus)).To(Equal("DELETED"))

			result, err := instanceService.ListInstances(ctx, nil, nil, true, nil, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(*result.Instances).To(HaveLen(1))
			Expect(string(*(*result.Instances)[0].DeletionStatus)).To(Equal("DELETED"))
		})

		It("surfaces deletion_status FAILED on the API struct after cleanup gives up", func() {
			agentName := "test-agent"
			inst := model.ServiceTypeInstance{
				ID:           uuid.New().String(),
				ServiceType:  "vm",
				Status:       "running",
				InstanceName: "failed-api-inst",
				Spec:         map[string]any{"cpu": 2},
				AgentName:    &agentName,
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())
			Expect(dataStore.ServiceTypeInstance().MarkForDeletion(ctx, inst.ID)).To(Succeed())
			Expect(dataStore.ServiceTypeInstance().MarkDeletionFailed(ctx, inst.ID)).To(Succeed())

			got, err := instanceService.GetInstance(ctx, inst.ID, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(got.DeletionStatus).NotTo(BeNil())
			Expect(string(*got.DeletionStatus)).To(Equal("FAILED"))
		})
	})

	Describe("InstanceService agent fields", func() {
		It("stores agent_name on instance record and surfaces it on the API struct (F19)", func() {
			agentName := "test-agent"
			inst := model.ServiceTypeInstance{
				ID:           uuid.New().String(),
				ServiceType:  "vm",
				Status:       "pending",
				InstanceName: "agent-inst",
				Spec:         map[string]any{"cpu": 2},
				AgentName:    &agentName,
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())

			var stored model.ServiceTypeInstance
			Expect(db.First(&stored, "id = ?", inst.ID).Error).NotTo(HaveOccurred())
			Expect(stored.AgentName).NotTo(BeNil())
			Expect(*stored.AgentName).To(Equal("test-agent"))

			// ModelToAPI must populate AgentName on the returned API struct
			// too, not just the DB row.
			got, err := instanceService.GetInstance(ctx, inst.ID, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(got.AgentName).NotTo(BeNil())
			Expect(*got.AgentName).To(Equal("test-agent"))
		})
	})

	Describe("Agent validation", func() {
		It("rejects creation when agent is not found", func() {
			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 2, "service_type": "vm"},
			}

			_, err := instanceService.CreateInstance(ctx, req, nil, "nonexistent-agent")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeNotFound))
		})

		It("rejects creation when agent is unavailable", func() {
			Expect(db.Create(&agentmodel.Agent{
				ID:           uuid.New().String(),
				Name:         "unavailable-agent",
				TopicName:    "dcm.agent.unavailable-agent",
				HealthStatus: agentmodel.AgentHealthStatusUnavailable,
				ServiceTypes: []string{"vm"},
			}).Error).NotTo(HaveOccurred())

			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 2, "service_type": "vm"},
			}

			_, err := instanceService.CreateInstance(ctx, req, nil, "unavailable-agent")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeUnavailable))
		})

		It("rejects creation when agent is congested", func() {
			Expect(db.Create(&agentmodel.Agent{
				ID:           uuid.New().String(),
				Name:         "congested-agent",
				TopicName:    "dcm.agent.congested-agent",
				HealthStatus: agentmodel.AgentHealthStatusCongested,
				ServiceTypes: []string{"vm"},
			}).Error).NotTo(HaveOccurred())

			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 2, "service_type": "vm"},
			}

			_, err := instanceService.CreateInstance(ctx, req, nil, "congested-agent")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeUnavailable))
		})

		It("rejects creation when agent does not serve the requested service type", func() {
			Expect(db.Create(&agentmodel.Agent{
				ID:           uuid.New().String(),
				Name:         "container-only-agent",
				TopicName:    "dcm.agent.container-only-agent",
				HealthStatus: agentmodel.AgentHealthStatusReady,
				ServiceTypes: []string{"container"},
			}).Error).NotTo(HaveOccurred())

			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 2, "service_type": "vm"},
			}

			_, err := instanceService.CreateInstance(ctx, req, nil, "container-only-agent")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeValidation))
			Expect(svcErr.Message).To(ContainSubstring("does not serve service type"))
		})

		It("accepts creation when agent is ready and serves the service type", func() {
			req := &resource_manager.ServiceTypeInstance{
				Spec: map[string]interface{}{"cpu": 2, "service_type": "vm"},
			}

			result, err := instanceService.CreateInstance(ctx, req, nil, "test-agent")

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
		})
	})

	Describe("ReassignAgent", func() {
		BeforeEach(func() {
			Expect(db.Create(&agentmodel.Agent{ID: uuid.New().String(), Name: "fallback-agent", TopicName: "dcm.agent.fallback-agent", HealthStatus: agentmodel.AgentHealthStatusReady, ServiceTypes: []string{"vm"}}).Error).NotTo(HaveOccurred())
		})

		It("reassigns when expectedCurrentAgent matches the instance's current agent", func() {
			inst := model.ServiceTypeInstance{
				ID:           uuid.New().String(),
				ServiceType:  "vm",
				Status:       "pending",
				InstanceName: "reassign-cas-match",
				Spec:         map[string]any{"cpu": 2},
				AgentName:    ptrString("test-agent"),
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())

			err := instanceService.ReassignAgent(ctx, inst.ID, "fallback-agent", "test-agent")

			Expect(err).NotTo(HaveOccurred())
			var stored model.ServiceTypeInstance
			Expect(db.First(&stored, "id = ?", inst.ID).Error).NotTo(HaveOccurred())
			Expect(*stored.AgentName).To(Equal("fallback-agent"))
		})

		It("rejects the reassignment when expectedCurrentAgent is stale (R2 T1: CAS parameter must be threaded end-to-end, not re-derived from a fresh read)", func() {
			// Proves expectedCurrentAgent actually reaches ReassignAndReset's
			// CAS rather than being silently overridden by a fresh Get()
			// inside ReassignAgent, which would defeat the whole guard: a
			// caller passing a stale/excluded agent it observed earlier
			// must be rejected here even though the DB's current agent_name
			// ("test-agent") looks otherwise eligible (pending, not deleted).
			inst := model.ServiceTypeInstance{
				ID:           uuid.New().String(),
				ServiceType:  "vm",
				Status:       "pending",
				InstanceName: "reassign-cas-stale",
				Spec:         map[string]any{"cpu": 2},
				AgentName:    ptrString("test-agent"),
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())

			err := instanceService.ReassignAgent(ctx, inst.ID, "fallback-agent", "some-other-agent-the-caller-thinks-is-current")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeConflict))

			var stored model.ServiceTypeInstance
			Expect(db.First(&stored, "id = ?", inst.ID).Error).NotTo(HaveOccurred())
			Expect(*stored.AgentName).To(Equal("test-agent"))
		})

		It("returns validation error when agentName is empty", func() {
			inst := model.ServiceTypeInstance{
				ID:           uuid.New().String(),
				ServiceType:  "vm",
				Status:       "pending",
				InstanceName: "reassign-empty-agent",
				Spec:         map[string]any{"cpu": 2},
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())

			err := instanceService.ReassignAgent(ctx, inst.ID, "", "")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeValidation))
		})

		It("returns unavailable error when agent store is not configured", func() {
			// Regression test: ReassignAgent previously had no guard of its
			// own before calling validateAgent (unlike CreateInstance), and
			// validateAgent's nil-agentStore check used to silently skip
			// validation (return nil) instead of erroring. A nil agentStore
			// must fail fast here, not be treated as "agent is valid".
			inst := model.ServiceTypeInstance{
				ID:           uuid.New().String(),
				ServiceType:  "vm",
				Status:       "pending",
				InstanceName: "reassign-no-agent-store",
				Spec:         map[string]any{"cpu": 2},
			}
			Expect(db.Create(&inst).Error).NotTo(HaveOccurred())

			pub := messaging.NewPublisher(&stubJetStream{})
			noAgentStoreService := rmsvc.NewInstanceService(dataStore, pub, nil)

			err := noAgentStoreService.ReassignAgent(ctx, inst.ID, "test-agent", "")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeUnavailable))
		})
	})
})
