package store_test

import (
	"context"
	"encoding/base64"

	agentmodel "github.com/dcm-project/control-plane/internal/agent/store/model"
	"github.com/dcm-project/control-plane/internal/placement/store"
	"github.com/dcm-project/control-plane/internal/placement/store/model"
	placementtypes "github.com/dcm-project/control-plane/internal/placement/types"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var _ = Describe("Resource Store", func() {
	var (
		db           *gorm.DB
		requestStore store.Resource
		ctx          context.Context
	)

	BeforeEach(func() {
		var err error
		db, err = gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(db.AutoMigrate(&agentmodel.Agent{}, &model.Resource{})).To(Succeed())

		requestStore = store.NewResource(db)
		ctx = context.Background()
	})

	AfterEach(func() {
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
	})

	Describe("Create", func() {
		It("persists the resource without optional fields", func() {
			agent := "test-agent"
			approval := "APPROVED"
			r := model.Resource{
				ID:                    uuid.New().String(),
				RunID:                 "run-1",
				Name:                  "main",
				CatalogItemInstanceId: "catalog-instance-123",
				Spec:                  map[string]any{"cpu": "2", "memory": "4Gi"},
				AgentName:             &agent,
				ApprovalStatus:        &approval,
				Path:                  "resources/" + uuid.New().String(),
			}
			created, err := requestStore.Create(ctx, r)

			Expect(err).NotTo(HaveOccurred())
			Expect(created.ID).To(Equal(r.ID))
			Expect(created.CatalogItemInstanceId).To(Equal("catalog-instance-123"))
			Expect(created.Spec).To(Equal(map[string]any{"cpu": "2", "memory": "4Gi"}))
			Expect(created.AgentName).NotTo(BeNil())
			Expect(*created.AgentName).To(Equal("test-agent"))
			Expect(created.ApprovalStatus).NotTo(BeNil())
			Expect(*created.ApprovalStatus).To(Equal("APPROVED"))
		})

		It("returns error for duplicate ID", func() {
			id := uuid.New().String()
			agent := "test-agent"
			approval := "APPROVED"
			r1 := model.Resource{
				ID:                    id,
				RunID:                 "run-1",
				Name:                  "main",
				CatalogItemInstanceId: "catalog-instance-123",
				Spec:                  map[string]any{"cpu": "2"},
				AgentName:             &agent,
				ApprovalStatus:        &approval,
				Path:                  "resources/" + id,
			}
			_, err := requestStore.Create(ctx, r1)
			Expect(err).NotTo(HaveOccurred())

			// Attempt to create another resource with same ID
			r2 := model.Resource{
				ID:                    id,
				RunID:                 "run-1",
				Name:                  "main",
				CatalogItemInstanceId: "catalog-instance-456",
				Spec:                  map[string]any{"cpu": "4"},
				AgentName:             &agent,
				ApprovalStatus:        &approval,
				Path:                  "resources/" + id,
			}
			_, err = requestStore.Create(ctx, r2)

			Expect(err).To(Equal(store.ErrResourceIdExist))
		})
	})

	Describe("CreateBatch", func() {
		It("persists multiple resources in one call", func() {
			agent := "test-agent"
			approval := "APPROVED"
			id1 := uuid.New().String()
			id2 := uuid.New().String()
			rows := []model.Resource{
				{
					ID:                    id1,
					RunID:                 "run-batch-1",
					Name:                  "db",
					CatalogItemInstanceId: "catalog-instance-123",
					Spec:                  map[string]any{"cpu": "2"},
					AgentName:             &agent,
					ApprovalStatus:        &approval,
					Path:                  "resources/" + id1,
					DagLevel:              0,
				},
				{
					ID:                    id2,
					RunID:                 "run-batch-1",
					Name:                  "app",
					CatalogItemInstanceId: "catalog-instance-123",
					Spec:                  map[string]any{"cpu": "4"},
					RequiresResources:     []string{"db"},
					AgentName:             &agent,
					ApprovalStatus:        &approval,
					Path:                  "resources/" + id2,
					DagLevel:              1,
				},
			}

			created, err := requestStore.CreateBatch(ctx, rows)
			Expect(err).NotTo(HaveOccurred())
			Expect(created).To(HaveLen(2))
			Expect(created[0].ID).To(Equal(id1))
			Expect(created[1].ID).To(Equal(id2))

			listed, err := requestStore.ListByRunID(ctx, "run-batch-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(listed).To(HaveLen(2))
		})

		It("returns error when any resource ID already exists", func() {
			agent := "test-agent"
			approval := "APPROVED"
			existingID := uuid.New().String()
			_, err := requestStore.Create(ctx, model.Resource{
				ID:                    existingID,
				RunID:                 "run-existing",
				Name:                  "main",
				CatalogItemInstanceId: "catalog-instance-123",
				Spec:                  map[string]any{"cpu": "2"},
				AgentName:             &agent,
				ApprovalStatus:        &approval,
				Path:                  "resources/" + existingID,
			})
			Expect(err).NotTo(HaveOccurred())

			newID := uuid.New().String()
			_, err = requestStore.CreateBatch(ctx, []model.Resource{
				{
					ID:                    newID,
					RunID:                 "run-batch-dup",
					Name:                  "a",
					CatalogItemInstanceId: "catalog-instance-123",
					Spec:                  map[string]any{},
					AgentName:             &agent,
					ApprovalStatus:        &approval,
					Path:                  "resources/" + newID,
				},
				{
					ID:                    existingID,
					RunID:                 "run-batch-dup",
					Name:                  "b",
					CatalogItemInstanceId: "catalog-instance-123",
					Spec:                  map[string]any{},
					AgentName:             &agent,
					ApprovalStatus:        &approval,
					Path:                  "resources/" + existingID,
				},
			})
			Expect(err).To(Equal(store.ErrResourceIdExist))
		})
	})

	Describe("Get", func() {
		It("retrieves by ID", func() {
			agent := "test-agent"
			approval := "APPROVED"
			r := model.Resource{
				ID:                    uuid.New().String(),
				RunID:                 "run-1",
				Name:                  "main",
				CatalogItemInstanceId: "catalog-instance-456",
				Spec:                  map[string]any{"test": "data"},
				AgentName:             &agent,
				ApprovalStatus:        &approval,
				Path:                  "resources/" + uuid.New().String(),
			}
			_, _ = requestStore.Create(ctx, r)

			found, err := requestStore.Get(ctx, r.ID)

			Expect(err).NotTo(HaveOccurred())
			Expect(found.CatalogItemInstanceId).To(Equal("catalog-instance-456"))
		})

		It("returns ErrResourceNotFound for missing ID", func() {
			_, err := requestStore.Get(ctx, uuid.New().String())

			Expect(err).To(Equal(store.ErrResourceNotFound))
		})
	})

	Describe("ListRun", func() {
		var (
			agentA   = "agent-a"
			agentB   = "agent-b"
			approval = "APPROVED"
		)

		createResource := func(runID, name, agent, catalogID string) {
			_, err := requestStore.Create(ctx, model.Resource{
				ID:                    uuid.New().String(),
				RunID:                 runID,
				Name:                  name,
				CatalogItemInstanceId: catalogID,
				Spec:                  map[string]any{},
				AgentName:             &agent,
				ApprovalStatus:        &approval,
				Path:                  "resources/" + name,
			})
			Expect(err).NotTo(HaveOccurred())
		}

		BeforeEach(func() {
			// run-1: 2 resources (would span a page_size=2 resource list alone)
			createResource("run-1", "db", agentA, "cat-1")
			createResource("run-1", "app", agentA, "cat-1")
			// run-2: 1 resource, different agent
			createResource("run-2", "main", agentB, "cat-2")
			// run-3: 2 resources
			createResource("run-3", "db", agentA, "cat-3")
			createResource("run-3", "app", agentA, "cat-3")
		})

		It("paginates by run_id and returns complete resource sets", func() {
			page1, err := requestStore.ListRun(ctx, &store.ResourceListOptions{PageSize: 2})
			Expect(err).NotTo(HaveOccurred())
			Expect(page1.NextPageToken).NotTo(BeNil())
			// run-1 (2 resources) + run-2 (1)
			Expect(page1.Resources).To(HaveLen(3))
			Expect(page1.Resources[0].RunID).To(Equal("run-1"))
			Expect(page1.Resources[1].RunID).To(Equal("run-1"))
			Expect(page1.Resources[2].RunID).To(Equal("run-2"))

			page2, err := requestStore.ListRun(ctx, &store.ResourceListOptions{
				PageSize:  2,
				PageToken: page1.NextPageToken,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(page2.NextPageToken).To(BeNil())
			Expect(page2.Resources).To(HaveLen(2))
			Expect(page2.Resources[0].RunID).To(Equal("run-3"))
		})

		It("does not split a multi-resource run across pages", func() {
			// PageSize=1 must return both resources of run-1 together
			page1, err := requestStore.ListRun(ctx, &store.ResourceListOptions{PageSize: 1})
			Expect(err).NotTo(HaveOccurred())
			Expect(page1.Resources).To(HaveLen(2))
			Expect(page1.Resources[0].RunID).To(Equal("run-1"))
			Expect(page1.Resources[1].RunID).To(Equal("run-1"))
			Expect(page1.NextPageToken).NotTo(BeNil())
		})

		It("filters by agent name", func() {
			page, err := requestStore.ListRun(ctx, &store.ResourceListOptions{
				AgentName: &agentB,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(page.Resources).To(HaveLen(1))
			Expect(page.Resources[0].RunID).To(Equal("run-2"))
			Expect(*page.Resources[0].AgentName).To(Equal(agentB))
			Expect(page.NextPageToken).To(BeNil())
		})

		It("excludes resources with no agent name when filtering by agent name", func() {
			_, err := requestStore.Create(ctx, model.Resource{
				ID:                    uuid.New().String(),
				RunID:                 "run-unassigned",
				Name:                  "db",
				CatalogItemInstanceId: "cat-unassigned",
				Spec:                  map[string]any{},
				ApprovalStatus:        &approval,
				Path:                  "resources/db",
			})
			Expect(err).NotTo(HaveOccurred())

			page, err := requestStore.ListRun(ctx, &store.ResourceListOptions{
				AgentName: &agentB,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(page.Resources).To(HaveLen(1))
			Expect(page.Resources[0].RunID).To(Equal("run-2"))
		})

		It("treats a blank agent name as no filter", func() {
			blank := "   "
			page, err := requestStore.ListRun(ctx, &store.ResourceListOptions{
				AgentName: &blank,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(page.Resources).To(HaveLen(5))
		})

		It("excludes other agents' resources within a mixed-agent run", func() {
			createResource("run-mixed", "db", agentA, "cat-mixed")
			createResource("run-mixed", "app", agentB, "cat-mixed")

			page, err := requestStore.ListRun(ctx, &store.ResourceListOptions{
				AgentName: &agentB,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(page.Resources).To(HaveLen(2))
			for _, r := range page.Resources {
				Expect(*r.AgentName).To(Equal(agentB))
			}
			Expect(page.Resources[0].RunID).To(Equal("run-2"))
			Expect(page.Resources[1].RunID).To(Equal("run-mixed"))
			Expect(page.Resources[1].Name).To(Equal("app"))
		})

		It("defaults PageSize when zero or negative", func() {
			// PageSize=0 should fallback to default (100) and not error
			pageZero, err := requestStore.ListRun(ctx, &store.ResourceListOptions{
				PageSize: 0,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(pageZero.Resources).To(HaveLen(5))
			Expect(pageZero.NextPageToken).To(BeNil())

			// Negative PageSize should also fallback to default (100)
			pageNegative, err := requestStore.ListRun(ctx, &store.ResourceListOptions{
				PageSize: -1,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(pageNegative.Resources).To(HaveLen(5))
			Expect(pageNegative.NextPageToken).To(BeNil())
		})

		It("treats malformed PageToken as starting from offset 0", func() {
			// Malformed, non-base64 token should be treated as offset 0
			pageToken := "!!not-base64!!"
			page, err := requestStore.ListRun(ctx, &store.ResourceListOptions{
				PageToken: &pageToken,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(page.Resources).To(HaveLen(5))

			// Well-formed base64 that does not decode to an integer should also be treated as offset 0
			malformedToken := base64.StdEncoding.EncodeToString([]byte("not-an-int"))
			page, err = requestStore.ListRun(ctx, &store.ResourceListOptions{
				PageToken: &malformedToken,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(page.Resources).To(HaveLen(5))
		})
	})

	Describe("Delete", func() {
		It("deletes the resource", func() {
			agent := "test-agent"
			approval := "APPROVED"
			r := model.Resource{
				ID:                    uuid.New().String(),
				RunID:                 "run-1",
				Name:                  "main",
				CatalogItemInstanceId: "cat-del",
				Spec:                  map[string]any{},
				AgentName:             &agent,
				ApprovalStatus:        &approval,
				Path:                  "resources/del",
			}
			_, _ = requestStore.Create(ctx, r)

			err := requestStore.Delete(ctx, r.ID)

			Expect(err).NotTo(HaveOccurred())

			_, err = requestStore.Get(ctx, r.ID)
			Expect(err).To(Equal(store.ErrResourceNotFound))
		})

		It("returns ErrResourceNotFound for missing ID", func() {
			err := requestStore.Delete(ctx, uuid.New().String())

			Expect(err).To(Equal(store.ErrResourceNotFound))
		})
	})

	Describe("UpdateStatusByRunID", func() {
		It("updates status for all resources in the run", func() {
			agent := "test-agent"
			approval := "APPROVED"
			id1 := uuid.New().String()
			id2 := uuid.New().String()
			_, err := requestStore.CreateBatch(ctx, []model.Resource{
				{
					ID:                    id1,
					RunID:                 "run-status-1",
					Name:                  "db",
					CatalogItemInstanceId: "cat-1",
					Spec:                  map[string]any{},
					AgentName:             &agent,
					ApprovalStatus:        &approval,
					Path:                  "resources/" + id1,
					Status:                "PENDING",
					DagLevel:              0,
				},
				{
					ID:                    id2,
					RunID:                 "run-status-1",
					Name:                  "app",
					CatalogItemInstanceId: "cat-1",
					Spec:                  map[string]any{},
					AgentName:             &agent,
					ApprovalStatus:        &approval,
					Path:                  "resources/" + id2,
					Status:                "PENDING",
					DagLevel:              1,
				},
			})
			Expect(err).NotTo(HaveOccurred())

			err = requestStore.UpdateStatusByRunID(ctx, "run-status-1", "PENDING_DELETION")
			Expect(err).NotTo(HaveOccurred())

			listed, err := requestStore.ListByRunID(ctx, "run-status-1")
			Expect(err).NotTo(HaveOccurred())
			Expect(listed).To(HaveLen(2))
			Expect(listed[0].Status).To(Equal("PENDING_DELETION"))
			Expect(listed[1].Status).To(Equal("PENDING_DELETION"))
		})

		It("returns ErrResourceNotFound when run has no resources", func() {
			err := requestStore.UpdateStatusByRunID(ctx, "missing-run", "PENDING_DELETION")
			Expect(err).To(Equal(store.ErrResourceNotFound))
		})
	})

	Describe("UpdateStatusFrom", func() {
		It("transitions status only when current status matches", func() {
			agent := "test-agent"
			approval := "APPROVED"
			id := uuid.New().String()
			_, err := requestStore.Create(ctx, model.Resource{
				ID:                    id,
				RunID:                 "run-cas-1",
				Name:                  "app",
				CatalogItemInstanceId: "cat-1",
				Spec:                  map[string]any{},
				AgentName:             &agent,
				ApprovalStatus:        &approval,
				Path:                  "resources/" + id,
				Status:                "PENDING",
			})
			Expect(err).NotTo(HaveOccurred())

			applied, err := requestStore.UpdateStatusFrom(ctx, id, []string{"PENDING"}, "PROVISIONING")
			Expect(err).NotTo(HaveOccurred())
			Expect(applied).To(BeTrue())

			updated, err := requestStore.Get(ctx, id)
			Expect(err).NotTo(HaveOccurred())
			Expect(updated.Status).To(Equal("PROVISIONING"))

			applied, err = requestStore.UpdateStatusFrom(ctx, id, []string{"PENDING"}, "RUNNING")
			Expect(err).NotTo(HaveOccurred())
			Expect(applied).To(BeFalse())
		})

		It("returns false without error when fromStatuses is empty", func() {
			applied, err := requestStore.UpdateStatusFrom(ctx, uuid.New().String(), nil, "RUNNING")
			Expect(err).NotTo(HaveOccurred())
			Expect(applied).To(BeFalse())
		})
	})

	Describe("PrepareRunDeletion", func() {
		It("records cleanup intent and preserves deleted or in-flight resource states", func() {
			agent := "test-agent"
			approval := "APPROVED"
			rows := []model.Resource{
				{ID: uuid.New().String(), RunID: "run-cleanup-intent", Name: "pending", CatalogItemInstanceId: "cat", Spec: map[string]any{}, AgentName: &agent, ApprovalStatus: &approval, Path: "resources/pending", Status: "PENDING", DagLevel: 0},
				{ID: uuid.New().String(), RunID: "run-cleanup-intent", Name: "deleting", CatalogItemInstanceId: "cat", Spec: map[string]any{}, AgentName: &agent, ApprovalStatus: &approval, Path: "resources/deleting", Status: "DELETING", DagLevel: 1},
				{ID: uuid.New().String(), RunID: "run-cleanup-intent", Name: "deleted", CatalogItemInstanceId: "cat", Spec: map[string]any{}, AgentName: &agent, ApprovalStatus: &approval, Path: "resources/deleted", Status: "DELETED", DagLevel: 2},
			}
			_, err := requestStore.CreateBatch(ctx, rows)
			Expect(err).NotTo(HaveOccurred())

			Expect(requestStore.PrepareRunDeletion(ctx, "run-cleanup-intent", store.CleanupIntentRollback)).To(Succeed())
			listed, err := requestStore.ListByRunID(ctx, "run-cleanup-intent")
			Expect(err).NotTo(HaveOccurred())
			Expect(listed[0].Status).To(Equal("PENDING_DELETION"))
			Expect(listed[0].CleanupIntent).To(Equal(string(store.CleanupIntentRollback)))
			Expect(listed[1].Status).To(Equal("DELETING"))
			Expect(listed[1].CleanupIntent).To(Equal(string(store.CleanupIntentRollback)))
			Expect(listed[2].Status).To(Equal("DELETED"))
			Expect(listed[2].CleanupIntent).To(Equal(string(store.CleanupIntentRollback)))

			Expect(requestStore.PrepareRunDeletion(ctx, "run-cleanup-intent", store.CleanupIntentExplicit)).To(Succeed())
			Expect(requestStore.PrepareRunDeletion(ctx, "run-cleanup-intent", store.CleanupIntentRollback)).To(Succeed())
			listed, err = requestStore.ListByRunID(ctx, "run-cleanup-intent")
			Expect(err).NotTo(HaveOccurred())
			for _, resource := range listed {
				Expect(resource.CleanupIntent).To(Equal(string(store.CleanupIntentExplicit)))
			}
		})

		It("returns ErrResourceNotFound for an empty run", func() {
			Expect(requestStore.PrepareRunDeletion(ctx, "missing-run", store.CleanupIntentExplicit)).To(Equal(store.ErrResourceNotFound))
		})
	})

	Describe("UpdateAgentError", func() {
		It("stores only non-older details for the assigned agent and accepts a sequence retry", func() {
			agent := "test-agent"
			approval := "APPROVED"
			id := uuid.New().String()
			_, err := requestStore.Create(ctx, model.Resource{
				ID:                    id,
				RunID:                 "run-agent-error",
				Name:                  "main",
				CatalogItemInstanceId: "cat-agent-error",
				Spec:                  map[string]any{},
				AgentName:             &agent,
				ApprovalStatus:        &approval,
				Path:                  "resources/" + id,
				Status:                "PENDING",
			})
			Expect(err).NotTo(HaveOccurred())

			statusCode := 503
			latest := placementtypes.AgentErrorDetails{
				Error:   "latest failure",
				Message: "latest summary",
				ProviderError: &placementtypes.AgentProviderError{
					StatusCode: &statusCode,
					Message:    "latest provider body",
				},
			}
			accepted, err := requestStore.UpdateAgentError(ctx, id, agent, 2, latest)
			Expect(err).NotTo(HaveOccurred())
			Expect(accepted).To(BeTrue())

			accepted, err = requestStore.UpdateAgentError(ctx, id, agent, 1, placementtypes.AgentErrorDetails{Error: "old failure"})
			Expect(err).NotTo(HaveOccurred())
			Expect(accepted).To(BeFalse())

			accepted, err = requestStore.UpdateAgentError(ctx, id, agent, 2, latest)
			Expect(err).NotTo(HaveOccurred())
			Expect(accepted).To(BeTrue())

			accepted, err = requestStore.UpdateAgentError(ctx, id, "superseded-agent", 3, placementtypes.AgentErrorDetails{Error: "stale agent"})
			Expect(err).NotTo(HaveOccurred())
			Expect(accepted).To(BeFalse())

			stored, err := requestStore.Get(ctx, id)
			Expect(err).NotTo(HaveOccurred())
			Expect(stored.AgentErrorStreamSequence).To(Equal(uint64(2)))
			Expect(stored.AgentErrorDetails).NotTo(BeNil())
			Expect(*stored.AgentErrorDetails).To(Equal(latest))
		})
	})

	Describe("UpdatePlacementDecision", func() {
		It("updates agent and approval for a resource", func() {
			agent := "old-agent"
			approval := "PENDING"
			id := uuid.New().String()
			_, err := requestStore.Create(ctx, model.Resource{
				ID:                    id,
				RunID:                 "run-placement-1",
				Name:                  "app",
				CatalogItemInstanceId: "cat-1",
				Spec:                  map[string]any{},
				AgentName:             &agent,
				ApprovalStatus:        &approval,
				Path:                  "resources/" + id,
				Status:                "PENDING",
				DagLevel:              1,
			})
			Expect(err).NotTo(HaveOccurred())

			err = requestStore.UpdatePlacementDecision(ctx, id, "new-agent", "APPROVED")
			Expect(err).NotTo(HaveOccurred())

			updated, err := requestStore.Get(ctx, id)
			Expect(err).NotTo(HaveOccurred())
			Expect(*updated.AgentName).To(Equal("new-agent"))
			Expect(*updated.ApprovalStatus).To(Equal("APPROVED"))
		})

		It("returns ErrResourceNotFound for missing ID", func() {
			err := requestStore.UpdatePlacementDecision(ctx, uuid.New().String(), "provider", "APPROVED")
			Expect(err).To(Equal(store.ErrResourceNotFound))
		})
	})
})
