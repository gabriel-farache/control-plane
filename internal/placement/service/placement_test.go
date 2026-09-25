package service_test

import (
	"context"
	"errors"
	"net/http"
	"sync"

	agentmodel "github.com/dcm-project/control-plane/internal/agent/store/model"
	placementagent "github.com/dcm-project/control-plane/internal/placement/agent"
	"github.com/dcm-project/control-plane/internal/placement/policy"
	"github.com/dcm-project/control-plane/internal/placement/service"
	"github.com/dcm-project/control-plane/internal/placement/sprm"
	"github.com/dcm-project/control-plane/internal/placement/store"
	"github.com/dcm-project/control-plane/internal/placement/store/model"
	"github.com/dcm-project/control-plane/internal/placement/types"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// mockPolicyClient is a mock implementation of policy.Client for testing
type mockPolicyClient struct {
	EvaluateFunc func(ctx context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error)
}

// Evaluate calls the mock function if set, otherwise returns a default approved response
func (m *mockPolicyClient) Evaluate(ctx context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
	if m.EvaluateFunc != nil {
		return m.EvaluateFunc(ctx, req)
	}
	// Default: approve with the original spec
	return &policy.EvaluateResponse{
		Status:        "APPROVED",
		SelectedAgent: "default-agent",
		EvaluatedSpec: req.Spec,
	}, nil
}

// mockSPRMClient is a mock implementation of sprm.Client for testing
type mockSPRMClient struct {
	CreateResourceFunc         func(ctx context.Context, req sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error)
	GetOutputSpecFunc          func(ctx context.Context, resourceID string) (*sprm.GetOutputSpecResponse, error)
	DeleteResourceFunc         func(ctx context.Context, resourceId string) error
	DeleteResourceDeferredFunc func(ctx context.Context, resourceId string) error
	ReassignResourceFunc       func(ctx context.Context, resourceId string, agentName string, expectedCurrentAgent string) error
}

// CreateResource calls the mock function if set, otherwise returns a default success response
func (m *mockSPRMClient) CreateResource(ctx context.Context, req sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
	if m.CreateResourceFunc != nil {
		return m.CreateResourceFunc(ctx, req)
	}
	// Default: successful creation
	return &sprm.CreateResourceResponse{
		ID:     req.ID,
		Status: "provisioning",
	}, nil
}

func (m *mockSPRMClient) GetOutputSpec(ctx context.Context, resourceID string) (*sprm.GetOutputSpecResponse, error) {
	if m.GetOutputSpecFunc != nil {
		return m.GetOutputSpecFunc(ctx, resourceID)
	}
	return &sprm.GetOutputSpecResponse{OutputSpec: map[string]any{}}, nil
}

// DeleteResource calls the mock function if set, otherwise returns success
func (m *mockSPRMClient) DeleteResource(ctx context.Context, resourceId string) error {
	if m.DeleteResourceFunc != nil {
		return m.DeleteResourceFunc(ctx, resourceId)
	}
	return nil
}

// DeleteResourceDeferred calls the mock function if set, otherwise returns success
func (m *mockSPRMClient) DeleteResourceDeferred(ctx context.Context, resourceId string) error {
	if m.DeleteResourceDeferredFunc != nil {
		return m.DeleteResourceDeferredFunc(ctx, resourceId)
	}
	return nil
}

// ReassignResource calls the mock function if set, otherwise returns success
func (m *mockSPRMClient) ReassignResource(ctx context.Context, resourceId string, agentName string, expectedCurrentAgent string) error {
	if m.ReassignResourceFunc != nil {
		return m.ReassignResourceFunc(ctx, resourceId, agentName, expectedCurrentAgent)
	}
	return nil
}

type blockingResourceStore struct {
	store.Resource
	runID    string
	snapshot chan struct{}
	release  chan struct{}
	mu       sync.Mutex
	blocked  bool
}

func (s *blockingResourceStore) ListByRunID(ctx context.Context, runID string) (model.ResourceList, error) {
	resources, err := s.Resource.ListByRunID(ctx, runID)
	if err != nil || runID != s.runID {
		return resources, err
	}
	pendingDeletion := false
	for _, resource := range resources {
		if resource.Status == types.ResourceStatusPendingDeletion {
			pendingDeletion = true
			break
		}
	}
	if !pendingDeletion {
		return resources, nil
	}

	s.mu.Lock()
	shouldBlock := !s.blocked
	s.blocked = true
	s.mu.Unlock()
	if shouldBlock {
		close(s.snapshot)
		<-s.release
	}
	return resources, nil
}

type resourceOverrideStore struct {
	store.Store
	resource store.Resource
}

func (s *resourceOverrideStore) Resource() store.Resource {
	return s.resource
}

type deletionRaceResourceStore struct {
	store.Resource
	resourceID string
	runID      string
	traced     bool
	raceErr    error
}

func (s *deletionRaceResourceStore) Get(ctx context.Context, id string) (*model.Resource, error) {
	resource, err := s.Resource.Get(ctx, id)
	if err != nil || id != s.resourceID || s.traced {
		return resource, err
	}
	s.traced = true
	if err := s.Resource.PrepareRunDeletion(ctx, s.runID, store.CleanupIntentExplicit); err != nil {
		s.raceErr = err
		return resource, err
	}
	s.raceErr = s.Resource.UpdateStatus(ctx, id, types.ResourceStatusDeleting)
	return resource, nil
}

type mockAgentClient struct {
	agents []placementagent.Info
	err    error
}

func (m *mockAgentClient) ListReadyAgents(_ context.Context) ([]placementagent.Info, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.agents, nil
}

func runningEvent(resourceID string) types.ResourceStatusEvent {
	return types.ResourceStatusEvent{
		ResourceID: resourceID,
		Status:     types.ResourceStatusRunning,
	}
}

func getStoredResource(ctx context.Context, dataStore store.Store, id string) *model.Resource {
	r, err := dataStore.Resource().Get(ctx, id)
	Expect(err).NotTo(HaveOccurred())
	return r
}

func expectStoredResourceMissing(ctx context.Context, dataStore store.Store, id string) {
	_, err := dataStore.Resource().Get(ctx, id)
	Expect(err).To(HaveOccurred())
	Expect(errors.Is(err, store.ErrResourceNotFound)).To(BeTrue())
}

func singleResourceRun(catalogID string, spec map[string]any, resourceID *string) *types.CreateRunRequest {
	input := types.ResourceInput{
		Name: "main",
		Spec: spec,
		ID:   resourceID,
	}
	return &types.CreateRunRequest{
		CatalogItemInstanceId: catalogID,
		RunId:                 uuid.New().String(),
		Resources:             []types.ResourceInput{input},
	}
}

var _ = Describe("PlacementService", func() {
	var (
		db           *gorm.DB
		dataStore    store.Store
		mockPolicy   *mockPolicyClient
		mockSPRM     *mockSPRMClient
		agentClient  *mockAgentClient
		placementSvc *service.PlacementService
		ctx          context.Context
	)

	BeforeEach(func() {
		var err error
		db, err = gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(db.AutoMigrate(&agentmodel.Agent{}, &model.Resource{})).To(Succeed())

		for _, name := range []string{"default-agent", "test-agent", "modified-agent", "async-agent", "new-agent", "fallback-agent"} {
			Expect(db.Create(&agentmodel.Agent{ID: uuid.New().String(), Name: name, TopicName: "dcm.agent." + name}).Error).NotTo(HaveOccurred())
		}

		dataStore = store.NewStore(db)
		mockPolicy = &mockPolicyClient{}
		mockSPRM = &mockSPRMClient{}
		agentClient = &mockAgentClient{agents: []placementagent.Info{
			{Name: "agent-a", Environment: "prod", ServiceTypes: []string{"vm"}, Cost: "low"},
			{Name: "agent-b", Environment: "prod", ServiceTypes: []string{"vm"}, Cost: "medium"},
		}}
		placementSvc = service.NewPlacementService(dataStore, mockPolicy, mockSPRM, service.WithAgentClient(agentClient))
		ctx = context.Background()
	})

	AfterEach(func() {
		sqlDB, _ := db.DB()
		_ = sqlDB.Close()
	})

	Describe("CreateRun", func() {
		It("creates resource with APPROVED status and agent routing", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{
					Status:        "APPROVED",
					SelectedAgent: "test-agent",
					EvaluatedSpec: req.Spec,
				}, nil
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-123",
				Spec:                  map[string]any{"cpu": 2, "memory": "4GB"},
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.RunId).NotTo(BeEmpty())
			Expect(result.Resources).To(HaveLen(1))
			Expect(result.Resources[0].Id).NotTo(BeNil())
			Expect(result.CatalogItemInstanceId).To(Equal("catalog-123"))
			Expect(result.Resources[0].Spec).To(HaveKey("cpu"))
			Expect(result.Resources[0].Spec).To(HaveKey("memory"))
			Expect(result.Resources[0].ApprovalStatus).NotTo(BeNil())
			Expect(*result.Resources[0].ApprovalStatus).To(Equal("APPROVED"))
			Expect(result.Resources[0].AgentName).NotTo(BeNil())
			Expect(*result.Resources[0].AgentName).To(Equal("test-agent"))

			stored := getStoredResource(ctx, dataStore, *result.Resources[0].Id)
			Expect(stored.ApprovalStatus).NotTo(BeNil())
			Expect(*stored.ApprovalStatus).To(Equal("APPROVED"))
			Expect(stored.AgentName).NotTo(BeNil())
			Expect(*stored.AgentName).To(Equal("test-agent"))
		})

		It("rejects empty run_id", func() {
			req := singleResourceRun("catalog-123", map[string]any{"cpu": 1}, nil)
			req.RunId = ""
			_, err := placementSvc.CreateRun(ctx, req)
			Expect(err).To(HaveOccurred())
			svcErr, ok := err.(*service.ServiceError)
			Expect(ok).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeValidation))
		})

		It("rejects duplicate run_id", func() {
			runID := uuid.New().String()
			req1 := singleResourceRun("catalog-123", map[string]any{"cpu": 1}, nil)
			req1.RunId = runID
			_, err := placementSvc.CreateRun(ctx, req1)
			Expect(err).NotTo(HaveOccurred())

			req2 := singleResourceRun("catalog-456", map[string]any{"cpu": 2}, nil)
			req2.RunId = runID
			_, err = placementSvc.CreateRun(ctx, req2)
			Expect(err).To(HaveOccurred())
			svcErr, ok := err.(*service.ServiceError)
			Expect(ok).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeConflict))
		})

		It("creates resource with MODIFIED status and agent routing", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				modifiedSpec := make(map[string]any)
				for k, v := range req.Spec {
					modifiedSpec[k] = v
				}
				modifiedSpec["modified_field"] = "policy_value"
				return &policy.EvaluateResponse{
					Status:        "MODIFIED",
					SelectedAgent: "modified-agent",
					EvaluatedSpec: modifiedSpec,
				}, nil
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-456",
				Spec:                  map[string]any{"cpu": 4},
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.Resources[0].Spec).To(HaveKey("cpu"))
			Expect(result.Resources[0].Spec).NotTo(HaveKey("modified_field")) // Original spec preserved
			Expect(result.Resources[0].ApprovalStatus).NotTo(BeNil())
			Expect(*result.Resources[0].ApprovalStatus).To(Equal("MODIFIED"))
			Expect(result.Resources[0].AgentName).NotTo(BeNil())
			Expect(*result.Resources[0].AgentName).To(Equal("modified-agent"))

			stored := getStoredResource(ctx, dataStore, *result.Resources[0].Id)
			Expect(stored.ApprovalStatus).NotTo(BeNil())
			Expect(*stored.ApprovalStatus).To(Equal("MODIFIED"))
			Expect(stored.AgentName).NotTo(BeNil())
			Expect(*stored.AgentName).To(Equal("modified-agent"))
		})

		It("creates resource with specified ID", func() {
			specifiedID := "custom-resource-id"
			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-789",
				Spec:                  map[string]any{"cpu": 1},
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, &specifiedID))

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(*result.Resources[0].Id).To(Equal(specifiedID))
		})

		It("passes available agents to policy", func() {
			var capturedReq policy.EvaluateRequest
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				capturedReq = req
				return &policy.EvaluateResponse{
					Status:        "APPROVED",
					SelectedAgent: "test-agent",
					EvaluatedSpec: req.Spec,
				}, nil
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "cat-avail-agents",
				Spec:                  map[string]any{"service_type": "vm"},
			}
			_, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))
			Expect(err).NotTo(HaveOccurred())

			Expect(capturedReq.AvailableAgents).NotTo(BeEmpty())
			names := make([]string, 0, len(capturedReq.AvailableAgents))
			for _, a := range capturedReq.AvailableAgents {
				names = append(names, a.Name)
			}
			Expect(names).To(ContainElement("agent-a"))
			Expect(names).To(ContainElement("agent-b"))
		})

		It("fails closed and does not evaluate policy when listing available agents errors", func() {
			agentClient.err = errors.New("agent registry unavailable")
			policyCalled := false
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				policyCalled = true
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "test-agent", EvaluatedSpec: req.Spec}, nil
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun("catalog-agents-error", map[string]any{"cpu": 1}, nil))

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			Expect(policyCalled).To(BeFalse())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeInternal))
		})

		It("returns error when policy validation fails (400)", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, _ policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return nil, &policy.HTTPError{StatusCode: 400, Body: "bad request"}
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-invalid",
				Spec:                  map[string]any{"invalid": "spec"},
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodeValidation))
		})

		It("returns error when policy rejects request (406)", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, _ policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return nil, &policy.HTTPError{StatusCode: 406, Body: "rejected"}
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-rejected",
				Spec:                  map[string]any{"cpu": 100},
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodePolicyRejected))
		})

		It("returns error when policy conflict occurs (409)", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, _ policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return nil, &policy.HTTPError{StatusCode: 409, Body: "conflict"}
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-conflict",
				Spec:                  map[string]any{"cpu": 2},
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodePolicyConflict))
		})

		It("returns error when policy engine fails (500)", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, _ policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return nil, &policy.HTTPError{StatusCode: 500, Body: "internal error"}
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-error",
				Spec:                  map[string]any{"cpu": 2},
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodePolicyInternalError))
		})

		It("returns error when policy returns unmapped HTTP status code", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, _ policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return nil, &policy.HTTPError{StatusCode: 418, Body: "I'm a teapot"}
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-teapot",
				Spec:                  map[string]any{"cpu": 2},
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodePolicyError))
			Expect(svcErr.Message).To(ContainSubstring("policy evaluation failed with status 418"))
			Expect(svcErr.Message).To(ContainSubstring("I'm a teapot"))
		})

		It("returns error when policy response is missing selected agent", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{
					Status:        "APPROVED",
					SelectedAgent: "",
					EvaluatedSpec: req.Spec,
				}, nil
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-no-agent",
				Spec:                  map[string]any{"cpu": 2},
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodePolicyInternalError))
			Expect(svcErr.Message).To(ContainSubstring("missing selected agent"))
		})

		It("returns error when policy client communication fails", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, _ policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return nil, errors.New("connection refused")
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-network-error",
				Spec:                  map[string]any{"cpu": 2},
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodePolicyInternalError))
			Expect(svcErr.Message).To(ContainSubstring("policy client communication error"))
			Expect(svcErr.Message).To(ContainSubstring("connection refused"))
		})

		It("returns conflict error when duplicate ID is used", func() {
			resourceID := "duplicate-id"
			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-dup",
				Spec:                  map[string]any{"cpu": 2},
			}

			// Create first resource
			result1, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, &resourceID))
			Expect(err).NotTo(HaveOccurred())
			Expect(result1).NotTo(BeNil())

			// Try to create second resource with same ID
			result2, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, &resourceID))
			Expect(err).To(HaveOccurred())
			Expect(result2).To(BeNil())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodeConflict))
			Expect(svcErr.Message).To(ContainSubstring("already exists"))
		})

		It("returns error and rolls back DB when SPRM creation fails (400)", func() {
			mockSPRM.CreateResourceFunc = func(_ context.Context, _ sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				return nil, &sprm.HTTPError{StatusCode: 400, Body: "invalid request"}
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-sprm-400",
				Spec:                  map[string]any{"cpu": 2},
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodeValidation))

			// Verify resource was NOT persisted in DB (rollback worked)
			resources, err := dataStore.Resource().ListRun(ctx, &store.ResourceListOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(resources.Resources).To(BeEmpty())
		})

		It("returns error and rolls back DB when SPRM create fails with canceled request context", func() {
			reqCtx, cancelReq := context.WithCancel(ctx)
			mockSPRM.CreateResourceFunc = func(context.Context, sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				cancelReq()
				return nil, context.Canceled
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-sprm-cancel",
				Spec:                  map[string]any{"cpu": 2},
			}

			result, err := placementSvc.CreateRun(reqCtx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())

			resources, err := dataStore.Resource().ListRun(ctx, &store.ResourceListOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(resources.Resources).To(BeEmpty())
		})

		It("returns error and rolls back DB when SPRM creation fails (500)", func() {
			mockSPRM.CreateResourceFunc = func(_ context.Context, _ sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				return nil, &sprm.HTTPError{StatusCode: 500, Body: "internal error"}
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-sprm-500",
				Spec:                  map[string]any{"cpu": 2},
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodeSPRMError))

			// Verify resource was NOT persisted in DB (rollback worked)
			resources, err := dataStore.Resource().ListRun(ctx, &store.ResourceListOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(resources.Resources).To(BeEmpty())
		})

		It("returns provisioning error when SPRM creation fails (422)", func() {
			mockSPRM.CreateResourceFunc = func(_ context.Context, _ sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				return nil, &sprm.HTTPError{StatusCode: 422, Body: "agent validation failed"}
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-sprm-422",
				Spec:                  map[string]any{"cpu": 2},
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodeProvisioningError))

			// Verify resource was NOT persisted in DB (rollback worked)
			resources, err := dataStore.Resource().ListRun(ctx, &store.ResourceListOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(resources.Resources).To(BeEmpty())
		})

		It("does not rollback on 202 from SPRM", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{
					Status:        "APPROVED",
					SelectedAgent: "async-agent",
					EvaluatedSpec: req.Spec,
				}, nil
			}
			mockSPRM.CreateResourceFunc = func(_ context.Context, req sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				return &sprm.CreateResourceResponse{
					ID:     req.ID,
					Status: "accepted",
				}, nil
			}

			resource := &types.Resource{
				CatalogItemInstanceId: "cat-202",
				Spec:                  map[string]any{"service_type": "vm"},
			}

			result, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())

			stored := getStoredResource(ctx, dataStore, *result.Resources[0].Id)
			Expect(stored).NotTo(BeNil())
		})
	})

	Describe("DeleteRun", func() {
		It("starts deletion for existing resource by setting DELETING status", func() {
			// Create a resource first
			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-delete",
				Spec:                  map[string]any{"cpu": 2},
			}
			created, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))
			Expect(err).NotTo(HaveOccurred())

			// Delete the resource
			err = placementSvc.DeleteRun(ctx, created.RunId)
			Expect(err).NotTo(HaveOccurred())

			// Verify initial delete at highest dag level has started
			stored := getStoredResource(ctx, dataStore, *created.Resources[0].Id)
			Expect(stored.Status).To(Equal(types.ResourceStatusDeleting))
		})

		It("marks all resources PENDING_DELETION and starts delete from highest dag level", func() {
			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-delete-multi",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(created.Resources).To(HaveLen(2))

			var dbID, appID string
			nameByID := make(map[string]string, 2)
			for _, r := range created.Resources {
				nameByID[*r.Id] = r.Name
				switch r.Name {
				case "db":
					dbID = *r.Id
					Expect(r.DagLevel).To(Equal(0))
				case "app":
					appID = *r.Id
					Expect(r.DagLevel).To(Equal(1))
				}
			}

			deletedOrder := make([]string, 0, 2)
			mockSPRM.DeleteResourceFunc = func(_ context.Context, resourceID string) error {
				deletedOrder = append(deletedOrder, nameByID[resourceID])
				return nil
			}

			err = placementSvc.DeleteRun(ctx, created.RunId)
			Expect(err).NotTo(HaveOccurred())

			app, err := dataStore.Resource().Get(ctx, appID)
			Expect(err).NotTo(HaveOccurred())
			Expect(app.Status).To(Equal(types.ResourceStatusDeleting))

			db, err := dataStore.Resource().Get(ctx, dbID)
			Expect(err).NotTo(HaveOccurred())
			Expect(db.Status).To(Equal(types.ResourceStatusPendingDeletion))
			Expect(deletedOrder).To(Equal([]string{"app"}))
		})

		It("returns not found error for non-existent resource", func() {
			err := placementSvc.DeleteRun(ctx, "non-existent-id")

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodeNotFound))
		})

		It("treats SPRM deletion not found (404) as idempotent success", func() {
			// Create a resource first
			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-sprm-404",
				Spec:                  map[string]any{"cpu": 2},
			}
			created, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))
			Expect(err).NotTo(HaveOccurred())

			// Mock SPRM delete to fail with 404
			mockSPRM.DeleteResourceFunc = func(_ context.Context, _ string) error {
				return &sprm.HTTPError{StatusCode: 404, Body: "not found in SPRM"}
			}

			// Try to delete the resource
			err = placementSvc.DeleteRun(ctx, created.RunId)

			Expect(err).NotTo(HaveOccurred())
			_, err = dataStore.Resource().Get(ctx, *created.Resources[0].Id)
			Expect(errors.Is(err, store.ErrResourceNotFound)).To(BeTrue())
		})

		It("returns error when SPRM deletion fails (500)", func() {
			// Create a resource first
			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-sprm-500",
				Spec:                  map[string]any{"cpu": 2},
			}
			created, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))
			Expect(err).NotTo(HaveOccurred())

			// Mock SPRM delete to fail with 500
			mockSPRM.DeleteResourceFunc = func(_ context.Context, _ string) error {
				return &sprm.HTTPError{StatusCode: 500, Body: "internal error"}
			}

			// Try to delete the resource
			err = placementSvc.DeleteRun(ctx, created.RunId)

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodeSPRMError))

			// Verify resource still exists in DB (SPRM delete failed, so DB delete didn't happen)
			_ = getStoredResource(ctx, dataStore, *created.Resources[0].Id)
		})
	})

	Describe("Status-driven orchestration", func() {
		It("progresses next DAG level on OnResourceRunning", func() {
			createdKinds := make([]string, 0, 2)
			mockSPRM.CreateResourceFunc = func(_ context.Context, req sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				kind, _ := req.Spec["kind"].(string)
				createdKinds = append(createdKinds, kind)
				return &sprm.CreateResourceResponse{ID: req.ID, Status: "provisioning"}, nil
			}

			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-progress",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var dbID string
			for _, r := range created.Resources {
				if r.Name == "db" {
					dbID = *r.Id
				}
			}
			Expect(createdKinds).To(Equal([]string{"db"}))

			err = placementSvc.OnResourceRunning(ctx, runningEvent(dbID))
			Expect(err).NotTo(HaveOccurred())
			Expect(createdKinds).To(Equal([]string{"db", "app"}))
		})

		It("re-evaluates policy for the next DAG level on OnResourceRunning", func() {
			evaluateCalls := 0
			sprmSpecs := make(map[string]map[string]any)
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				evaluateCalls++
				evaluatedSpec := map[string]any{}
				for k, v := range req.Spec {
					evaluatedSpec[k] = v
				}
				if kind, _ := req.Spec["kind"].(string); kind == "app" && evaluateCalls > 2 {
					evaluatedSpec["policy_at_progress"] = true
				}
				return &policy.EvaluateResponse{
					Status:        "APPROVED",
					SelectedAgent: "progress-agent",
					EvaluatedSpec: evaluatedSpec,
				}, nil
			}
			mockSPRM.CreateResourceFunc = func(_ context.Context, req sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				specCopy := map[string]any{}
				for k, v := range req.Spec {
					specCopy[k] = v
				}
				sprmSpecs[req.ID] = specCopy
				return &sprm.CreateResourceResponse{ID: req.ID, Status: "provisioning"}, nil
			}

			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-policy-progress",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var dbID, appID string
			for _, r := range created.Resources {
				switch r.Name {
				case "db":
					dbID = *r.Id
				case "app":
					appID = *r.Id
				}
			}
			Expect(evaluateCalls).To(Equal(2))

			err = placementSvc.OnResourceRunning(ctx, runningEvent(dbID))
			Expect(err).NotTo(HaveOccurred())
			Expect(evaluateCalls).To(Equal(3))
			Expect(sprmSpecs[appID]).To(HaveKey("policy_at_progress"))

			storedApp := getStoredResource(ctx, dataStore, appID)
			Expect(storedApp.AgentName).NotTo(BeNil())
			Expect(*storedApp.AgentName).To(Equal("progress-agent"))
		})

		It("binds apply-time CEL from RUNNING dependency outputs before provisioning", func() {
			var appCreateSpec map[string]any
			mockSPRM.GetOutputSpecFunc = func(_ context.Context, _ string) (*sprm.GetOutputSpecResponse, error) {
				return &sprm.GetOutputSpecResponse{
					OutputSpec: map[string]any{"connection_string": "postgres://db:5432/orders"},
				}, nil
			}
			mockSPRM.CreateResourceFunc = func(_ context.Context, req sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				if kind, _ := req.Spec["kind"].(string); kind == "app" {
					specCopy := map[string]any{}
					for k, v := range req.Spec {
						specCopy[k] = v
					}
					appCreateSpec = specCopy
				}
				return &sprm.CreateResourceResponse{ID: req.ID, Status: "provisioning"}, nil
			}

			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-cel-bind",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{
						Name:              "app",
						Spec:              map[string]any{"kind": "app", "database_url": "${db.connection_string}"},
						RequiresResources: []string{"db"},
					},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var dbID string
			for _, r := range created.Resources {
				if r.Name == "db" {
					dbID = *r.Id
				}
			}

			err = placementSvc.OnResourceRunning(ctx, runningEvent(dbID))
			Expect(err).NotTo(HaveOccurred())
			Expect(appCreateSpec).NotTo(BeNil())
			Expect(appCreateSpec["database_url"]).To(Equal("postgres://db:5432/orders"))
		})

		It("returns not found when the RUNNING resource does not exist", func() {
			err := placementSvc.OnResourceRunning(ctx, runningEvent(uuid.New().String()))
			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodeNotFound))
		})

		It("returns nil when no pending resources are ready to provision", func() {
			resource := &types.Resource{
				CatalogItemInstanceId: "catalog-single",
				Spec:                  map[string]any{"kind": "db"},
			}
			created, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))
			Expect(err).NotTo(HaveOccurred())
			dbID := *created.Resources[0].Id

			createCalls := 0
			mockSPRM.CreateResourceFunc = func(_ context.Context, _ sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				createCalls++
				return &sprm.CreateResourceResponse{Status: "provisioning"}, nil
			}

			err = placementSvc.OnResourceRunning(ctx, runningEvent(dbID))
			Expect(err).NotTo(HaveOccurred())
			Expect(createCalls).To(Equal(0))
		})

		It("ignores late RUNNING callbacks during teardown", func() {
			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-running-teardown",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var dbID string
			for _, r := range created.Resources {
				if r.Name == "db" {
					dbID = *r.Id
				}
			}

			err = placementSvc.DeleteRun(ctx, created.RunId)
			Expect(err).NotTo(HaveOccurred())

			createCalls := 0
			mockSPRM.CreateResourceFunc = func(_ context.Context, _ sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				createCalls++
				return &sprm.CreateResourceResponse{Status: "provisioning"}, nil
			}

			err = placementSvc.OnResourceRunning(ctx, runningEvent(dbID))
			Expect(err).NotTo(HaveOccurred())
			Expect(createCalls).To(Equal(0))

			stored := getStoredResource(ctx, dataStore, dbID)
			Expect(stored.Status).To(Equal(types.ResourceStatusPendingDeletion))
		})

		It("returns a validation error when CEL binding fails", func() {
			mockSPRM.GetOutputSpecFunc = func(_ context.Context, _ string) (*sprm.GetOutputSpecResponse, error) {
				return &sprm.GetOutputSpecResponse{OutputSpec: map[string]any{"kind": "db"}}, nil
			}

			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-cel-fail",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{
						Name:              "app",
						Spec:              map[string]any{"database_url": "${db.connection_string}"},
						RequiresResources: []string{"db"},
					},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var dbID string
			for _, r := range created.Resources {
				if r.Name == "db" {
					dbID = *r.Id
				}
			}

			err = placementSvc.OnResourceRunning(ctx, runningEvent(dbID))
			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodeValidation))

			var appID string
			for _, r := range created.Resources {
				if r.Name == "app" {
					appID = *r.Id
				}
			}
			app, err := dataStore.Resource().Get(ctx, appID)
			Expect(err).NotTo(HaveOccurred())
			Expect(app.Status).To(Equal(types.ResourceStatusPending))
		})

		It("returns an SPRM error when GetOutputSpec fails", func() {
			mockSPRM.GetOutputSpecFunc = func(_ context.Context, _ string) (*sprm.GetOutputSpecResponse, error) {
				return nil, &sprm.HTTPError{StatusCode: http.StatusNotFound, Body: "instance not found"}
			}

			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-output-fail",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var dbID string
			for _, r := range created.Resources {
				if r.Name == "db" {
					dbID = *r.Id
				}
			}

			err = placementSvc.OnResourceRunning(ctx, runningEvent(dbID))
			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(err).To(BeAssignableToTypeOf(svcErr))
			svcErr = err.(*service.ServiceError)
			Expect(svcErr.Code).To(Equal(service.ErrCodeNotFound))
		})

		It("progresses reverse DAG deletion on OnResourceDeleted", func() {
			deleteOrder := make([]string, 0, 2)

			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-reverse-delete",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(created.Resources).To(HaveLen(2))

			var dbID, appID string
			nameByID := map[string]string{}
			for _, r := range created.Resources {
				nameByID[*r.Id] = r.Name
				switch r.Name {
				case "db":
					dbID = *r.Id
				case "app":
					appID = *r.Id
				}
			}

			mockSPRM.DeleteResourceFunc = func(_ context.Context, resourceID string) error {
				deleteOrder = append(deleteOrder, nameByID[resourceID])
				return nil
			}

			err = placementSvc.DeleteRun(ctx, created.RunId)
			Expect(err).NotTo(HaveOccurred())
			Expect(deleteOrder).To(Equal([]string{"app"}))

			err = placementSvc.OnResourceDeleted(ctx, appID)
			Expect(err).NotTo(HaveOccurred())
			Expect(deleteOrder).To(Equal([]string{"app", "db"}))

			db, err := dataStore.Resource().Get(ctx, dbID)
			Expect(err).NotTo(HaveOccurred())
			Expect(db.Status).To(Equal(types.ResourceStatusDeleting))

			err = placementSvc.OnResourceDeleted(ctx, dbID)
			Expect(err).NotTo(HaveOccurred())

			_, err = dataStore.Resource().Get(ctx, appID)
			Expect(errors.Is(err, store.ErrResourceNotFound)).To(BeTrue())
			_, err = dataStore.Resource().Get(ctx, dbID)
			Expect(errors.Is(err, store.ErrResourceNotFound)).To(BeTrue())
		})

		It("treats SPRM deletion not found during OnResourceDeleted progression as idempotent success", func() {
			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-reverse-delete-404",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var dbID, appID string
			for _, r := range created.Resources {
				switch r.Name {
				case "db":
					dbID = *r.Id
				case "app":
					appID = *r.Id
				}
			}

			mockSPRM.DeleteResourceFunc = func(_ context.Context, resourceID string) error {
				if resourceID == dbID {
					return &sprm.HTTPError{StatusCode: 404, Body: "not found in SPRM"}
				}
				return nil
			}

			err = placementSvc.DeleteRun(ctx, created.RunId)
			Expect(err).NotTo(HaveOccurred())

			err = placementSvc.OnResourceDeleted(ctx, appID)
			Expect(err).NotTo(HaveOccurred())

			_, err = dataStore.Resource().Get(ctx, appID)
			Expect(errors.Is(err, store.ErrResourceNotFound)).To(BeTrue())
			_, err = dataStore.Resource().Get(ctx, dbID)
			Expect(errors.Is(err, store.ErrResourceNotFound)).To(BeTrue())
		})

		It("does not delete live siblings when a single resource is deleted outside DeleteRun", func() {
			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-stray-delete",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var dbID, appID string
			for _, r := range created.Resources {
				switch r.Name {
				case "db":
					dbID = *r.Id
				case "app":
					appID = *r.Id
				}
			}

			err = placementSvc.OnResourceDeleted(ctx, dbID)
			Expect(err).NotTo(HaveOccurred())

			db, err := dataStore.Resource().Get(ctx, dbID)
			Expect(err).NotTo(HaveOccurred())
			Expect(db.Status).To(Equal(types.ResourceStatusDeleted))

			app, err := dataStore.Resource().Get(ctx, appID)
			Expect(err).NotTo(HaveOccurred())
			Expect(app.Status).To(Equal(types.ResourceStatusPending))
		})

		It("persists only the latest assigned-agent error and starts cleanup once", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "test-agent", EvaluatedSpec: req.Spec}, nil
			}
			created, err := placementSvc.CreateRun(ctx, singleResourceRun("catalog-agent-error", map[string]any{"kind": "vm"}, nil))
			Expect(err).NotTo(HaveOccurred())
			resourceID := *created.Resources[0].Id

			deleteCalls := 0
			mockSPRM.DeleteResourceFunc = func(_ context.Context, id string) error {
				Expect(id).To(Equal(resourceID))
				deleteCalls++
				return nil
			}
			latest := types.AgentErrorDetails{Error: "latest failure", Message: "latest summary"}

			Expect(placementSvc.OnAgentError(ctx, resourceID, "test-agent", 2, latest)).To(Succeed())
			Expect(placementSvc.OnAgentError(ctx, resourceID, "test-agent", 1, types.AgentErrorDetails{Error: "old failure"})).To(Succeed())
			Expect(placementSvc.OnAgentError(ctx, resourceID, "stale-agent", 3, types.AgentErrorDetails{Error: "stale agent"})).To(Succeed())
			Expect(placementSvc.OnAgentError(ctx, resourceID, "test-agent", 2, latest)).To(Succeed())

			stored := getStoredResource(ctx, dataStore, resourceID)
			Expect(stored.AgentErrorStreamSequence).To(Equal(uint64(2)))
			Expect(stored.AgentErrorDetails).NotTo(BeNil())
			Expect(*stored.AgentErrorDetails).To(Equal(latest))
			Expect(stored.AgentName).NotTo(BeNil())
			Expect(*stored.AgentName).To(Equal("test-agent"))
			Expect(deleteCalls).To(Equal(1))
		})

		It("does not overwrite or redispatch an explicit delete racing with an agent error", func() {
			created, err := placementSvc.CreateRun(ctx, singleResourceRun("catalog-agent-error-delete-race", map[string]any{"kind": "vm"}, nil))
			Expect(err).NotTo(HaveOccurred())
			resourceID := *created.Resources[0].Id
			Expect(dataStore.Resource().UpdateStatus(ctx, resourceID, types.ResourceStatusRunning)).To(Succeed())

			racingResource := &deletionRaceResourceStore{
				Resource:   dataStore.Resource(),
				resourceID: resourceID,
				runID:      created.RunId,
			}
			placementSvc = service.NewPlacementService(
				&resourceOverrideStore{Store: dataStore, resource: racingResource},
				mockPolicy,
				mockSPRM,
				service.WithAgentClient(agentClient),
			)
			deleteCalls := 0
			mockSPRM.DeleteResourceFunc = func(_ context.Context, _ string) error {
				deleteCalls++
				return nil
			}

			Expect(placementSvc.OnAgentError(ctx, resourceID, "default-agent", 1, types.AgentErrorDetails{Error: "provider failed"})).To(Succeed())
			Expect(racingResource.raceErr).NotTo(HaveOccurred())

			stored := getStoredResource(ctx, dataStore, resourceID)
			Expect(stored.Status).To(Equal(types.ResourceStatusDeleting))
			Expect(stored.CleanupIntent).To(Equal(string(store.CleanupIntentExplicit)))
			Expect(stored.AgentErrorDetails).NotTo(BeNil())
			Expect(deleteCalls).To(Equal(0))
		})

		It("resumes the failed same-level delete without redispatching a successful sibling", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "test-agent", EvaluatedSpec: req.Spec}, nil
			}
			created, err := placementSvc.CreateRun(ctx, &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-agent-error-sibling-retry",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "alpha", Spec: map[string]any{"kind": "vm"}},
					{Name: "zeta", Spec: map[string]any{"kind": "vm"}},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			ids := make(map[string]string)
			for _, resource := range created.Resources {
				ids[resource.Name] = *resource.Id
			}

			deleteCalls := map[string]int{}
			mockSPRM.DeleteResourceFunc = func(_ context.Context, id string) error {
				deleteCalls[id]++
				if id == ids["zeta"] && deleteCalls[id] == 1 {
					return errors.New("temporary SPRM failure")
				}
				return nil
			}
			details := types.AgentErrorDetails{Error: "agent failure"}

			Expect(placementSvc.OnAgentError(ctx, ids["alpha"], "test-agent", 9, details)).To(HaveOccurred())
			failedSibling := getStoredResource(ctx, dataStore, ids["zeta"])
			Expect(failedSibling.Status).To(Equal(types.ResourceStatusPendingDeletion))
			Expect(placementSvc.OnAgentError(ctx, ids["alpha"], "test-agent", 9, details)).To(Succeed())

			Expect(deleteCalls[ids["alpha"]]).To(Equal(1))
			Expect(deleteCalls[ids["zeta"]]).To(Equal(2))
		})

		It("retries rollback after an equal-sequence agent error redelivery", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "test-agent", EvaluatedSpec: req.Spec}, nil
			}
			created, err := placementSvc.CreateRun(ctx, singleResourceRun("catalog-agent-error-retry", map[string]any{"kind": "vm"}, nil))
			Expect(err).NotTo(HaveOccurred())
			resourceID := *created.Resources[0].Id

			deleteCalls := 0
			mockSPRM.DeleteResourceFunc = func(_ context.Context, id string) error {
				Expect(id).To(Equal(resourceID))
				deleteCalls++
				if deleteCalls == 1 {
					return errors.New("temporary SPRM failure")
				}
				return nil
			}
			details := types.AgentErrorDetails{Error: "agent failure"}

			Expect(placementSvc.OnAgentError(ctx, resourceID, "test-agent", 7, details)).To(HaveOccurred())
			Expect(placementSvc.OnAgentError(ctx, resourceID, "test-agent", 7, details)).To(Succeed())

			stored := getStoredResource(ctx, dataStore, resourceID)
			Expect(stored.Status).To(Equal(types.ResourceStatusDeleting))
			Expect(deleteCalls).To(Equal(2))
		})

		It("does not reset DELETED after a fast acknowledgement races a delete error", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "test-agent", EvaluatedSpec: req.Spec}, nil
			}
			created, err := placementSvc.CreateRun(ctx, &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-agent-error-delete-ack-race",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			ids := make(map[string]string)
			for _, resource := range created.Resources {
				ids[resource.Name] = *resource.Id
			}

			deleteCalls := make(map[string]int)
			mockSPRM.DeleteResourceFunc = func(callCtx context.Context, id string) error {
				deleteCalls[id]++
				if id == ids["app"] && deleteCalls[id] == 1 {
					Expect(placementSvc.OnResourceDeleted(callCtx, id)).To(Succeed())
					return errors.New("delete response lost after acknowledgement")
				}
				return nil
			}

			Expect(placementSvc.OnAgentError(ctx, ids["db"], "test-agent", 1, types.AgentErrorDetails{Error: "provider failed"})).To(HaveOccurred())
			app := getStoredResource(ctx, dataStore, ids["app"])
			Expect(app.Status).To(Equal(types.ResourceStatusDeleted))
			Expect(getStoredResource(ctx, dataStore, ids["db"]).Status).To(Equal(types.ResourceStatusDeleting))
			Expect(deleteCalls[ids["app"]]).To(Equal(1))
		})

		It("does not redispatch after a stale deletion progress snapshot loses its claim", func() {
			created, err := placementSvc.CreateRun(ctx, singleResourceRun("catalog-agent-error-delete-claim", map[string]any{"kind": "vm"}, nil))
			Expect(err).NotTo(HaveOccurred())
			resourceID := *created.Resources[0].Id
			resource := getStoredResource(ctx, dataStore, resourceID)
			agentName := *resource.AgentName
			details := types.AgentErrorDetails{Error: "provider failed"}
			accepted, err := dataStore.Resource().UpdateAgentError(ctx, resourceID, agentName, 21, details)
			Expect(err).NotTo(HaveOccurred())
			Expect(accepted).To(BeTrue())
			Expect(dataStore.Resource().UpdateStatus(ctx, resourceID, types.ResourceStatusFailed)).To(Succeed())

			blockingResource := &blockingResourceStore{
				Resource: dataStore.Resource(),
				runID:    created.RunId,
				snapshot: make(chan struct{}),
				release:  make(chan struct{}),
			}
			placementSvc = service.NewPlacementService(&resourceOverrideStore{Store: dataStore, resource: blockingResource}, mockPolicy, mockSPRM, service.WithAgentClient(agentClient))
			var releaseOnce sync.Once
			releaseStaleSnapshot := func() { releaseOnce.Do(func() { close(blockingResource.release) }) }
			defer releaseStaleSnapshot()

			deleteCalls := 0
			mockSPRM.DeleteResourceFunc = func(_ context.Context, id string) error {
				deleteCalls++
				if deleteCalls == 1 {
					return placementSvc.OnResourceDeleted(ctx, id)
				}
				return nil
			}

			firstCall := make(chan error, 1)
			go func() {
				firstCall <- placementSvc.OnAgentError(ctx, resourceID, agentName, 21, details)
			}()
			Eventually(blockingResource.snapshot).Should(BeClosed())

			Expect(placementSvc.OnAgentError(ctx, resourceID, agentName, 21, details)).To(Succeed())
			Expect(getStoredResource(ctx, dataStore, resourceID).Status).To(Equal(types.ResourceStatusDeleted))
			releaseStaleSnapshot()
			Expect(<-firstCall).To(Succeed())

			retained := getStoredResource(ctx, dataStore, resourceID)
			Expect(retained.Status).To(Equal(types.ResourceStatusDeleted))
			Expect(retained.CleanupIntent).To(Equal(string(store.CleanupIntentRollback)))
			Expect(deleteCalls).To(Equal(1))
		})

		It("retains the complete run and agent error after automatic rollback completes", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "test-agent", EvaluatedSpec: req.Spec}, nil
			}
			created, err := placementSvc.CreateRun(ctx, &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-agent-error-retain",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			ids := make(map[string]string)
			for _, resource := range created.Resources {
				ids[resource.Name] = *resource.Id
			}
			details := types.AgentErrorDetails{Error: "provider failed", Message: "rollback summary"}
			mockSPRM.DeleteResourceFunc = func(context.Context, string) error { return nil }

			Expect(placementSvc.OnAgentError(ctx, ids["db"], "test-agent", 11, details)).To(Succeed())
			Expect(getStoredResource(ctx, dataStore, ids["app"]).Status).To(Equal(types.ResourceStatusDeleting))
			Expect(placementSvc.OnResourceDeleted(ctx, ids["app"])).To(Succeed())
			Expect(placementSvc.OnResourceDeleted(ctx, ids["db"])).To(Succeed())

			resources, err := dataStore.Resource().ListByRunID(ctx, created.RunId)
			Expect(err).NotTo(HaveOccurred())
			Expect(resources).To(HaveLen(2))
			for _, resource := range resources {
				Expect(resource.CatalogItemInstanceId).To(Equal("catalog-agent-error-retain"))
				Expect(resource.Status).To(Equal(types.ResourceStatusDeleted))
			}
			var failed model.Resource
			for _, resource := range resources {
				if resource.ID == ids["db"] {
					failed = resource
				}
			}
			Expect(failed.AgentErrorDetails).NotTo(BeNil())
			Expect(*failed.AgentErrorDetails).To(Equal(details))
			Expect(failed.AgentErrorStreamSequence).To(Equal(uint64(11)))
		})

		It("explicitly deletes a retained failed run without repeating external cleanup", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "test-agent", EvaluatedSpec: req.Spec}, nil
			}
			created, err := placementSvc.CreateRun(ctx, singleResourceRun("catalog-agent-error-explicit-delete", map[string]any{"kind": "vm"}, nil))
			Expect(err).NotTo(HaveOccurred())
			resourceID := *created.Resources[0].Id
			deleteCalls := 0
			mockSPRM.DeleteResourceFunc = func(context.Context, string) error {
				deleteCalls++
				return nil
			}

			Expect(placementSvc.OnAgentError(ctx, resourceID, "test-agent", 12, types.AgentErrorDetails{Error: "provider failed"})).To(Succeed())
			Expect(placementSvc.OnResourceDeleted(ctx, resourceID)).To(Succeed())
			Expect(deleteCalls).To(Equal(1))
			retained, err := dataStore.Resource().ListByRunID(ctx, created.RunId)
			Expect(err).NotTo(HaveOccurred())
			Expect(retained).To(HaveLen(1))

			Expect(placementSvc.DeleteRun(ctx, created.RunId)).To(Succeed())
			removed, err := dataStore.Resource().ListByRunID(ctx, created.RunId)
			Expect(err).NotTo(HaveOccurred())
			Expect(removed).To(BeEmpty())
			Expect(deleteCalls).To(Equal(1))
		})

		It("lets explicit deletion override retention while rollback cleanup is in flight", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "test-agent", EvaluatedSpec: req.Spec}, nil
			}
			created, err := placementSvc.CreateRun(ctx, &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-agent-error-delete-in-flight",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			ids := make(map[string]string)
			for _, resource := range created.Resources {
				ids[resource.Name] = *resource.Id
			}
			deleteCalls := make(map[string]int)
			mockSPRM.DeleteResourceFunc = func(_ context.Context, id string) error {
				deleteCalls[id]++
				return nil
			}

			Expect(placementSvc.OnAgentError(ctx, ids["db"], "test-agent", 13, types.AgentErrorDetails{Error: "provider failed"})).To(Succeed())
			Expect(deleteCalls[ids["app"]]).To(Equal(1))
			Expect(placementSvc.DeleteRun(ctx, created.RunId)).To(Succeed())
			Expect(deleteCalls[ids["app"]]).To(Equal(1))

			Expect(placementSvc.OnResourceDeleted(ctx, ids["app"])).To(Succeed())
			Expect(placementSvc.OnResourceDeleted(ctx, ids["db"])).To(Succeed())
			resources, err := dataStore.Resource().ListByRunID(ctx, created.RunId)
			Expect(err).NotTo(HaveOccurred())
			Expect(resources).To(BeEmpty())
			Expect(deleteCalls[ids["app"]]).To(Equal(1))
			Expect(deleteCalls[ids["db"]]).To(Equal(1))
		})

		It("starts rollback on OnResourceFailed", func() {
			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-failed",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var dbID, appID string
			for _, r := range created.Resources {
				if r.Name == "db" {
					dbID = *r.Id
				}
				if r.Name == "app" {
					appID = *r.Id
				}
			}

			err = placementSvc.OnResourceFailed(ctx, dbID)
			Expect(err).NotTo(HaveOccurred())

			app, err := dataStore.Resource().Get(ctx, appID)
			Expect(err).NotTo(HaveOccurred())
			Expect(app.Status).To(Equal(types.ResourceStatusDeleting))
		})

		It("does not duplicate SPRM create on redelivered OnResourceRunning", func() {
			createCalls := make(map[string]int)
			mockSPRM.CreateResourceFunc = func(_ context.Context, req sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				createCalls[req.ID]++
				return &sprm.CreateResourceResponse{ID: req.ID, Status: "provisioning"}, nil
			}

			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-dup-running",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var dbID, appID string
			for _, r := range created.Resources {
				switch r.Name {
				case "db":
					dbID = *r.Id
				case "app":
					appID = *r.Id
				}
			}

			err = placementSvc.OnResourceRunning(ctx, runningEvent(dbID))
			Expect(err).NotTo(HaveOccurred())
			Expect(createCalls[appID]).To(Equal(1))

			err = placementSvc.OnResourceRunning(ctx, runningEvent(dbID))
			Expect(err).NotTo(HaveOccurred())
			Expect(createCalls[appID]).To(Equal(1))

			app, err := dataStore.Resource().Get(ctx, appID)
			Expect(err).NotTo(HaveOccurred())
			Expect(app.Status).To(Equal(types.ResourceStatusProvisioning))
		})

		It("retries teardown when OnResourceFailed is redelivered", func() {
			deleteCalls := 0
			mockSPRM.DeleteResourceFunc = func(_ context.Context, _ string) error {
				deleteCalls++
				return nil
			}

			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-dup-failed",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var dbID string
			for _, r := range created.Resources {
				if r.Name == "db" {
					dbID = *r.Id
				}
			}

			err = placementSvc.OnResourceFailed(ctx, dbID)
			Expect(err).NotTo(HaveOccurred())
			firstDeleteCalls := deleteCalls

			err = placementSvc.OnResourceFailed(ctx, dbID)
			Expect(err).NotTo(HaveOccurred())
			Expect(deleteCalls).To(BeNumerically(">=", firstDeleteCalls))
		})

		It("releases PROVISIONING and retries SPRM create after a retryable failure", func() {
			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-retry-create",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var dbID, appID string
			for _, r := range created.Resources {
				switch r.Name {
				case "db":
					dbID = *r.Id
				case "app":
					appID = *r.Id
				}
			}

			createAttempts := 0
			mockSPRM.CreateResourceFunc = func(_ context.Context, req sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				if req.ID == appID {
					createAttempts++
					if createAttempts == 1 {
						return nil, &sprm.HTTPError{StatusCode: 503, Body: "unavailable"}
					}
				}
				return &sprm.CreateResourceResponse{ID: req.ID, Status: "provisioning"}, nil
			}

			err = placementSvc.OnResourceRunning(ctx, runningEvent(dbID))
			Expect(err).To(HaveOccurred())

			app, err := dataStore.Resource().Get(ctx, appID)
			Expect(err).NotTo(HaveOccurred())
			Expect(app.Status).To(Equal(types.ResourceStatusPending))

			err = placementSvc.OnResourceRunning(ctx, runningEvent(dbID))
			Expect(err).NotTo(HaveOccurred())
			Expect(createAttempts).To(Equal(2))

			app, err = dataStore.Resource().Get(ctx, appID)
			Expect(err).NotTo(HaveOccurred())
			Expect(app.Status).To(Equal(types.ResourceStatusProvisioning))
		})

		It("releases PROVISIONING after a terminal SPRM create error", func() {
			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-terminal-create",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var dbID, appID string
			for _, r := range created.Resources {
				switch r.Name {
				case "db":
					dbID = *r.Id
				case "app":
					appID = *r.Id
				}
			}

			mockSPRM.CreateResourceFunc = func(_ context.Context, req sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				if req.ID == appID {
					return nil, &sprm.HTTPError{StatusCode: http.StatusUnprocessableEntity, Body: "invalid spec"}
				}
				return &sprm.CreateResourceResponse{ID: req.ID, Status: "provisioning"}, nil
			}

			err = placementSvc.OnResourceRunning(ctx, runningEvent(dbID))
			Expect(err).To(HaveOccurred())

			app, err := dataStore.Resource().Get(ctx, appID)
			Expect(err).NotTo(HaveOccurred())
			Expect(app.Status).To(Equal(types.ResourceStatusPending))
		})

		It("rolls DELETING back to PENDING_DELETION after a retryable SPRM delete failure and retries on redelivery", func() {
			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-retry-delete",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var appID string
			for _, r := range created.Resources {
				if r.Name == "app" {
					appID = *r.Id
				}
			}

			deleteAttempts := 0
			mockSPRM.DeleteResourceFunc = func(_ context.Context, resourceID string) error {
				if resourceID == appID {
					deleteAttempts++
					if deleteAttempts == 1 {
						return &sprm.HTTPError{StatusCode: http.StatusServiceUnavailable, Body: "unavailable"}
					}
				}
				return nil
			}

			err = placementSvc.DeleteRun(ctx, created.RunId)
			Expect(err).To(HaveOccurred())

			app, err := dataStore.Resource().Get(ctx, appID)
			Expect(err).NotTo(HaveOccurred())
			Expect(app.Status).To(Equal(types.ResourceStatusPendingDeletion))

			err = placementSvc.DeleteRun(ctx, created.RunId)
			Expect(err).NotTo(HaveOccurred())
			Expect(deleteAttempts).To(Equal(2))
		})
	})

	Describe("RehydrateResource", func() {
		var (
			oldResourceID string
			oldRunID      string
			catalogID     string
		)

		BeforeEach(func() {
			oldResourceID = "old-resource-id"
			catalogID = "catalog-rehydrate"

			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{
					Status:        "APPROVED",
					SelectedAgent: "test-agent",
					EvaluatedSpec: req.Spec,
				}, nil
			}

			resource := &types.Resource{
				CatalogItemInstanceId: catalogID,
				Spec:                  map[string]any{"cpu": 2, "memory": "4GB"},
			}
			created, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, &oldResourceID))
			Expect(err).NotTo(HaveOccurred())
			oldRunID = created.RunId
		})

		It("rehydrates a run successfully", func() {
			newRunID := "new-run-id"

			result, err := placementSvc.RehydrateResource(ctx, oldRunID, newRunID)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.Id).NotTo(BeNil())
			Expect(*result.Id).NotTo(Equal(oldResourceID))
			Expect(result.RunId).To(Equal(newRunID))
			Expect(result.CatalogItemInstanceId).To(Equal(catalogID))
			Expect(result.Spec).To(HaveKey("cpu"))
			Expect(result.Spec).To(HaveKey("memory"))
			Expect(*result.ApprovalStatus).To(Equal("APPROVED"))
			Expect(result.AgentName).NotTo(BeNil())
			Expect(*result.AgentName).To(Equal("test-agent"))

			oldStored := getStoredResource(ctx, dataStore, oldResourceID)
			Expect(oldStored.Status).To(Equal(types.ResourceStatusDeleting))
			Expect(placementSvc.OnResourceDeleted(ctx, oldResourceID)).To(Succeed())
			expectStoredResourceMissing(ctx, dataStore, oldResourceID)
			// Verify new resource exists
			stored := getStoredResource(ctx, dataStore, *result.Id)
			Expect(stored.CatalogItemInstanceId).To(Equal(catalogID))
			Expect(stored.RunID).To(Equal(newRunID))
			Expect(stored.AgentName).NotTo(BeNil())
			Expect(*stored.AgentName).To(Equal("test-agent"))
		})

		It("re-evaluates policy and assigns new agent", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{
					Status:        "APPROVED",
					SelectedAgent: "new-agent",
					EvaluatedSpec: req.Spec,
				}, nil
			}

			result, err := placementSvc.RehydrateResource(ctx, oldRunID, "new-run-agent")

			Expect(err).NotTo(HaveOccurred())
			stored := getStoredResource(ctx, dataStore, *result.Id)
			Expect(stored.AgentName).NotTo(BeNil())
			Expect(*stored.AgentName).To(Equal("new-agent"))
		})

		It("fails closed and does not re-evaluate policy when listing available agents errors", func() {
			agentClient.err = errors.New("agent registry unavailable")
			policyCalled := false
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				policyCalled = true
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "test-agent", EvaluatedSpec: req.Spec}, nil
			}

			result, err := placementSvc.RehydrateResource(ctx, oldRunID, "new-run-agents-error")

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			Expect(policyCalled).To(BeFalse())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeInternal))

			// Old resource must survive an aborted rehydration.
			_ = getStoredResource(ctx, dataStore, oldResourceID)
		})

		It("preserves original spec and sends evaluated spec to SPRM", func() {
			var capturedSPRMSpec map[string]any
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				modifiedSpec := make(map[string]any)
				for k, v := range req.Spec {
					modifiedSpec[k] = v
				}
				modifiedSpec["policy_added"] = "value"
				return &policy.EvaluateResponse{
					Status:        "MODIFIED",
					SelectedAgent: "test-agent",
					EvaluatedSpec: modifiedSpec,
				}, nil
			}
			mockSPRM.CreateResourceFunc = func(_ context.Context, req sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				capturedSPRMSpec = req.Spec
				return &sprm.CreateResourceResponse{ID: req.ID, Status: "provisioning"}, nil
			}

			result, err := placementSvc.RehydrateResource(ctx, oldRunID, "new-run-spec")

			Expect(err).NotTo(HaveOccurred())
			Expect(*result.ApprovalStatus).To(Equal("MODIFIED"))
			// DB stores original spec (no policy_added field)
			Expect(result.Spec).NotTo(HaveKey("policy_added"))
			Expect(result.Spec).To(HaveKey("cpu"))
			// SPRM received the evaluated spec (with policy_added field)
			Expect(capturedSPRMSpec).To(HaveKey("policy_added"))
		})

		It("returns not found when run does not exist", func() {
			result, err := placementSvc.RehydrateResource(ctx, "non-existent", "new-run")

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeNotFound))
		})

		It("rehydrates a run with multiple resources", func() {
			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-rehydrate-multi",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())
			Expect(created.Resources).To(HaveLen(2))

			var oldDBID, oldAppID string
			nameByID := make(map[string]string, 2)
			for _, r := range created.Resources {
				nameByID[*r.Id] = r.Name
				switch r.Name {
				case "db":
					oldDBID = *r.Id
				case "app":
					oldAppID = *r.Id
				}
			}

			deleteOrder := make([]string, 0, 2)
			mockSPRM.DeleteResourceFunc = func(_ context.Context, resourceID string) error {
				deleteOrder = append(deleteOrder, nameByID[resourceID])
				return nil
			}

			newRunID := "new-run-multi"
			result, err := placementSvc.RehydrateResource(ctx, created.RunId, newRunID)

			Expect(err).NotTo(HaveOccurred())
			Expect(result).NotTo(BeNil())
			Expect(result.RunId).To(Equal(newRunID))
			Expect(result.Name).To(Equal("db"))
			Expect(deleteOrder).To(Equal([]string{"app"}))

			Expect(placementSvc.OnResourceDeleted(ctx, oldAppID)).To(Succeed())
			Expect(deleteOrder).To(Equal([]string{"app", "db"}))
			Expect(placementSvc.OnResourceDeleted(ctx, oldDBID)).To(Succeed())
			expectStoredResourceMissing(ctx, dataStore, oldDBID)
			expectStoredResourceMissing(ctx, dataStore, oldAppID)

			rehydrated, err := placementSvc.GetRun(ctx, newRunID)
			Expect(err).NotTo(HaveOccurred())
			Expect(rehydrated.Resources).To(HaveLen(2))
			for _, r := range rehydrated.Resources {
				Expect(*r.Id).NotTo(Equal(oldDBID))
				Expect(*r.Id).NotTo(Equal(oldAppID))
			}
		})

		It("returns error and rolls back when SPRM creation fails on multi-resource rehydrate", func() {
			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-rehydrate-multi-sprm-fail",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var oldDBID, oldAppID string
			for _, r := range created.Resources {
				switch r.Name {
				case "db":
					oldDBID = *r.Id
				case "app":
					oldAppID = *r.Id
				}
			}

			mockSPRM.CreateResourceFunc = func(_ context.Context, _ sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				return nil, &sprm.HTTPError{StatusCode: 500, Body: "sprm error"}
			}
			deleteCalled := false
			mockSPRM.DeleteResourceFunc = func(_ context.Context, _ string) error {
				deleteCalled = true
				return nil
			}

			newRunID := "new-run-multi-sprm-fail"
			result, err := placementSvc.RehydrateResource(ctx, created.RunId, newRunID)

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeSPRMError))
			Expect(deleteCalled).To(BeFalse())

			_ = getStoredResource(ctx, dataStore, oldDBID)
			_ = getStoredResource(ctx, dataStore, oldAppID)

			_, err = placementSvc.GetRun(ctx, newRunID)
			Expect(err).To(HaveOccurred())
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeNotFound))
		})

		It("returns error when policy rejects re-evaluation (406)", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, _ policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return nil, &policy.HTTPError{StatusCode: 406, Body: "rejected"}
			}

			result, err := placementSvc.RehydrateResource(ctx, oldRunID, "new-run-406")

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodePolicyRejected))

			_ = getStoredResource(ctx, dataStore, oldResourceID)
		})

		It("returns error when policy fails (500)", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, _ policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return nil, &policy.HTTPError{StatusCode: 500, Body: "internal error"}
			}

			result, err := placementSvc.RehydrateResource(ctx, oldRunID, "new-run-500")

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodePolicyInternalError))

			_ = getStoredResource(ctx, dataStore, oldResourceID)
		})

		It("returns error when policy returns empty agent", func() {
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{
					Status:        "APPROVED",
					SelectedAgent: "",
					EvaluatedSpec: req.Spec,
				}, nil
			}

			result, err := placementSvc.RehydrateResource(ctx, oldRunID, "new-run-empty")

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodePolicyInternalError))
		})

		It("returns error and rolls back when SPRM creation fails", func() {
			mockSPRM.CreateResourceFunc = func(_ context.Context, _ sprm.CreateResourceRequest) (*sprm.CreateResourceResponse, error) {
				return nil, &sprm.HTTPError{StatusCode: 500, Body: "sprm error"}
			}

			result, err := placementSvc.RehydrateResource(ctx, oldRunID, "new-run-sprm-fail")

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeSPRMError))

			_ = getStoredResource(ctx, dataStore, oldResourceID)
		})

		It("returns error and rolls back new run when old-run delete fails after create", func() {
			Expect(dataStore.Resource().UpdateStatus(ctx, oldResourceID, types.ResourceStatusRunning)).To(Succeed())

			mockSPRM.DeleteResourceFunc = func(_ context.Context, resourceID string) error {
				if resourceID == oldResourceID {
					return &sprm.HTTPError{StatusCode: 500, Body: "delete failed"}
				}
				return nil
			}

			newRunID := "new-run-delete-fail"
			result, err := placementSvc.RehydrateResource(ctx, oldRunID, newRunID)

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeSPRMError))

			_ = getStoredResource(ctx, dataStore, oldResourceID)
			_, err = placementSvc.GetRun(ctx, newRunID)
			Expect(err).To(HaveOccurred())
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeNotFound))

			oldStored := getStoredResource(ctx, dataStore, oldResourceID)
			Expect(oldStored.Status).To(Equal(types.ResourceStatusRunning))
		})

		It("does not rewrite DELETED old resources when a later delete dispatch fails", func() {
			req := &types.CreateRunRequest{
				CatalogItemInstanceId: "catalog-rehydrate-partial-delete",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"kind": "db"}},
					{Name: "app", Spec: map[string]any{"kind": "app"}, RequiresResources: []string{"db"}},
				},
			}
			created, err := placementSvc.CreateRun(ctx, req)
			Expect(err).NotTo(HaveOccurred())

			var oldDBID, oldAppID string
			for _, r := range created.Resources {
				switch r.Name {
				case "db":
					oldDBID = *r.Id
				case "app":
					oldAppID = *r.Id
				}
			}

			mockSPRM.DeleteResourceFunc = func(_ context.Context, resourceID string) error {
				switch resourceID {
				case oldAppID:
					return &sprm.HTTPError{StatusCode: 404, Body: "not found"}
				case oldDBID:
					return &sprm.HTTPError{StatusCode: 500, Body: "delete failed"}
				default:
					return nil
				}
			}

			newRunID := "new-run-partial-old-delete"
			result, err := placementSvc.RehydrateResource(ctx, created.RunId, newRunID)

			Expect(err).To(HaveOccurred())
			Expect(result).To(BeNil())

			app, err := dataStore.Resource().Get(ctx, oldAppID)
			Expect(err).NotTo(HaveOccurred())
			Expect(app.Status).To(Equal(types.ResourceStatusDeleted))

			db, err := dataStore.Resource().Get(ctx, oldDBID)
			Expect(err).NotTo(HaveOccurred())
			Expect(db.Status).To(Equal(types.ResourceStatusPending))
		})
	})

	Describe("ListRun", func() {
		It("paginates by run and keeps multi-resource runs intact", func() {
			_, err := placementSvc.CreateRun(ctx, &types.CreateRunRequest{
				CatalogItemInstanceId: "cat-list-1",
				RunId:                 "list-run-1",
				Resources: []types.ResourceInput{
					{Name: "db", Spec: map[string]any{"size": "small"}},
					{Name: "app", Spec: map[string]any{"image": "nginx"}, RequiresResources: []string{"db"}},
				},
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = placementSvc.CreateRun(ctx, &types.CreateRunRequest{
				CatalogItemInstanceId: "cat-list-2",
				RunId:                 "list-run-2",
				Resources:             []types.ResourceInput{{Name: "main", Spec: map[string]any{"cpu": 1}}},
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = placementSvc.CreateRun(ctx, &types.CreateRunRequest{
				CatalogItemInstanceId: "cat-list-3",
				RunId:                 "list-run-3",
				Resources:             []types.ResourceInput{{Name: "main", Spec: map[string]any{"cpu": 2}}},
			})
			Expect(err).NotTo(HaveOccurred())

			page1, err := placementSvc.ListRun(ctx, &store.ResourceListOptions{PageSize: 2})
			Expect(err).NotTo(HaveOccurred())
			Expect(page1.Runs).To(HaveLen(2))
			Expect(page1.NextPageToken).NotTo(BeNil())
			Expect(page1.Runs[0].RunId).To(Equal("list-run-1"))
			Expect(page1.Runs[0].Resources).To(HaveLen(2))

			page2, err := placementSvc.ListRun(ctx, &store.ResourceListOptions{
				PageSize:  2,
				PageToken: page1.NextPageToken,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(page2.Runs).To(HaveLen(1))
			Expect(page2.Runs[0].RunId).To(Equal("list-run-3"))
			Expect(page2.NextPageToken).To(BeNil())
		})
	})

	Describe("ReEvaluateWithExclude", func() {
		It("re-evaluates with excluded agent, calls SPRM to reassign, and persists the new agent", func() {
			resource := &types.Resource{
				CatalogItemInstanceId: "cat-reeval-1",
				Spec:                  map[string]any{"service_type": "vm"},
			}
			created, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))
			Expect(err).NotTo(HaveOccurred())
			resourceID := *created.Resources[0].Id
			Expect(*getStoredResource(ctx, dataStore, resourceID).AgentName).To(Equal("default-agent"))

			var evalReq policy.EvaluateRequest
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				evalReq = req
				return &policy.EvaluateResponse{
					Status:        "APPROVED",
					SelectedAgent: "fallback-agent",
					EvaluatedSpec: map[string]any{},
				}, nil
			}

			var reassignedID, reassignedAgent, reassignedExpectedCurrent string
			mockSPRM.ReassignResourceFunc = func(_ context.Context, resourceId string, agentName string, expectedCurrentAgent string) error {
				reassignedID = resourceId
				reassignedAgent = agentName
				reassignedExpectedCurrent = expectedCurrentAgent
				return nil
			}

			err = placementSvc.ReEvaluateWithExclude(ctx, resourceID, []string{"failed-agent"})

			Expect(err).NotTo(HaveOccurred())
			Expect(evalReq.ExcludeAgents).To(ConsistOf("failed-agent"))
			Expect(reassignedID).To(Equal(resourceID))
			Expect(reassignedAgent).To(Equal("fallback-agent"))
			// The CAS-critical value: must be the resource's own
			// pre-reassignment agent ("default-agent" from CreateRun), not
			// the excluded agent (which need not be the same in general)
			// and not re-derived at SPRM/SP call time.
			Expect(reassignedExpectedCurrent).To(Equal("default-agent"))

			stored := getStoredResource(ctx, dataStore, resourceID)
			Expect(stored.AgentName).NotTo(BeNil())
			Expect(*stored.AgentName).To(Equal("fallback-agent"))
		})

		It("returns error when no viable agent remains", func() {
			resource := &types.Resource{
				CatalogItemInstanceId: "cat-no-agent",
				Spec:                  map[string]any{"service_type": "vm"},
			}

			created, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))
			Expect(err).NotTo(HaveOccurred())
			resourceID := *created.Resources[0].Id

			mockPolicy.EvaluateFunc = func(_ context.Context, _ policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return nil, &policy.HTTPError{StatusCode: 404, Body: "no agents"}
			}

			err = placementSvc.ReEvaluateWithExclude(ctx, resourceID, []string{"all-agents"})

			Expect(err).To(HaveOccurred())

			stored := getStoredResource(ctx, dataStore, resourceID)
			Expect(*stored.AgentName).To(Equal("default-agent"))
		})

		It("returns error and leaves the resource unchanged when SPRM reassignment fails", func() {
			resource := &types.Resource{
				CatalogItemInstanceId: "cat-reeval-fail",
				Spec:                  map[string]any{"service_type": "vm"},
			}
			created, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))
			Expect(err).NotTo(HaveOccurred())
			resourceID := *created.Resources[0].Id
			Expect(*getStoredResource(ctx, dataStore, resourceID).AgentName).To(Equal("default-agent"))

			mockPolicy.EvaluateFunc = func(_ context.Context, _ policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{
					Status:        "APPROVED",
					SelectedAgent: "fallback-agent",
					EvaluatedSpec: map[string]any{},
				}, nil
			}
			mockSPRM.ReassignResourceFunc = func(_ context.Context, _ string, _ string, _ string) error {
				return &sprm.HTTPError{StatusCode: 503, Body: "sprm unavailable"}
			}

			err = placementSvc.ReEvaluateWithExclude(ctx, resourceID, []string{"failed-agent"})

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeUnavailable))

			stored := getStoredResource(ctx, dataStore, resourceID)
			Expect(*stored.AgentName).To(Equal("default-agent"))
		})

		It("fails closed and does not re-evaluate policy when listing available agents errors", func() {
			resource := &types.Resource{
				CatalogItemInstanceId: "cat-reeval-agents-error",
				Spec:                  map[string]any{"service_type": "vm"},
			}
			created, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))
			Expect(err).NotTo(HaveOccurred())
			resourceID := *created.Resources[0].Id

			agentClient.err = errors.New("agent registry unavailable")
			policyCalled := false
			mockPolicy.EvaluateFunc = func(_ context.Context, _ policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				policyCalled = true
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "fallback-agent", EvaluatedSpec: map[string]any{}}, nil
			}

			err = placementSvc.ReEvaluateWithExclude(ctx, resourceID, []string{"failed-agent"})

			Expect(err).To(HaveOccurred())
			Expect(policyCalled).To(BeFalse())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeInternal))

			stored := getStoredResource(ctx, dataStore, resourceID)
			Expect(*stored.AgentName).To(Equal("default-agent"))
		})

		It("returns not found when resource does not exist", func() {
			err := placementSvc.ReEvaluateWithExclude(ctx, "non-existent", []string{"failed-agent"})

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeNotFound))
		})

		It("rejects a policy response that selects an excluded agent instead of reassigning to it (R2 S3: finding #7)", func() {
			// Defensive check: nothing besides the Rego policy itself
			// enforces exclude_agents, so a misbehaving/misconfigured policy
			// could hand back the very agent the caller asked to avoid. Must
			// be rejected rather than silently reassigning the instance
			// right back to the failing agent.
			resource := &types.Resource{
				CatalogItemInstanceId: "cat-reeval-excluded",
				Spec:                  map[string]any{"service_type": "vm"},
			}
			created, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))
			Expect(err).NotTo(HaveOccurred())
			resourceID := *created.Resources[0].Id

			mockPolicy.EvaluateFunc = func(_ context.Context, _ policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{
					Status:        "APPROVED",
					SelectedAgent: "failed-agent",
					EvaluatedSpec: map[string]any{},
				}, nil
			}
			reassignCalled := false
			mockSPRM.ReassignResourceFunc = func(_ context.Context, _ string, _ string, _ string) error {
				reassignCalled = true
				return nil
			}

			err = placementSvc.ReEvaluateWithExclude(ctx, resourceID, []string{"failed-agent"})

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(reassignCalled).To(BeFalse())

			stored := getStoredResource(ctx, dataStore, resourceID)
			Expect(*stored.AgentName).To(Equal("default-agent"))
		})

		It("propagates a failure to persist the new agent_name instead of silently leaving it stale (R2 S3: finding #10)", func() {
			resource := &types.Resource{
				CatalogItemInstanceId: "cat-reeval-persist-fail",
				Spec:                  map[string]any{"service_type": "vm"},
			}
			created, err := placementSvc.CreateRun(ctx, singleResourceRun(resource.CatalogItemInstanceId, resource.Spec, nil))
			Expect(err).NotTo(HaveOccurred())
			resourceID := *created.Resources[0].Id

			mockPolicy.EvaluateFunc = func(_ context.Context, _ policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{
					Status:        "APPROVED",
					SelectedAgent: "fallback-agent",
					EvaluatedSpec: map[string]any{},
				}, nil
			}
			// SPRM/sp-side reassignment succeeds, but the resource row backing
			// placement's own agent_name is deleted as a side effect (simulating
			// e.g. a concurrent delete), so the subsequent UpdateAgentName finds
			// zero rows and fails.
			mockSPRM.ReassignResourceFunc = func(_ context.Context, resourceId string, _ string, _ string) error {
				Expect(db.Delete(&model.Resource{}, "id = ?", resourceId).Error).NotTo(HaveOccurred())
				return nil
			}

			err = placementSvc.ReEvaluateWithExclude(ctx, resourceID, []string{"failed-agent"})

			Expect(err).To(HaveOccurred())
			var svcErr *service.ServiceError
			Expect(errors.As(err, &svcErr)).To(BeTrue())
			Expect(svcErr.Code).To(Equal(service.ErrCodeInternal))
		})

		// Multi-resource run-sibling proactive reassignment (thread 9 upgrade):
		// a sibling still pointed at the excluded agent gets reassigned
		// alongside the primary resource, instead of waiting for its own
		// independent sweep timeout.
		createSiblingRun := func(catalogID string, primarySpec, siblingSpec map[string]any) (primaryID, siblingID string) {
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "failed-agent", EvaluatedSpec: req.Spec}, nil
			}
			created, err := placementSvc.CreateRun(ctx, &types.CreateRunRequest{
				CatalogItemInstanceId: catalogID,
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "primary", Spec: primarySpec},
					{Name: "sibling", Spec: siblingSpec},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(created.Resources).To(HaveLen(2))
			return *created.Resources[0].Id, *created.Resources[1].Id
		}

		It("proactively reassigns a run-sibling stuck on the excluded agent", func() {
			primaryID, siblingID := createSiblingRun("cat-reeval-sibling-pending",
				map[string]any{"service_type": "vm"}, map[string]any{"service_type": "db"})

			var evaluatedSpecs []map[string]any
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				evaluatedSpecs = append(evaluatedSpecs, req.Spec)
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "fallback-agent", EvaluatedSpec: req.Spec}, nil
			}
			reassigned := map[string]string{}
			expectedCurrentByID := map[string]string{}
			mockSPRM.ReassignResourceFunc = func(_ context.Context, resourceId string, agentName string, expectedCurrentAgent string) error {
				reassigned[resourceId] = agentName
				expectedCurrentByID[resourceId] = expectedCurrentAgent
				return nil
			}

			err := placementSvc.ReEvaluateWithExclude(ctx, primaryID, []string{"failed-agent"})

			Expect(err).NotTo(HaveOccurred())
			Expect(reassigned).To(HaveKeyWithValue(primaryID, "fallback-agent"))
			Expect(reassigned).To(HaveKeyWithValue(siblingID, "fallback-agent"))
			Expect(*getStoredResource(ctx, dataStore, siblingID).AgentName).To(Equal("fallback-agent"))
			// Each resource's own excluded agent is what gets CASed, not a
			// value shared across the primary/sibling reassignment calls.
			Expect(expectedCurrentByID).To(HaveKeyWithValue(primaryID, "failed-agent"))
			Expect(expectedCurrentByID).To(HaveKeyWithValue(siblingID, "failed-agent"))

			specs := make([]any, len(evaluatedSpecs))
			for i, s := range evaluatedSpecs {
				specs[i] = s["service_type"]
			}
			Expect(specs).To(ContainElement("db"))
		})

		It("leaves a sibling untouched when it is not CAS-eligible for reassignment (provisioning/running/queued)", func() {
			primaryID, siblingID := createSiblingRun("cat-reeval-sibling-ineligible",
				map[string]any{"service_type": "vm"}, map[string]any{"service_type": "db"})

			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "fallback-agent", EvaluatedSpec: req.Spec}, nil
			}
			reassigned := map[string]string{}
			mockSPRM.ReassignResourceFunc = func(_ context.Context, resourceId string, agentName string, _ string) error {
				if resourceId == siblingID {
					// Mirrors ReassignAndReset's CAS rejecting a sibling that
					// isn't pending/cancelled sp-side (e.g. provisioning,
					// running, or queued on the excluded agent).
					return &sprm.HTTPError{StatusCode: 409, Body: "instance is not eligible for reassignment"}
				}
				reassigned[resourceId] = agentName
				return nil
			}

			err := placementSvc.ReEvaluateWithExclude(ctx, primaryID, []string{"failed-agent"})

			Expect(err).NotTo(HaveOccurred())
			Expect(reassigned).To(HaveKeyWithValue(primaryID, "fallback-agent"))
			Expect(reassigned).NotTo(HaveKey(siblingID))
			Expect(*getStoredResource(ctx, dataStore, siblingID).AgentName).To(Equal("failed-agent"))
		})

		It("leaves a sibling on a different, non-excluded agent alone", func() {
			// A 3-resource run where only one sibling shares the excluded
			// agent with the primary; the other was already routed
			// elsewhere and must not be touched.
			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				agent := "failed-agent"
				if req.Spec["service_type"] == "other" {
					agent = "healthy-agent"
				}
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: agent, EvaluatedSpec: req.Spec}, nil
			}
			created, err := placementSvc.CreateRun(ctx, &types.CreateRunRequest{
				CatalogItemInstanceId: "cat-reeval-sibling-mixed",
				RunId:                 uuid.New().String(),
				Resources: []types.ResourceInput{
					{Name: "primary", Spec: map[string]any{"service_type": "vm"}},
					{Name: "sibling-same-agent", Spec: map[string]any{"service_type": "db"}},
					{Name: "sibling-other-agent", Spec: map[string]any{"service_type": "other"}},
				},
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(created.Resources).To(HaveLen(3))
			primaryID, siblingSameID, siblingOtherID := *created.Resources[0].Id, *created.Resources[1].Id, *created.Resources[2].Id

			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "fallback-agent", EvaluatedSpec: req.Spec}, nil
			}
			reassigned := map[string]string{}
			mockSPRM.ReassignResourceFunc = func(_ context.Context, resourceId string, agentName string, _ string) error {
				reassigned[resourceId] = agentName
				return nil
			}

			err = placementSvc.ReEvaluateWithExclude(ctx, primaryID, []string{"failed-agent"})

			Expect(err).NotTo(HaveOccurred())
			Expect(reassigned).To(HaveKeyWithValue(primaryID, "fallback-agent"))
			Expect(reassigned).To(HaveKeyWithValue(siblingSameID, "fallback-agent"))
			Expect(reassigned).NotTo(HaveKey(siblingOtherID))
			Expect(*getStoredResource(ctx, dataStore, siblingOtherID).AgentName).To(Equal("healthy-agent"))
		})

		It("still succeeds for the primary resource when a sibling's own re-evaluation fails (best-effort isolation)", func() {
			primaryID, siblingID := createSiblingRun("cat-reeval-sibling-policy-fail",
				map[string]any{"service_type": "vm"}, map[string]any{"service_type": "db"})

			mockPolicy.EvaluateFunc = func(_ context.Context, req policy.EvaluateRequest) (*policy.EvaluateResponse, error) {
				if req.Spec["service_type"] == "db" {
					return nil, &policy.HTTPError{StatusCode: 404, Body: "no agents for db"}
				}
				return &policy.EvaluateResponse{Status: "APPROVED", SelectedAgent: "fallback-agent", EvaluatedSpec: req.Spec}, nil
			}
			reassigned := map[string]string{}
			mockSPRM.ReassignResourceFunc = func(_ context.Context, resourceId string, agentName string, _ string) error {
				reassigned[resourceId] = agentName
				return nil
			}

			err := placementSvc.ReEvaluateWithExclude(ctx, primaryID, []string{"failed-agent"})

			Expect(err).NotTo(HaveOccurred())
			Expect(reassigned).To(HaveKeyWithValue(primaryID, "fallback-agent"))
			Expect(reassigned).NotTo(HaveKey(siblingID))
			Expect(*getStoredResource(ctx, dataStore, siblingID).AgentName).To(Equal("failed-agent"))
		})
	})
})
