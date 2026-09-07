package store_test

import (
	"context"

	agentmodel "github.com/dcm-project/control-plane/internal/agent/store/model"
	"github.com/dcm-project/control-plane/internal/sp/store/model"
	rmstore "github.com/dcm-project/control-plane/internal/sp/store/resource_manager"
	"github.com/dcm-project/control-plane/internal/sp/testutil"
	"github.com/google/uuid"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func newServiceTypeInstance(instanceName string, spec map[string]any) model.ServiceTypeInstance {
	return model.ServiceTypeInstance{
		ID:           uuid.New().String(),
		Status:       "PROVISIONING",
		InstanceName: instanceName,
		Spec:         spec,
	}
}

func newServiceTypeInstanceWithType(instanceName, serviceType string, spec map[string]any) model.ServiceTypeInstance {
	inst := newServiceTypeInstance(instanceName, spec)
	inst.ServiceType = serviceType
	return inst
}

func newServiceTypeInstanceWithAgent(instanceName, agentName string, spec map[string]any) model.ServiceTypeInstance {
	inst := newServiceTypeInstance(instanceName, spec)
	inst.AgentName = &agentName
	return inst
}

var _ = Describe("ServiceTypeInstance Store", func() {
	var (
		db  *gorm.DB
		s   rmstore.ServiceTypeInstance
		ctx context.Context
	)

	addInstanceToStore := func(instance model.ServiceTypeInstance) *model.ServiceTypeInstance {
		created, err := s.Create(ctx, instance)
		Expect(err).NotTo(HaveOccurred())
		return created
	}

	BeforeEach(func() {
		var err error
		db, err = gorm.Open(sqlite.Open(":memory:"), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Silent),
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(db.AutoMigrate(&agentmodel.Agent{}, &model.ServiceTypeInstance{})).To(Succeed())

		s = rmstore.NewServiceTypeInstance(db, testutil.FastServiceTypeInstanceRetry()...)
		ctx = context.Background()
	})

	AfterEach(func() {
		sqlDB, err := db.DB()
		Expect(err).NotTo(HaveOccurred())
		Expect(sqlDB.Close()).To(Succeed())
	})

	Describe("Create", func() {
		It("persists the instance", func() {
			instance := newServiceTypeInstance(
				"instance-1",
				map[string]any{"cpu": 2})
			created, err := s.Create(ctx, instance)

			Expect(err).NotTo(HaveOccurred())
			Expect(created.ID).To(Equal(instance.ID))
		})

		It("retries on transient DB errors", func() {
			// Close DB to simulate transient failure
			sqlDB, err := db.DB()
			Expect(err).NotTo(HaveOccurred())
			_ = sqlDB.Close()

			instance := newServiceTypeInstance(
				"retry-test",
				map[string]any{"cpu": 1})

			_, err = s.Create(ctx, instance)

			// Should fail after retries exhausted
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("database is closed"))
		})

		It("returns ErrInstanceConflict for duplicate ID without retrying forever", func() {
			instance := newServiceTypeInstance("dup-inst", map[string]any{"cpu": 1})
			_, err := s.Create(ctx, instance)
			Expect(err).NotTo(HaveOccurred())

			_, err = s.Create(ctx, instance)
			Expect(err).To(MatchError(rmstore.ErrInstanceConflict))
		})
	})

	Describe("Get", func() {
		It("retrieves by ID", func() {
			seeded := newServiceTypeInstance("get-inst", map[string]any{"cpu": 1})
			addInstanceToStore(seeded)

			found, err := s.Get(ctx, seeded.ID, false)

			Expect(err).NotTo(HaveOccurred())
			Expect(found).NotTo(BeNil())
			Expect(found.InstanceName).To(Equal("get-inst"))
		})

		It("returns ErrInstanceNotFound for missing ID", func() {
			_, err := s.Get(ctx, uuid.New().String(), false)
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})
	})

	Describe("List", func() {
		BeforeEach(func() {
			addInstanceToStore(newServiceTypeInstance("instance1", map[string]any{}))
			addInstanceToStore(newServiceTypeInstance("instance2", map[string]any{}))
			addInstanceToStore(newServiceTypeInstance("instance3", map[string]any{}))
		})

		It("returns all instances when opts is nil", func() {
			result, err := s.List(ctx, nil)
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Instances).To(HaveLen(3))
			Expect(result.NextPageToken).To(BeNil())
		})

		It("applies pagination with page size", func() {
			result, err := s.List(ctx, &rmstore.ServiceTypeInstanceListOptions{
				PageSize: 2,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Instances).To(HaveLen(2))
			Expect(result.NextPageToken).NotTo(BeNil())
		})

		It("filters by service type", func() {
			vmType := "vm"
			containerType := "container"
			addInstanceToStore(newServiceTypeInstanceWithType("vm-inst", vmType, map[string]any{}))
			addInstanceToStore(newServiceTypeInstanceWithType("container-inst", containerType, map[string]any{}))

			result, err := s.List(ctx, &rmstore.ServiceTypeInstanceListOptions{
				ServiceType: &vmType,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Instances).To(HaveLen(1))
			Expect(result.Instances[0].ServiceType).To(Equal("vm"))

			result, err = s.List(ctx, &rmstore.ServiceTypeInstanceListOptions{
				ServiceType: &containerType,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Instances).To(HaveLen(1))
			Expect(result.Instances[0].ServiceType).To(Equal("container"))
		})

		It("filters by agent name", func() {
			addInstanceToStore(newServiceTypeInstanceWithAgent("agent-a-inst", "agent-a", map[string]any{}))
			addInstanceToStore(newServiceTypeInstanceWithAgent("agent-b-inst", "agent-b", map[string]any{}))

			agentA := "agent-a"
			result, err := s.List(ctx, &rmstore.ServiceTypeInstanceListOptions{
				AgentName: &agentA,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Instances).To(HaveLen(1))
			Expect(*result.Instances[0].AgentName).To(Equal("agent-a"))
		})

		It("excludes instances with no agent name when filtering by agent name", func() {
			addInstanceToStore(newServiceTypeInstanceWithAgent("agent-a-inst", "agent-a", map[string]any{}))
			addInstanceToStore(newServiceTypeInstance("unassigned-inst", map[string]any{}))

			agentA := "agent-a"
			result, err := s.List(ctx, &rmstore.ServiceTypeInstanceListOptions{
				AgentName: &agentA,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Instances).To(HaveLen(1))
			Expect(*result.Instances[0].AgentName).To(Equal("agent-a"))
		})

		It("treats a blank agent name as no filter", func() {
			addInstanceToStore(newServiceTypeInstanceWithAgent("agent-a-inst", "agent-a", map[string]any{}))
			addInstanceToStore(newServiceTypeInstance("unassigned-inst", map[string]any{}))

			blank := "   "
			result, err := s.List(ctx, &rmstore.ServiceTypeInstanceListOptions{
				AgentName: &blank,
			})
			Expect(err).NotTo(HaveOccurred())
			// 3 base instances from the outer BeforeEach + the 2 created above.
			Expect(result.Instances).To(HaveLen(5))
		})

		It("combines service type and agent name filters with AND semantics", func() {
			vmType, containerType := "vm", "container"
			agentA, agentB := "agent-a", "agent-b"
			vmAgentA := newServiceTypeInstanceWithType("vm-agent-a", vmType, map[string]any{})
			vmAgentA.AgentName = &agentA
			addInstanceToStore(vmAgentA)
			vmAgentB := newServiceTypeInstanceWithType("vm-agent-b", vmType, map[string]any{})
			vmAgentB.AgentName = &agentB
			addInstanceToStore(vmAgentB)
			addInstanceToStore(newServiceTypeInstanceWithType("container-agent-a", containerType, map[string]any{}))

			result, err := s.List(ctx, &rmstore.ServiceTypeInstanceListOptions{
				ServiceType: &vmType,
				AgentName:   &agentA,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Instances).To(HaveLen(1))
			Expect(result.Instances[0].InstanceName).To(Equal("vm-agent-a"))
		})

		It("returns next page using page token", func() {
			// Get first page
			firstPage, err := s.List(ctx, &rmstore.ServiceTypeInstanceListOptions{
				PageSize: 2,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(firstPage.Instances).To(HaveLen(2))
			Expect(firstPage.NextPageToken).NotTo(BeNil())

			// Get second page using token
			secondPage, err := s.List(ctx, &rmstore.ServiceTypeInstanceListOptions{
				PageSize:  2,
				PageToken: firstPage.NextPageToken,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(secondPage.Instances).To(HaveLen(1))
			Expect(secondPage.NextPageToken).To(BeNil())
		})
	})

	Describe("HardDelete", func() {
		It("removes the instance", func() {
			instance := newServiceTypeInstance("to-delete", map[string]any{})
			addInstanceToStore(instance)

			Expect(s.HardDelete(ctx, instance.ID)).To(Succeed())

			_, err := s.Get(ctx, instance.ID, false)
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})

		It("returns ErrInstanceNotFound for missing ID", func() {
			err := s.HardDelete(ctx, uuid.New().String())
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})

		It("does not retry on permanent errors (not found)", func() {
			nonExistentID := uuid.New().String()
			err := s.HardDelete(ctx, nonExistentID)

			// Should return ErrInstanceNotFound immediately (permanent error)
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})
	})

	// AC-08 (TC-11): the plain, unconditional variant used by internal
	// (non-CE-driven) callers - no agent_name gating - must keep working
	// exactly as before this fix, independent of the *FromAgent sibling.
	Describe("MarkDeletionComplete", func() {
		It("sets deletion_status to DELETED regardless of agent_name", func() {
			instance := newServiceTypeInstance("soft-delete-plain", map[string]any{})
			addInstanceToStore(instance)

			Expect(s.MarkDeletionComplete(ctx, instance.ID)).To(Succeed())

			found, err := s.Get(ctx, instance.ID, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(*found.DeletionStatus).To(Equal("DELETED"))
		})

		It("returns ErrInstanceNotFound for a genuinely missing ID", func() {
			err := s.MarkDeletionComplete(ctx, uuid.New().String())
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})
	})

	Describe("UpdateStatus", func() {
		It("updates status and status message by instance ID", func() {
			instance := newServiceTypeInstance("status-inst", map[string]any{"cpu": "2"})
			addInstanceToStore(instance)

			err := s.UpdateStatus(ctx, instance.ID, "RUNNING", "VM is running", nil)
			Expect(err).NotTo(HaveOccurred())

			found, err := s.Get(ctx, instance.ID, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.Status).To(Equal("RUNNING"))
			Expect(found.StatusMessage).To(Equal("VM is running"))
		})

		It("returns ErrInstanceNotFound for non-existent instance", func() {
			err := s.UpdateStatus(ctx, "non-existent", "RUNNING", "message", nil)
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})

		It("persists output_spec when provided", func() {
			instance := newServiceTypeInstance("output-spec-inst", map[string]any{"cpu": "2"})
			addInstanceToStore(instance)

			outputSpec := map[string]any{"connection_string": "postgres://db:5432/orders"}
			err := s.UpdateStatus(ctx, instance.ID, "RUNNING", "VM is running", outputSpec)
			Expect(err).NotTo(HaveOccurred())

			found, err := s.Get(ctx, instance.ID, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.OutputSpec).To(Equal(outputSpec))
		})
	})

	Describe("UpdateStatusFrom", func() {
		It("transitions status only when current status and agent_name match", func() {
			agentName := "agent-a"
			instance := newServiceTypeInstance("cas-inst", map[string]any{"cpu": "2"})
			instance.AgentName = &agentName
			addInstanceToStore(instance)

			applied, err := s.UpdateStatusFrom(ctx, instance.ID, []string{"PROVISIONING"}, agentName, "RUNNING", "up")
			Expect(err).NotTo(HaveOccurred())
			Expect(applied).To(BeTrue())

			found, err := s.Get(ctx, instance.ID, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.Status).To(Equal("RUNNING"))
		})

		It("does not transition when current status does not match", func() {
			agentName := "agent-a"
			instance := newServiceTypeInstance("cas-inst-2", map[string]any{"cpu": "2"})
			instance.AgentName = &agentName
			addInstanceToStore(instance)

			applied, err := s.UpdateStatusFrom(ctx, instance.ID, []string{"RUNNING"}, agentName, "STOPPED", "down")
			Expect(err).NotTo(HaveOccurred())
			Expect(applied).To(BeFalse())

			found, err := s.Get(ctx, instance.ID, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.Status).To(Equal("PROVISIONING"))
		})

		// An agent_name mismatch rejects an otherwise-valid transition, even
		// though the status-only WHERE would have matched.
		It("does not transition when status matches but agent_name does not (identity check)", func() {
			currentAgent := "agent-b"
			instance := newServiceTypeInstance("cas-inst-3", map[string]any{"cpu": "2"})
			instance.Status = "PENDING"
			instance.AgentName = &currentAgent
			addInstanceToStore(instance)

			applied, err := s.UpdateStatusFrom(ctx, instance.ID, []string{"PENDING"}, "agent-a-stale", "PROVISIONING", "")
			Expect(err).NotTo(HaveOccurred())
			Expect(applied).To(BeFalse())

			found, err := s.Get(ctx, instance.ID, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.Status).To(Equal("PENDING"))
			Expect(*found.AgentName).To(Equal(currentAgent))
		})

		It("does not transition when the instance's agent_name is NULL (defensive: never treated as a match)", func() {
			instance := newServiceTypeInstance("cas-inst-4", map[string]any{"cpu": "2"})
			instance.Status = "PENDING"
			addInstanceToStore(instance)

			applied, err := s.UpdateStatusFrom(ctx, instance.ID, []string{"PENDING"}, "any-agent", "PROVISIONING", "")
			Expect(err).NotTo(HaveOccurred())
			Expect(applied).To(BeFalse())
		})
	})

	Describe("MarkQueued", func() {
		It("transitions pending to queued and resets pending_started_at when agent_name matches", func() {
			agentName := "agent-a"
			instance := newServiceTypeInstance("queue-inst", map[string]any{})
			instance.Status = "pending"
			instance.AgentName = &agentName
			addInstanceToStore(instance)

			Expect(s.MarkQueued(ctx, instance.ID, agentName)).To(Succeed())

			found, err := s.Get(ctx, instance.ID, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.Status).To(Equal("queued"))
			Expect(found.PendingStartedAt).NotTo(BeNil())
		})

		It("returns ErrInstanceNotFound when instance is not pending", func() {
			agentName := "agent-a"
			instance := newServiceTypeInstance("not-pending-inst", map[string]any{})
			instance.AgentName = &agentName
			addInstanceToStore(instance)

			err := s.MarkQueued(ctx, instance.ID, agentName)
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})

		// An agent_name mismatch rejects an otherwise-valid pending ->
		// queued transition, so a stale request-queued can't reset the
		// queued timer under a new agent.
		It("returns ErrInstanceNotFound when status matches but agent_name does not (identity check)", func() {
			currentAgent := "agent-b"
			instance := newServiceTypeInstance("queue-inst-mismatch", map[string]any{})
			instance.Status = "pending"
			instance.AgentName = &currentAgent
			addInstanceToStore(instance)

			err := s.MarkQueued(ctx, instance.ID, "agent-a-stale")
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))

			found, err := s.Get(ctx, instance.ID, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.Status).To(Equal("pending"))
		})
	})

	Describe("HardDeleteFromAgent", func() {
		It("removes the instance when agent_name matches", func() {
			agentName := "agent-a"
			instance := newServiceTypeInstance("to-delete-agent", map[string]any{})
			instance.AgentName = &agentName
			addInstanceToStore(instance)

			Expect(s.HardDeleteFromAgent(ctx, instance.ID, agentName)).To(Succeed())

			_, err := s.Get(ctx, instance.ID, false)
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})

		// TC-12: mismatch is rejected with the same sentinel as "not found" -
		// callers already treat that as "ack, don't retry" either way, and
		// the row must survive (not be deleted) since the event's agent no
		// longer owns it.
		It("returns ErrInstanceNotFound and leaves the row intact when agent_name does not match", func() {
			currentAgent := "agent-b"
			instance := newServiceTypeInstance("to-delete-mismatch", map[string]any{})
			instance.AgentName = &currentAgent
			addInstanceToStore(instance)

			err := s.HardDeleteFromAgent(ctx, instance.ID, "agent-a-stale")
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))

			found, getErr := s.Get(ctx, instance.ID, false)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(found.ID).To(Equal(instance.ID))
		})

		It("returns ErrInstanceNotFound for a genuinely missing ID", func() {
			err := s.HardDeleteFromAgent(ctx, uuid.New().String(), "agent-a")
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})
	})

	Describe("MarkDeletionCompleteFromAgent", func() {
		It("sets deletion_status to DELETED when agent_name matches", func() {
			agentName := "agent-a"
			instance := newServiceTypeInstance("soft-delete-agent", map[string]any{})
			instance.AgentName = &agentName
			addInstanceToStore(instance)

			Expect(s.MarkDeletionCompleteFromAgent(ctx, instance.ID, agentName)).To(Succeed())

			found, err := s.Get(ctx, instance.ID, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(*found.DeletionStatus).To(Equal("DELETED"))
		})

		// TC-12: mismatch is rejected with the same sentinel as "not found",
		// and does not touch deletion_status - the row must not be silently
		// tombstoned by an event from an agent that no longer owns it.
		It("returns ErrInstanceNotFound and leaves deletion_status untouched when agent_name does not match", func() {
			currentAgent := "agent-b"
			instance := newServiceTypeInstance("soft-delete-mismatch", map[string]any{})
			instance.AgentName = &currentAgent
			addInstanceToStore(instance)

			err := s.MarkDeletionCompleteFromAgent(ctx, instance.ID, "agent-a-stale")
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))

			found, getErr := s.Get(ctx, instance.ID, true)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(found.DeletionStatus).To(BeNil())
		})

		It("returns ErrInstanceNotFound for a genuinely missing ID", func() {
			err := s.MarkDeletionCompleteFromAgent(ctx, uuid.New().String(), "agent-a")
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})
	})

	Describe("ReassignAndReset", func() {
		It("re-points agent_name and resets to a fresh pending state, preserving retry_count", func() {
			instance := newServiceTypeInstanceWithAgent("reassign-inst", "old-agent", map[string]any{})
			instance.Status = model.StatusPending
			instance.RetryCount = 2
			addInstanceToStore(instance)

			Expect(s.ReassignAndReset(ctx, instance.ID, "new-agent", "old-agent")).To(Succeed())

			found, err := s.Get(ctx, instance.ID, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.Status).To(Equal("pending"))
			// retry_count is NOT reset: it's cumulative across every agent
			// tried, so maxRetries is still enforced globally.
			Expect(found.RetryCount).To(Equal(2))
			Expect(found.AgentName).NotTo(BeNil())
			Expect(*found.AgentName).To(Equal("new-agent"))
			Expect(found.PendingStartedAt).NotTo(BeNil())
		})

		It("returns ErrInstanceNotFound for missing ID", func() {
			err := s.ReassignAndReset(ctx, uuid.New().String(), "new-agent", "old-agent")
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})

		It("returns ErrInstanceNotEligible when expectedCurrentAgent no longer matches (lost the race to a concurrent reassignment)", func() {
			// Guards the fix for a real cross-replica race: two callers
			// (e.g. a sweep-claimed primary heal and a sibling self-heal
			// from a different resource in the same run) could otherwise
			// both pass a status-only CAS and both publish a create to a
			// different agent for the same instance, since status stays
			// "pending" across a successful reassignment.
			instance := newServiceTypeInstanceWithAgent("reassign-stale-agent", "agent-that-already-moved-on", map[string]any{})
			instance.Status = model.StatusPending
			addInstanceToStore(instance)

			err := s.ReassignAndReset(ctx, instance.ID, "new-agent", "agent-the-caller-still-thinks-is-current")
			Expect(err).To(MatchError(rmstore.ErrInstanceNotEligible))

			found, getErr := s.Get(ctx, instance.ID, false)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(*found.AgentName).To(Equal("agent-that-already-moved-on"))
		})

		It("returns ErrInstanceNotEligible when the instance is mid-deletion", func() {
			instance := newServiceTypeInstanceWithAgent("reassign-deleting", "old-agent", map[string]any{})
			instance.Status = model.StatusDeleting
			addInstanceToStore(instance)

			err := s.ReassignAndReset(ctx, instance.ID, "new-agent", "old-agent")
			Expect(err).To(MatchError(rmstore.ErrInstanceNotEligible))

			found, getErr := s.Get(ctx, instance.ID, true)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(found.Status).To(Equal(model.StatusDeleting))
		})

		It("returns ErrInstanceNotEligible for a pending_deletion instance", func() {
			instance := newServiceTypeInstanceWithAgent("reassign-pending-deletion", "old-agent", map[string]any{})
			instance.Status = model.StatusPendingDeletion
			addInstanceToStore(instance)

			err := s.ReassignAndReset(ctx, instance.ID, "new-agent", "old-agent")
			Expect(err).To(MatchError(rmstore.ErrInstanceNotEligible))
		})

		It("returns ErrInstanceNotEligible for a provisioning instance (split-brain: agent A may already be actively provisioning)", func() {
			// Simulates: sweep claims a pending-timeout retry, then before
			// selfHeal runs, the response consumer applies a genuine
			// creation-acknowledged from the ORIGINAL agent (pending ->
			// provisioning). ReassignAndReset must not silently re-point a
			// "provisioning" instance onto a second agent - that would mean
			// two agents both believe they own provisioning for the same
			// instance.
			originalAgent := "original-agent"
			instance := newServiceTypeInstance("reassign-provisioning", map[string]any{})
			instance.Status = model.StatusProvisioning
			instance.AgentName = &originalAgent
			addInstanceToStore(instance)

			err := s.ReassignAndReset(ctx, instance.ID, "new-agent", originalAgent)
			Expect(err).To(MatchError(rmstore.ErrInstanceNotEligible))

			found, getErr := s.Get(ctx, instance.ID, true)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(found.Status).To(Equal(model.StatusProvisioning))
			Expect(found.AgentName).NotTo(BeNil())
			Expect(*found.AgentName).To(Equal(originalAgent))
		})

		It("allows reassigning a cancelled instance (cancelQueuedInstance's self-heal path)", func() {
			instance := newServiceTypeInstanceWithAgent("reassign-cancelled", "old-agent", map[string]any{})
			instance.Status = model.StatusCancelled
			addInstanceToStore(instance)

			Expect(s.ReassignAndReset(ctx, instance.ID, "new-agent", "old-agent")).To(Succeed())

			found, err := s.Get(ctx, instance.ID, false)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.Status).To(Equal(model.StatusPending))
			Expect(*found.AgentName).To(Equal("new-agent"))
		})

		It("returns ErrInstanceNotEligible for a queued instance (not yet cancelled by the sweep's CAS)", func() {
			instance := newServiceTypeInstanceWithAgent("reassign-queued", "old-agent", map[string]any{})
			instance.Status = model.StatusQueued
			addInstanceToStore(instance)

			err := s.ReassignAndReset(ctx, instance.ID, "new-agent", "old-agent")
			Expect(err).To(MatchError(rmstore.ErrInstanceNotEligible))
		})

		It("returns ErrInstanceNotEligible for a pending instance with a delete already scheduled against it (R2 S2: finding #1)", func() {
			// A deferred DeleteInstance never touches Status, so a "pending"
			// instance can have deletion_status=SCHEDULED. sweepPending's own
			// filter is the primary defense, but this store method must not
			// rely solely on its one caller to enforce that - it should
			// refuse to resurrect/reassign such an instance no matter who
			// calls it.
			deletionStatus := "SCHEDULED"
			instance := newServiceTypeInstanceWithAgent("reassign-delete-scheduled", "old-agent", map[string]any{})
			instance.Status = model.StatusPending
			instance.DeletionStatus = &deletionStatus
			addInstanceToStore(instance)

			err := s.ReassignAndReset(ctx, instance.ID, "new-agent", "old-agent")
			Expect(err).To(MatchError(rmstore.ErrInstanceNotEligible))

			found, getErr := s.Get(ctx, instance.ID, true)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(found.Status).To(Equal(model.StatusPending))
			Expect(found.DeletionStatus).NotTo(BeNil())
			Expect(*found.DeletionStatus).To(Equal("SCHEDULED"))
		})
	})

	Describe("MarkForDeletion", func() {
		It("sets deletion_status to SCHEDULED", func() {
			instance := newServiceTypeInstance("mark-del", map[string]any{})
			addInstanceToStore(instance)

			Expect(s.MarkForDeletion(ctx, instance.ID, false)).To(Succeed())

			found, err := s.Get(ctx, instance.ID, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(*found.DeletionStatus).To(Equal("SCHEDULED"))
			Expect(found.RetryCount).To(Equal(0))
			Expect(found.DeletionRequestedAt).NotTo(BeNil())
		})

		It("hides instance from default Get", func() {
			instance := newServiceTypeInstance("mark-hidden", map[string]any{})
			addInstanceToStore(instance)

			Expect(s.MarkForDeletion(ctx, instance.ID, false)).To(Succeed())

			_, err := s.Get(ctx, instance.ID, false)
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})

		It("returns ErrInstanceNotFound for missing ID", func() {
			err := s.MarkForDeletion(ctx, uuid.New().String(), false)
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})
	})

	Describe("ListPendingDeletions", func() {
		It("returns only SCHEDULED instances", func() {
			inst1 := addInstanceToStore(newServiceTypeInstance("pending1", map[string]any{}))
			inst2 := addInstanceToStore(newServiceTypeInstance("pending2", map[string]any{}))
			addInstanceToStore(newServiceTypeInstance("active", map[string]any{}))

			Expect(s.MarkForDeletion(ctx, inst1.ID, false)).To(Succeed())
			Expect(s.MarkForDeletion(ctx, inst2.ID, false)).To(Succeed())

			pending, err := s.ListPendingDeletions(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(pending).To(HaveLen(2))
		})

		It("excludes FAILED instances", func() {
			inst := addInstanceToStore(newServiceTypeInstance("failed", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, false)).To(Succeed())
			Expect(s.MarkDeletionFailed(ctx, inst.ID)).To(Succeed())

			pending, err := s.ListPendingDeletions(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(pending).To(BeEmpty())
		})

		It("excludes DELETED instances", func() {
			inst := addInstanceToStore(newServiceTypeInstance("deleted", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, false)).To(Succeed())
			Expect(s.MarkDeletionComplete(ctx, inst.ID)).To(Succeed())

			pending, err := s.ListPendingDeletions(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(pending).To(BeEmpty())
		})

		It("returns empty when no pending deletions exist", func() {
			addInstanceToStore(newServiceTypeInstance("active", map[string]any{}))

			pending, err := s.ListPendingDeletions(ctx)
			Expect(err).NotTo(HaveOccurred())
			Expect(pending).To(BeEmpty())
		})
	})

	Describe("IncrementDeletionRetry", func() {
		It("increments retry count and sets last_deletion_attempt", func() {
			inst := addInstanceToStore(newServiceTypeInstance("retry-inst", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, false)).To(Succeed())

			Expect(s.IncrementDeletionRetry(ctx, inst.ID)).To(Succeed())

			found, err := s.Get(ctx, inst.ID, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.RetryCount).To(Equal(1))
			Expect(found.LastDeletionAttempt).NotTo(BeNil())

			Expect(s.IncrementDeletionRetry(ctx, inst.ID)).To(Succeed())

			found, err = s.Get(ctx, inst.ID, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.RetryCount).To(Equal(2))
		})

		It("returns ErrInstanceNotFound for missing ID", func() {
			err := s.IncrementDeletionRetry(ctx, uuid.New().String())
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})
	})

	Describe("MarkDeletionFailed", func() {
		It("sets deletion_status to FAILED", func() {
			inst := addInstanceToStore(newServiceTypeInstance("fail-inst", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, false)).To(Succeed())

			Expect(s.MarkDeletionFailed(ctx, inst.ID)).To(Succeed())

			found, err := s.Get(ctx, inst.ID, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(*found.DeletionStatus).To(Equal("FAILED"))
		})

		It("returns ErrInstanceNotFound for missing ID", func() {
			err := s.MarkDeletionFailed(ctx, uuid.New().String())
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})

		It("does not overwrite DELETED with FAILED", func() {
			inst := addInstanceToStore(newServiceTypeInstance("fail-after-deleted", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, false)).To(Succeed())
			Expect(s.MarkDeletionComplete(ctx, inst.ID)).To(Succeed())

			Expect(s.MarkDeletionFailed(ctx, inst.ID)).To(Succeed())

			found, err := s.Get(ctx, inst.ID, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(*found.DeletionStatus).To(Equal("DELETED"))
		})
	})

	Describe("ResetRetryCount", func() {
		It("resets retry count and status to SCHEDULED", func() {
			inst := addInstanceToStore(newServiceTypeInstance("reset-inst", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, false)).To(Succeed())
			Expect(s.IncrementDeletionRetry(ctx, inst.ID)).To(Succeed())
			Expect(s.IncrementDeletionRetry(ctx, inst.ID)).To(Succeed())
			Expect(s.MarkDeletionFailed(ctx, inst.ID)).To(Succeed())

			Expect(s.ResetRetryCount(ctx, inst.ID, false)).To(Succeed())

			found, err := s.Get(ctx, inst.ID, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(*found.DeletionStatus).To(Equal("SCHEDULED"))
			Expect(found.RetryCount).To(Equal(0))
			Expect(found.LastDeletionAttempt).To(BeNil())
		})

		It("returns ErrInstanceNotFound for missing ID", func() {
			err := s.ResetRetryCount(ctx, uuid.New().String(), false)
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})
	})

	Describe("FinalizeDeletionAcknowledged", func() {
		agentName := "agent-a"

		It("hard-deletes when still-pending (SCHEDULED) and hard_delete=true and agent_name matches", func() {
			inst := addInstanceToStore(newServiceTypeInstanceWithAgent("finalize-hard", agentName, map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, true)).To(Succeed())

			finalized, hardDeleted, wasFailed, err := s.FinalizeDeletionAcknowledged(ctx, inst.ID, agentName)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeTrue())
			Expect(hardDeleted).To(BeTrue())
			Expect(wasFailed).To(BeFalse())

			_, getErr := s.Get(ctx, inst.ID, true)
			Expect(getErr).To(MatchError(rmstore.ErrInstanceNotFound))
		})

		It("soft-completes when still-pending (SCHEDULED) and hard_delete=false", func() {
			inst := addInstanceToStore(newServiceTypeInstanceWithAgent("finalize-soft", agentName, map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, false)).To(Succeed())

			finalized, hardDeleted, wasFailed, err := s.FinalizeDeletionAcknowledged(ctx, inst.ID, agentName)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeTrue())
			Expect(hardDeleted).To(BeFalse())
			Expect(wasFailed).To(BeFalse())

			found, getErr := s.Get(ctx, inst.ID, true)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(*found.DeletionStatus).To(Equal("DELETED"))
		})

		// REQ-DEL-10: FAILED is still-pending, not terminal.
		It("finalizes and reports wasFailed=true when the row is FAILED (retries exhausted)", func() {
			inst := addInstanceToStore(newServiceTypeInstanceWithAgent("finalize-failed", agentName, map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, true)).To(Succeed())
			Expect(s.MarkDeletionFailed(ctx, inst.ID)).To(Succeed())

			finalized, hardDeleted, wasFailed, err := s.FinalizeDeletionAcknowledged(ctx, inst.ID, agentName)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeTrue())
			Expect(hardDeleted).To(BeTrue())
			Expect(wasFailed).To(BeTrue())
		})

		It("does not finalize when agent_name does not match, row untouched", func() {
			inst := addInstanceToStore(newServiceTypeInstanceWithAgent("finalize-mismatch", agentName, map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, true)).To(Succeed())

			finalized, _, _, err := s.FinalizeDeletionAcknowledged(ctx, inst.ID, "agent-b-stale")
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeFalse())

			found, getErr := s.Get(ctx, inst.ID, true)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(*found.DeletionStatus).To(Equal("SCHEDULED"))
		})

		It("does not finalize when the instance is not still-pending (already DELETED)", func() {
			inst := addInstanceToStore(newServiceTypeInstanceWithAgent("finalize-already-done", agentName, map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, false)).To(Succeed())
			Expect(s.MarkDeletionComplete(ctx, inst.ID)).To(Succeed())

			finalized, _, _, err := s.FinalizeDeletionAcknowledged(ctx, inst.ID, agentName)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeFalse())
		})

		It("does not finalize and returns no error for a genuinely missing ID", func() {
			finalized, _, _, err := s.FinalizeDeletionAcknowledged(ctx, uuid.New().String(), agentName)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeFalse())
		})

		// REQ-DEL-11 AC-1: decision must use the CURRENT hard_delete value.
		It("resolves according to the current hard_delete value, not a stale one from before a concurrent ResetRetryCount", func() {
			inst := addInstanceToStore(newServiceTypeInstanceWithAgent("finalize-race", agentName, map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, true)).To(Succeed())

			// Simulates a concurrent re-delete landing before this finalize call.
			Expect(s.ResetRetryCount(ctx, inst.ID, false)).To(Succeed())

			finalized, hardDeleted, _, err := s.FinalizeDeletionAcknowledged(ctx, inst.ID, agentName)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeTrue())
			Expect(hardDeleted).To(BeFalse())

			found, getErr := s.Get(ctx, inst.ID, true)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(*found.DeletionStatus).To(Equal("DELETED"))
		})
	})

	Describe("FinalizeAuditGiveUp", func() {
		It("hard-deletes when SCHEDULED and hard_delete=true", func() {
			inst := addInstanceToStore(newServiceTypeInstance("giveup-hard", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, true)).To(Succeed())

			finalized, hardDeleted, err := s.FinalizeAuditGiveUp(ctx, inst.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeTrue())
			Expect(hardDeleted).To(BeTrue())

			_, getErr := s.Get(ctx, inst.ID, true)
			Expect(getErr).To(MatchError(rmstore.ErrInstanceNotFound))
		})

		It("soft-completes when SCHEDULED and hard_delete=false", func() {
			inst := addInstanceToStore(newServiceTypeInstance("giveup-soft", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, false)).To(Succeed())

			finalized, hardDeleted, err := s.FinalizeAuditGiveUp(ctx, inst.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeTrue())
			Expect(hardDeleted).To(BeFalse())

			found, getErr := s.Get(ctx, inst.ID, true)
			Expect(getErr).NotTo(HaveOccurred())
			Expect(*found.DeletionStatus).To(Equal("DELETED"))
		})

		It("does not finalize when the instance is no longer SCHEDULED (already FAILED)", func() {
			inst := addInstanceToStore(newServiceTypeInstance("giveup-failed", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, true)).To(Succeed())
			Expect(s.MarkDeletionFailed(ctx, inst.ID)).To(Succeed())

			finalized, _, err := s.FinalizeAuditGiveUp(ctx, inst.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeFalse())
		})

		It("does not finalize and returns no error for a genuinely missing ID", func() {
			finalized, _, err := s.FinalizeAuditGiveUp(ctx, uuid.New().String())
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeFalse())
		})

		// REQ-DEL-11 AC-1: same race-freedom proof, no-agent-gate path.
		It("resolves according to the current hard_delete value, not a stale one from before a concurrent ResetRetryCount", func() {
			inst := addInstanceToStore(newServiceTypeInstance("giveup-race", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, true)).To(Succeed())

			Expect(s.ResetRetryCount(ctx, inst.ID, false)).To(Succeed())

			finalized, hardDeleted, err := s.FinalizeAuditGiveUp(ctx, inst.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(finalized).To(BeTrue())
			Expect(hardDeleted).To(BeFalse())
		})
	})

	Describe("Get with showDeleted", func() {
		It("returns soft-deleted instance when showDeleted is true", func() {
			inst := addInstanceToStore(newServiceTypeInstance("soft-del", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, false)).To(Succeed())

			found, err := s.Get(ctx, inst.ID, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.ID).To(Equal(inst.ID))
			Expect(*found.DeletionStatus).To(Equal("SCHEDULED"))
		})

		It("returns DELETED instance when showDeleted is true", func() {
			inst := addInstanceToStore(newServiceTypeInstance("soft-del-complete", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, false)).To(Succeed())
			Expect(s.MarkDeletionComplete(ctx, inst.ID)).To(Succeed())

			found, err := s.Get(ctx, inst.ID, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.ID).To(Equal(inst.ID))
			Expect(*found.DeletionStatus).To(Equal("DELETED"))
		})

		It("returns FAILED instance when showDeleted is true", func() {
			inst := addInstanceToStore(newServiceTypeInstance("soft-del-failed", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, false)).To(Succeed())
			Expect(s.MarkDeletionFailed(ctx, inst.ID)).To(Succeed())

			found, err := s.Get(ctx, inst.ID, true)
			Expect(err).NotTo(HaveOccurred())
			Expect(found.ID).To(Equal(inst.ID))
			Expect(*found.DeletionStatus).To(Equal("FAILED"))
		})

		It("returns not found for soft-deleted instance when showDeleted is false", func() {
			inst := addInstanceToStore(newServiceTypeInstance("soft-del2", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, inst.ID, false)).To(Succeed())

			_, err := s.Get(ctx, inst.ID, false)
			Expect(err).To(MatchError(rmstore.ErrInstanceNotFound))
		})
	})

	Describe("List with ShowDeleted", func() {
		It("excludes soft-deleted instances by default", func() {
			addInstanceToStore(newServiceTypeInstance("active", map[string]any{}))
			deleted := addInstanceToStore(newServiceTypeInstance("deleted", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, deleted.ID, false)).To(Succeed())

			result, err := s.List(ctx, &rmstore.ServiceTypeInstanceListOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Instances).To(HaveLen(1))
		})

		It("includes soft-deleted instances when ShowDeleted is true", func() {
			addInstanceToStore(newServiceTypeInstance("active", map[string]any{}))
			deleted := addInstanceToStore(newServiceTypeInstance("deleted", map[string]any{}))
			Expect(s.MarkForDeletion(ctx, deleted.ID, false)).To(Succeed())

			result, err := s.List(ctx, &rmstore.ServiceTypeInstanceListOptions{ShowDeleted: true})
			Expect(err).NotTo(HaveOccurred())
			Expect(result.Instances).To(HaveLen(2))
		})
	})

	Describe("ExistsByID", func() {
		It("returns true when instance exists", func() {
			instance := newServiceTypeInstance("exists", map[string]any{})
			addInstanceToStore(instance)

			exists, err := s.ExistsByID(ctx, instance.ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeTrue())
		})

		It("returns false when instance is missing", func() {
			exists, err := s.ExistsByID(ctx, uuid.New().String())
			Expect(err).NotTo(HaveOccurred())
			Expect(exists).To(BeFalse())
		})
	})
})
