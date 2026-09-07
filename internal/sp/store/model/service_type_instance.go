// Package model defines database models for the service provider store.
package model

import (
	"time"
)

type ServiceTypeInstance struct {
	ID          string `gorm:"primaryKey;type:varchar(63)"`
	ServiceType string `gorm:"column:service_type;not null;default:'';index"`
	// idx_sti_status_pending backs the periodic sweepPending/sweepQueued
	// hot-path queries (status = ? AND pending_started_at < ?), which
	// without it would degrade to a full table scan on every sweep tick as
	// the instance count grows.
	Status        string         `gorm:"column:status;not null;index:idx_sti_status_pending,priority:1"`
	StatusMessage string         `gorm:"column:status_message"`
	OutputSpec    map[string]any `gorm:"column:output_spec;type:jsonb;serializer:json"`
	InstanceName  string         `gorm:"column:instance_name;not null"`
	Spec          map[string]any `gorm:"column:spec;type:jsonb;serializer:json;not null"`
	CreateTime    time.Time      `gorm:"column:create_time;autoCreateTime"`
	UpdateTime    time.Time      `gorm:"column:update_time;autoUpdateTime"`

	// AgentName is a plain string reference to the owning agent's natural key.
	// Intentionally NOT a GORM foreign key: agents can be deregistered while
	// instances still exist (orphans are tolerated and resolved by the
	// cleanup scheduler). Application-level validation (see
	// InstanceService.validateAgent) enforces the agent exists and is ready
	// before this field is ever set.
	AgentName        *string    `gorm:"column:agent_name"`
	PendingStartedAt *time.Time `gorm:"column:pending_started_at;index:idx_sti_status_pending,priority:2"`

	// Soft-delete fields for deferred deletion (rehydration flow)
	//
	// deletion_status is indexed for ListPendingDeletions (WHERE
	// deletion_status = 'SCHEDULED') and the default Get/List visibility
	// filter (WHERE deletion_status IS NULL), both hot paths run on every
	// cleanup-scheduler tick and API list call respectively.
	DeletionStatus      *string    `gorm:"column:deletion_status;index"`
	RetryCount          int        `gorm:"column:retry_count;default:0"`
	LastDeletionAttempt *time.Time `gorm:"column:last_deletion_attempt"`
	DeletionRequestedAt *time.Time `gorm:"column:deletion_requested_at"`

	// HardDelete records the deletion mode at enrollment time (true =
	// purge on ack, false = tombstone), so ack/scheduler handling never
	// depends on the racy post-publish status=deleting write.
	HardDelete bool `gorm:"column:hard_delete;not null;default:false"`
}

type ServiceTypeInstanceList []ServiceTypeInstance
