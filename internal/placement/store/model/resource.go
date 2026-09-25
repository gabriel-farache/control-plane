// Package model defines the database models for resource storage.
package model

import (
	"time"

	"github.com/dcm-project/control-plane/internal/placement/types"
)

// Resource represents a resource provisioning request within a placement run.
//
// AgentName is a soft reference to an agent by name, not a GORM-level
// foreign key: an earlier version declared one with constraint:OnDelete:RESTRICT,
// which meant an agent could never be deregistered/deleted while any
// resource (including old, terminal ones) still referenced its name - the
// same class of problem addressed for ServiceTypeInstance.AgentName (F4).
// Agent existence/health is validated at the application layer instead
// (see placement/service), same as the sp domain.
type Resource struct {
	ID                       string                   `gorm:"primaryKey;type:varchar(63)"`
	RunID                    string                   `gorm:"column:run_id;type:varchar(63);index;not null"`
	CatalogItemInstanceId    string                   `gorm:"column:catalog_item_instance_id;not null"`
	Name                     string                   `gorm:"column:name;type:varchar(63);not null"`
	Spec                     map[string]any           `gorm:"column:original_spec;type:jsonb;serializer:json;not null"`
	RequiresResources        []string                 `gorm:"column:requires_resources;type:jsonb;serializer:json"`
	DagLevel                 int                      `gorm:"column:dag_level;not null;default:0"`
	Status                   string                   `gorm:"column:status;type:varchar(63);not null;default:PENDING"`
	CleanupIntent            string                   `gorm:"column:cleanup_intent;type:varchar(31);not null;default:''"`
	AgentName                *string                  `gorm:"column:agent_name"`
	AgentErrorDetails        *types.AgentErrorDetails `gorm:"column:agent_error_details;type:jsonb;serializer:json"`
	AgentErrorStreamSequence uint64                   `gorm:"column:agent_error_stream_sequence;not null;default:0"`
	ApprovalStatus           *string                  `gorm:"column:approval_status;not null"`
	Path                     string                   `gorm:"column:path;not null"`
	CreateTime               time.Time                `gorm:"column:create_time;autoCreateTime"`
	UpdateTime               time.Time                `gorm:"column:update_time;autoUpdateTime"`
}

// ResourceList is a list of requests
type ResourceList []Resource
