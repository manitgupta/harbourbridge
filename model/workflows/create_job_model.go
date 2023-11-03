package workflows

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
)

type CreateJobWorkflowInput struct {
	JobId              string
	StreamingCfg       streaming.StreamingCfg
	Conv               *internal.Conv
	TargetDetails      profiles.TargetProfile
	SourceDatabaseName string
	SourceDatabaseType string
}

type CreateJobWorkflowOutput struct {
	WorkflowStatus string
}
