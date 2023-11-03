package activities

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
)

type SetupDataflowActivityInput struct {
	Conv         *internal.Conv
	GcpProjectId string
	StreamingCfg streaming.StreamingCfg
	TargetDetails profiles.TargetProfile
}
type SetupDataflowActivityOutput struct{
	JobId string
	GCloudCmd string
}
