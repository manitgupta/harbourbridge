package activities

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
)

type ValidateJobConfigActivityInput struct {
	StreamingCfg streaming.StreamingCfg
	TargetDbName string
	Conv         *internal.Conv
}

type ValidateJobConfigActivityOutput struct {
	StreamingCfg streaming.StreamingCfg
}
