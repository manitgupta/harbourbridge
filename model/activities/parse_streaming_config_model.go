package activities

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
)

type ParseJobConfigActivityInput struct {
	JobConfigFilePath string
	TargetDbName      string
	Conv *internal.Conv
}

type ParseJobConfigActivityOutput struct {
	StreamingCfg streaming.StreamingCfg
}
