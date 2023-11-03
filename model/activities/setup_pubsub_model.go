package activities

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
)

type SetupPubSubActivityInput struct {
	GcpProjectId         string
	GcsConnectionProfile streaming.DstConnCfg
	SourceDbName         string
	StreamingCfg streaming.StreamingCfg
}
type SetupPubSubActivityOutput struct{
	streaming.StreamingCfg
}
