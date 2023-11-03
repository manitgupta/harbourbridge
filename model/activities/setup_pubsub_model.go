package activities

import (
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
)

type SetupPubSubActivityInput struct {
	GcpProjectId         string
	GcsConnectionProfile streaming.DstConnCfg
	SourceDbName         string
}
type SetupPubSubActivityOutput struct{
	PubsubCfg internal.PubsubCfg
}
