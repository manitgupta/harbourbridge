package activities

import "github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"

type SetupDatastreamActivityInput struct {
	DatastreamCfg streaming.DatastreamCfg
	SourceDatabaseName string
	GcpProjectId string
	
}
type SetupDatastreamActivityOutput struct{}
