package workflows

import "github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"

type CreateJobWorkflowInput struct {
	JobId           string
	JobConfigFilePath string
	SessionFilePath string
	TargetDetails  profiles.TargetProfile
	SourceDatabaseName string
	SourceDatabaseType string
}

type CreateJobWorkflowOutput struct {
}
