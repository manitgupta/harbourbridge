package converters

import (
	"context"
	"time"

	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
	workflowModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/workflows"
)

func GenerateSetupDatastreamActivityInput(createJobWorkflowInput workflowModel.CreateJobWorkflowInput, parseJobConfigActivityOutput activityModel.ParseJobConfigActivityOutput) (activityModel.SetupDatastreamActivityInput, error) {
	project, _, _, err := createJobWorkflowInput.TargetDetails.GetResourceIds(context.Background(), time.Now(), "", nil)
	if err != nil {
		return activityModel.SetupDatastreamActivityInput{}, err
	}
	return activityModel.SetupDatastreamActivityInput{
		SourceDatabaseName: createJobWorkflowInput.SourceDatabaseName,
		GcpProjectId:       project,
		DatastreamCfg:      parseJobConfigActivityOutput.StreamingCfg.DatastreamCfg,
	}, nil
}
