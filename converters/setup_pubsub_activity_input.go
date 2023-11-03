package converters

import (
	"context"
	"time"

	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
	workflowModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/workflows"
)

func GenerateSetupPubSubActivityInput(createJobWorkflowInput workflowModel.CreateJobWorkflowInput, validateJobConfigActivityOutput activityModel.ValidateJobConfigActivityOutput) (activityModel.SetupPubSubActivityInput, error) {
	project, _, _, err := createJobWorkflowInput.TargetDetails.GetResourceIds(context.Background(), time.Now(), "", nil)
	if err != nil {
		return activityModel.SetupPubSubActivityInput{}, err
	}
	return activityModel.SetupPubSubActivityInput{
		GcpProjectId:         project,
		SourceDbName:         createJobWorkflowInput.SourceDatabaseName,
		GcsConnectionProfile: validateJobConfigActivityOutput.StreamingCfg.DatastreamCfg.DestinationConnectionConfig,
		StreamingCfg: validateJobConfigActivityOutput.StreamingCfg,
	}, nil
}
