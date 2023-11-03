package converters

import (
	"context"
	"time"

	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
	workflowModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/workflows"
)

func GenerateSetupDataflowActivityInput(createJobWorkflowInput workflowModel.CreateJobWorkflowInput, createConvActivityFileOutput activityModel.CreateConvActivityOutput, setupPubSubActivityOutput activityModel.SetupPubSubActivityOutput) (activityModel.SetupDataflowActivityInput, error) {
	project, _, _, err := createJobWorkflowInput.TargetDetails.GetResourceIds(context.Background(), time.Now(), "", nil)
	if err != nil {
		return activityModel.SetupDataflowActivityInput{}, err
	}
	return activityModel.SetupDataflowActivityInput{
		Conv:          createConvActivityFileOutput.Conv,
		GcpProjectId:  project,
		StreamingCfg:  setupPubSubActivityOutput.StreamingCfg,
		TargetDetails: createJobWorkflowInput.TargetDetails,
	}, nil
}
