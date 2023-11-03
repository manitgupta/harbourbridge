package converters

import (
	"context"
	"time"

	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
	workflowModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/workflows"
)

func GenerateValidateJobConfigActivityInput(createJobWorkflowInput workflowModel.CreateJobWorkflowInput, createConvActivityFileOutput activityModel.CreateConvActivityOutput) (activityModel.ValidateJobConfigActivityInput, error) {
	_, _, dbName, err := createJobWorkflowInput.TargetDetails.GetResourceIds(context.Background(), time.Now(), "", nil)
	if err != nil {
		return activityModel.ValidateJobConfigActivityInput{}, err
	}
	return activityModel.ValidateJobConfigActivityInput{
		Conv:         createConvActivityFileOutput.Conv,
		TargetDbName: dbName,
		StreamingCfg: createJobWorkflowInput.StreamingCfg,
	}, nil
}
