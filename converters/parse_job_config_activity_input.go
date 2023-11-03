package converters

import (
	"context"
	"time"

	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
	workflowModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/workflows"
)

func GenerateParseJobConfigActivityInput(createJobWorkflowInput workflowModel.CreateJobWorkflowInput, createConvActivityFileOutput activityModel.CreateConvActivityOutput) (activityModel.ParseJobConfigActivityInput, error) {
	_, _, dbName, err := createJobWorkflowInput.TargetDetails.GetResourceIds(context.Background(), time.Now(), "", nil)
	if err != nil {
		return activityModel.ParseJobConfigActivityInput{}, err
	}
	return activityModel.ParseJobConfigActivityInput{
		Conv: createConvActivityFileOutput.Conv,
		TargetDbName: dbName,
		JobConfigFilePath: createJobWorkflowInput.JobConfigFilePath,
	}, nil
}
