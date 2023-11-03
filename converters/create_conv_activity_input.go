package converters

import (
	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
	workflowModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/workflows"
)

func GenerateCreateConvActivityInput(createJobWorkflowInput workflowModel.CreateJobWorkflowInput) (activityModel.CreateConvActivityInput, error) {
	return activityModel.CreateConvActivityInput{
		SessionFilePath: createJobWorkflowInput.SessionFilePath,
	}, nil
}
