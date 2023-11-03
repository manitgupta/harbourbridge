package converters

import (
	"context"
	"fmt"
	"time"

	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
	workflowModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/workflows"
)

func GenerateValidateSchemaActivityInput(createJobWorkflowInput workflowModel.CreateJobWorkflowInput, createConvActivityFileOutput activityModel.CreateConvActivityOutput) (activityModel.ValidateSchemaActivityInput, error) {
	project, instance, dbName, err := createJobWorkflowInput.TargetDetails.GetResourceIds(context.Background(), time.Now(), "", nil)
	if err != nil {
		return activityModel.ValidateSchemaActivityInput{}, err
	}
	return activityModel.ValidateSchemaActivityInput{
		Conv: createConvActivityFileOutput.Conv,
		DbURI: fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName),
	}, nil
}
