package activities

import (
	"context"
	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
)

func CreateConvActivity(ctx context.Context, input activityModel.CreateConvActivityInput) (activityModel.CreateConvActivityOutput, error) {
	return activityModel.CreateConvActivityOutput{Conv: input.Conv}, nil
}
