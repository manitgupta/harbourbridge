package activities

import (
	"context"
	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/utils"
)

func CreateConvActivity(ctx context.Context, input activityModel.CreateConvActivityInput) (activityModel.CreateConvActivityOutput, error) {
	conv, err := utils.ReadSessionFile(input.SessionFilePath)
	if err != nil {
		return activityModel.CreateConvActivityOutput{}, err
	}
	return activityModel.CreateConvActivityOutput{Conv: conv}, nil
}
