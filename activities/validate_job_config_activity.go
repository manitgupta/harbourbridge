package activities

import (
	"context"
	"fmt"

	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
)

func ValidateJobConfigActivity(ctx context.Context, input activityModel.ValidateJobConfigActivityInput) (activityModel.ValidateJobConfigActivityOutput, error) {
	tableList, err := common.GetIncludedSrcTablesFromConv(input.Conv)
	if err != nil {
		return activityModel.ValidateJobConfigActivityOutput{}, fmt.Errorf("error fetching tables from conv: %v", err)
	}
	fmt.Printf("Fetched table list in activity: %v\n", tableList)
	streamingCfg, err := streaming.ValidateStreamingConfig(&input.StreamingCfg, input.TargetDbName, tableList)
	if err != nil {
		return activityModel.ValidateJobConfigActivityOutput{}, fmt.Errorf("error reading streaming config: %v", err)
	}
	return activityModel.ValidateJobConfigActivityOutput{StreamingCfg: *streamingCfg}, nil
}
