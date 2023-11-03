package activities

import (
	"context"
	"fmt"

	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/sources/common"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
)

func ParseJobConfigActivity(ctx context.Context, input activityModel.ParseJobConfigActivityInput) (activityModel.ParseJobConfigActivityOutput, error) {
	tableList, err := common.GetIncludedSrcTablesFromConv(input.Conv)
	streamingCfg, err := streaming.ReadStreamingConfig(input.JobConfigFilePath, input.TargetDbName, tableList)
	if err != nil {
		return activityModel.ParseJobConfigActivityOutput{}, fmt.Errorf("error reading streaming config: %v", err)
	}
	return activityModel.ParseJobConfigActivityOutput{StreamingCfg: streamingCfg}, nil
}
