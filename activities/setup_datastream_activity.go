package activities

import (
	"context"
	"fmt"

	datastreamAccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/datastream"
	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/utils"
)

func SetupDatastreamActivity(ctx context.Context, input activityModel.SetupDatastreamActivityInput) (activityModel.SetupDataflowActivityOutput, error) {
	fmt.Println("Launching stream ", fmt.Sprintf("projects/%s/locations/%s", input.GcpProjectId, input.DatastreamCfg.StreamLocation))
	datastreamClient := datastreamAccessor.GetInstance(ctx)
	prefix := input.DatastreamCfg.DestinationConnectionConfig.Prefix
	prefix = utils.ConcatDirectoryPath(prefix, "data")
	//Step 1. First create the datastream
	err := datastreamAccessor.CreateStream(ctx, datastreamClient, input.GcpProjectId, prefix, input.SourceDatabaseName, input.SourceDatabaseName, input.DatastreamCfg)
	if err != nil {
		fmt.Printf("Error creating datastream: %v\n", err)
		return activityModel.SetupDataflowActivityOutput{}, err
	}
	// Step 2. Update the datastream and set it to running
	err = datastreamAccessor.UpdateStream(ctx, datastreamClient, input.GcpProjectId, prefix, input.DatastreamCfg)
	if err != nil {
		fmt.Printf("Error updating datastream: %v\n", err)
		return activityModel.SetupDataflowActivityOutput{}, err
	}
	return activityModel.SetupDataflowActivityOutput{}, nil
}
