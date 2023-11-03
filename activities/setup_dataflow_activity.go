package activities

import (
	"context"
	"encoding/json"
	"fmt"

	datastreamAccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/datastream"
	storageAccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/storage"
	dataflowAccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/dataflow"
	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
)



func SetupDataflowActivity(ctx context.Context, input activityModel.SetupDataflowActivityInput) (activityModel.SetupDataflowActivityOutput, error) {
	convJSON, err := json.MarshalIndent(input.Conv, "", " ")
	if err != nil {
		return activityModel.SetupDataflowActivityOutput{}, fmt.Errorf("can't encode session state to JSON: %v", err)
	}

	//Step 1. Stage needed files in GCS before starting Dataflow. 
	gcsClient := storageAccessor.GetInstance(ctx)
	err = storageAccessor.WriteToGCS(ctx, gcsClient, input.StreamingCfg.TmpDir, "session.json", string(convJSON))
	transformationContextMap := map[string]interface{}{
		"SchemaToShardId": input.StreamingCfg.DataflowCfg.DbNameToShardIdMap,
	}
	transformationContext, err := json.Marshal(transformationContextMap)
	if err != nil {
		return activityModel.SetupDataflowActivityOutput{}, fmt.Errorf("failed to compute transformation context: %s", err.Error())
	}
	err = storageAccessor.WriteToGCS(ctx, gcsClient, input.StreamingCfg.TmpDir, "transformationContext.json", string(transformationContext))
	if err != nil {
		return activityModel.SetupDataflowActivityOutput{}, fmt.Errorf("error while writing to GCS: %v", err)
	}
	// Step 2. Get path of destination GCS from Datastream
	datastreamClient := datastreamAccessor.GetInstance(ctx)
	gcsProfile, err := datastreamAccessor.GetGCSPathFromConnectionProfile(ctx, datastreamClient, input.GcpProjectId, input.StreamingCfg.DatastreamCfg.DestinationConnectionConfig)
	if err != nil {
		return activityModel.SetupDataflowActivityOutput{}, fmt.Errorf("failed to fetch GCS path from connection profile: %s", err.Error())
	}
	inputFilePattern := "gs://" + gcsProfile.Bucket + gcsProfile.RootPath + input.StreamingCfg.DatastreamCfg.DestinationConnectionConfig.Prefix
	if inputFilePattern[len(inputFilePattern)-1] != '/' {
		inputFilePattern = inputFilePattern + "/"
	}
	fmt.Println("Reading files from datastream destination ", inputFilePattern)
	// Step 3. Launch Dataflow job
	dataflowClient := dataflowAccessor.GetInstance(ctx)
	jobId, gCloudCmd, err :=dataflowAccessor.LaunchDataflowJob(ctx, dataflowClient, input.StreamingCfg, input.TargetDetails, inputFilePattern, input.GcpProjectId)
	if err != nil {
		return activityModel.SetupDataflowActivityOutput{}, fmt.Errorf("failed to launch dataflow job: %s", err.Error())
	}
	return activityModel.SetupDataflowActivityOutput{JobId: jobId, GCloudCmd: gCloudCmd}, nil
}
