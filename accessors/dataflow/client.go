package dataflow

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	dataflowpb "cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	otherUtils "github.com/GoogleCloudPlatform/spanner-migration-tool/utils"
)

var (
	// Default value for maxWorkers.
	maxWorkers int32 = 50
	// Default value for NumWorkers.
	numWorkers int32 = 1
	// Max allowed value for maxWorkers and numWorkers.
	MAX_WORKER_LIMIT int32 = 1000
	// Min allowed value for maxWorkers and numWorkers.
	MIN_WORKER_LIMIT int32 = 1
)

var once sync.Once

var dataflowClient *dataflow.FlexTemplatesClient

func GetInstance(ctx context.Context) *dataflow.FlexTemplatesClient {
	if dataflowClient == nil {
		once.Do(func() {
			dataflowClient, _ = dataflow.NewFlexTemplatesClient(ctx)
		})
		return dataflowClient
	}
	return dataflowClient
}

func LaunchDataflowJob(ctx context.Context, dataflowClient *dataflow.FlexTemplatesClient, streamingCfg streaming.StreamingCfg, spannerDetails profiles.TargetProfile, inputFilePattern string, gcpProjectId string) (string, string, error) {
	project, instance, dbName, _ := spannerDetails.GetResourceIds(ctx, time.Now(), "", nil)
	dataflowCfg := streamingCfg.DataflowCfg
	fmt.Println("Launching dataflow job ", dataflowCfg.JobName, " in ", project, "-", dataflowCfg.Location)
	var dataflowHostProjectId string
	if streamingCfg.DataflowCfg.HostProjectId == "" {
		dataflowHostProjectId, _ = utils.GetProject()
	} else {
		dataflowHostProjectId = streamingCfg.DataflowCfg.HostProjectId
	}

	dataflowSubnetwork := ""

	// If custom network is not selected, use public IP. Typical for internal testing flow.
	workerIpAddressConfig := dataflowpb.WorkerIPAddressConfiguration_WORKER_IP_PUBLIC

	if streamingCfg.DataflowCfg.Network != "" {
		workerIpAddressConfig = dataflowpb.WorkerIPAddressConfiguration_WORKER_IP_PRIVATE
		if streamingCfg.DataflowCfg.Subnetwork == "" {
			return "", "", fmt.Errorf("if network is specified, subnetwork cannot be empty")
		} else {
			dataflowSubnetwork = fmt.Sprintf("https://www.googleapis.com/compute/v1/projects/%s/regions/%s/subnetworks/%s", dataflowHostProjectId, streamingCfg.DataflowCfg.Location, dataflowCfg.Subnetwork)
		}
	}

	if streamingCfg.DataflowCfg.MaxWorkers != "" {
		intVal, err := strconv.ParseInt(streamingCfg.DataflowCfg.MaxWorkers, 10, 64)
		if err != nil {
			return "", "", fmt.Errorf("could not parse MaxWorkers parameter %s, please provide a positive integer as input", streamingCfg.DataflowCfg.MaxWorkers)
		}
		maxWorkers = int32(intVal)
		if maxWorkers < MIN_WORKER_LIMIT || maxWorkers > MAX_WORKER_LIMIT {
			return "", "", fmt.Errorf("maxWorkers should lie in the range [%d, %d]", MIN_WORKER_LIMIT, MAX_WORKER_LIMIT)
		}
	}
	if streamingCfg.DataflowCfg.NumWorkers != "" {
		intVal, err := strconv.ParseInt(streamingCfg.DataflowCfg.NumWorkers, 10, 64)
		if err != nil {
			return "", "", fmt.Errorf("could not parse NumWorkers parameter %s, please provide a positive integer as input", dataflowCfg.NumWorkers)
		}
		numWorkers = int32(intVal)
		if numWorkers < MIN_WORKER_LIMIT || numWorkers > MAX_WORKER_LIMIT {
			return "", "", fmt.Errorf("numWorkers should lie in the range [%d, %d]", MIN_WORKER_LIMIT, MAX_WORKER_LIMIT)
		}
	}
	launchParameters := &dataflowpb.LaunchFlexTemplateParameter{
		JobName:  streamingCfg.DataflowCfg.JobName,
		Template: &dataflowpb.LaunchFlexTemplateParameter_ContainerSpecGcsPath{ContainerSpecGcsPath: "gs://dataflow-templates-southamerica-west1/2023-09-12-00_RC00/flex/Cloud_Datastream_to_Spanner"},
		Parameters: map[string]string{
			"inputFilePattern":              otherUtils.ConcatDirectoryPath(inputFilePattern, "data"),
			"streamName":                    fmt.Sprintf("projects/%s/locations/%s/streams/%s", gcpProjectId, streamingCfg.DatastreamCfg.StreamLocation, streamingCfg.DatastreamCfg.StreamId),
			"instanceId":                    instance,
			"databaseId":                    dbName,
			"sessionFilePath":               streamingCfg.TmpDir + "session.json",
			"deadLetterQueueDirectory":      inputFilePattern + "dlq",
			"transformationContextFilePath": streamingCfg.TmpDir + "transformationContext.json",
			"gcsPubSubSubscription":         fmt.Sprintf("projects/%s/subscriptions/%s", gcpProjectId, streamingCfg.PubsubCfg.SubscriptionId),
		},
		Environment: &dataflowpb.FlexTemplateRuntimeEnvironment{
			MaxWorkers:            maxWorkers,
			NumWorkers:            numWorkers,
			ServiceAccountEmail:   streamingCfg.DataflowCfg.ServiceAccountEmail,
			AutoscalingAlgorithm:  2, // 2 corresponds to AUTOSCALING_ALGORITHM_BASIC
			EnableStreamingEngine: true,
			Network:               streamingCfg.DataflowCfg.Network,
			Subnetwork:            dataflowSubnetwork,
			IpConfiguration:       workerIpAddressConfig,
		},
	}
	req := &dataflowpb.LaunchFlexTemplateRequest{
		ProjectId:       gcpProjectId,
		LaunchParameter: launchParameters,
		Location:        streamingCfg.DataflowCfg.Location,
	}
	fmt.Println("Created flex template request body...")

	respDf, err := dataflowClient.LaunchFlexTemplate(ctx, req)
	if err != nil {
		fmt.Printf("flexTemplateRequest: %+v\n", req)
		return "", "", fmt.Errorf("unable to launch template: %v", err)
	}
	gcloudDfCmd := utils.GetGcloudDataflowCommand(req)
	fmt.Printf("\nEquivalent gCloud command for job %s:\n%s\n\n", req.LaunchParameter.JobName, gcloudDfCmd)
	return respDf.Job.Id, gcloudDfCmd, nil
	
}