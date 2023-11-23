// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	"cloud.google.com/go/dataflow/apiv1beta3/dataflowpb"
	datastream "cloud.google.com/go/datastream/apiv1"
	"cloud.google.com/go/datastream/apiv1/datastreampb"
	dashboard "cloud.google.com/go/monitoring/dashboard/apiv1"
	"cloud.google.com/go/monitoring/dashboard/apiv1/dashboardpb"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"google.golang.org/api/iterator"
)

type ShardCleanupOptions struct {
	Dataflow   bool
	Datastream bool
	Pubsub     bool
	Monitoring bool
}

type JobCleanupOptions struct {
	Dataflow   bool
	Datastream bool
	Pubsub     bool
	Monitoring bool
}

func InitiateJobCleanup(ctx context.Context, jobCleanupOptions JobCleanupOptions, jobExecutionData JobExecutionData, shardExecutionDataList []ShardExecutionData, project string, instance string) {
	//initiate shard level cleanup
	for _, generatedResources := range shardExecutionDataList {
		// converting to a differnet struct to prevent job level struct leakage into shard level actions
		shardCleanupOptions := ShardCleanupOptions(jobCleanupOptions)
		logger.Log.Info(fmt.Sprintf("Initiating cleanup for jobId: %s, dataShardId: %s\n", generatedResources.MigrationJobId, generatedResources.DataShardId))
		err := initiateShardCleanup(ctx, shardCleanupOptions, generatedResources, project, instance)
		if err != nil {
			logger.Log.Debug(fmt.Sprintf("Unable to cleanup resources for jobId: %s, dataShardId: %s: %v\n", generatedResources.MigrationJobId, generatedResources.DataShardId, err))
		} else {
			logger.Log.Info(fmt.Sprintf("Successfully cleaned up resources for jobId: %v\n", generatedResources.MigrationJobId))
		}
	}
	// initiate job level
	if jobCleanupOptions.Monitoring {
		var aggMonitoringResources internal.MonitoringResources
		err := json.Unmarshal([]byte(jobExecutionData.AggMonitoringResources), &aggMonitoringResources)
		if err != nil {
			logger.Log.Debug("Unable to read aggregate monitoring metadata for deletion\n")
		} else {
			cleanupMonitoringDashboard(ctx, aggMonitoringResources, project)
		}
	}
}

func GetJobDetails(ctx context.Context, migrationJobId string, dataShardIds []string, project string, instance string) (JobExecutionData, []ShardExecutionData, error) {
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, constants.METADATA_DB)
	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
		return JobExecutionData{}, nil, err
	}
	defer client.Close()
	txn := client.ReadOnlyTransaction()
	defer txn.Close()

	//fetch job level data
	jobQuery := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT 
								MigrationJobId,
								SpannerDatabaseName,
								TO_JSON_STRING(AggMonitoringResources) AS AggMonitoringResources,
								IsShardedMigration
							FROM JobExecutionData 
							WHERE MigrationJobId = '%s'`, migrationJobId),
	}
	iter := txn.Query(ctx, jobQuery)
	var jobExecutionData JobExecutionData
	err = iter.Do(func(row *spanner.Row) error {
		if err := row.ToStruct(&jobExecutionData); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		err = fmt.Errorf("can't fetch job details from db for migration job: %s: %v", migrationJobId, err)
		return JobExecutionData{}, nil, err
	}

	//fetch shard level data
	shardQuery := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT 
								MigrationJobId,
								DataShardId,
								TO_JSON_STRING(DataflowResources) AS DataflowResources,
								TO_JSON_STRING(DatastreamResources) AS DatastreamResources,
								TO_JSON_STRING(PubsubResources) AS PubsubResources,
								TO_JSON_STRING(MonitoringResources) AS MonitoringResources
							FROM ShardExecutionData 
							WHERE MigrationJobId = '%s'`, migrationJobId),
	}
	iter = txn.Query(ctx, shardQuery)
	shardExecutionDataList := []ShardExecutionData{}
	for {
		row, e := iter.Next()
		if e == iterator.Done {
			break
		}
		if e != nil {
			err = e
			break
		}
		var shardExecutionData ShardExecutionData
		row.ToStruct(&shardExecutionData)
		if filterbyDataShardId(shardExecutionData.DataShardId, dataShardIds) {
			shardExecutionDataList = append(shardExecutionDataList, shardExecutionData)
		}
	}
	return jobExecutionData, shardExecutionDataList, err
}

func GetInstanceDetails(ctx context.Context, targetProfile profiles.TargetProfile) (string, string, error) {
	var err error
	project := targetProfile.Conn.Sp.Project
	if project == "" {
		project, err = utils.GetProject()
		if err != nil {
			return "", "", fmt.Errorf("can't get project: %v", err)
		}
	}

	instance := targetProfile.Conn.Sp.Instance
	if instance == "" {
		instance, err = utils.GetInstance(ctx, project, os.Stdout)
		if err != nil {
			return "", "", fmt.Errorf("can't get instance: %v", err)
		}
	}
	return project, instance, nil
}

func initiateShardCleanup(ctx context.Context, options ShardCleanupOptions, shardExecutionData ShardExecutionData, project string, instance string) error {
	if options.Dataflow {
		var dataflowResources internal.DataflowResources
		err := json.Unmarshal([]byte(shardExecutionData.DataflowResources), &dataflowResources)
		if err != nil {
			logger.Log.Debug("Unable to read Dataflow metadata for deletion\n")
		} else {
			cleanupDataflowJob(ctx, dataflowResources, project)
		}
	}
	if options.Datastream {
		var datastreamResources internal.DatastreamResources
		err := json.Unmarshal([]byte(shardExecutionData.DatastreamResources), &datastreamResources)
		if err != nil {
			logger.Log.Debug("Unable to read Datastream metadata for deletion\n")
		} else {
			cleanupDatastream(ctx, datastreamResources, project)
		}
	}
	if options.Pubsub {
		var pubsubResources internal.PubsubResources
		err := json.Unmarshal([]byte(shardExecutionData.PubsubResources), &pubsubResources)
		if err != nil {
			logger.Log.Debug("Unable to read Pubsub metadata for deletion\n")
		} else {
			cleanupPubsubResources(ctx, pubsubResources, project)
		}
	}
	if options.Monitoring {
		var monitoringResources internal.MonitoringResources
		err := json.Unmarshal([]byte(shardExecutionData.MonitoringResources), &monitoringResources)
		if err != nil {
			logger.Log.Debug("Unable to read monitoring metadata for deletion\n")
		} else {
			cleanupMonitoringDashboard(ctx, monitoringResources, project)
		}
	}
	return nil
}

func filterbyDataShardId(fetchedDataShardId string, configuredDataShardIds []string) bool {
	if configuredDataShardIds == nil {
		return true
	}
	for _, configuredDataShardId := range configuredDataShardIds {
		if fetchedDataShardId == configuredDataShardId {
			return true
		}
	}
	return false
}

func cleanupPubsubResources(ctx context.Context, pubsubResources internal.PubsubResources, project string) {
	logger.Log.Debug("Attempting to delete pubsub topic and subscription...\n")
	pubsubClient, err := pubsub.NewClient(ctx, project)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("pubsub client can not be created: %v", err))
		return
	} 
	defer pubsubClient.Close()
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("storage client can not be created: %v", err))
		return
	} 
	defer storageClient.Close()
	subscription := pubsubClient.Subscription(pubsubResources.SubscriptionId)
	err = subscription.Delete(ctx)
	if err != nil {
		logger.Log.Info(fmt.Sprintf("Cleanup of the pubsub subscription: %s Failed, please clean up the pubsub subscription manually\n error=%v\n", pubsubResources.SubscriptionId, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted subscription: %s\n\n", pubsubResources.SubscriptionId))
	}

	topic := pubsubClient.Topic(pubsubResources.TopicId)
	err = topic.Delete(ctx)
	if err != nil {
		logger.Log.Info(fmt.Sprintf("Cleanup of the pubsub topic: %s Failed, please clean up the pubsub topic manually\n error=%v\n", pubsubResources.TopicId, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted topic: %s\n\n", pubsubResources.TopicId))
	}

	bucket := storageClient.Bucket(pubsubResources.BucketName)
	if err := bucket.DeleteNotification(ctx, pubsubResources.NotificationId); err != nil {
		logger.Log.Info(fmt.Sprintf("Cleanup of GCS pubsub notification: %s failed.\n error=%v\n", pubsubResources.NotificationId, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted GCS pubsub notification: %s\n\n", pubsubResources.NotificationId))
	}
}

func cleanupMonitoringDashboard(ctx context.Context, monitoringResources internal.MonitoringResources, projectID string) {
	logger.Log.Debug("Attempting to delete monitoring resources...\n")
	client, err := dashboard.NewDashboardsClient(ctx)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Cleanup of the monitoring dashboard: %s Failed, please clean up the dashboard manually\n error=%v\n", monitoringResources.DashboardName, err))
		return
	} 
	defer client.Close()
	req := &dashboardpb.DeleteDashboardRequest{
		Name: fmt.Sprintf("projects/%s/dashboards/%s", projectID, monitoringResources.DashboardName),
	}
	err = client.DeleteDashboard(ctx, req)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Cleanup of the monitoring dashboard: %s Failed, please clean up the dashboard manually\n error=%v\n", monitoringResources.DashboardName, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted Monitoring Dashboard: %s\n\n", monitoringResources.DashboardName))
	}
}

func cleanupDatastream(ctx context.Context, datastreamResources internal.DatastreamResources, project string) {
	logger.Log.Debug("Attempting to delete datastream stream...\n")
	datastreamClient, err := datastream.NewClient(ctx)
	logger.Log.Debug("Created datastream client...")
	if err != nil {
		logger.Log.Error(fmt.Sprintf("datastream client can not be created: %v", err))
		return
	} 
	defer datastreamClient.Close()
	req := &datastreampb.DeleteStreamRequest{
		Name: fmt.Sprintf("projects/%s/locations/%s/streams/%s", project, datastreamResources.Region, datastreamResources.DatastreamName),
	}
	_, err = datastreamClient.DeleteStream(ctx, req)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Cleanup of the datastream stream: %s Failed, please clean up the datastream stream manually\n error=%v\n", datastreamResources.DatastreamName, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted datastream stream: %s\n\n", datastreamResources.DatastreamName))
	}
}

func cleanupDataflowJob(ctx context.Context, dataflowResources internal.DataflowResources, project string) {
	logger.Log.Debug("Attempting to delete dataflow job...\n")
	dataflowClient, err := dataflow.NewJobsV1Beta3Client(ctx)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("dataflow client can not be created: %v", err))
		return 
	} 
	defer dataflowClient.Close()
	job := &dataflowpb.Job{
		Id:             dataflowResources.JobId,
		ProjectId:      project,
		RequestedState: dataflowpb.JobState_JOB_STATE_CANCELLED,
	}

	dfReq := &dataflowpb.UpdateJobRequest{
		ProjectId: project,
		JobId:     dataflowResources.JobId,
		Location:  dataflowResources.Region,
		Job:       job,
	}
	_, err = dataflowClient.UpdateJob(ctx, dfReq)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("Cleanup of the dataflow job: %s Failed, please clean up the dataflow job manually\n error=%v\n", dataflowResources.JobId, err))
	} else {
		logger.Log.Info(fmt.Sprintf("Successfully deleted dataflow job: %s\n\n", dataflowResources.JobId))
	}
}
