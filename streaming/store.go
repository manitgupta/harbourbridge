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
	"time"

	"cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
)

// Stores the migration job level data post orchestration of a migration job
type JobExecutionData struct {
	MigrationJobId         string
	SpannerDatabaseName    string
	AggMonitoringResources string
	IsShardedMigration     bool
	CreatedAt              time.Time
	UpdatedAt              time.Time
}

// Stores the shard level execution data post orchestration of a migration job
type ShardExecutionData struct {
	MigrationJobId      string
	DataShardId         string
	DataflowResources   string
	DatastreamResources string
	PubsubResources     string
	MonitoringResources string
	CreatedAt           time.Time
	UpdatedAt           time.Time
}

// PersistJobExecutionData stores all the metadata associated with a job orchestration for a minimal downtime migration in the metadata db. An example of this metadata is job level data such as the spanner database name.
func PersistJobExecutionData(ctx context.Context, targetProfile profiles.TargetProfile, sourceProfile profiles.SourceProfile, conv *internal.Conv, migrationJobId string, isSharded bool) (err error) {
	project, instance, dbName, err := targetProfile.GetResourceIds(ctx, time.Now(), sourceProfile.Driver, nil)
	if err != nil {
		err = fmt.Errorf("can't get resource ids: %v", err)
		return err
	}
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, constants.METADATA_DB)
	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
		return err
	}
	defer client.Close()
	if err != nil {
		err = fmt.Errorf("can't create database client: %v", err)
		return err
	}
	err = writeJobExecutionMetadata(ctx, migrationJobId, isSharded, dbName, time.Now(), client)
	if err != nil {
		err = fmt.Errorf("can't store generated resources for datashard: %v", err)
		return err
	}
	logger.Log.Info(fmt.Sprintf("Generated resources stored successfully for migration jobId: %s. You can also look at the 'spannermigrationtool_metadata' database in your spanner instance to get this jobId at a later point of time.\n", migrationJobId))
	return nil
}

func UpdateJobExecutionDataWithAggregateMonitoringResources(ctx context.Context, targetProfile profiles.TargetProfile, sourceProfile profiles.SourceProfile, conv *internal.Conv, migrationJobId string) error {
	logger.Log.Debug(fmt.Sprintf("Storing aggregate monitoring dashboard for migration jobId: %s\n", migrationJobId))
	project, instance, _, err := targetProfile.GetResourceIds(ctx, time.Now(), sourceProfile.Driver, nil)
	if err != nil {
		err = fmt.Errorf("can't get resource ids: %v", err)
		return err
	}
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, constants.METADATA_DB)
	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
		return err
	}
	defer client.Close()
	aggMonitoringResourcesBytes, err := json.Marshal(conv.Audit.StreamingStats.AggMonitoringResources)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("internal error occurred while persisting metadata for migration job %s: %v\n", migrationJobId, err))
		return err
	}
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		mutation := spanner.Update("JobExecutionData", []string{"MigrationJobId", "AggMonitoringResources", "UpdatedAt"}, []interface{}{migrationJobId, string(aggMonitoringResourcesBytes), time.Now()})
		err = txn.BufferWrite([]*spanner.Mutation{mutation})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		err = fmt.Errorf("can't store aggregate monitoring resources for migration job %s: %v", migrationJobId, err)
		return err
	}
	return nil
}

// PersistShardExecutionData stores all the metadata associated with a shard orchestration for a minimal downtime migration in the metadata db. An example of this metadata is generated resources.
func PersistShardExecutionData(ctx context.Context, targetProfile profiles.TargetProfile, sourceProfile profiles.SourceProfile, conv *internal.Conv, migrationJobId string, dataShardId string) (err error) {
	project, instance, _, err := targetProfile.GetResourceIds(ctx, time.Now(), sourceProfile.Driver, nil)
	if err != nil {
		err = fmt.Errorf("can't get resource ids: %v", err)
		return err
	}
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, constants.METADATA_DB)
	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
		return err
	}
	defer client.Close()
	if err != nil {
		err = fmt.Errorf("can't create database client: %v", err)
		return err
	}
	err = writeShardExecutionMetadata(ctx, migrationJobId, dataShardId, conv.Audit.StreamingStats.DataflowResources, conv.Audit.StreamingStats.DatastreamResources, conv.Audit.StreamingStats.PubsubResources, conv.Audit.StreamingStats.MonitoringResources, time.Now(), client)
	if err != nil {
		err = fmt.Errorf("can't store generated resources for datashard: %v", err)
		return err
	}
	logger.Log.Info(fmt.Sprintf("Generated resources stored successfully for migration jobId: %s, dataShardId: %s. You can also look at the 'spannermigrationtool_metadata' database in your spanner instance to get this jobId at a later point of time.\n", migrationJobId, dataShardId))
	return nil
}

func writeJobExecutionMetadata(ctx context.Context, migrationJobId string, isShardedMigration bool, spannerDatabaseName string, createTimestamp time.Time, client *spanner.Client) error {
	aggMonitoringResourcesBytes, err := json.Marshal(interface{}(nil))
	if err != nil {
		logger.Log.Error(fmt.Sprintf("internal error occurred while persisting metadata for migration job %s: %v\n", migrationJobId, err))
		return err
	}
	jobExecutionData := JobExecutionData{
		MigrationJobId:         migrationJobId,
		SpannerDatabaseName:    spannerDatabaseName,
		IsShardedMigration:     isShardedMigration,
		AggMonitoringResources: string(aggMonitoringResourcesBytes),
		CreatedAt:              createTimestamp,
		UpdatedAt:              createTimestamp,
	}
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		mutation, err := spanner.InsertStruct("JobExecutionData", jobExecutionData)
		if err != nil {
			return err
		}
		err = txn.BufferWrite([]*spanner.Mutation{mutation})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't store generated resources for migration job %s: %v\n", migrationJobId, err))
		return err
	}
	return nil
}

func writeShardExecutionMetadata(ctx context.Context, migrationJobId string, dataShardId string, dataflowResources internal.DataflowResources, datastreamResources internal.DatastreamResources, pubsubResources internal.PubsubResources, monitoringResources internal.MonitoringResources, createTimestamp time.Time, client *spanner.Client) error {
	pubsubResourcesBytes, err := json.Marshal(pubsubResources)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't marshal pubsub resources for data shard %s: %v\n", dataShardId, err))
		return err
	}
	dataflowResourcesBytes, err := json.Marshal(dataflowResources)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't marshal dataflow resources for data shard %s: %v\n", dataShardId, err))
		return err
	}
	datastreamResourcesBytes, err := json.Marshal(datastreamResources)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't marshal datastream resources for data shard %s: %v\n", dataShardId, err))
		return err
	}
	monitoringResourcesBytes, err := json.Marshal(monitoringResources)
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't marshal monitoring resources for data shard %s: %v\n", dataShardId, err))
		return err
	}
	logger.Log.Debug(fmt.Sprintf("Storing generated resources for data shard %s...\n", dataShardId))
	shardExecutionData := ShardExecutionData{
		MigrationJobId:      migrationJobId,
		DataShardId:         dataShardId,
		DatastreamResources: string(datastreamResourcesBytes),
		DataflowResources:   string(dataflowResourcesBytes),
		PubsubResources:     string(pubsubResourcesBytes),
		MonitoringResources: string(monitoringResourcesBytes),
		CreatedAt:           createTimestamp,
		UpdatedAt:           createTimestamp,
	}
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		mutation, err := spanner.InsertStruct("ShardExecutionData", shardExecutionData)
		if err != nil {
			return err
		}
		err = txn.BufferWrite([]*spanner.Mutation{mutation})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		logger.Log.Error(fmt.Sprintf("can't store generated resources for data shard %s: %v\n", dataShardId, err))
		return err
	}
	return nil
}
