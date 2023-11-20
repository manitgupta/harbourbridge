package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
)

// StoreGeneratedResources stores all the generated resources for a minimal downtime migration pipeline in the metadata db.
func PersistGeneratedResources(ctx context.Context, targetProfile profiles.TargetProfile, sourceProfile profiles.SourceProfile, conv *internal.Conv, migrationJobId string, dataShardId string) (err error) {
	project, instance, dbName, err := targetProfile.GetResourceIds(ctx, time.Now(), sourceProfile.Driver, nil)
	if err != nil {
		err = fmt.Errorf("can't get resource ids: %v", err)
		return err
	}
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, "spannermigrationtool_metadata")
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
	err = storeGeneratedResourcesForSingleShard(ctx, conv, migrationJobId, dataShardId, dbName, client)
	if err != nil {
		err = fmt.Errorf("can't store generated resources for datashard: %v", err)
		return err
	}
	fmt.Printf("Generated resources stored successfully for migration jobId: %s. You can also look at the 'spannermigrationtool_metadata' database in your spanner instance to get this jobId at a later point of time.\n", migrationJobId)
	return nil
}

// Stores generated resources for a non-sharded migration, returns an err if unsuccessful
func storeGeneratedResourcesForSingleShard(ctx context.Context, conv *internal.Conv, migrationJobId string, dataShardId string, dbName string, client *spanner.Client) error {
	err := writeGeneratedResourcesToMetadata(ctx, migrationJobId, dataShardId, conv.Audit.StreamingStats.DataflowResources, conv.Audit.StreamingStats.DatastreamResources, conv.Audit.StreamingStats.PubsubResources, dbName, time.Now(), client)
	if err != nil {
		fmt.Printf("can't store generated resources: %v\n", err)
		return err
	}
	return nil
}

func writeGeneratedResourcesToMetadata(ctx context.Context, migrationJobId string, dataShardId string, dataflowResources internal.DataflowResources, datastreamResources internal.DatastreamResources, pubsubResources internal.PubsubResources, spannerDatabaseName string, createTimestamp time.Time, client *spanner.Client) error {
	pubsubResourcesBytes, err := json.Marshal(pubsubResources)
	if err != nil {
		fmt.Printf("can't marshal pubsub resources for data shard %s: %v\n", dataShardId, err)
		return err
	}
	dataflowResourcesBytes, err := json.Marshal(dataflowResources)
	if err != nil {
		fmt.Printf("can't marshal dataflow resources for data shard %s: %v\n", dataShardId, err)
		return err
	}
	datastreamResourcesBytes, err := json.Marshal(datastreamResources)
	if err != nil {
		fmt.Printf("can't marshal datastream resources for data shard %s: %v\n", dataShardId, err)
		return err
	}
	fmt.Printf("Storing generated resources for data shard %s...\n", dataShardId)
	generatedResources := internal.GeneratedResources{
		MigrationJobId:      migrationJobId,
		DataShardId:         dataShardId,
		DatastreamResources: string(datastreamResourcesBytes),
		DataflowResources:   string(dataflowResourcesBytes),
		PubsubResources:     string(pubsubResourcesBytes),
		SpannerDatabaseName: spannerDatabaseName,
		CreateTimestamp:     createTimestamp,
	}
	_, err = client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		mutation, err := spanner.InsertStruct("GeneratedResources", generatedResources)
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
		fmt.Printf("can't store generated resources for data shard %s: %v\n", dataShardId, err)
		return err
	}
	return nil
}