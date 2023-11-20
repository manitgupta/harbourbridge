package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	datastream "cloud.google.com/go/datastream/apiv1"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"google.golang.org/api/iterator"
)

type CleanupOptions struct {
	Dataflow   bool
	Datastream bool
	Pubsub     bool
	MetaDb     bool
}

func InitiateCleanup(ctx context.Context, options CleanupOptions, generatedResources internal.GeneratedResources, project string, instance string) error {
	if options.Dataflow {
		logger.Log.Debug("Attempting to delete dataflow job...\n")
		c, err := dataflow.NewJobsV1Beta3Client(ctx)
		if err != nil {
			return fmt.Errorf("could not create job client: %v", err)
		}
		defer c.Close()
		logger.Log.Debug("Created dataflow job client...")
		var dataflowResources internal.DataflowResources
		err = json.Unmarshal([]byte(generatedResources.DataflowResources), &dataflowResources)
		if err != nil {
			logger.Log.Debug("Unable to read Dataflow metadata for deletion\n")
			return err
		}
		err = CleanupDataflowJob(ctx, c, dataflowResources.JobId, project, dataflowResources.Region)
		if err != nil {
			logger.Log.Debug(fmt.Sprintf("Cleanup of the dataflow job: %s was unsuccessful, please clean up the job manually. err = %v\n", dataflowResources.JobId, err))
		} else {
			logger.Log.Info("Dataflow job cleaned up successfully.\n")
		}
	}
	if options.Datastream {
		logger.Log.Debug("Attempting to delete datastream stream...\n")
		dsClient, err := datastream.NewClient(ctx)
		logger.Log.Debug("Created datastream client...")
		if err != nil {
			return fmt.Errorf("datastream client can not be created: %v", err)
		}
		defer dsClient.Close()
		var datastreamResources internal.DatastreamResources
		err = json.Unmarshal([]byte(generatedResources.DatastreamResources), &datastreamResources)
		if err != nil {
			logger.Log.Debug("Unable to read Datastream metadata for deletion\n")
			return err
		}
		err = CleanupDatastream(ctx, dsClient, datastreamResources.DatastreamName, project, datastreamResources.Region)
		if err != nil {
			fmt.Printf("Cleanup of the datastream: %s was unsuccessful, please clean up the stream manually. err = %v\n", generatedResources.DatastreamResources, err)
		} else {
			logger.Log.Info("Datastream stream cleaned up successfully.\n")
		}
	}
	if options.Pubsub {
		logger.Log.Debug("Attempting to delete pubsub topic and subscription...\n")
		pubsubClient, err := pubsub.NewClient(ctx, project)
		if err != nil {
			return fmt.Errorf("pubsub client cannot be created: %v", err)
		}
		defer pubsubClient.Close()

		storageClient, err := storage.NewClient(ctx)
		if err != nil {
			return fmt.Errorf("storage client cannot be created: %v", err)
		}
		defer storageClient.Close()
		var pubsubCfg internal.PubsubResources
		err = json.Unmarshal([]byte(generatedResources.PubsubResources), &pubsubCfg)
		if err != nil {
			fmt.Printf("Unable to read Pubsub metadata for deletion\n")
			return err
		}
		CleanupPubsubResources(ctx, pubsubClient, storageClient, pubsubCfg, project)
	}
	if options.MetaDb {
		if !(options.Datastream || options.Dataflow || options.Pubsub) {
			logger.Log.Info("Not all resources were specified for cleanup, metadata for this job will not be deleted!\n")
		} else {
			logger.Log.Debug("Attempting to cleanup metadata database...\n")
			dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, "spannermigrationtool_metadata")
			client, err := utils.GetClient(ctx, dbURI)
			if err != nil {
				return fmt.Errorf("can't create client for db %s: %v", dbURI, err)
			}
			defer client.Close()
			m := []*spanner.Mutation{
				spanner.Delete("GeneratedResources", spanner.Key{generatedResources.MigrationJobId, generatedResources.DataShardId}),
			}
			_, err = client.Apply(ctx, m)
			if err != nil {
				return fmt.Errorf("unable to cleanup metadata database of the migration job: %s, dataShardId: %s, err: %v", generatedResources.MigrationJobId, generatedResources.DataShardId, err)
			} else {
				logger.Log.Info("Metadata cleanup successful.\n")
			}
		}
	}
	return nil
}

func GetJobDetails(ctx context.Context, migrationJobId string, dataShardIds []string, targetProfile profiles.TargetProfile, project string, instance string) ([]internal.GeneratedResources, error) {
	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, "spannermigrationtool_metadata")
	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
		return nil, err
	}
	defer client.Close()
	txn := client.ReadOnlyTransaction()
	defer txn.Close()

	query := spanner.Statement{
		SQL: fmt.Sprintf(`SELECT 
								MigrationJobId,
								DataShardId,
								TO_JSON_STRING(DataflowResources) AS DataflowResources,
								TO_JSON_STRING(DatastreamResources) AS DatastreamResources,
								TO_JSON_STRING(PubsubResources) AS PubsubResources,
								SpannerDatabaseName,
								CreateTimestamp
							FROM GeneratedResources 
							WHERE MigrationJobId = '%s'`, migrationJobId),
	}

	iter := txn.Query(ctx, query)
	result := []internal.GeneratedResources{}
	for {
		row, e := iter.Next()
		if e == iterator.Done {
			break
		}
		if e != nil {
			err = e
			break
		}
		var generatedResources internal.GeneratedResources
		row.ToStruct(&generatedResources)
		if filterbyDataShardId(generatedResources.DataShardId, dataShardIds) {
			result = append(result, generatedResources)
		}
	}

	return result, err
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
