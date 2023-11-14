package cmd

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path"

	dataflow "cloud.google.com/go/dataflow/apiv1beta3"
	datastream "cloud.google.com/go/datastream/apiv1"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/spanner"
	"cloud.google.com/go/storage"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	"github.com/google/subcommands"
	"google.golang.org/api/iterator"
)

type CleanupCmd struct {
	jobId         string
	dataShardIds  string
	targetProfile string
	datastream    bool
	dataflow      bool
	pubsub        bool
	logLevel      string
	validate      bool
}

// Name returns the name of operation.
func (cmd *CleanupCmd) Name() string {
	return "cleanup"
}

// Synopsis returns summary of operation.
func (cmd *CleanupCmd) Synopsis() string {
	return "cleanup cleans up the generated resources for a provided jobId"
}

// Usage returns usage info of the command.
func (cmd *CleanupCmd) Usage() string {
	return fmt.Sprintf(`%v cleanup --jobId=[jobId] --datastream --dataflow ...

Cleanup GCP resources generated as part of setting up a migration pipeline by providing a 
jobId generated during the job creation.
`, path.Base(os.Args[0]))
}

// SetFlags sets the flags.
func (cmd *CleanupCmd) SetFlags(f *flag.FlagSet) {
	f.StringVar(&cmd.jobId, "jobId", "", "Flag for specifying the migration jobId")
	f.StringVar(&cmd.dataShardIds, "dataShardIds", "", "Flag for specifying a comma separated list of dataShardIds to be cleaned up. Defaults to ALL shards. Optional flag, and only valid for a sharded migration.")
	f.StringVar(&cmd.targetProfile, "target-profile", "", "Flag for specifying connection profile for target database e.g., \"dialect=postgresql\"")
	f.BoolVar(&cmd.datastream, "datastream", false, "Flag for specifying if Datastream streams associated with the migration job should be cleaned up or not. Defaults to FALSE.")
	f.BoolVar(&cmd.dataflow, "dataflow", false, "Flag for specifying if Dataflow job associated with the migration job should be cleaned up or not. Defaults to FALSE.")
	f.BoolVar(&cmd.pubsub, "pubsub", false, "Flag for specifying if pubsub associated with the migration job should be cleaned up or not. Defaults to FALSE.")
	f.StringVar(&cmd.logLevel, "log-level", "DEBUG", "Configure the logging level for the command (INFO, DEBUG), defaults to DEBUG")
	f.BoolVar(&cmd.validate, "validate", false, "Flag for validating if all the required input parameters are present")
}

func (cmd *CleanupCmd) Execute(ctx context.Context, f *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	err := logger.InitializeLogger(cmd.logLevel)
	if err != nil {
		fmt.Println("Error initialising logger, did you specify a valid log-level? [DEBUG, INFO, WARN, ERROR, FATAL]", err)
		return subcommands.ExitFailure
	}
	targetProfile, err := profiles.NewTargetProfile(cmd.targetProfile)
	if err != nil {
		fmt.Printf("Target profile is not properly configured, this is needed for SMT to lookup job details in the metadata database: %v", err)
		return subcommands.ExitFailure
	}
	project, instance, err := getInstanceDetails(ctx, targetProfile)
	if err != nil {
		fmt.Printf("can't get resource ids: %v\n", err)
		return subcommands.ExitFailure
	}
	dataShardIds, err := profiles.ParseList(cmd.dataShardIds)
	if err != nil {
		fmt.Printf("Could not parse datashardIds: %v", err)
		return subcommands.ExitFailure
	}
	generatedResourcesList, err := getJobDetails(ctx, cmd.jobId, dataShardIds, targetProfile, project, instance)
	if err != nil {
		fmt.Printf("Unable to fetch job details from the internal metadata database: %v!", err)
		return subcommands.ExitFailure
	}
	for _, generatedResources := range generatedResourcesList {
		err = initiateCleanup(ctx, cmd, generatedResources, project, "asia-east2")
		if err != nil {
			fmt.Printf("Unable to cleanup resources")
		}
	}
	return subcommands.ExitSuccess
}

func initiateCleanup(ctx context.Context, cmd *CleanupCmd, generatedResources internal.GeneratedResources, project string, region string) error {
	if cmd.dataflow {
		fmt.Printf("Attempting to delete dataflow job...\n")
		c, err := dataflow.NewJobsV1Beta3Client(ctx)
		if err != nil {
			return fmt.Errorf("could not create job client: %v", err)
		}
		defer c.Close()
		fmt.Println("Created dataflow job client...")
		var dataflowResources internal.ShardedDataflowJobResources
		err = json.Unmarshal([]byte(generatedResources.DataflowResources), &dataflowResources)
		if err != nil {
			fmt.Printf("Unable to read Dataflow metadata for deletion\n")
			return err
		}
		err = streaming.CleanupDataflowJob(ctx, c, dataflowResources.JobId, project, region)
		if err != nil {
			fmt.Printf("Cleanup of the dataflow job: %s was unsuccessful, please clean up the job manually\n", dataflowResources.JobId)
		} else {
			fmt.Printf("Dataflow job cleaned up successfully.\n")
		}
	}
	if cmd.datastream {
		fmt.Printf("Attempting to delete datastream streams...\n")
		dsClient, err := datastream.NewClient(ctx)
		fmt.Println("Created datastream client...")
		if err != nil {
			return fmt.Errorf("datastream client can not be created: %v", err)
		}
		defer dsClient.Close()
		err = streaming.CleanupDatastream(ctx, dsClient, generatedResources.DatastreamResources, project, region)
		if err != nil {
			fmt.Printf("Cleanup of the datastream: %s was unsuccessful, please clean up the stream manually\n", generatedResources.DatastreamResources)
		}
	}
	if cmd.pubsub {
		fmt.Printf("Attempting to delete pubsub topic and subscription...\n")
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
		var pubsubCfg internal.PubsubCfg
		err = json.Unmarshal([]byte(generatedResources.PubsubResources), &pubsubCfg)
		if err != nil {
			fmt.Printf("Unable to read Pubsub metadata for deletion\n")
			return err
		}
		streaming.CleanupPubsubResources(ctx, pubsubClient, storageClient, pubsubCfg, project)
	}
	return nil
}

func getJobDetails(ctx context.Context, migrationJobId string, dataShardIds []string, targetProfile profiles.TargetProfile, project string, instance string) ([]internal.GeneratedResources, error) {
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
								DatastreamResources,
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

func getInstanceDetails(ctx context.Context, targetProfile profiles.TargetProfile) (string, string, error) {
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
