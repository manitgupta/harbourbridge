package cmd

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	"github.com/google/subcommands"
)

type CleanupCmd struct {
	jobId         string
	dataShardIds  string
	targetProfile string
	datastream    bool
	dataflow      bool
	pubsub        bool
	metadataDb    bool
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
	f.BoolVar(&cmd.metadataDb, "meta", false, "Flag for specifying if the jobId entry for the specified jobId should be deleted from the internal metadata database. This will only have an effect if all the resources (dataflow, datastream, and pubsub) were specified for deletion. Defaults to FALSE.")
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
		logger.Log.Debug(fmt.Sprintf("Target profile is not properly configured, this is needed for SMT to lookup job details in the metadata database: %v\n", err))
		return subcommands.ExitFailure
	}
	project, instance, err := streaming.GetInstanceDetails(ctx, targetProfile)
	if err != nil {
		logger.Log.Debug(fmt.Sprintf("can't get resource ids: %v\n", err))
		return subcommands.ExitFailure
	}
	dataShardIds, err := profiles.ParseList(cmd.dataShardIds)
	if err != nil {
		logger.Log.Debug(fmt.Sprintf("Could not parse datashardIds: %v\n", err))
		return subcommands.ExitFailure
	}
	if !(cmd.datastream || cmd.dataflow || cmd.pubsub) {
		logger.Log.Error("At least one of datastream, dataflow or pubsub must be specified, we recommend cleaning up all resources!\n")
		return subcommands.ExitUsageError
	}
	// all input parameters have been validated
	if cmd.validate {
		fmt.Println("All input parameters are valid.")
		return subcommands.ExitSuccess
	}
	generatedResourcesList, err := streaming.GetJobDetails(ctx, cmd.jobId, dataShardIds, targetProfile, project, instance)
	if err != nil {
		logger.Log.Debug(fmt.Sprintf("Unable to fetch job details from the internal metadata database: %v\n", err))
		return subcommands.ExitFailure
	}
	for _, generatedResources := range generatedResourcesList {
		cleanupOptions := streaming.CleanupOptions{
			Datastream: cmd.datastream,
			Dataflow:   cmd.dataflow,
			Pubsub:     cmd.pubsub,
			MetaDb:     cmd.metadataDb,
		}
		logger.Log.Info(fmt.Sprintf("Initiating cleanup for jobId: %s, dataShardId: %s\n", generatedResources.MigrationJobId, generatedResources.DataShardId))
		err = streaming.InitiateCleanup(ctx, cleanupOptions, generatedResources, project, instance)
		if err != nil {
			logger.Log.Debug(fmt.Sprintf("Unable to cleanup resources: %v\n", err))
			return subcommands.ExitFailure
		} else {
			logger.Log.Info(fmt.Sprintf("Successfully cleaned up resources for jobId: %v\n", cmd.jobId))
		}
	}
	return subcommands.ExitSuccess
}
