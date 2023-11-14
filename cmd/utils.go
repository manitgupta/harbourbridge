// Copyright 2022 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	sp "cloud.google.com/go/spanner"
	database "cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/conversion"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/spanner/writer"
)

var (
	badDataFile = ".dropped.txt"
	schemaFile  = ".schema.txt"
	sessionFile = ".session.json"
)

const (
	DefaultWritersLimit  = 40
	completionPercentage = 100
)

// CreateDatabaseClient creates new database client and admin client.
func CreateDatabaseClient(ctx context.Context, targetProfile profiles.TargetProfile, driver, dbName string, ioHelper utils.IOStreams) (*database.DatabaseAdminClient, *sp.Client, string, error) {
	if targetProfile.Conn.Sp.Dbname == "" {
		targetProfile.Conn.Sp.Dbname = dbName
	}
	project, instance, dbName, err := targetProfile.GetResourceIds(ctx, time.Now(), driver, ioHelper.Out)
	if err != nil {
		return nil, nil, "", err
	}
	fmt.Println("Using Google Cloud project:", project)
	fmt.Println("Using Cloud Spanner instance:", instance)
	utils.PrintPermissionsWarning(driver, ioHelper.Out)

	dbURI := fmt.Sprintf("projects/%s/instances/%s/databases/%s", project, instance, dbName)
	adminClient, err := utils.NewDatabaseAdminClient(ctx)
	if err != nil {
		err = fmt.Errorf("can't create admin client: %v", utils.AnalyzeError(err, dbURI))
		return nil, nil, dbURI, err
	}
	client, err := utils.GetClient(ctx, dbURI)
	if err != nil {
		err = fmt.Errorf("can't create client for db %s: %v", dbURI, err)
		return adminClient, nil, dbURI, err
	}
	return adminClient, client, dbURI, nil
}

// PrepareMigrationPrerequisites creates source and target profiles, opens a new IOStream and generates the database name.
func PrepareMigrationPrerequisites(sourceProfileString, targetProfileString, source string) (profiles.SourceProfile, profiles.TargetProfile, utils.IOStreams, string, error) {
	targetProfile, err := profiles.NewTargetProfile(targetProfileString)
	if err != nil {
		return profiles.SourceProfile{}, profiles.TargetProfile{}, utils.IOStreams{}, "", err
	}

	sourceProfile, err := profiles.NewSourceProfile(sourceProfileString, source)
	if err != nil {
		return profiles.SourceProfile{}, targetProfile, utils.IOStreams{}, "", err
	}
	sourceProfile.Driver, err = sourceProfile.ToLegacyDriver(source)
	if err != nil {
		return profiles.SourceProfile{}, targetProfile, utils.IOStreams{}, "", err
	}

	dumpFilePath := ""
	if sourceProfile.Ty == profiles.SourceProfileTypeFile && (sourceProfile.File.Format == "" || sourceProfile.File.Format == "dump") {
		dumpFilePath = sourceProfile.File.Path
	}
	ioHelper := utils.NewIOStreams(sourceProfile.Driver, dumpFilePath)
	if ioHelper.SeekableIn != nil {
		defer ioHelper.In.Close()
	}

	dbName, err := utils.GetDatabaseName(sourceProfile.Driver, time.Now())
	if err != nil {
		err = fmt.Errorf("can't generate database name for prefix: %v", err)
		return sourceProfile, targetProfile, ioHelper, "", err
	}
	return sourceProfile, targetProfile, ioHelper, dbName, nil
}

// MigrateData creates database and populates data in it.
func MigrateDatabase(ctx context.Context, targetProfile profiles.TargetProfile, sourceProfile profiles.SourceProfile, dbName string, ioHelper *utils.IOStreams, cmd interface{}, conv *internal.Conv, migrationError *error) (*writer.BatchWriter, error) {
	var (
		bw  *writer.BatchWriter
		err error
	)
	defer func() {
		if err != nil && migrationError != nil {
			*migrationError = err
		}
	}()
	adminClient, client, dbURI, err := CreateDatabaseClient(ctx, targetProfile, sourceProfile.Driver, dbName, *ioHelper)
	if err != nil {
		err = fmt.Errorf("can't create database client: %v", err)
		return nil, err
	}
	defer adminClient.Close()
	defer client.Close()
	switch v := cmd.(type) {
	case *SchemaCmd:
		err = migrateSchema(ctx, targetProfile, sourceProfile, ioHelper, conv, dbURI, adminClient)
	case *DataCmd:
		bw, err = migrateData(ctx, targetProfile, sourceProfile, ioHelper, conv, dbURI, adminClient, client, v)
	case *SchemaAndDataCmd:
		bw, err = migrateSchemaAndData(ctx, targetProfile, sourceProfile, ioHelper, conv, dbURI, adminClient, client, v)
	}
	if err != nil {
		err = fmt.Errorf("can't migrate database: %v", err)
		return nil, err
	}
	return bw, nil
}

// StoreGeneratedResources stores all the generated resources for a minimal downtime migration pipeline in the metadata db.
func StoreGeneratedResources(ctx context.Context, targetProfile profiles.TargetProfile, sourceProfile profiles.SourceProfile, driver string, ioHelper *utils.IOStreams, conv *internal.Conv, migrationJobId string) (err error) {
	fmt.Println("Storing generated resources...")
	project, instance, dbName, err := targetProfile.GetResourceIds(ctx, time.Now(), driver, ioHelper.Out)
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
	if sourceProfile.Ty == profiles.SourceProfileTypeConnection {
		err := storeGeneratedResourcesForSingleShard(ctx, conv, migrationJobId, dbName, client)
		if err != nil {
			err = fmt.Errorf("can't store generated resources : %v", err)
			return err
		}
	} else {
		errShards, _ := storeGeneratedResourcesForShards(ctx, conv, migrationJobId, dbName, client)
		if len(errShards) != 0 {
			err = fmt.Errorf("generated resources could not be stored for %d shards, datashardsIds: %v", len(errShards), errShards)
			return err
		}
	}
	fmt.Printf("Generated resources stored successfully for migration jobId: %s. You can also look at the 'spannermigrationtool_metadata' database in your spanner instance to get this jobId at a later point of time.\n", migrationJobId)
	return nil
}

// Stores generated resources for a non-sharded migration, returns an err if unsuccessful
func storeGeneratedResourcesForSingleShard(ctx context.Context, conv *internal.Conv, migrationJobId string, dbName string, client *sp.Client) error {
	dataflowResources := internal.ShardedDataflowJobResources{JobId: conv.Audit.StreamingStats.DataflowJobId, GcloudCmd: conv.Audit.StreamingStats.DataflowGcloudCmd}
	err := writeGeneratedResourcesToMetadata(ctx, migrationJobId, "smt-default", dataflowResources, conv.Audit.StreamingStats.DataStreamName, conv.Audit.StreamingStats.PubsubCfg, dbName, time.Now(), client)
	if err != nil {
		fmt.Printf("can't store generated resources: %v\n", err)
		return err
	}
	return nil
}

// Stores generated resources for all the shards, returns a list of failed shards
func storeGeneratedResourcesForShards(ctx context.Context, conv *internal.Conv, migrationJobId string, dbName string, client *sp.Client) ([]string, error) {
	var dataShardIds []string
	for dataShardId := range conv.Audit.StreamingStats.ShardToDataStreamNameMap {
		dataShardIds = append(dataShardIds, dataShardId)
	}
	var errShards []string
	for _, dataShardId := range dataShardIds {
		err := writeGeneratedResourcesToMetadata(ctx, migrationJobId, dataShardId, conv.Audit.StreamingStats.ShardToDataflowInfoMap[dataShardId], conv.Audit.StreamingStats.ShardToDataStreamNameMap[dataShardId], conv.Audit.StreamingStats.ShardToPubsubIdMap[dataShardId], dbName, time.Now(), client)
		if err != nil {
			fmt.Printf("can't store generated resources for data shard %s: %v\n", dataShardId, err)
			errShards = append(errShards, dataShardId)
		}
	}
	return errShards, nil
}

func writeGeneratedResourcesToMetadata(ctx context.Context, migrationJobId string, dataShardId string, dataflowResources internal.ShardedDataflowJobResources, datastreamResources string, pubsubResources internal.PubsubCfg, spannerDatabaseName string, createTimestamp time.Time, client *sp.Client) error {
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
	fmt.Printf("Storing generated resources for data shard %s...\n", dataShardId)
	generatedResources := internal.GeneratedResources{
		MigrationJobId:      migrationJobId,
		DataShardId:         dataShardId,
		DatastreamResources: datastreamResources,
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

func migrateSchema(ctx context.Context, targetProfile profiles.TargetProfile, sourceProfile profiles.SourceProfile,
	ioHelper *utils.IOStreams, conv *internal.Conv, dbURI string, adminClient *database.DatabaseAdminClient) error {
	err := conversion.CreateOrUpdateDatabase(ctx, adminClient, dbURI, sourceProfile.Driver, conv, ioHelper.Out, sourceProfile.Config.ConfigType)
	if err != nil {
		err = fmt.Errorf("can't create/update database: %v", err)
		return err
	}
	conv.Audit.Progress.UpdateProgress("Schema migration complete.", completionPercentage, internal.SchemaMigrationComplete)
	return nil
}

func migrateData(ctx context.Context, targetProfile profiles.TargetProfile, sourceProfile profiles.SourceProfile,
	ioHelper *utils.IOStreams, conv *internal.Conv, dbURI string, adminClient *database.DatabaseAdminClient, client *sp.Client, cmd *DataCmd) (*writer.BatchWriter, error) {
	var (
		bw  *writer.BatchWriter
		err error
	)
	if !sourceProfile.UseTargetSchema() {
		err = validateExistingDb(ctx, conv.SpDialect, dbURI, adminClient, client, conv)
		if err != nil {
			err = fmt.Errorf("error while validating existing database: %v", err)
			return nil, err
		}
	}
	bw, err = conversion.DataConv(ctx, sourceProfile, targetProfile, ioHelper, client, conv, true, cmd.WriteLimit)
	if err != nil {
		err = fmt.Errorf("can't finish data conversion for db %s: %v", dbURI, err)
		return nil, err
	}
	conv.Audit.Progress.UpdateProgress("Data migration complete.", completionPercentage, internal.DataMigrationComplete)
	if !cmd.SkipForeignKeys {
		if err = conversion.UpdateDDLForeignKeys(ctx, adminClient, dbURI, conv, ioHelper.Out, sourceProfile.Driver, sourceProfile.Config.ConfigType); err != nil {
			err = fmt.Errorf("can't perform update schema on db %s with foreign keys: %v", dbURI, err)
			return bw, err
		}
	}
	return bw, nil
}

func migrateSchemaAndData(ctx context.Context, targetProfile profiles.TargetProfile, sourceProfile profiles.SourceProfile,
	ioHelper *utils.IOStreams, conv *internal.Conv, dbURI string, adminClient *database.DatabaseAdminClient, client *sp.Client, cmd *SchemaAndDataCmd) (*writer.BatchWriter, error) {
	err := conversion.CreateOrUpdateDatabase(ctx, adminClient, dbURI, sourceProfile.Driver, conv, ioHelper.Out, sourceProfile.Config.ConfigType)
	if err != nil {
		err = fmt.Errorf("can't create/update database: %v", err)
		return nil, err
	}
	conv.Audit.Progress.UpdateProgress("Schema migration complete.", completionPercentage, internal.SchemaMigrationComplete)
	bw, err := conversion.DataConv(ctx, sourceProfile, targetProfile, ioHelper, client, conv, true, cmd.WriteLimit)
	if err != nil {
		err = fmt.Errorf("can't finish data conversion for db %s: %v", dbURI, err)
		return nil, err
	}

	conv.Audit.Progress.UpdateProgress("Data migration complete.", completionPercentage, internal.DataMigrationComplete)
	if !cmd.SkipForeignKeys {
		if err = conversion.UpdateDDLForeignKeys(ctx, adminClient, dbURI, conv, ioHelper.Out, sourceProfile.Driver, sourceProfile.Config.ConfigType); err != nil {
			err = fmt.Errorf("can't perform update schema on db %s with foreign keys: %v", dbURI, err)
			return bw, err
		}
	}
	return bw, nil
}
