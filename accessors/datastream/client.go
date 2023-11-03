package datastream

import (
	"context"
	"fmt"
	"sync"

	datastream "cloud.google.com/go/datastream/apiv1"
	datastreampb "cloud.google.com/go/datastream/apiv1/datastreampb"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/streaming"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/utils/constants"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

var once sync.Once

var datastreamClient *datastream.Client

func GetInstance(ctx context.Context) *datastream.Client {
	if datastreamClient == nil {
		once.Do(func() {
			datastreamClient, _ = datastream.NewClient(ctx)
		})
		return datastreamClient
	}
	return datastreamClient
}

func GetGCSPathFromConnectionProfile(ctx context.Context, datastreamClient *datastream.Client, projectID string, datastreamDestinationConnCfg streaming.DstConnCfg) (*datastreampb.GcsProfile, error) {
	dstProf := fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", projectID, datastreamDestinationConnCfg.Location, datastreamDestinationConnCfg.Name)
	res, err := datastreamClient.GetConnectionProfile(ctx, &datastreampb.GetConnectionProfileRequest{Name: dstProf})
	if err != nil {
		return nil, fmt.Errorf("could not get connection profiles: %v", err)
	}
	// Fetch the GCS path from the target connection profile.
	gcsProfile := res.Profile.(*datastreampb.ConnectionProfile_GcsProfile).GcsProfile
	return gcsProfile, nil
}

func CreateStream(ctx context.Context, datastreamClient *datastream.Client, gcpProjectId string, prefix string, sourceDatabaseType string, sourceDatabaseName string, datastreamCfg streaming.DatastreamCfg) error {
	gcsDstCfg := &datastreampb.GcsDestinationConfig{
		Path:       prefix,
		FileFormat: &datastreampb.GcsDestinationConfig_AvroFileFormat{},
	}
	srcCfg := &datastreampb.SourceConfig{
		SourceConnectionProfile: fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", gcpProjectId, datastreamCfg.SourceConnectionConfig.Location, datastreamCfg.SourceConnectionConfig.Name),
	}
	var dbList []profiles.LogicalShard
	dbList = append(dbList, profiles.LogicalShard{DbName: sourceDatabaseName})
	err := getSourceStreamConfig(srcCfg, sourceDatabaseType, dbList, datastreamCfg)
	if err != nil {
		return fmt.Errorf("could not get source stream config: %v", err)
	}

	dstCfg := &datastreampb.DestinationConfig{
		DestinationConnectionProfile: fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", gcpProjectId, datastreamCfg.DestinationConnectionConfig.Location, datastreamCfg.DestinationConnectionConfig.Name),
		DestinationStreamConfig:      &datastreampb.DestinationConfig_GcsDestinationConfig{GcsDestinationConfig: gcsDstCfg},
	}
	streamInfo := &datastreampb.Stream{
		DisplayName:       datastreamCfg.StreamDisplayName,
		SourceConfig:      srcCfg,
		DestinationConfig: dstCfg,
		State:             datastreampb.Stream_RUNNING,
		BackfillStrategy:  &datastreampb.Stream_BackfillAll{BackfillAll: &datastreampb.Stream_BackfillAllStrategy{}},
	}
	createStreamRequest := &datastreampb.CreateStreamRequest{
		Parent:   fmt.Sprintf("projects/%s/locations/%s", gcpProjectId, datastreamCfg.StreamLocation),
		StreamId: datastreamCfg.StreamId,
		Stream:   streamInfo,
	}

	fmt.Println("Created stream request..")

	dsOp, err := datastreamClient.CreateStream(ctx, createStreamRequest)
	if err != nil {
		fmt.Printf("cannot create stream: createStreamRequest: %+v\n", createStreamRequest)
		return fmt.Errorf("cannot create stream: %v ", err)
	}

	_, err = dsOp.Wait(ctx)
	if err != nil {
		fmt.Printf("datastream create operation failed: createStreamRequest: %+v\n", createStreamRequest)
		return fmt.Errorf("datastream create operation failed: %v", err)
	}
	fmt.Println("Successfully created stream ", datastreamCfg.StreamId)
	return nil
}

func UpdateStream(ctx context.Context, datastreamClient *datastream.Client, gcpProjectId string, prefix string, datastreamCfg streaming.DatastreamCfg) error {
	gcsDstCfg := &datastreampb.GcsDestinationConfig{
		Path:       prefix,
		FileFormat: &datastreampb.GcsDestinationConfig_AvroFileFormat{},
	}
	srcCfg := &datastreampb.SourceConfig{
		SourceConnectionProfile: fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", gcpProjectId, datastreamCfg.SourceConnectionConfig.Location, datastreamCfg.SourceConnectionConfig.Name),
	}
	dstCfg := &datastreampb.DestinationConfig{
		DestinationConnectionProfile: fmt.Sprintf("projects/%s/locations/%s/connectionProfiles/%s", gcpProjectId, datastreamCfg.DestinationConnectionConfig.Location, datastreamCfg.DestinationConnectionConfig.Name),
		DestinationStreamConfig:      &datastreampb.DestinationConfig_GcsDestinationConfig{GcsDestinationConfig: gcsDstCfg},
	}

	streamInfo := &datastreampb.Stream{
		DisplayName:       datastreamCfg.StreamDisplayName,
		SourceConfig:      srcCfg,
		DestinationConfig: dstCfg,
		State:             datastreampb.Stream_RUNNING,
		BackfillStrategy:  &datastreampb.Stream_BackfillAll{BackfillAll: &datastreampb.Stream_BackfillAllStrategy{}},
	}
	streamInfo.Name = fmt.Sprintf("projects/%s/locations/%s/streams/%s", gcpProjectId, datastreamCfg.StreamLocation, datastreamCfg.StreamId)
	updateStreamRequest := &datastreampb.UpdateStreamRequest{
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"state"}},
		Stream:     streamInfo,
	}
	upOp, err := datastreamClient.UpdateStream(ctx, updateStreamRequest)
	if err != nil {
		return fmt.Errorf("could not create update request: %v", err)
	}
	_, err = upOp.Wait(ctx)
	if err != nil {
		return fmt.Errorf("update stream operation failed: %v", err)
	}
	fmt.Println("Done")
	return nil
}

// dbName is the name of the database to be migrated.
// tabeList is the common list of tables that need to be migrated from each database
func getMysqlSourceStreamConfig(dbList []profiles.LogicalShard, tableList []string) *datastreampb.SourceConfig_MysqlSourceConfig {
	mysqlTables := []*datastreampb.MysqlTable{}
	for _, table := range tableList {
		includeTable := &datastreampb.MysqlTable{
			Table: table,
		}
		mysqlTables = append(mysqlTables, includeTable)
	}
	includeDbList := []*datastreampb.MysqlDatabase{}
	for _, db := range dbList {
		//create include db object
		includeDb := &datastreampb.MysqlDatabase{
			Database:    db.DbName,
			MysqlTables: mysqlTables,
		}
		includeDbList = append(includeDbList, includeDb)
	}
	//TODO: Clean up fmt.Printf logs and replace them with zap logger.
	fmt.Printf("Include DB List for datastream: %+v\n", includeDbList)
	mysqlSrcCfg := &datastreampb.MysqlSourceConfig{
		IncludeObjects:             &datastreampb.MysqlRdbms{MysqlDatabases: includeDbList},
		MaxConcurrentBackfillTasks: 50,
	}
	return &datastreampb.SourceConfig_MysqlSourceConfig{MysqlSourceConfig: mysqlSrcCfg}
}

func getOracleSourceStreamConfig(dbName string, tableList []string) *datastreampb.SourceConfig_OracleSourceConfig {
	oracleTables := []*datastreampb.OracleTable{}
	for _, table := range tableList {
		includeTable := &datastreampb.OracleTable{
			Table: table,
		}
		oracleTables = append(oracleTables, includeTable)
	}
	oracledb := &datastreampb.OracleSchema{
		Schema:       dbName,
		OracleTables: oracleTables,
	}
	oracleSrcCfg := &datastreampb.OracleSourceConfig{
		IncludeObjects:             &datastreampb.OracleRdbms{OracleSchemas: []*datastreampb.OracleSchema{oracledb}},
		MaxConcurrentBackfillTasks: 50,
	}
	return &datastreampb.SourceConfig_OracleSourceConfig{OracleSourceConfig: oracleSrcCfg}
}

func getPostgreSQLSourceStreamConfig(properties string) (*datastreampb.SourceConfig_PostgresqlSourceConfig, error) {
	params, err := profiles.ParseMap(properties)
	if err != nil {
		return nil, fmt.Errorf("could not parse properties: %v", err)
	}
	var excludeObjects []*datastreampb.PostgresqlSchema
	for _, s := range []string{"information_schema", "postgres", "pg_catalog", "pg_temp_1", "pg_toast", "pg_toast_temp_1"} {
		excludeObjects = append(excludeObjects, &datastreampb.PostgresqlSchema{
			Schema: s,
		})
	}
	replicationSlot, replicationSlotExists := params["replicationSlot"]
	publication, publicationExists := params["publication"]
	if !replicationSlotExists || !publicationExists {
		return nil, fmt.Errorf("replication slot or publication not specified")
	}
	postgresSrcCfg := &datastreampb.PostgresqlSourceConfig{
		ExcludeObjects:             &datastreampb.PostgresqlRdbms{PostgresqlSchemas: excludeObjects},
		ReplicationSlot:            replicationSlot,
		Publication:                publication,
		MaxConcurrentBackfillTasks: 50,
	}
	return &datastreampb.SourceConfig_PostgresqlSourceConfig{PostgresqlSourceConfig: postgresSrcCfg}, nil
}

func getSourceStreamConfig(srcCfg *datastreampb.SourceConfig, sourceDatabaseType string, dbList []profiles.LogicalShard, datastreamCfg streaming.DatastreamCfg) error {
	switch sourceDatabaseType {
	case constants.MYSQL:
		// For MySQL, it supports sharded migrations and batching databases in a physical machine into a single
		//Datastream, so dbList is passed.
		srcCfg.SourceStreamConfig = getMysqlSourceStreamConfig(dbList, datastreamCfg.TableList)
		return nil
	case constants.ORACLE:
		// For Oracle, no sharded migrations or db batching support, so the dbList always contains only one element.
		srcCfg.SourceStreamConfig = getOracleSourceStreamConfig(dbList[0].DbName, datastreamCfg.TableList)
		return nil
	case constants.POSTGRES:
		// For Postgres, tables need to be configured at the schema level, which will require more information List<Dbs> and Map<Schema, List<Tables>>
		// instead of List<Dbs> and List<Tables>. Becuase of this we do not configure postgres datastream at individual table level currently.
		sourceStreamConfig, err := getPostgreSQLSourceStreamConfig(datastreamCfg.Properties)
		if err == nil {
			srcCfg.SourceStreamConfig = sourceStreamConfig
		}
		return err
	default:
		return fmt.Errorf("only MySQL, Oracle and PostgreSQL are supported as source streams")
	}
}
