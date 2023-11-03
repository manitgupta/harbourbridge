package activities

import (
	"context"
	"fmt"

	spannerAdminAccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner/admin"
	spannerAccessor "github.com/GoogleCloudPlatform/spanner-migration-tool/accessors/spanner/client"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/common/utils"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
)

func ValidateSchemaActivity(ctx context.Context, input activityModel.ValidateSchemaActivityInput) (activityModel.ValidateSchemaActivityOutput, error) {
	spannerAdminClient := spannerAdminAccessor.GetInstance(ctx)
	dbExists, err := spannerAdminAccessor.CheckExistingDb(ctx, spannerAdminClient, input.DbURI)
	if err != nil {
		err = fmt.Errorf("can't verify target database: %v", err)
		return activityModel.ValidateSchemaActivityOutput{ValidationResult: "ERROR"}, err
	}
	if !dbExists {
		err = fmt.Errorf("target database doesn't exist")
		return activityModel.ValidateSchemaActivityOutput{ValidationResult: "ERROR"}, err
	}
	var nonEmptyTableName string
	spannerClient := spannerAccessor.GetInstance(ctx, input.DbURI)
	nonEmptyTableName, err = spannerAccessor.ValidateTables(ctx, spannerClient, input.Conv.SpDialect)
	if err != nil {
		err = fmt.Errorf("error validating the tables: %v", err)
		return activityModel.ValidateSchemaActivityOutput{ValidationResult: "ERROR"}, err
	}
	if nonEmptyTableName != "" {
		fmt.Printf("WARNING: Some tables in the database are non-empty e.g %s, overwriting these tables can lead to unintended behaviour. If this is unintended, please reconsider your migration attempt.\n\n", nonEmptyTableName)
	}
	spannerConv := internal.MakeConv()
	spannerConv.SpDialect = input.Conv.SpDialect
	err = utils.ReadSpannerSchema(ctx, spannerConv, spannerClient)
	if err != nil {
		err = fmt.Errorf("can't read spanner schema: %v", err)
		return activityModel.ValidateSchemaActivityOutput{ValidationResult: "ERROR"}, err
	}
	err = utils.CompareSchema(input.Conv, spannerConv)
	if err != nil {
		err = fmt.Errorf("error while comparing the schema from session file and existing spanner schema: %v", err)
		return activityModel.ValidateSchemaActivityOutput{ValidationResult: "FAILURE"}, err
	}
	return activityModel.ValidateSchemaActivityOutput{ValidationResult: "SUCCESS"}, nil
}