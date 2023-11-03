package workflows

import (
	"fmt"
	"time"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/activities"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/converters"
	activityModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/activities"
	workflowModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/workflows"
	"go.temporal.io/sdk/workflow"
)

func CreateJobWorkflow(ctx workflow.Context, createJobWorkflowInput workflowModel.CreateJobWorkflowInput) (workflowModel.CreateJobWorkflowOutput, error) {
	options := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour,
	}
	ctx = workflow.WithActivityOptions(ctx, options)

	//Step 1. Parse the session file supplied into a conv object
	createConvActivityInput, err := converters.GenerateCreateConvActivityInput(createJobWorkflowInput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{}, err
	}

	var createConvActivityOutput activityModel.CreateConvActivityOutput
	err = workflow.ExecuteActivity(ctx, activities.CreateConvActivity, createConvActivityInput).Get(ctx, &createConvActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{}, err
	}

	//Step 2. Validate supplied schema against existing database
	validateSchemaActivityInput, err := converters.GenerateValidateSchemaActivityInput(createJobWorkflowInput, createConvActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{}, err
	}

	var validateSchemaActivityOutput activityModel.ValidateSchemaActivityOutput
	err = workflow.ExecuteActivity(ctx, activities.ValidateSchemaActivity, validateSchemaActivityInput).Get(ctx, &validateSchemaActivityOutput)
	if err != nil {
		fmt.Printf("Validation failed with error: %v\n", err)
		return workflowModel.CreateJobWorkflowOutput{}, err
	}

	//Step 3. Parse streaming config
	parseJobConfigActivityInput, err := converters.GenerateParseJobConfigActivityInput(createJobWorkflowInput, createConvActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{}, err
	}

	var parseJobConfigActivityOutput activityModel.ParseJobConfigActivityOutput
	err = workflow.ExecuteActivity(ctx, activities.ParseJobConfigActivity, parseJobConfigActivityInput).Get(ctx, &parseJobConfigActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{}, err
	}

	//Step 4. Setup pubsub resources
	setupPubSubActivityInput, err := converters.GenerateSetupPubSubActivityInput(createJobWorkflowInput, parseJobConfigActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{}, err
	}

	var setupPubSubActivityOutput activityModel.SetupPubSubActivityOutput
	err = workflow.ExecuteActivity(ctx, activities.SetupPubSubActivity, setupPubSubActivityInput).Get(ctx, &setupPubSubActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{}, err
	}

	// Step 5. Setup Datastream
	setupDatastreamActivityInput, err := converters.GenerateSetupDatastreamActivityInput(createJobWorkflowInput, parseJobConfigActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{}, err
	}

	var setupDatastreamActivityOutput activityModel.SetupDatastreamActivityOutput
	err = workflow.ExecuteActivity(ctx, activities.SetupDatastreamActivity, setupDatastreamActivityInput).Get(ctx, &setupDatastreamActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{}, err
	}
	// Step 6. Setup Dataflow
	setupDataflowActivityInput, err := converters.GenerateSetupDataflowActivityInput(createJobWorkflowInput, createConvActivityOutput, parseJobConfigActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{}, err
	}
	
	var setupDataflowActivityOutput activityModel.SetupDataflowActivityOutput
	err = workflow.ExecuteActivity(ctx, activities.SetupDataflowActivity, setupDataflowActivityInput).Get(ctx, &setupDataflowActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{}, err
	}

	return workflowModel.CreateJobWorkflowOutput{}, nil
}
