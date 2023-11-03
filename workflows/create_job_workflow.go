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
		return workflowModel.CreateJobWorkflowOutput{WorkflowStatus: "ERROR"}, err
	}

	//Step 3. Parse streaming config
	validateJobConfigActivityInput, err := converters.GenerateValidateJobConfigActivityInput(createJobWorkflowInput, createConvActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{WorkflowStatus: "ERROR"}, err
	}

	var validateJobConfigActivityOutput activityModel.ValidateJobConfigActivityOutput
	err = workflow.ExecuteActivity(ctx, activities.ValidateJobConfigActivity, validateJobConfigActivityInput).Get(ctx, &validateJobConfigActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{WorkflowStatus: "ERROR"}, err
	}

	//Step 4. Setup pubsub resources
	setupPubSubActivityInput, err := converters.GenerateSetupPubSubActivityInput(createJobWorkflowInput, validateJobConfigActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{WorkflowStatus: "ERROR"}, err
	}

	var setupPubSubActivityOutput activityModel.SetupPubSubActivityOutput
	err = workflow.ExecuteActivity(ctx, activities.SetupPubSubActivity, setupPubSubActivityInput).Get(ctx, &setupPubSubActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{WorkflowStatus: "ERROR"}, err
	}

	// Step 5. Setup Datastream
	setupDatastreamActivityInput, err := converters.GenerateSetupDatastreamActivityInput(createJobWorkflowInput, validateJobConfigActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{WorkflowStatus: "ERROR"}, err
	}

	var setupDatastreamActivityOutput activityModel.SetupDatastreamActivityOutput
	err = workflow.ExecuteActivity(ctx, activities.SetupDatastreamActivity, setupDatastreamActivityInput).Get(ctx, &setupDatastreamActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{WorkflowStatus: "ERROR"}, err
	}
	// Step 6. Setup Dataflow
	setupDataflowActivityInput, err := converters.GenerateSetupDataflowActivityInput(createJobWorkflowInput, createConvActivityOutput, setupPubSubActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{WorkflowStatus: "ERROR"}, err
	}

	var setupDataflowActivityOutput activityModel.SetupDataflowActivityOutput
	err = workflow.ExecuteActivity(ctx, activities.SetupDataflowActivity, setupDataflowActivityInput).Get(ctx, &setupDataflowActivityOutput)
	if err != nil {
		return workflowModel.CreateJobWorkflowOutput{WorkflowStatus: "ERROR"}, err
	}

	return workflowModel.CreateJobWorkflowOutput{WorkflowStatus: "SUCCESS"}, nil
}
