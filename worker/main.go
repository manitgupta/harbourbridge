package main

import (
	"fmt"
	"os"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/activities"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/utils/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/workflows"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	// Create the client object just once per process
	c, err := client.Dial(client.Options{})
	if err != nil {
		fmt.Printf("unable to create Temporal client = %v\n", err)
	}
	defer c.Close()
	logger.InitializeLogger("INFO")

	// This worker hosts both Workflow and Activity functions
	w := worker.New(c, constants.CREATE_WORKFLOW_QUEUE, worker.Options{})
	worker.EnableVerboseLogging(false)
	w.RegisterWorkflow(workflows.CreateJobWorkflow)
	w.RegisterActivity(activities.CreateConvActivity)
	w.RegisterActivity(activities.ValidateSchemaActivity)
	w.RegisterActivity(activities.ValidateJobConfigActivity)
	w.RegisterActivity(activities.SetupPubSubActivity)
	w.RegisterActivity(activities.SetupDatastreamActivity)
	w.RegisterActivity(activities.SetupDataflowActivity)

	// Start listening to the Task Queue
	err = w.Run(worker.InterruptCh())
	if err != nil {
		fmt.Printf("unable to start Worker = %v\n", err)
		os.Exit(1)
	}
}
