/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"context"
	"fmt"
	"os"

	workflowModel "github.com/GoogleCloudPlatform/spanner-migration-tool/model/workflows"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/utils/constants"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/workflows"
	"github.com/spf13/cobra"
	"go.temporal.io/sdk/client"
)

// job represents the migrationJobs command
var createJobCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a migration job",
	Long: `A longer description that spans multiple lines and likely contains examples
and usage of using your command. For example:

Cobra is a CLI library for Go that empowers applications.
This application is a tool to generate the needed files
to quickly create a Cobra application.`,
	Args: cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		
		jobId := args[0]
		jobConfigFilePath, _ := cmd.Flags().GetString("jobConfigFilePath")
		sessionFilePath, _ := cmd.Flags().GetString("sessionFilePath")
		targetDetails, _ := cmd.Flags().GetString("targetDetails")
		sourceDbName, _ := cmd.Flags().GetString("sourceDbName")
		source, _ := cmd.Flags().GetString("source")

		// Create the client object just once per process
		c, err := client.Dial(client.Options{})
		if err != nil {
			fmt.Printf("unable to create Temporal client = %v \n", err)
			os.Exit(1)
		}
		defer c.Close()
	
		options := client.StartWorkflowOptions{
			ID:        jobId,
			TaskQueue: constants.CREATE_WORKFLOW_QUEUE,
		}

		targetProfile, err := profiles.NewTargetProfile(targetDetails)
		if err != nil {
			os.Exit(1)
		}
		//Convert CLI input into the Workflow input
		createJobWorkflowInput := workflowModel.CreateJobWorkflowInput{
			JobId: jobId,
			JobConfigFilePath: jobConfigFilePath,
			SessionFilePath: sessionFilePath,
			TargetDetails: targetProfile,
			SourceDatabaseName: sourceDbName,
			SourceDatabaseType: source,
		}
	
		// Start the Workflow
		we, err := c.ExecuteWorkflow(context.Background(), options, workflows.CreateJobWorkflow, createJobWorkflowInput)
		if err != nil {
			fmt.Printf("unable to complete Workflow = %v \n", err)
			os.Exit(1)

		}
		var createJobWorkflowOutput workflowModel.CreateJobWorkflowOutput
		err = we.Get(context.Background(), &createJobWorkflowOutput)
		if err != nil {
			fmt.Printf("unable to get Workflow result = %v \n", err)
			os.Exit(1)
		}
		os.Exit(0)
	},
}

func init() {
	rootCmd.AddCommand(createJobCmd)
	createJobCmd.Flags().StringP("jobConfigFilePath", "j", "", "Local path to the job config file")
	createJobCmd.Flags().StringP("sessionFilePath", "f", "", "Local path to the session file")
	createJobCmd.Flags().StringP("targetDetails", "t", "", "Details of the target spanner")
	createJobCmd.Flags().StringP("sourceDbName", "n", "", "Name of the source database")
	createJobCmd.Flags().StringP("source", "s", "", "Type of the source database (MYSQL, POSTGRES)")
	
	createJobCmd.MarkFlagRequired("jobConfigFilePath")
	createJobCmd.MarkFlagRequired("sessionFilePath")
	createJobCmd.MarkFlagRequired("targetDetails")
	createJobCmd.MarkFlagRequired("sourceDbName")
	createJobCmd.MarkFlagRequired("source")
}
