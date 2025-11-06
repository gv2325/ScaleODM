package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/hotosm/scaleodm/app/workflows"
)

func main() {
	ctx := context.Background()

	// Create client
	client, err := workflows.NewClient("/home/coder/.kube/config", "argo")
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Example 1: Create and submit a workflow
	fmt.Println("Creating ODM workflow...")
	config := workflows.NewDefaultODMConfig("my-project-id")
	
	// Customize config if needed
	config.S3Bucket = "my-bucket"
	config.ODMArgs = []string{"--fast-orthophoto", "--dsm", "--dtm"}

	wf, err := client.CreateODMWorkflow(ctx, config)
	if err != nil {
		log.Fatalf("Failed to create workflow: %v", err)
	}
	fmt.Printf("Workflow created: %s\n", wf.Name)

	// Example 2: Watch workflow until completion
	fmt.Println("Watching workflow...")
	completedWf, err := client.WatchWorkflow(ctx, wf.Name)
	if err != nil {
		log.Fatalf("Failed to watch workflow: %v", err)
	}
	fmt.Printf("Workflow completed with phase: %s\n", completedWf.Status.Phase)

	// Example 3: Get workflow status
	phase, message, err := client.GetWorkflowStatus(ctx, wf.Name)
	if err != nil {
		log.Fatalf("Failed to get workflow status: %v", err)
	}
	fmt.Printf("Workflow phase: %s, message: %s\n", phase, message)

	// Example 4: Get workflow logs
	fmt.Println("\nRetrieving workflow logs...")
	err = client.GetWorkflowLogs(ctx, wf.Name, os.Stdout)
	if err != nil {
		log.Fatalf("Failed to get workflow logs: %v", err)
	}

	// Example 5: List all workflows
	fmt.Println("\nListing all workflows...")
	wfList, err := client.ListWorkflows(ctx, "")
	if err != nil {
		log.Fatalf("Failed to list workflows: %v", err)
	}
	for _, w := range wfList.Items {
		fmt.Printf("- %s (Phase: %s)\n", w.Name, w.Status.Phase)
	}

	// Example 6: Check if workflow is complete
	isComplete, err := client.IsWorkflowComplete(ctx, wf.Name)
	if err != nil {
		log.Fatalf("Failed to check workflow completion: %v", err)
	}
	fmt.Printf("Workflow complete: %v\n", isComplete)

	// Example 7: Delete workflow (optional, uncomment if needed)
	// fmt.Println("Deleting workflow...")
	// err = client.DeleteWorkflow(ctx, wf.Name)
	// if err != nil {
	// 	log.Fatalf("Failed to delete workflow: %v", err)
	// }
	// fmt.Println("Workflow deleted")
}

// Example: Create workflow and poll for completion
func CreateAndWaitForWorkflow() {
	ctx := context.Background()
	
	client, err := workflows.NewClient("/home/coder/.kube/config", "argo")
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Create workflow
	config := workflows.NewDefaultODMConfig("my-project")
	wf, err := client.CreateODMWorkflow(ctx, config)
	if err != nil {
		log.Fatalf("Failed to create workflow: %v", err)
	}

	fmt.Printf("Workflow %s created, waiting for completion...\n", wf.Name)

	// Poll for completion
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	timeout := time.After(30 * time.Minute)

	for {
		select {
		case <-timeout:
			log.Fatal("Workflow timed out")
		case <-ticker.C:
			phase, message, err := client.GetWorkflowStatus(ctx, wf.Name)
			if err != nil {
				log.Printf("Error checking status: %v", err)
				continue
			}

			fmt.Printf("Status: %s - %s\n", phase, message)

			if phase == "Succeeded" {
				fmt.Println("Workflow succeeded!")
				
				// Get final logs
				fmt.Println("\nFinal logs:")
				err = client.GetWorkflowLogs(ctx, wf.Name, os.Stdout)
				if err != nil {
					log.Printf("Warning: failed to get logs: %v", err)
				}
				return
			} else if phase == "Failed" || phase == "Error" {
				fmt.Printf("Workflow failed with phase: %s\n", phase)
				
				// Get error logs
				fmt.Println("\nError logs:")
				err = client.GetWorkflowLogs(ctx, wf.Name, os.Stdout)
				if err != nil {
					log.Printf("Warning: failed to get logs: %v", err)
				}
				return
			}
		}
	}
}

// Example: Stream logs in real-time while workflow runs
func StreamLogsWhileRunning() {
	ctx := context.Background()
	
	client, err := workflows.NewClient("/home/coder/.kube/config", "argo")
	if err != nil {
		log.Fatalf("Failed to create client: %v", err)
	}

	// Create workflow
	config := workflows.NewDefaultODMConfig("streaming-project")
	wf, err := client.CreateODMWorkflow(ctx, config)
	if err != nil {
		log.Fatalf("Failed to create workflow: %v", err)
	}

	fmt.Printf("Workflow %s created\n", wf.Name)

	// Start goroutine to watch workflow
	done := make(chan bool)
	go func() {
		completedWf, err := client.WatchWorkflow(ctx, wf.Name)
		if err != nil {
			log.Printf("Watch error: %v", err)
		} else {
			fmt.Printf("\nWorkflow completed with status: %s\n", completedWf.Status.Phase)
		}
		done <- true
	}()

	// Periodically fetch and display logs
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			// Fetch final logs
			fmt.Println("\n=== FINAL LOGS ===")
			err = client.GetWorkflowLogs(ctx, wf.Name, os.Stdout)
			if err != nil {
				log.Printf("Failed to get final logs: %v", err)
			}
			return
		case <-ticker.C:
			fmt.Println("\n=== CURRENT LOGS ===")
			err = client.GetWorkflowLogs(ctx, wf.Name, os.Stdout)
			if err != nil {
				log.Printf("Failed to get logs: %v", err)
			}
		}
	}
}
