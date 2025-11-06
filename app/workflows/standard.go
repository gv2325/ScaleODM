package workflows

import (
	"context"
	"fmt"
	"io"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	wfv1 "github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	workflowclient "github.com/argoproj/argo-workflows/v3/pkg/client/clientset/versioned"
	apiv1 "k8s.io/api/core/v1"
)

// Client wraps the Argo Workflows client and Kubernetes client
type Client struct {
	wfClientset *workflowclient.Clientset
	k8sClient   *kubernetes.Clientset
	namespace   string
}

// NewClient creates a new Argo Workflows client with Kubernetes client
func NewClient(kubeconfig, namespace string) (*Client, error) {
	var config *rest.Config
	var err error

	if kubeconfig == "" {
		// Use in-cluster config
		config, err = rest.InClusterConfig()
	} else {
		// Use kubeconfig file
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes config: %w", err)
	}

	// Create Argo Workflows clientset
	wfClientset, err := workflowclient.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow clientset: %w", err)
	}

	// Create Kubernetes clientset for accessing pods
	k8sClient, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	return &Client{
		wfClientset: wfClientset,
		k8sClient:   k8sClient,
		namespace:   namespace,
	}, nil
}

// ODMPipelineConfig holds configuration for ODM pipeline workflow
type ODMPipelineConfig struct {
	ProjectID      string
	S3Bucket       string
	S3Prefix       string
	S3Region       string
	MaxTransfer    string
	ServiceAccount string
	RcloneImage    string
	ODMImage       string
	ODMArgs        []string
}

// NewDefaultODMConfig returns default configuration
func NewDefaultODMConfig(projectID string) *ODMPipelineConfig {
	return &ODMPipelineConfig{
		ProjectID:      projectID,
		S3Bucket:       "drone-tm-public",
		S3Prefix:       "dtm-data/projects/a93e99f5-5aab-4316-b6f8-0acd56975df3",
		S3Region:       "us-east-1",
		MaxTransfer:    "1G",
		ServiceAccount: "argo-odm",
		RcloneImage:    "docker.io/rclone/rclone:1",
		ODMImage:       "opendronemap/odm:latest",
		ODMArgs:        []string{"--fast-orthophoto"},
	}
}

// CreateODMWorkflow creates and submits an ODM processing workflow
func (c *Client) CreateODMWorkflow(ctx context.Context, config *ODMPipelineConfig) (*wfv1.Workflow, error) {
	wf := c.buildODMWorkflow(config)

	created, err := c.wfClientset.ArgoprojV1alpha1().Workflows(c.namespace).Create(
		ctx,
		wf,
		metav1.CreateOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create workflow: %w", err)
	}

	return created, nil
}

// buildODMWorkflow constructs the workflow specification
func (c *Client) buildODMWorkflow(config *ODMPipelineConfig) *wfv1.Workflow {
	// Rclone environment variables
	rcloneEnv := []apiv1.EnvVar{
		{Name: "RCLONE_CONFIG_S3_TYPE", Value: "s3"},
		{Name: "RCLONE_CONFIG_S3_PROVIDER", Value: "AWS"},
		{Name: "RCLONE_CONFIG_S3_ENV_AUTH", Value: "false"},
		{Name: "RCLONE_CONFIG_S3_REGION", Value: config.S3Region},
		{Name: "RCLONE_CONFIG_S3_ACL", Value: "public-read"},
	}

	// Download container
	downloadContainer := wfv1.ContainerNode{
		Container: apiv1.Container{
			Name:    "rclone-download",
			Image:   config.RcloneImage,
			Command: []string{"/bin/sh", "-c"},
			Args: []string{
				fmt.Sprintf(`
echo "Downloading S3 bucket files..."
PROJECT_ID="{{workflow.name}}"
echo "Project ID: $PROJECT_ID"
rclone copy s3:%s/%s/${PROJECT_ID}/images/ /workspace/${PROJECT_ID}/images --progress --max-transfer %s --cutoff-mode soft
echo "Download complete. Files in /workspace/${PROJECT_ID}/images:"
ls -lh /workspace/${PROJECT_ID}/images
				`, config.S3Bucket, config.S3Prefix, config.MaxTransfer),
			},
			Env: rcloneEnv,
		},
	}

	// ODM processing container
	odmArgsStr := ""
	for _, arg := range config.ODMArgs {
		odmArgsStr += arg + " "
	}
	odmContainer := wfv1.ContainerNode{
		Container: apiv1.Container{
			Name:    "odm-process",
			Image:   config.ODMImage,
			Command: []string{"/bin/bash", "-c"},
			Args: []string{
				fmt.Sprintf(`
echo "Running ODM orthophoto process..."
PROJECT_ID="{{workflow.name}}"
echo "Processing project: $PROJECT_ID"
odm_args="%s--project-path /workspace ${PROJECT_ID}"
echo "Executing: python3 run.py $odm_args"
python3 run.py $odm_args
				`, odmArgsStr),
			},
		},
		Dependencies: []string{"rclone-download"},
	}

	// Upload container
	uploadContainer := wfv1.ContainerNode{
		Container: apiv1.Container{
			Name:    "rclone-upload",
			Image:   config.RcloneImage,
			Command: []string{"/bin/sh", "-c"},
			Args: []string{
				fmt.Sprintf(`
echo "Listing output orthophotos..."
PROJECT_ID="{{workflow.name}}"
ls -lh /workspace/${PROJECT_ID}/odm_orthophoto || echo "No orthophoto yet."
echo "Uploading results to S3..."
rclone copy /workspace/${PROJECT_ID}/odm_orthophoto/ s3:%s/%s/${PROJECT_ID}/outputs/ --progress
echo "Upload complete."
				`, config.S3Bucket, config.S3Prefix),
			},
			Env: rcloneEnv,
		},
		Dependencies: []string{"odm-process"},
	}

	// Create workflow
	wf := &wfv1.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "odm-pipeline-",
			Namespace:    c.namespace,
		},
		Spec: wfv1.WorkflowSpec{
			Entrypoint:         "main",
			ServiceAccountName: config.ServiceAccount,
			Templates: []wfv1.Template{
				{
					Name: "main",
					Volumes: []apiv1.Volume{
						{
							Name: "workspace",
							VolumeSource: apiv1.VolumeSource{
								EmptyDir: &apiv1.EmptyDirVolumeSource{},
							},
						},
					},
					ContainerSet: &wfv1.ContainerSetTemplate{
						VolumeMounts: []apiv1.VolumeMount{
							{
								Name:      "workspace",
								MountPath: "/workspace",
							},
						},
						Containers: []wfv1.ContainerNode{
							downloadContainer,
							odmContainer,
							uploadContainer,
						},
					},
				},
			},
		},
	}

	return wf
}

// GetWorkflow retrieves a workflow by name
func (c *Client) GetWorkflow(ctx context.Context, name string) (*wfv1.Workflow, error) {
	wf, err := c.wfClientset.ArgoprojV1alpha1().Workflows(c.namespace).Get(
		ctx,
		name,
		metav1.GetOptions{},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get workflow: %w", err)
	}
	return wf, nil
}

// ListWorkflows lists workflows with optional label selector
func (c *Client) ListWorkflows(ctx context.Context, labelSelector string) (*wfv1.WorkflowList, error) {
	wfList, err := c.wfClientset.ArgoprojV1alpha1().Workflows(c.namespace).List(
		ctx,
		metav1.ListOptions{
			LabelSelector: labelSelector,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to list workflows: %w", err)
	}
	return wfList, nil
}

// DeleteWorkflow deletes a workflow by name
func (c *Client) DeleteWorkflow(ctx context.Context, name string) error {
	err := c.wfClientset.ArgoprojV1alpha1().Workflows(c.namespace).Delete(
		ctx,
		name,
		metav1.DeleteOptions{},
	)
	if err != nil {
		return fmt.Errorf("failed to delete workflow: %w", err)
	}
	return nil
}

// GetWorkflowLogs retrieves logs for a workflow
func (c *Client) GetWorkflowLogs(ctx context.Context, workflowName string, writer io.Writer) error {
	wf, err := c.GetWorkflow(ctx, workflowName)
	if err != nil {
		return err
	}

	// Get logs for each node in the workflow
	for nodeName, node := range wf.Status.Nodes {
		if node.Type != wfv1.NodeTypePod {
			continue
		}

		fmt.Fprintf(writer, "\n=== Logs for node: %s ===\n", nodeName)

		// Get pod logs using Kubernetes client
		podName := node.ID
		podClient := c.k8sClient.CoreV1().Pods(c.namespace)

		// Get container names from the pod
		pod, err := podClient.Get(ctx, podName, metav1.GetOptions{})
		if err != nil {
			fmt.Fprintf(writer, "Warning: failed to get pod %s: %v\n", podName, err)
			continue
		}

		// Get logs for each container
		for _, container := range pod.Spec.Containers {
			fmt.Fprintf(writer, "\n--- Container: %s ---\n", container.Name)

			req := podClient.GetLogs(podName, &apiv1.PodLogOptions{
				Container: container.Name,
			})

			stream, err := req.Stream(ctx)
			if err != nil {
				fmt.Fprintf(writer, "Warning: failed to get logs for container %s: %v\n", container.Name, err)
				continue
			}
			defer stream.Close()

			_, err = io.Copy(writer, stream)
			if err != nil {
				fmt.Fprintf(writer, "Warning: failed to copy logs: %v\n", err)
			}
		}
	}

	return nil
}

// WatchWorkflow watches a workflow until completion and returns the final workflow
func (c *Client) WatchWorkflow(ctx context.Context, workflowName string) (*wfv1.Workflow, error) {
	watcher, err := c.wfClientset.ArgoprojV1alpha1().Workflows(c.namespace).Watch(
		ctx,
		metav1.ListOptions{
			FieldSelector: fmt.Sprintf("metadata.name=%s", workflowName),
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to watch workflow: %w", err)
	}
	defer watcher.Stop()

	for event := range watcher.ResultChan() {
		wf, ok := event.Object.(*wfv1.Workflow)
		if !ok {
			continue
		}

		if wf.Status.Phase == wfv1.WorkflowSucceeded ||
			wf.Status.Phase == wfv1.WorkflowFailed ||
			wf.Status.Phase == wfv1.WorkflowError {
			return wf, nil
		}
	}

	return nil, fmt.Errorf("watch ended unexpectedly")
}

// GetWorkflowStatus returns the current phase and message of a workflow
func (c *Client) GetWorkflowStatus(ctx context.Context, workflowName string) (wfv1.WorkflowPhase, string, error) {
	wf, err := c.GetWorkflow(ctx, workflowName)
	if err != nil {
		return "", "", err
	}
	return wf.Status.Phase, wf.Status.Message, nil
}

// IsWorkflowComplete checks if a workflow has completed (succeeded, failed, or error)
func (c *Client) IsWorkflowComplete(ctx context.Context, workflowName string) (bool, error) {
	phase, _, err := c.GetWorkflowStatus(ctx, workflowName)
	if err != nil {
		return false, err
	}

	return phase == wfv1.WorkflowSucceeded ||
		phase == wfv1.WorkflowFailed ||
		phase == wfv1.WorkflowError, nil
}
