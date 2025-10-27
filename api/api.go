package api

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humago"
	_ "github.com/danielgtaylor/huma/v2/formats/cbor"

	"github.com/hotosm/scaleodm/queue"
)

// Make the API and JobQueue available on each endpoint
type API struct {
	queue *queue.Queue
	api   huma.API
}

// NewAPI creates the Huma API and registers routes.
// It returns the API object and the HTTP handler (stdlib mux) that should be served.
func NewAPI(queue *queue.Queue) (*API, http.Handler) {
	config := huma.DefaultConfig("ScaleODM API", "1.0.0")
	config.DocsPath = "/"
	config.OpenAPIPath = "/openapi.json"
	config.Servers = []*huma.Server{
		{URL: "http://localhost:8080", Description: "ScaleODM"},
	}

	mux := http.NewServeMux()
	humaAPI := humago.New(mux, config)
	a := &API{queue: queue, api: humaAPI}
	a.registerRoutes()

	return a, mux
}

// registerRoutes registers all API operations
func (a *API) registerRoutes() {
	// Health check
	huma.Register(a.api, huma.Operation{
		OperationID: "health-check",
		Method:      http.MethodGet,
		Path:        "/health",
		Summary:     "Health check",
		Description: "Returns service health status",
		Tags:        []string{"System"},
	}, func(ctx context.Context, input *struct{}) (*HealthResponse, error) {
		if err := a.queue.HealthCheck(ctx); err != nil {
			return nil, huma.NewError(503, "Database unavailable", err)
		}
		resp := &HealthResponse{}
		resp.Body.HealthStatus = "healthy"
		resp.Body.Timestamp = time.Now().UTC().Format(time.RFC3339)
		return resp, nil
	})

	// Enqueue job
	huma.Register(a.api, huma.Operation{
		OperationID: "enqueue-job",
		Method:      http.MethodPost,
		Path:        "/api/v1/jobs",
		Summary:     "Enqueue a new job",
		Description: "Adds a job to the processing queue",
		Tags:        []string{"Jobs"},
	}, func(ctx context.Context, input *struct {
		Body EnqueueRequest `json:"body"`
	}) (*EnqueueResponse, error) {
		req := input.Body

		if req.ClusterID == "" {
			return nil, huma.NewError(400, "cluster_id is required")
		}
		if req.JobType == "" {
			return nil, huma.NewError(400, "job_type is required")
		}
		if req.Payload == nil {
			return nil, huma.NewError(400, "payload is required")
		}

		job, err := a.queue.Enqueue(ctx, req.ClusterID, req.JobType, req.Payload, req.Priority)
		if err != nil {
			log.Printf("Failed to enqueue job: %v", err)
			return nil, huma.NewError(500, "Failed to enqueue job", err)
		}

		resp := &EnqueueResponse{}
		resp.Body.JobID = job.ID
		resp.Body.JobStatus = job.Status
		resp.Body.CreatedAt = job.CreatedAt
		return resp, nil
	})

	// Get job
	huma.Register(a.api, huma.Operation{
		OperationID: "get-job",
		Method:      http.MethodGet,
		Path:        "/api/v1/jobs/{jobID}",
		Summary:     "Get job by ID",
		Tags:        []string{"Jobs"},
	}, func(ctx context.Context, input *struct {
		JobID int64 `path:"jobID" minimum:"1"`
	}) (*JobResponse, error) {
		job, err := a.queue.GetJob(ctx, input.JobID)
		if err != nil {
			return nil, huma.NewError(500, "Failed to retrieve job", err)
		}
		if job == nil {
			return nil, huma.NewError(404, "Job not found")
		}
		return jobToResponse(job), nil
	})

	// List jobs
	huma.Register(a.api, huma.Operation{
		OperationID: "list-jobs",
		Method:      http.MethodGet,
		Path:        "/api/v1/jobs",
		Summary:     "List jobs",
		Tags:        []string{"Jobs"},
	}, func(ctx context.Context, input *struct {
		ClusterID string `query:"cluster_id"`
		JobStatus string `query:"status"`
		JobType   string `query:"job_type"`
		Limit     int    `query:"limit" minimum:"1" maximum:"1000" default:"50"`
	}) (*JobListResponse, error) {
		jobs, err := a.queue.ListJobs(ctx, input.ClusterID, input.JobStatus, input.JobType, input.Limit)
		if err != nil {
			return nil, huma.NewError(500, "Failed to list jobs", err)
		}

		resp := &JobListResponse{}
		resp.Body.Jobs = make([]JobResponse, len(jobs))
		for i, j := range jobs {
			resp.Body.Jobs[i] = *jobToResponse(j)
		}
		resp.Body.Total = len(jobs)
		return resp, nil
	})

	// Cancel job
	huma.Register(a.api, huma.Operation{
		OperationID: "cancel-job",
		Method:      http.MethodDelete,
		Path:        "/api/v1/jobs/{jobID}",
		Summary:     "Cancel a pending job",
		Tags:        []string{"Jobs"},
	}, func(ctx context.Context, input *struct {
		JobID int64 `path:"jobID"`
	}) (*MessageResponse, error) {
		err := a.queue.CancelJob(ctx, input.JobID)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return nil, huma.NewError(404, "Job not found or not pending")
			}
			return nil, huma.NewError(500, "Failed to cancel job", err)
		}
		resp := &MessageResponse{}
		resp.Body.Message = "Job cancelled successfully"
		return resp, nil
	})

	// Cluster status
	huma.Register(a.api, huma.Operation{
		OperationID: "get-cluster-status",
		Method:      http.MethodGet,
		Path:        "/api/v1/clusters/{clusterID}/status",
		Summary:     "Get cluster status",
		Tags:        []string{"Clusters"},
	}, func(ctx context.Context, input *struct {
		ClusterID string `path:"clusterID"`
	}) (*ClusterStatusResponse, error) {
		pending, err := a.queue.GetQueueDepth(ctx, input.ClusterID)
		if err != nil {
			return nil, huma.NewError(500, "Failed to get queue depth", err)
		}

		maxJobs, currentJobs, err := a.queue.GetClusterCapacity(ctx, input.ClusterID)
		if err != nil {
			maxJobs, currentJobs = 10, 0
		}

		processing, err := a.queue.GetProcessingJobsCount(ctx, input.ClusterID)
		if err != nil {
			processing = 0
		}

		resp := &ClusterStatusResponse{}
		resp.Body.ClusterID = input.ClusterID
		resp.Body.PendingJobs = pending
		resp.Body.ProcessingJobs = processing
		resp.Body.MaxConcurrent = maxJobs
		resp.Body.CurrentJobs = currentJobs
		resp.Body.AvailableWorkers = max(0, maxJobs-currentJobs)
		return resp, nil
	})

	// Update capacity
	huma.Register(a.api, huma.Operation{
		OperationID: "update-cluster-capacity",
		Method:      http.MethodPost,
		Path:        "/api/v1/clusters/{clusterID}/capacity",
		Summary:     "Update cluster capacity",
		Tags:        []string{"Clusters"},
	}, func(ctx context.Context, input *struct {
		ClusterID string                `path:"clusterID"`
		Body      CapacityUpdateRequest `json:"body"`
	}) (*MessageResponse, error) {
		if input.Body.MaxConcurrentJobs <= 0 {
			return nil, huma.NewError(400, "max_concurrent_jobs must be positive")
		}
		if input.Body.CurrentJobs < 0 {
			return nil, huma.NewError(400, "current_jobs cannot be negative")
		}

		err := a.queue.UpdateClusterCapacity(ctx, input.ClusterID, input.Body.MaxConcurrentJobs, input.Body.CurrentJobs)
		if err != nil {
			return nil, huma.NewError(500, "Failed to update capacity", err)
		}

		resp := &MessageResponse{}
		resp.Body.Message = "Cluster capacity updated successfully"
		return resp, nil
	})

	// Cluster jobs
	huma.Register(a.api, huma.Operation{
		OperationID: "get-cluster-jobs",
		Method:      http.MethodGet,
		Path:        "/api/v1/clusters/{clusterID}/jobs",
		Summary:     "List jobs for a cluster",
		Tags:        []string{"Clusters"},
	}, func(ctx context.Context, input *struct {
		ClusterID     string `path:"clusterID"`
		ClusterStatus string `query:"status"`
		Limit         int    `query:"limit" minimum:"1" maximum:"1000" default:"50"`
	}) (*ClusterJobsResponse, error) {
		jobs, err := a.queue.ListJobs(ctx, input.ClusterID, input.ClusterStatus, "", input.Limit)
		if err != nil {
			return nil, huma.NewError(500, "Failed to get jobs", err)
		}

		resp := &ClusterJobsResponse{}
		resp.Body.ClusterID = input.ClusterID
		resp.Body.Jobs = make([]JobResponse, len(jobs))
		for i, j := range jobs {
			resp.Body.Jobs[i] = *jobToResponse(j)
		}
		resp.Body.Total = len(jobs)
		return resp, nil
	})

	// Queue stats
	huma.Register(a.api, huma.Operation{
		OperationID: "get-queue-stats",
		Method:      http.MethodGet,
		Path:        "/api/v1/queue/stats",
		Summary:     "Get queue statistics",
		Tags:        []string{"Queue"},
	}, func(ctx context.Context, input *struct{}) (*QueueStatsResponse, error) {
		stats, err := a.queue.GetQueueStatistics(ctx)
		if err != nil {
			return nil, huma.NewError(500, "Failed to get stats", err)
		}

		resp := &QueueStatsResponse{}
		if stats != nil {
			resp.Body.TotalJobs = getInt(stats, "total_jobs")
			resp.Body.PendingJobs = getInt(stats, "pending_jobs")
			resp.Body.ProcessingJobs = getInt(stats, "processing_jobs")
			resp.Body.CompletedJobs = getInt(stats, "completed_jobs")
			resp.Body.FailedJobs = getInt(stats, "failed_jobs")
			resp.Body.CancelledJobs = getInt(stats, "cancelled_jobs")
			resp.Body.ByCluster = getMap(stats, "by_cluster")
			resp.Body.ByJobType = getMap(stats, "by_job_type")
			if v, ok := stats["avg_processing_time"].(float64); ok {
				resp.Body.AvgProcessingTime = &v
			}
		}
		return resp, nil
	})
}

// Helper: convert queue.Job to JobResponse
func jobToResponse(job *queue.Job) *JobResponse {
	return &JobResponse{
		ID:           job.ID,
		ClusterID:    job.ClusterID,
		JobType:      job.JobType,
		Payload:      job.Payload,
		JobStatus:    job.Status,
		Priority:     job.Priority,
		CreatedAt:    job.CreatedAt,
		ClaimedAt:    job.ClaimedAt,
		ClaimedBy:    job.ClaimedBy,
		CompletedAt:  job.CompletedAt,
		ErrorMessage: job.ErrorMessage,
	}
}

// Helpers for stats
func getInt(m map[string]interface{}, key string) int {
	if v, ok := m[key].(int); ok {
		return v
	}
	return 0
}

func getMap(m map[string]interface{}, key string) map[string]int {
	if v, ok := m[key].(map[string]int); ok {
		return v
	}
	return nil
}

// max helper
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
