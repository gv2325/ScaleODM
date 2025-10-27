package api

import (
	"encoding/json"
	"time"

	"github.com/hotosm/scaleodm/queue"
)

type EnqueueRequest struct {
	ClusterID string                 `json:"cluster_id" doc:"Cluster to enqueue job on"`
	JobType   string                 `json:"job_type" doc:"Type of job (e.g. odm)"`
	Payload   map[string]interface{} `json:"payload" doc:"Job payload"`
	Priority  int                    `json:"priority,omitempty" doc:"Job priority (higher = sooner)"`
}

type EnqueueResponse struct {
	Body struct {
		JobID     int64           `json:"job_id" example:"123"`
		JobStatus queue.JobStatus `json:"status" example:"pending"`
		CreatedAt time.Time       `json:"created_at" example:"2025-04-05T12:00:00Z"`
	} `json:"body"`
}

type JobResponse struct {
	ID           int64           `json:"id"`
	ClusterID    string          `json:"cluster_id"`
	JobType      string          `json:"job_type"`
	Payload      json.RawMessage `json:"payload"`
	JobStatus    queue.JobStatus `json:"status"`
	Priority     int             `json:"priority"`
	CreatedAt    time.Time       `json:"created_at"`
	ClaimedAt    *time.Time      `json:"claimed_at,omitempty"`
	ClaimedBy    *string         `json:"claimed_by,omitempty"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty"`
	ErrorMessage *string         `json:"error_message,omitempty"`
}

type JobListResponse struct {
	Body struct {
		Jobs  []JobResponse `json:"jobs"`
		Total int           `json:"total"`
	} `json:"body"`
}

type ClusterStatusResponse struct {
	Body struct {
		ClusterID        string `json:"cluster_id"`
		PendingJobs      int    `json:"pending_jobs"`
		ProcessingJobs   int    `json:"processing_jobs"`
		MaxConcurrent    int    `json:"max_concurrent_jobs"`
		CurrentJobs      int    `json:"current_jobs"`
		AvailableWorkers int    `json:"available_workers"`
	} `json:"body"`
}

type ClusterJobsResponse struct {
	Body struct {
		ClusterID string        `json:"cluster_id"`
		Jobs      []JobResponse `json:"jobs"`
		Total     int           `json:"total"`
	} `json:"body"`
}

type CapacityUpdateRequest struct {
	MaxConcurrentJobs int `json:"max_concurrent_jobs" minimum:"1" example:"10"`
	CurrentJobs       int `json:"current_jobs" minimum:"0" example:"3"`
}

type MessageResponse struct {
	Body struct {
		Message string `json:"message" example:"Success"`
	} `json:"body"`
}

type HealthResponse struct {
	Body struct {
		HealthStatus string `json:"status" example:"healthy"`
		Timestamp    string `json:"timestamp" example:"2025-04-05T12:00:00Z"`
	} `json:"body"`
}

type QueueStatsResponse struct {
	Body struct {
		TotalJobs         int            `json:"total_jobs"`
		PendingJobs       int            `json:"pending_jobs"`
		ProcessingJobs    int            `json:"processing_jobs"`
		CompletedJobs     int            `json:"completed_jobs"`
		FailedJobs        int            `json:"failed_jobs"`
		CancelledJobs     int            `json:"cancelled_jobs"`
		ByCluster         map[string]int `json:"by_cluster,omitempty"`
		ByJobType         map[string]int `json:"by_job_type,omitempty"`
		AvgProcessingTime *float64       `json:"avg_processing_time,omitempty"`
	} `json:"body"`
}
