package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/hotosm/scaleodm/db"
)

type JobStatus string

const (
	StatusPending    JobStatus = "pending"
	StatusProcessing JobStatus = "processing"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
)

type Job struct {
	ID           int64           `json:"id"`
	ClusterID    string          `json:"cluster_id"`
	JobType      string          `json:"job_type"`
	Payload      json.RawMessage `json:"payload"`
	Status       JobStatus       `json:"status"`
	Priority     int             `json:"priority"`
	CreatedAt    time.Time       `json:"created_at"`
	ClaimedAt    *time.Time      `json:"claimed_at,omitempty"`
	ClaimedBy    *string         `json:"claimed_by,omitempty"`
	CompletedAt  *time.Time      `json:"completed_at,omitempty"`
	ErrorMessage *string         `json:"error_message,omitempty"`
}

type Queue struct {
	db *db.DB
}

func NewQueue(db *db.DB) *Queue {
	return &Queue{db: db}
}

// Enqueue adds a new job to the queue
func (q *Queue) Enqueue(ctx context.Context, clusterID, jobType string, payload interface{}, priority int) (*Job, error) {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	query := `
		INSERT INTO job_queue (cluster_id, job_type, payload, priority)
		VALUES ($1, $2, $3, $4)
		RETURNING id, cluster_id, job_type, payload, status, priority, created_at
	`

	job := &Job{}
	err = q.db.Pool.QueryRow(ctx, query, clusterID, jobType, payloadJSON, priority).Scan(
		&job.ID, &job.ClusterID, &job.JobType, &job.Payload,
		&job.Status, &job.Priority, &job.CreatedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to enqueue job: %w", err)
	}

	return job, nil
}

// ClaimJob attempts to claim the next available job using SKIP LOCKED
func (q *Queue) ClaimJob(ctx context.Context, clusterID, workerID string) (*Job, error) {
	query := `
		UPDATE job_queue
		SET status = $1, claimed_at = NOW(), claimed_by = $2
		WHERE id = (
			SELECT id FROM job_queue
			WHERE (cluster_id = $3 OR cluster_id = 'global') 
			  AND status = 'pending'
			ORDER BY 
				CASE WHEN cluster_id = $3 THEN 0 ELSE 1 END,  -- Prefer local jobs
				priority DESC, 
				created_at
			FOR UPDATE SKIP LOCKED
			LIMIT 1
		)
		RETURNING id, cluster_id, job_type, payload, status, priority, 
		          created_at, claimed_at, claimed_by
	`

	job := &Job{}
	err := q.db.Pool.QueryRow(ctx, query, StatusProcessing, workerID, clusterID).Scan(
		&job.ID, &job.ClusterID, &job.JobType, &job.Payload,
		&job.Status, &job.Priority, &job.CreatedAt, &job.ClaimedAt, &job.ClaimedBy,
	)

	if err == pgx.ErrNoRows {
		return nil, nil // No jobs available
	}
	if err != nil {
		return nil, fmt.Errorf("failed to claim job: %w", err)
	}

	return job, nil
}

// CompleteJob marks a job as completed
func (q *Queue) CompleteJob(ctx context.Context, jobID int64) error {
	query := `
		UPDATE job_queue
		SET status = $1, completed_at = NOW()
		WHERE id = $2
	`
	_, err := q.db.Pool.Exec(ctx, query, StatusCompleted, jobID)
	if err != nil {
		return fmt.Errorf("failed to complete job: %w", err)
	}
	return nil
}

// FailJob marks a job as failed with an error message
func (q *Queue) FailJob(ctx context.Context, jobID int64, errorMsg string) error {
	query := `
		UPDATE job_queue
		SET status = $1, completed_at = NOW(), error_message = $2
		WHERE id = $3
	`
	_, err := q.db.Pool.Exec(ctx, query, StatusFailed, errorMsg, jobID)
	if err != nil {
		return fmt.Errorf("failed to mark job as failed: %w", err)
	}
	return nil
}

// GetQueueDepth returns the number of pending jobs for a cluster
func (q *Queue) GetQueueDepth(ctx context.Context, clusterID string) (int, error) {
	query := `
		SELECT COUNT(*) FROM job_queue
		WHERE cluster_id = $1 AND status = 'pending'
	`
	var count int
	err := q.db.Pool.QueryRow(ctx, query, clusterID).Scan(&count)
	return count, err
}

// UpdateClusterCapacity updates the capacity information for a cluster
func (q *Queue) UpdateClusterCapacity(ctx context.Context, clusterID string, maxJobs, currentJobs int) error {
	query := `
		INSERT INTO cluster_capacity (cluster_id, max_concurrent_jobs, current_jobs, last_heartbeat)
		VALUES ($1, $2, $3, NOW())
		ON CONFLICT (cluster_id) 
		DO UPDATE SET 
			max_concurrent_jobs = EXCLUDED.max_concurrent_jobs,
			current_jobs = EXCLUDED.current_jobs,
			last_heartbeat = NOW()
	`
	_, err := q.db.Pool.Exec(ctx, query, clusterID, maxJobs, currentJobs)
	return err
}

// GetClusterCapacity retrieves capacity information for a cluster
func (q *Queue) GetClusterCapacity(ctx context.Context, clusterID string) (maxJobs, currentJobs int, err error) {
	query := `
		SELECT max_concurrent_jobs, current_jobs 
		FROM cluster_capacity
		WHERE cluster_id = $1
	`
	err = q.db.Pool.QueryRow(ctx, query, clusterID).Scan(&maxJobs, &currentJobs)
	return
}

// Add these methods to queue.go

// GetJob retrieves a job by ID
func (q *Queue) GetJob(ctx context.Context, jobID int64) (*Job, error) {
	query := `
		SELECT id, cluster_id, job_type, payload, status, priority,
		       created_at, claimed_at, claimed_by, completed_at, error_message
		FROM job_queue
		WHERE id = $1
	`

	job := &Job{}
	err := q.db.Pool.QueryRow(ctx, query, jobID).Scan(
		&job.ID, &job.ClusterID, &job.JobType, &job.Payload,
		&job.Status, &job.Priority, &job.CreatedAt, &job.ClaimedAt,
		&job.ClaimedBy, &job.CompletedAt, &job.ErrorMessage,
	)

	if err == pgx.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	return job, nil
}

// ListJobs retrieves jobs with optional filters
func (q *Queue) ListJobs(ctx context.Context, clusterID, status, jobType string, limit int) ([]*Job, error) {
	baseQuery := `
		SELECT id, cluster_id, job_type, payload, status, priority,
		       created_at, claimed_at, claimed_by, completed_at, error_message
		FROM job_queue
		WHERE 1=1
	`
	args := []interface{}{}
	argCount := 0

	if clusterID != "" {
		argCount++
		baseQuery += fmt.Sprintf(" AND cluster_id = $%d", argCount)
		args = append(args, clusterID)
	}

	if status != "" {
		argCount++
		baseQuery += fmt.Sprintf(" AND status = $%d", argCount)
		args = append(args, status)
	}

	if jobType != "" {
		argCount++
		baseQuery += fmt.Sprintf(" AND job_type = $%d", argCount)
		args = append(args, jobType)
	}

	baseQuery += " ORDER BY created_at DESC"

	if limit > 0 {
		argCount++
		baseQuery += fmt.Sprintf(" LIMIT $%d", argCount)
		args = append(args, limit)
	}

	rows, err := q.db.Pool.Query(ctx, baseQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list jobs: %w", err)
	}
	defer rows.Close()

	jobs := []*Job{}
	for rows.Next() {
		job := &Job{}
		err := rows.Scan(
			&job.ID, &job.ClusterID, &job.JobType, &job.Payload,
			&job.Status, &job.Priority, &job.CreatedAt, &job.ClaimedAt,
			&job.ClaimedBy, &job.CompletedAt, &job.ErrorMessage,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan job: %w", err)
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// CancelJob cancels a pending job
func (q *Queue) CancelJob(ctx context.Context, jobID int64) error {
	query := `
		UPDATE job_queue
		SET status = 'failed', error_message = 'Cancelled by user'
		WHERE id = $1 AND status = 'pending'
	`
	result, err := q.db.Pool.Exec(ctx, query, jobID)
	if err != nil {
		return fmt.Errorf("failed to cancel job: %w", err)
	}

	if result.RowsAffected() == 0 {
		return fmt.Errorf("job not found or not pending")
	}

	return nil
}

// GetProcessingJobsCount returns the number of processing jobs for a cluster
func (q *Queue) GetProcessingJobsCount(ctx context.Context, clusterID string) (int, error) {
	query := `
		SELECT COUNT(*) FROM job_queue
		WHERE cluster_id = $1 AND status = 'processing'
	`
	var count int
	err := q.db.Pool.QueryRow(ctx, query, clusterID).Scan(&count)
	return count, err
}

// GetQueueStatistics returns overall queue statistics
func (q *Queue) GetQueueStatistics(ctx context.Context) (map[string]interface{}, error) {
	query := `
		SELECT 
			COUNT(*) as total,
			COUNT(*) FILTER (WHERE status = 'pending') as pending,
			COUNT(*) FILTER (WHERE status = 'processing') as processing,
			COUNT(*) FILTER (WHERE status = 'completed') as completed,
			COUNT(*) FILTER (WHERE status = 'failed') as failed
		FROM job_queue
	`

	var total, pending, processing, completed, failed int
	err := q.db.Pool.QueryRow(ctx, query).Scan(
		&total, &pending, &processing, &completed, &failed,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get queue statistics: %w", err)
	}

	stats := map[string]interface{}{
		"total_jobs":      total,
		"pending_jobs":    pending,
		"processing_jobs": processing,
		"completed_jobs":  completed,
		"failed_jobs":     failed,
	}

	return stats, nil
}

func (q *Queue) HealthCheck(ctx context.Context) error {
	return q.db.HealthCheck(ctx)
}
