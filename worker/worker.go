package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/hotosm/scaleodm/queue"
)

type Worker struct {
	id        string
	clusterID string
	queue     *queue.Queue
	processor JobProcessor
}

// JobProcessor defines the interface for processing jobs
type JobProcessor interface {
	Process(ctx context.Context, job *queue.Job) error
}

// NewWorker creates a new worker instance
func NewWorker(id, clusterID string, queue *queue.Queue, processor JobProcessor) *Worker {
	return &Worker{
		id:        id,
		clusterID: clusterID,
		queue:     queue,
		processor: processor,
	}
}

// Start begins the worker loop
func (w *Worker) Start(ctx context.Context) error {
	log.Printf("[Worker %s] Starting for cluster %s", w.id, w.clusterID)

	// Check the db every 5 seconds
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[Worker %s] Shutting down", w.id)
			return ctx.Err()
		case <-ticker.C:
			if err := w.processNextJob(ctx); err != nil {
				log.Printf("[Worker %s] Error: %v", w.id, err)
			}
		}
	}
}

func (w *Worker) processNextJob(ctx context.Context) error {
	// Claim next job
	job, err := w.queue.ClaimJob(ctx, w.clusterID, w.id)
	if err != nil {
		return fmt.Errorf("failed to claim job: %w", err)
	}

	if job == nil {
		// No jobs available, continue polling
		return nil
	}

	log.Printf("[Worker %s] Processing job %d (type: %s)", w.id, job.ID, job.JobType)

	// Process the job
	if err := w.processor.Process(ctx, job); err != nil {
		log.Printf("[Worker %s] Job %d failed: %v", w.id, job.ID, err)
		if failErr := w.queue.FailJob(ctx, job.ID, err.Error()); failErr != nil {
			return fmt.Errorf("failed to mark job as failed: %w", failErr)
		}
		return nil
	}

	// Mark job as completed
	if err := w.queue.CompleteJob(ctx, job.ID); err != nil {
		return fmt.Errorf("failed to complete job: %w", err)
	}

	log.Printf("[Worker %s] Job %d completed successfully", w.id, job.ID)
	return nil
}

// ODMJobPayload represents the payload for an ODM processing job
type ODMJobPayload struct {
	ProjectID  string                 `json:"project_id"`
	ImageURLs  []string               `json:"image_urls"`
	NodeODMURL string                 `json:"nodeodm_url"`
	Options    map[string]interface{} `json:"options,omitempty"`
}

// ODMProcessor implements JobProcessor for NodeODM jobs
type ODMProcessor struct {
	// TODO
	// Add NodeODM client or HTTP client here
}

func (p *ODMProcessor) Process(ctx context.Context, job *queue.Job) error {
	var payload ODMJobPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("invalid payload: %w", err)
	}

	// TODO: Call NodeODM API
	// 1. POST to NodeODM to create task
	// 2. Upload images
	// 3. Start processing
	// 4. Poll for completion
	// 5. Download results

	log.Printf("Processing ODM job for project %s with %d images",
		payload.ProjectID, len(payload.ImageURLs))

	// Simulate work for now
	time.Sleep(2 * time.Second)

	return nil
}
