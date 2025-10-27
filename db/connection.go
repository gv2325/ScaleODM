package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type DB struct {
	Pool *pgxpool.Pool // Changed to export Pool
}

// Create a new database connection pool
func NewDB(connString string) (*DB, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection string: %w", err)
	}

	// Configure pool settings
	config.MaxConns = 20
	config.MinConns = 5
	config.MaxConnLifetime = time.Hour
	config.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Verify connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return &DB{Pool: pool}, nil
}

// Close closes the database connection pool
func (db *DB) Close() {
	db.Pool.Close()
}

// InitSchema creates the required tables and indexes
func (db *DB) InitSchema(ctx context.Context) error {
	schema := `
	CREATE TABLE IF NOT EXISTS job_queue (
		id BIGSERIAL PRIMARY KEY,
		cluster_id TEXT NOT NULL,
		job_type TEXT NOT NULL,
		payload JSONB NOT NULL,
		status TEXT DEFAULT 'pending',
		priority INTEGER DEFAULT 0,
		created_at TIMESTAMPTZ DEFAULT NOW(),
		claimed_at TIMESTAMPTZ,
		claimed_by TEXT,
		completed_at TIMESTAMPTZ,
		error_message TEXT
	);

	CREATE TABLE IF NOT EXISTS cluster_capacity (
		cluster_id TEXT PRIMARY KEY,
		max_concurrent_jobs INTEGER DEFAULT 10,
		current_jobs INTEGER DEFAULT 0,
		last_heartbeat TIMESTAMPTZ DEFAULT NOW()
	);

	CREATE INDEX IF NOT EXISTS idx_job_queue_pending 
		ON job_queue(cluster_id, status, priority DESC, created_at)
		WHERE status = 'pending';
	`

	_, err := db.Pool.Exec(ctx, schema)
	return err
}

// Ping the db to check its available
func (db *DB) HealthCheck(ctx context.Context) error {
	return db.Pool.Ping(ctx)
}
