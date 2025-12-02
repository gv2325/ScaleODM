package testutil

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// TestDBURL returns the database URL from SCALEODM_DATABASE_URL environment variable
func TestDBURL() string {
	dbURL := os.Getenv("SCALEODM_DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://odm:odm@localhost:31101/scaleodm?sslmode=disable"
	}
	return dbURL
}

// WaitForDB waits for the database to be available
func WaitForDB(dbURL string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		config, err := pgxpool.ParseConfig(dbURL)
		if err != nil {
			return fmt.Errorf("failed to parse connection string: %w", err)
		}

		pool, err := pgxpool.NewWithConfig(context.Background(), config)
		if err != nil {
			time.Sleep(1 * time.Second)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = pool.Ping(ctx)
		cancel()
		pool.Close()

		if err == nil {
			return nil
		}

		time.Sleep(1 * time.Second)
	}

	return fmt.Errorf("database not available after %v", timeout)
}

// TestS3Endpoint returns the S3 endpoint from SCALEODM_S3_ENDPOINT environment variable
func TestS3Endpoint() string {
	endpoint := os.Getenv("SCALEODM_S3_ENDPOINT")
	if endpoint == "" {
		endpoint = "localhost:31102"
	}
	return endpoint
}

// TestS3AccessKey returns the S3 access key from SCALEODM_S3_ACCESS_KEY environment variable
func TestS3AccessKey() string {
	accessKey := os.Getenv("SCALEODM_S3_ACCESS_KEY")
	if accessKey == "" {
		accessKey = "odm"
	}
	return accessKey
}

// TestS3SecretKey returns the S3 secret key from SCALEODM_S3_SECRET_KEY environment variable
func TestS3SecretKey() string {
	secretKey := os.Getenv("SCALEODM_S3_SECRET_KEY")
	if secretKey == "" {
		secretKey = "somelongpassword"
	}
	return secretKey
}

// SetupTestS3Bucket creates a test bucket in MinIO if it doesn't exist
// This should be called before tests that use S3 buckets
// Tries both HTTP and HTTPS to handle different MinIO configurations
func SetupTestS3Bucket(ctx context.Context, bucketName string) error {
	endpoint := TestS3Endpoint()
	accessKey := TestS3AccessKey()
	secretKey := TestS3SecretKey()

	// Try HTTP first (typical for local MinIO), then HTTPS
	for _, secure := range []bool{false, true} {
		client, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
			Secure: secure,
		})
		if err != nil {
			if secure {
				// If HTTPS also fails, return the error
				return fmt.Errorf("failed to create MinIO client (tried HTTP and HTTPS): %w", err)
			}
			// Try HTTPS next
			continue
		}

		// Check if bucket exists
		exists, err := client.BucketExists(ctx, bucketName)
		if err != nil {
			if secure {
				// If HTTPS also fails, return the error
				return fmt.Errorf("failed to check if bucket exists (tried HTTP and HTTPS): %w", err)
			}
			// Try HTTPS next
			continue
		}

		if !exists {
			// Create bucket
			err = client.MakeBucket(ctx, bucketName, minio.MakeBucketOptions{
				Region: "us-east-1",
			})
			if err != nil {
				if secure {
					return fmt.Errorf("failed to create bucket %q (tried HTTP and HTTPS): %w", bucketName, err)
				}
				// Try HTTPS next
				continue
			}
		}

		// Success
		return nil
	}

	return fmt.Errorf("failed to set up bucket %q: all connection attempts failed", bucketName)
}

