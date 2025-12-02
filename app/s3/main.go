package s3

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/hotosm/scaleodm/app/config"
)

// S3Credentials holds AWS credentials for job execution
type S3Credentials struct {
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string // Empty for static credentials
}

// GetS3JobCreds returns S3 credentials to send to a job
// Returns temporary STS credentials if STS_ROLE_ARN is configured, otherwise static credentials
// Returns nil if no credentials are configured (for public bucket access)
// region is the AWS region to use for STS operations (defaults to "us-east-1" if empty)
func GetS3JobCreds(region string) (*S3Credentials, error) {
	// Check if we have any credentials configured
	if config.SCALEODM_S3_ACCESS_KEY == "" || config.SCALEODM_S3_SECRET_KEY == "" {
		return nil, nil // No credentials - will attempt public access
	}

	if region == "" {
		region = "us-east-1" // Default region
	}

	if config.SCALEODM_S3_STS_ROLE_ARN != "" {
		return getS3TempCreds(region)
	}
	return &S3Credentials{
		AccessKeyID:     config.SCALEODM_S3_ACCESS_KEY,
		SecretAccessKey: config.SCALEODM_S3_SECRET_KEY,
		SessionToken:    "", // No session token for static credentials
	}, nil
}

// ResolveCredentials resolves S3 credentials with fallback logic:
// 1. Use provided credentials if available (and generate STS temp creds if STS_ROLE_ARN is set)
// 2. Fall back to environment variables (GetS3JobCreds)
// 3. Return error if credentials are required but unavailable
// Returns error only if credentials are required but unavailable
// region is the AWS region to use for STS operations (e.g., "us-east-1")
func ResolveCredentials(provided *S3Credentials, requireForWrite bool, region string) (*S3Credentials, error) {
	// Priority 1: Use provided credentials
	if provided != nil && provided.AccessKeyID != "" && provided.SecretAccessKey != "" {
		// If STS_ROLE_ARN is configured, try to generate temporary credentials
		if config.SCALEODM_S3_STS_ROLE_ARN != "" {
			log.Println("STS_ROLE_ARN configured, attempting to generate temporary credentials from provided credentials")
			tempCreds, err := getS3TempCredsFromCreds(
				provided.AccessKeyID,
				provided.SecretAccessKey,
				config.SCALEODM_S3_STS_ROLE_ARN,
				config.SCALEODM_S3_STS_ENDPOINT,
				region,
			)
			if err != nil {
				log.Printf("Failed to generate STS credentials, falling back to provided credentials: %v", err)
				return provided, nil
			}
			return tempCreds, nil
		}
		// No STS_ROLE_ARN, use provided credentials directly
		return provided, nil
	}

	// Priority 2: Try environment variables
	envCreds, err := GetS3JobCreds(region)
	if err != nil {
		return nil, fmt.Errorf("failed to get credentials from environment: %w", err)
	}
	if envCreds != nil {
		return envCreds, nil
	}

	// Priority 3: No credentials - check if required
	if requireForWrite {
		return nil, fmt.Errorf("S3 credentials required for write operations but none provided")
	}

	// No credentials but not required - will attempt public access
	return nil, nil
}

func GetS3Client() *minio.Client {
	endpoint := config.SCALEODM_S3_ENDPOINT
	accessKey := config.SCALEODM_S3_ACCESS_KEY
	secretKey := config.SCALEODM_S3_SECRET_KEY

	// Determine if we should use secure connection
	// For AWS S3, always use secure. For custom endpoints, check if it's https
	secure := true
	if !strings.HasPrefix(endpoint, "https://") && !strings.HasPrefix(endpoint, "http://") {
		// No protocol specified, assume secure for AWS, but allow override via config
		secure = true
	} else if strings.HasPrefix(endpoint, "http://") {
		secure = false
		endpoint = strings.TrimPrefix(endpoint, "http://")
	} else if strings.HasPrefix(endpoint, "https://") {
		secure = true
		endpoint = strings.TrimPrefix(endpoint, "https://")
	}

	// MinIO client doesn't allow paths, query parameters, or fragments in endpoint
	// Endpoint should be just host:port or host
	// Strip any path, query, or fragment components
	if idx := strings.IndexAny(endpoint, "/?#"); idx != -1 {
		endpoint = endpoint[:idx]
	}

	// Remove trailing slash if present (shouldn't happen after above, but be safe)
	endpoint = strings.TrimSuffix(endpoint, "/")

	minioClient, err := minio.New(endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(accessKey, secretKey, ""),
		Secure: secure,
	})
	if err != nil {
		log.Fatalln(err)
	}

	return minioClient
}

// GetWorkflowLogsFromS3 fetches workflow logs from S3
// writeS3Path is the S3 path where logs are stored (e.g., s3://bucket/path/)
// Returns the log content as a string
func GetWorkflowLogsFromS3(ctx context.Context, client *minio.Client, writeS3Path string) (string, error) {
	// Parse S3 path: s3://bucket/path -> bucket and path
	if !strings.HasPrefix(writeS3Path, "s3://") {
		return "", fmt.Errorf("invalid S3 path: %s", writeS3Path)
	}

	pathParts := strings.TrimPrefix(writeS3Path, "s3://")
	parts := strings.SplitN(pathParts, "/", 2)
	if len(parts) < 1 {
		return "", fmt.Errorf("invalid S3 path format: %s", writeS3Path)
	}

	bucket := parts[0]
	prefix := ""
	if len(parts) > 1 {
		prefix = strings.TrimSuffix(parts[1], "/") + "/"
	}

	logObjectKey := prefix + ".workflow-logs.txt"

	// Get object from S3
	obj, err := client.GetObject(ctx, bucket, logObjectKey, minio.GetObjectOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to get log object from S3: %w", err)
	}
	defer obj.Close()

	// Read object content
	content, err := io.ReadAll(obj)
	if err != nil {
		return "", fmt.Errorf("failed to read log object: %w", err)
	}

	return string(content), nil
}

// ListFilesInS3Path lists files in the S3 path
// writeS3Path is the S3 path where files are stored (e.g., s3://bucket/path/)
// Returns a list of object names (without the prefix)
func ListFilesInS3Path(ctx context.Context, client *minio.Client, writeS3Path string) ([]string, error) {
	// Parse S3 path: s3://bucket/path -> bucket and path
	if !strings.HasPrefix(writeS3Path, "s3://") {
		return nil, fmt.Errorf("invalid S3 path: %s", writeS3Path)
	}

	pathParts := strings.TrimPrefix(writeS3Path, "s3://")
	parts := strings.SplitN(pathParts, "/", 2)
	if len(parts) < 1 {
		return nil, fmt.Errorf("invalid S3 path format: %s", writeS3Path)
	}

	bucket := parts[0]
	prefix := ""
	if len(parts) > 1 {
		prefix = strings.TrimSuffix(parts[1], "/") + "/"
	}

	// List objects with the prefix
	objectCh := client.ListObjects(ctx, bucket, minio.ListObjectsOptions{
		Prefix:    prefix,
		Recursive: false,
	})

	var files []string
	for object := range objectCh {
		if object.Err != nil {
			return nil, fmt.Errorf("failed to list objects: %w", object.Err)
		}
		// Remove the prefix from the object key to get just the filename
		fileName := strings.TrimPrefix(object.Key, prefix)
		// Skip hidden files and directories
		if fileName != "" && !strings.HasPrefix(fileName, ".") {
			files = append(files, fileName)
		}
	}

	return files, nil
}

// GeneratePresignedURL generates a pre-signed URL for downloading a file from S3
// writeS3Path is the S3 path where files are stored (e.g., s3://bucket/path/)
// fileName is the name of the file to download (e.g., "all.zip", "orthophoto.tif")
// expiry is how long the URL should be valid (defaults to 1 hour if 0)
// Returns the pre-signed URL as a string
func GeneratePresignedURL(ctx context.Context, client *minio.Client, writeS3Path, fileName string, expiry time.Duration) (string, error) {
	// Parse S3 path: s3://bucket/path -> bucket and path
	if !strings.HasPrefix(writeS3Path, "s3://") {
		return "", fmt.Errorf("invalid S3 path: %s", writeS3Path)
	}

	pathParts := strings.TrimPrefix(writeS3Path, "s3://")
	parts := strings.SplitN(pathParts, "/", 2)
	if len(parts) < 1 {
		return "", fmt.Errorf("invalid S3 path format: %s", writeS3Path)
	}

	bucket := parts[0]
	prefix := ""
	if len(parts) > 1 {
		prefix = strings.TrimSuffix(parts[1], "/") + "/"
	}

	objectKey := prefix + fileName

	// Default expiry to 1 hour if not specified
	if expiry == 0 {
		expiry = 1 * time.Hour
	}

	// Generate pre-signed URL
	presignedURL, err := client.PresignedGetObject(ctx, bucket, objectKey, expiry, url.Values{})
	if err != nil {
		return "", fmt.Errorf("failed to generate pre-signed URL for %s: %w", objectKey, err)
	}

	return presignedURL.String(), nil
}

// Get temp credentials via STS using environment variables
// region is the AWS region to use for STS operations (defaults to "us-east-1" if empty)
func getS3TempCreds(region string) (*S3Credentials, error) {
	if region == "" {
		region = "us-east-1" // Default region
	}
	return getS3TempCredsFromCreds(
		config.SCALEODM_S3_ACCESS_KEY,
		config.SCALEODM_S3_SECRET_KEY,
		config.SCALEODM_S3_STS_ROLE_ARN,
		config.SCALEODM_S3_STS_ENDPOINT,
		region,
	)
}

// Get temp credentials via STS using provided credentials
// region is the AWS region to use for STS operations (e.g., "us-east-1")
func getS3TempCredsFromCreds(accessKey, secretKey, roleARN, stsEndpoint, region string) (*S3Credentials, error) {
    // Generate unique session name for parallel jobs
    sessionName := "odm-job-" + uuid.New().String()

    // If no endpoint is provided, default to AWS STS global endpoint
    // (MinIO server also allows a direct STS endpoint provided by config)
    if stsEndpoint == "" {
        stsEndpoint = "https://sts.amazonaws.com"
        log.Printf("Using default AWS STS endpoint: %s", stsEndpoint)
    }

    if region == "" {
        region = "us-east-1" // fallback region
    }

    // Build proper STS options
    opts := credentials.STSAssumeRoleOptions{
        AccessKey:       accessKey,
        SecretKey:       secretKey,
        RoleARN:         roleARN,
        RoleSessionName: sessionName,
        DurationSeconds: 43200, // 12 hours, AWS default. Increase if provider supports it?
        Location:        region,
    }

    // Create temporary credential provider
    stsCreds, err := credentials.NewSTSAssumeRole(stsEndpoint, opts)
    if err != nil {
        return nil, fmt.Errorf("failed to create STS credentials: %w", err)
    }

    // Fetch credentials
    credCtx := &credentials.CredContext{}
    credsValue, err := stsCreds.GetWithContext(credCtx)
    if err != nil {
        return nil, fmt.Errorf("failed to get STS credentials: %w", err)
    }

    log.Println("Temporary S3 creds generated, expiry:", credsValue.Expiration.Format(time.RFC3339))

    return &S3Credentials{
        AccessKeyID:     credsValue.AccessKeyID,
        SecretAccessKey: credsValue.SecretAccessKey,
        SessionToken:    credsValue.SessionToken,
    }, nil
}
