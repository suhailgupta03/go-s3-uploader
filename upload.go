package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/zerodha/logf"
	"os"
	"time"
)

type Config struct {
	AWSConfigFile            string
	AWSSharedCredentialsFile string
}

type RetentionConfig struct {
	Use  bool
	Time time.Time
}
type S3 struct {
	AWSConfig       *Config
	BucketName      string
	RetentionConfig *RetentionConfig
	Lo              *logf.Logger
	client          *awss3.Client
	loadedConfig    aws.Config
}

type UploadId string

func generateRandomString(numBytes int) (*string, error) {
	randomBytes := make([]byte, numBytes)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return nil, err
	}

	randomString := hex.EncodeToString(randomBytes)
	return &randomString, nil
}

func (s3 *S3) bucketExists() (bool, error) {
	_, err := s3.client.HeadBucket(context.TODO(), &awss3.HeadBucketInput{
		Bucket: aws.String(s3.BucketName),
	})
	exists := true
	if err != nil {
		var apiError smithy.APIError
		if errors.As(err, &apiError) {
			switch apiError.(type) {
			case *types.NotFound:
				s3.Lo.Info("Bucket is available", "name", s3.BucketName)
				exists = false
				err = nil
			default:
				s3.Lo.Error("Either you don't have access to bucket or another error occurred", "bucket", s3.BucketName, "error", err)
			}
		}
	} else {
		s3.Lo.Info("Bucket exists and you already own it.", "bucket", s3.BucketName)
	}

	return exists, err
}

// createBucket creates S3 bucket. Enabled versioning is retention config is passed
func (s3 *S3) createBucket() error {
	lock := true
	_, err := s3.client.CreateBucket(context.TODO(), &awss3.CreateBucketInput{
		Bucket: aws.String(s3.BucketName),
		// The bucket and its contents are private.
		// Only the owner has access rights. This is also the default ACL for any new bucket.
		ACL:                        types.BucketCannedACLPrivate,
		ObjectLockEnabledForBucket: &lock,
		CreateBucketConfiguration: &types.CreateBucketConfiguration{
			LocationConstraint: types.BucketLocationConstraint(s3.loadedConfig.Region),
		},
	})
	if err != nil {
		s3.Lo.Error("Couldn't create bucket in Region", "bucket", s3.BucketName, "error", err)
	}

	if s3.RetentionConfig != nil {
		if _, err := s3.client.PutBucketVersioning(context.TODO(), &awss3.PutBucketVersioningInput{
			Bucket: aws.String(s3.BucketName),
			VersioningConfiguration: &types.VersioningConfiguration{
				Status: types.BucketVersioningStatusEnabled,
			},
		}); err != nil {
			s3.Lo.Error("Failed to enable versioning for bucket", "bucket", s3.BucketName, "error", err)
			return err
		}
	}
	return err
}

// UploadFile uploads to S3 and generates the upload id
func (s3 *S3) UploadFile(data []byte, identifier string) (*UploadId, error) {
	if s3.AWSConfig != nil {
		os.Setenv("AWS_CONFIG_FILE", s3.AWSConfig.AWSConfigFile)
		os.Setenv("AWS_SHARED_CREDENTIALS_FILE", s3.AWSConfig.AWSSharedCredentialsFile)
	}

	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		s3.Lo.Error("Error loading AWS config", "error", err)
		return nil, err
	}
	client := awss3.NewFromConfig(cfg)
	s3.client = client
	bucketExists, _ := s3.bucketExists()
	if !bucketExists {
		err := s3.createBucket()
		if err != nil {
			s3.Lo.Error("Failed to create the bucket")
			return nil, err
		}
	}

	randomString, _ := generateRandomString(6)
	uploadKey := *randomString + identifier
	uploadId := UploadId(uploadKey)
	_, err = client.PutObject(context.TODO(), &awss3.PutObjectInput{
		Bucket: aws.String(s3.BucketName),
		Key:    aws.String(uploadKey),
		Body:   bytes.NewReader(data),
	})

	s3.Lo.Info("Uploaded to S3", "uploadId", uploadId, "bucket", s3.BucketName)

	if s3.RetentionConfig != nil && s3.RetentionConfig.Use {
		_, err := client.PutObjectRetention(context.TODO(), &awss3.PutObjectRetentionInput{
			Bucket: aws.String(s3.BucketName),
			Key:    aws.String(uploadKey),
			Retention: &types.ObjectLockRetention{
				Mode:            types.ObjectLockRetentionModeCompliance,
				RetainUntilDate: aws.Time(s3.RetentionConfig.Time),
			},
		})
		if err != nil {
			s3.Lo.Error("Failed to add the retention policy", "error", err)
		}
	}

	return &uploadId, nil

}
