package persistence

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/client"
	aws_credentials "github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

////////////////////////////////////////////////////////////////////////////////

func withComponentLoggingField(ctx context.Context) context.Context {
	return logging.WithComponent(ctx, logging.ComponentS3)
}

func logMessageFromS3SDK(ctx context.Context, args ...interface{}) {
	logMessage := fmt.Sprintln(args...)
	logMessage = "S3 SDK: " + logMessage
	if strings.Contains(logMessage, "ERROR") {
		logging.Error(ctx, logMessage)
	} else {
		logging.Info(ctx, logMessage)
	}
}

////////////////////////////////////////////////////////////////////////////////

type s3ClientRetryer struct {
	client.DefaultRetryer
	metrics *s3Metrics
}

func (r *s3ClientRetryer) RetryRules(req *request.Request) time.Duration {
	r.metrics.OnRetry(req)

	return r.DefaultRetryer.RetryRules(req)
}

////////////////////////////////////////////////////////////////////////////////

type S3Client struct {
	s3          *aws_s3.S3
	callTimeout time.Duration
	metrics     *s3Metrics
}

func NewS3Client(
	ctx context.Context,
	endpoint string,
	region string,
	credentials S3Credentials,
	callTimeout time.Duration,
	registry metrics.Registry,
	maxRetriableErrorCount uint64,
) (*S3Client, error) {

	s3Metrics := newS3Metrics(registry, callTimeout)

	logLevel := aws.LogDebug | aws.LogDebugWithRequestErrors
	s3Logger := aws.LoggerFunc(func(args ...interface{}) {
		logMessageFromS3SDK(ctx, args...)
	})

	sessionConfig := &aws.Config{
		Credentials: aws_credentials.NewStaticCredentials(
			credentials.ID,
			credentials.Secret,
			"", // token - only required for temporary security credentials retrieved via STS, we don't need that
		),
		Endpoint:         &endpoint,
		Region:           &region,
		S3ForcePathStyle: aws.Bool(true), // otherwise, we get DNS DDOS errors in tests
		Retryer: &s3ClientRetryer{
			DefaultRetryer: client.DefaultRetryer{
				NumMaxRetries: int(maxRetriableErrorCount),
			},
			metrics: s3Metrics,
		},
		LogLevel: &logLevel,
		Logger:   s3Logger,
	}

	session, err := session.NewSession(sessionConfig)
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	return &S3Client{
		s3:          aws_s3.New(session),
		callTimeout: callTimeout,
		metrics:     s3Metrics,
	}, nil
}

func NewS3ClientFromConfig(
	ctx context.Context,
	config *persistence_config.S3Config,
	registry metrics.Registry,
) (*S3Client, error) {

	credentials, err := NewS3CredentialsFromFile(config.GetCredentialsFilePath())
	if err != nil {
		return nil, err
	}

	callTimeout, err := time.ParseDuration(config.GetCallTimeout())
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to parse callTimeout: %w",
			err,
		)
	}

	return NewS3Client(
		ctx,
		config.GetEndpoint(),
		config.GetRegion(),
		credentials,
		callTimeout,
		registry,
		config.GetMaxRetriableErrorCount(),
	)
}

////////////////////////////////////////////////////////////////////////////////

func (c *S3Client) CreateBucket(
	ctx context.Context,
	bucket string,
) (err error) {

	ctx = withComponentLoggingField(ctx)
	logging.Info(ctx, "creating bucket %v in s3", bucket)

	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()

	defer c.metrics.StatCall(ctx, "CreateBucket", bucket, "")(&err)

	_, err = c.s3.CreateBucketWithContext(ctx, &aws_s3.CreateBucketInput{
		Bucket: &bucket,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case aws_s3.ErrCodeBucketAlreadyOwnedByYou:
				// Bucket is already created
				return nil
			}
		}

		return errors.NewRetriableError(err)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (c *S3Client) GetObject(
	ctx context.Context,
	bucket string,
	key string,
) (o S3Object, err error) {

	ctx = withComponentLoggingField(ctx)
	logging.Info(ctx, "getting object from s3, bucket %v, key %v", bucket, key)

	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()

	defer c.metrics.StatCall(ctx, "GetObject", bucket, key)(&err)

	res, err := c.s3.GetObjectWithContext(ctx, &aws_s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case aws_s3.ErrCodeNoSuchKey:
				return S3Object{}, errors.NewSilentNonRetriableErrorf(
					"s3 object not found: %v",
					key,
				)
			case aws_s3.ErrCodeNoSuchBucket:
				return S3Object{}, errors.NewNonRetriableError(err)
			}
		}

		return S3Object{}, errors.NewRetriableError(err)
	}

	objData, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return S3Object{}, errors.NewRetriableError(err)
	}

	return S3Object{
		Data:     objData,
		Metadata: res.Metadata,
	}, nil
}

func (c *S3Client) PutObject(
	ctx context.Context,
	bucket string,
	key string,
	object S3Object,
) (err error) {

	ctx = withComponentLoggingField(ctx)
	logging.Info(ctx, "putting object to s3, bucket %v, key %v", bucket, key)

	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()

	defer c.metrics.StatCall(ctx, "PutObject", bucket, key)(&err)

	_, err = c.s3.PutObjectWithContext(ctx, &aws_s3.PutObjectInput{
		Bucket:          &bucket,
		Key:             &key,
		Body:            bytes.NewReader(object.Data),
		Metadata:        object.Metadata,
		ContentEncoding: aws.String("application/octet-stream"),
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case aws_s3.ErrCodeNoSuchBucket:
				return errors.NewNonRetriableError(err)
			}
		}

		return errors.NewRetriableError(err)
	}

	return nil
}

func (c *S3Client) DeleteObject(
	ctx context.Context,
	bucket string,
	key string,
) (err error) {

	ctx = withComponentLoggingField(ctx)
	logging.Info(
		ctx,
		"deleting object from s3, bucket %v, key %v",
		bucket,
		key,
	)

	ctx, cancel := context.WithTimeout(ctx, c.callTimeout)
	defer cancel()

	defer c.metrics.StatCall(ctx, "DeleteObject", bucket, key)(&err)

	_, err = c.s3.DeleteObjectWithContext(ctx, &aws_s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &key,
	})
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case aws_s3.ErrCodeNoSuchBucket:
				return errors.NewNonRetriableError(err)
			}
		}

		return errors.NewRetriableError(err)
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

type S3Object struct {
	Data     []byte
	Metadata map[string]*string
}

////////////////////////////////////////////////////////////////////////////////

type S3Credentials struct {
	ID     string `json:"id,omitempty"`
	Secret string `json:"secret,omitempty"`
}

func NewS3Credentials(id, secret string) S3Credentials {
	return S3Credentials{
		ID:     id,
		Secret: secret,
	}
}

func NewS3CredentialsFromFile(filePath string) (S3Credentials, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return S3Credentials{}, err
	}

	credentials := S3Credentials{}

	err = json.Unmarshal(file, &credentials)
	if err != nil {
		return S3Credentials{}, err
	}

	return credentials, nil
}
