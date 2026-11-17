package dataplane

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

type snapshotExportSourceError struct {
	err       error
	retryable bool
	timedOut  bool
}

func (e *snapshotExportSourceError) Error() string {
	return "source S3: " + e.err.Error()
}

func (e *snapshotExportSourceError) Unwrap() error {
	return e.err
}

type snapshotExportTargetError struct {
	err           error
	operation     string
	bucket        string
	key           string
	missingBucket bool
}

func (e *snapshotExportTargetError) Error() string {
	return fmt.Sprintf(
		"target S3 %v, bucket %q, key %q: %v",
		e.operation, e.bucket, e.key, e.err,
	)
}

func (e *snapshotExportTargetError) Unwrap() error {
	return e.err
}

// Read workers update this window without mutating the task's protobuf state.
// Only the checkpoint collector and Run copy it into the durable task state.
type snapshotExportSourceRetries struct {
	mu        sync.Mutex
	startedAt time.Time
	object    string
	timeout   time.Duration
	now       func() time.Time
}

func newSnapshotExportSourceRetries(
	state *protos.ExportSnapshotTaskState,
	timeout time.Duration,
) *snapshotExportSourceRetries {

	retries := &snapshotExportSourceRetries{
		object:  state.SourceRetryObject,
		timeout: timeout,
		now:     time.Now,
	}
	if state.SourceRetryStartedAt != 0 {
		retries.startedAt = time.Unix(0, state.SourceRetryStartedAt)
	}

	return retries
}

func (r *snapshotExportSourceRetries) checkpoint(
	state *protos.ExportSnapshotTaskState,
) {

	r.mu.Lock()
	defer r.mu.Unlock()
	state.SourceRetryStartedAt = 0
	if !r.startedAt.IsZero() {
		state.SourceRetryStartedAt = r.startedAt.UnixNano()
	}

	state.SourceRetryObject = r.object
}

func (r *snapshotExportSourceRetries) expired() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.startedAt.IsZero() &&
		!r.now().Before(r.startedAt.Add(r.timeout))
}

func (r *snapshotExportSourceRetries) read(
	ctx context.Context,
	object string,
	read func(context.Context) error,
) error {

	if err := ctx.Err(); err != nil {
		return err
	}

	// The S3 client bounds each call. A cumulative deadline would turn an
	// expired window into immediate timeouts on every following attempt.
	err := read(ctx)
	if err == nil {
		r.mu.Lock()
		if r.object == object {
			r.startedAt = time.Time{}
			r.object = ""
		}
		r.mu.Unlock()
		return nil
	}

	if parentErr := ctx.Err(); parentErr != nil {
		return parentErr
	}

	objectError := isSnapshotExportSourceObjectError(err)
	timedOut := !objectError && isSnapshotExportSourceTimeout(err)
	retryable := !objectError &&
		(timedOut || isSnapshotExportTransientError(err))
	if retryable {
		r.mu.Lock()
		if timedOut {
			// Timeouts yield the task indefinitely. A later non-timeout
			// transient error starts a new bounded failure window.
			r.startedAt = time.Time{}
			r.object = object
		} else if r.startedAt.IsZero() {
			r.startedAt = r.now()
			r.object = object
		}
		r.mu.Unlock()
	}

	// The source window is retried by Run, with its timestamp checkpointed.
	// Do not let the exporter's destination retry count apply to reads.
	return errors.NewNonRetriableError(&snapshotExportSourceError{
		err:       err,
		retryable: retryable,
		timedOut:  timedOut,
	})
}

func isSnapshotExportSourceTimeout(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	var timeout net.Error
	if errors.As(err, &timeout) && timeout.Timeout() {
		return true
	}

	var failure awserr.RequestFailure
	if errors.As(err, &failure) &&
		(failure.StatusCode() == http.StatusRequestTimeout ||
			failure.StatusCode() == http.StatusGatewayTimeout) {

		return true
	}

	var awsError awserr.Error
	if errors.As(err, &awsError) {
		switch awsError.Code() {
		case "RequestTimeout", "RequestTimeoutException":
			return true
		}

		return isSnapshotExportSourceTimeout(awsError.OrigErr())
	}

	return false
}

func isSnapshotExportSourceObjectError(err error) bool {
	var failure awserr.RequestFailure
	if errors.As(err, &failure) &&
		failure.StatusCode() == http.StatusNotFound {
		return true
	}

	for err != nil {
		var awsError awserr.Error
		if !errors.As(err, &awsError) {
			break
		}

		switch awsError.Code() {
		case "InternalError":
			return !isSnapshotExportSourceTimeout(err)
		case "NoSuchKey", "NoSuchVersion", "NotFound":
			return true
		}

		err = awsError.OrigErr()
	}

	return false
}

func isSnapshotExportTransientError(err error) bool {
	if errors.Is(err, errors.NewEmptyNonRetriableError()) ||
		errors.Is(err, errors.NewEmptyNonCancellableError()) {
		return false
	}

	if exporter.IsRetryableS3Error(err) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	var networkError net.Error
	if errors.As(err, &networkError) {
		return true
	}

	var failure awserr.RequestFailure
	if errors.As(err, &failure) {
		status := failure.StatusCode()
		return status >= 500 || status == http.StatusRequestTimeout ||
			status == http.StatusTooManyRequests
	}

	var awsError awserr.Error
	return errors.As(err, &awsError) &&
		isSnapshotExportTransientError(awsError.OrigErr())
}

type snapshotExportBackupReader struct {
	backup.ObjectReader
	retries *snapshotExportSourceRetries
}

func (r snapshotExportBackupReader) GetObject(
	ctx context.Context,
	key string,
) (persistence.S3Object, error) {

	var object persistence.S3Object
	err := r.retries.read(ctx, "backup/"+key, func(ctx context.Context) error {
		var err error
		object, err = r.ObjectReader.GetObject(ctx, key)
		return err
	})
	return object, err
}

type snapshotExportSource struct {
	exporter.Source
	retries *snapshotExportSourceRetries
}

func (s snapshotExportSource) ReadChunk(
	ctx context.Context,
	chunk *common.Chunk,
) error {

	if !chunk.StoredInS3 {
		return wrapSnapshotExportStageError(
			s.Source.ReadChunk(ctx, chunk), snapshotExportReadStage,
		)
	}

	return s.retries.read(ctx, "chunk/"+chunk.ID, func(ctx context.Context) error {
		return s.Source.ReadChunk(ctx, chunk)
	})
}

type snapshotExportRangeSource struct {
	snapshotExportSource
	storage.ChunkMapRangeReader
}

func newSnapshotExportSource(
	source exporter.Source,
	retries *snapshotExportSourceRetries,
) exporter.Source {

	reader := snapshotExportSource{Source: source, retries: retries}
	if ranges, ok := source.(storage.ChunkMapRangeReader); ok {
		return snapshotExportRangeSource{
			snapshotExportSource: reader,
			ChunkMapRangeReader:  ranges,
		}
	}

	return reader
}

type snapshotExportTarget struct {
	exporter.S3Destination
	uploads *snapshotExportUploadLimiter
}

func wrapSnapshotExportTargetError(
	err error,
	operation, bucket, key string,
) error {

	if err == nil {
		return nil
	}

	var failure awserr.RequestFailure
	if errors.As(err, &failure) &&
		(failure.StatusCode() == http.StatusUnauthorized ||
			failure.StatusCode() == http.StatusForbidden) {

		err = errors.NewNonRetriableError(err)
	}

	var awsError awserr.Error
	if errors.As(err, &awsError) {
		switch awsError.Code() {
		case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch",
			"ExpiredToken", "InvalidToken", "TokenRefreshRequired",
			"AuthorizationHeaderMalformed":
			err = errors.NewNonRetriableError(err)
		}
	}

	return &snapshotExportTargetError{
		err: err, operation: operation, bucket: bucket, key: key,
	}
}

func (d snapshotExportTarget) BucketExists(
	ctx context.Context,
	bucket string,
) (bool, error) {

	exists, err := d.S3Destination.BucketExists(ctx, bucket)
	if err == nil && !exists {
		return false, &snapshotExportTargetError{
			err: errors.NewNonRetriableErrorf(
				"destination bucket %v does not exist", bucket,
			),
			operation: "HeadBucket", bucket: bucket, missingBucket: true,
		}
	}

	return exists, wrapSnapshotExportTargetError(err, "HeadBucket", bucket, "")
}

func (d snapshotExportTarget) ObjectExists(
	ctx context.Context,
	bucket, key string,
) (bool, error) {

	exists, err := d.S3Destination.ObjectExists(ctx, bucket, key)
	return exists, wrapSnapshotExportTargetError(err, "HeadObject", bucket, key)
}

func (d snapshotExportTarget) DeleteObject(
	ctx context.Context,
	bucket, key string,
) error {

	return wrapSnapshotExportTargetError(
		d.S3Destination.DeleteObject(ctx, bucket, key),
		"DeleteObject", bucket, key,
	)
}

func (d snapshotExportTarget) PutObjectWithOptions(
	ctx context.Context,
	bucket, key string,
	object persistence.S3Object,
	options persistence.S3PutObjectOptions,
) error {

	generation, err := d.uploads.acquire(ctx)
	if err != nil {
		return err
	}

	err = wrapSnapshotExportTargetError(
		d.S3Destination.PutObjectWithOptions(
			ctx, bucket, key, object, options,
		),
		"PutObject", bucket, key,
	)
	d.uploads.finish(ctx, generation, err)
	return err
}

func (d snapshotExportTarget) GetObjectWithLimit(
	ctx context.Context,
	bucket, key string,
	limit int64,
) ([]byte, error) {

	data, err := d.S3Destination.GetObjectWithLimit(ctx, bucket, key, limit)
	return data, wrapSnapshotExportTargetError(err, "GetObject", bucket, key)
}
