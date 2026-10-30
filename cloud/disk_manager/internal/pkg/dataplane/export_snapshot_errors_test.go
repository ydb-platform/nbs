package dataplane

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/stretchr/testify/require"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"

	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
	api "github.com/ydb-platform/nbs/contrib/ydb/public/api/client/yc_private/compute/inner"
)

func TestExportTargetPublicErrors(t *testing.T) {
	for _, test := range []struct {
		code   string
		status int
		reason string
	}{
		{"AccessDenied", 403, "access to the destination was denied"},
		{"InvalidAccessKeyId", 403, "the S3 access key is invalid"},
		{"NoSuchBucket", 404, "the destination bucket does not exist"},
		{"EntityTooLarge", 400, "the destination rejected the object size"},
		{"BadDigest", 400, "the destination rejected the object checksum"},
		{"SlowDown", 503, "the destination is throttling requests"},
		{
			"InternalError", 500,
			"the destination storage service is unavailable",
		},
		{
			"UnknownServiceError", 500,
			"the destination storage service is unavailable",
		},
		{
			"SerializationError", 400,
			"the destination returned an invalid error response",
		},
	} {
		t.Run(test.code, func(t *testing.T) {
			original := awserr.NewRequestFailure(
				awserr.New(test.code, "secret response body", nil),
				test.status,
				"request-123",
			)
			target := wrapSnapshotExportTargetError(
				original, "PutObject", "bucket", "prefix/part-00000007.raw",
			)
			err := publicSnapshotExportError(
				newContext(), "snapshot",
				errors.NewNonRetriableError(fmt.Errorf(
					"private exporter context: %w", target,
				)),
			)
			require.ErrorIs(t, err, original)
			require.False(t, errors.CanRetry(err))
			require.True(t, errors.IsPublic(err))
			status := grpc_status.Convert(err)
			require.Equal(t, grpc_codes.Aborted, status.Code())
			require.Contains(t, status.Message(), test.reason)
			require.Contains(t, status.Message(), "PutObject")
			require.Contains(t, status.Message(), `bucket "bucket"`)
			require.Contains(t, status.Message(), "prefix/part-00000007.raw")
			require.Contains(t, status.Message(), test.code)
			require.Contains(t, status.Message(), fmt.Sprint(test.status))
			require.Contains(t, status.Message(), "request-123")
			for _, hidden := range []string{
				"secret response body", "private exporter context",
				"goroutine", "Non retriable", "source S3", "DiskService",
			} {
				require.NotContains(t, status.Message(), hidden)
				require.NotContains(t, err.Error(), hidden)
			}

			var saved tasks_storage.TaskState
			saved.SetError(err)
			require.Equal(t, status.Message(), saved.ErrorMessage)
			require.NotNil(t, saved.ErrorDetails)
			require.False(t, saved.ErrorDetails.Internal)
			require.Equal(t, status.Message(), saved.ErrorDetails.Message)

			// The control-plane task receives this error from WaitTask.
			parentError := errors.NewDetailedErrorFull(
				grpc_status.Error(saved.ErrorCode, saved.ErrorMessage),
				saved.ErrorDetails,
				saved.ErrorSilent,
			)
			var parent tasks_storage.TaskState
			parent.SetError(parentError)
			require.Equal(t, saved.ErrorMessage, parent.ErrorMessage)
			require.Equal(t, saved.ErrorDetails, parent.ErrorDetails)
		})
	}
}

func TestExportTransportPublicErrors(t *testing.T) {
	for _, test := range []struct {
		err    error
		reason string
	}{
		{
			awserr.New("RequestCanceled", "secret", context.DeadlineExceeded),
			"the destination request timed out",
		},
		{
			awserr.New("RequestError", "secret", &url.Error{
				Op: "PUT", URL: "https://private.example/?secret=token",
				Err: io.ErrUnexpectedEOF,
			}),
			"the destination connection closed unexpectedly",
		},
	} {
		err := publicSnapshotExportError(
			newContext(), "snapshot", wrapSnapshotExportTargetError(
				errors.NewRetriableError(test.err),
				"PutObject", "bucket", "key",
			),
		)
		require.True(t, errors.CanRetry(err))
		require.Contains(t, grpc_status.Convert(err).Message(), test.reason)
		require.NotContains(t, err.Error(), "secret")
		require.NotContains(t, err.Error(), "private.example")
	}
}

func TestExportInternalErrorsStayPrivate(t *testing.T) {
	for _, kind := range []string{"snapshot", "image"} {
		for _, test := range []struct {
			err     error
			message string
		}{
			{
				errors.NewNonRetriableError(&snapshotExportSourceError{
					err: fmt.Errorf("source S3 private endpoint and chunk ID"),
				}),
				fmt.Sprintf("Failed to read %v data.", kind),
			},
			{
				wrapSnapshotExportStageError(
					errors.New("YDB private endpoint and table name"),
					snapshotExportReadStage,
				),
				fmt.Sprintf("Failed to read %v data.", kind),
			},
			{
				wrapSnapshotExportStageError(
					errors.NewRetriableError(grpc_status.Error(
						grpc_codes.Internal, "DiskService private endpoint",
					)),
					snapshotExportCredentialsStage,
				),
				"Failed to obtain export access parameters.",
			},
			{
				errors.New("unexpected private internal details"),
				fmt.Sprintf("Failed to export %v.", kind),
			},
		} {
			err := publicSnapshotExportError(newContext(), kind, test.err)
			require.Equal(t, test.message, grpc_status.Convert(err).Message())
			require.Equal(t, errors.CanRetry(test.err), errors.CanRetry(err))
			require.ErrorIs(t, err, test.err)
			require.NotContains(t, err.Error(), "private")
		}
	}

	original := errors.NewRetriableErrorWithIgnoreRetryLimit(
		errors.New("private read error"),
	)
	err := publicSnapshotExportError(newContext(), "snapshot", original)
	var retry *errors.RetriableError
	require.ErrorAs(t, err, &retry)
	require.True(t, retry.IgnoreRetryLimit)
}

func TestExportPublicErrorsKeepInterruptions(t *testing.T) {
	ctx := newContext()
	require.NoError(t, publicSnapshotExportError(ctx, "snapshot", nil))
	for _, original := range []error{
		errors.NewWrongGenerationError(),
		errors.NewInterruptExecutionError(),
	} {
		require.Same(t, original, publicSnapshotExportError(
			ctx, "snapshot", original,
		))
	}

	ctx, cancel := context.WithCancel(ctx)
	cancel()
	original := errors.NewRetriableError(context.Canceled)
	require.Same(t, original, publicSnapshotExportError(
		ctx, "snapshot", original,
	))
}

func TestExportTargetErrorValuesAreBounded(t *testing.T) {
	err := publicSnapshotExportError(
		newContext(), "snapshot", wrapSnapshotExportTargetError(
			awserr.NewRequestFailure(
				awserr.New(strings.Repeat("X", 5000), "secret", nil),
				400, strings.Repeat("R", 5000),
			),
			"PutObject", strings.Repeat("B", 5000), strings.Repeat("K", 5000),
		),
	)
	require.Less(t, len(grpc_status.Convert(err).Message()), 2048)
	require.NotContains(t, err.Error(), "secret")
}

type failingExportDiskService struct {
	err error
}

func (c failingExportDiskService) GetExportCredentials(
	ctx context.Context,
	computeTaskID string,
	snapshotID string,
	imageID string,
) (*api.GetExportCredentialsResponse, error) {

	return nil, c.err
}

func TestExportTaskHidesDiskServiceErrors(t *testing.T) {
	for _, original := range []error{
		errors.NewRetriableError(grpc_status.Error(
			grpc_codes.Internal, "private Compute error and endpoint",
		)),
		errors.NewNonRetriableError(grpc_status.Error(
			grpc_codes.PermissionDenied, "private interservice auth error",
		)),
	} {
		task := &exportSnapshotTask{
			config: &server_config.ExportConfig{},
			request: &protos.ExportSnapshotRequest{
				SnapshotId: "snapshot", ComputeTaskId: "compute-task",
			},
			state:       &protos.ExportSnapshotTaskState{},
			diskService: failingExportDiskService{err: original},
		}
		execCtx := tasks_mocks.NewExecutionContextMock()
		execCtx.On("GetTaskID").Return("export-task")
		err := task.Run(newContext(), execCtx)
		require.ErrorIs(t, err, original)
		require.Equal(t, errors.CanRetry(original), errors.CanRetry(err))
		require.Equal(
			t, "Failed to obtain export access parameters.",
			grpc_status.Convert(err).Message(),
		)
		require.True(t, errors.IsPublic(err))
		require.NotContains(t, err.Error(), "private")
		execCtx.AssertExpectations(t)
	}
}
