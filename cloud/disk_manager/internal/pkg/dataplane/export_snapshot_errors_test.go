package dataplane

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/url"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/stretchr/testify/require"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	server_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/configs/server/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
	api "github.com/ydb-platform/nbs/contrib/ydb/public/api/client/yc_private/compute/inner"
)

func TestExportTargetPublicErrors(t *testing.T) {
	for _, test := range []struct {
		code    string
		status  int
		message string
	}{
		{"BadRequest", 400, "Bad Request"},
		{"AccessDenied", 403, "Access Denied"},
		{"InvalidAccessKeyId", 403, "The access key does not exist."},
		{"NoSuchBucket", 404, "The specified bucket does not exist."},
		{"EntityTooLarge", 400, "The object exceeds the maximum allowed size."},
		{"BadDigest", 400, "The Content-MD5 did not match."},
		{"SlowDown", 503, "Please reduce your request rate."},
		{
			"InternalError", 500,
			"We encountered an internal error, please try again.",
		},
		{
			"UnknownServiceError", 500,
			"Provider-specific error description.",
		},
		{
			"SerializationError", 400,
			"failed to unmarshal error message",
		},
		{
			"BucketMaxSizeExceeded", 403,
			"You have attempted to exceed the max size " +
				"configured for the bucket.",
		},
	} {
		for _, kind := range []string{"snapshot", "image"} {
			t.Run(kind+"/"+test.code, func(t *testing.T) {
				original := awserr.NewRequestFailure(
					awserr.New(test.code, test.message,
						errors.New("private AWS cause or response body")),
					test.status,
					"request-123",
				)
				target := wrapSnapshotExportTargetError(
					original, "PutObject", "bucket", "prefix/part-00000007.raw",
				)
				err := publicSnapshotExportError(
					newContext(), kind,
					errors.NewNonRetriableError(fmt.Errorf(
						"private exporter context: %w", target,
					)),
				)
				require.ErrorIs(t, err, original)
				require.False(t, errors.CanRetry(err))
				require.True(t, errors.IsPublic(err))
				status := grpc_status.Convert(err)
				require.Equal(t, grpc_codes.Aborted, status.Code())
				require.Equal(t, fmt.Sprintf(
					"%v: %v (HTTP %v, request ID: request-123)",
					test.code, test.message, test.status,
				), status.Message())
				for _, hidden := range []string{
					"private AWS cause or response body", "private exporter context",
					"goroutine", "Non retriable", "source S3", "DiskService",
					"PutObject", "prefix/part-00000007.raw",
				} {
					require.NotContains(t, status.Message(), hidden)
					require.NotContains(t, err.Error(), hidden)
				}

				var saved tasks_storage.TaskState
				saved.SetError(err)
				require.Equal(t, status.Message(), saved.ErrorMessage)
				require.NotNil(t, saved.ErrorDetails)
				require.Equal(t, int64(codes.Aborted), saved.ErrorDetails.Code)
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
}

func TestExportTransportPublicErrors(t *testing.T) {
	for _, test := range []struct {
		err    error
		reason string
	}{
		{
			awserr.New("RequestError", "send request failed", nil),
			"RequestError: send request failed",
		},
		{
			awserr.New("RequestError", "send request failed",
				errors.New("TLS certificate rejected for private.example")),
			"RequestError: send request failed",
		},
		{
			awserr.New("outer", "SDK wrapper message",
				awserr.New("RequestError", "secret", nil)),
			"outer: SDK wrapper message",
		},
		{
			awserr.New("RequestError", "send request failed", &url.Error{
				Op: "HEAD", URL: "https://private.example/?secret=token",
				Err: &net.DNSError{Err: "no such host", Name: "private.example"},
			}),
			"RequestError: send request failed",
		},
		{
			awserr.New("RequestCanceled", "request context canceled",
				context.DeadlineExceeded),
			"RequestCanceled: request context canceled",
		},
		{
			awserr.New("RequestError", "send request failed", &url.Error{
				Op: "PUT", URL: "https://private.example/?secret=token",
				Err: io.ErrUnexpectedEOF,
			}),
			"RequestError: send request failed",
		},
		{
			&net.DNSError{Err: "no such host", Name: "private.example"},
			"could not connect to the destination endpoint",
		},
		{
			io.ErrUnexpectedEOF, "the destination connection closed unexpectedly",
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

func TestExportTerminalConnectionErrorSurvivesTaskState(t *testing.T) {
	original := awserr.New("RequestError", "send request failed", nil)
	err := publicSnapshotExportError(
		newContext(), "snapshot", errors.NewNonRetriableError(
			fmt.Errorf("checking destination bucket: %w",
				wrapSnapshotExportTargetError(
					errors.NewRetriableError(original),
					"GetBucket", "test", "",
				)),
		),
	)
	require.ErrorIs(t, err, original)
	require.False(t, errors.CanRetry(err))

	var state tasks_storage.TaskState
	state.SetError(err)
	require.Equal(t, grpc_codes.Aborted, state.ErrorCode)
	require.NotNil(t, state.ErrorDetails)
	require.Equal(t, int64(codes.Aborted), state.ErrorDetails.Code)
	require.False(t, state.ErrorDetails.Internal)
	require.Equal(t, state.ErrorMessage, state.ErrorDetails.Message)
	require.Equal(t, "RequestError: send request failed", state.ErrorMessage)
	require.NotContains(t, state.ErrorMessage, "Retriable error")

	var parent tasks_storage.TaskState
	parent.SetError(errors.NewDetailedErrorFull(
		grpc_status.Error(state.ErrorCode, state.ErrorMessage),
		state.ErrorDetails, state.ErrorSilent,
	))
	require.Equal(t, state.ErrorCode, parent.ErrorCode)
	require.Equal(t, state.ErrorMessage, parent.ErrorMessage)
	require.Equal(t, state.ErrorDetails, parent.ErrorDetails)
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
				errors.NewNonRetriableError(&snapshotExportSourceError{
					err: awserr.New("AccessDenied", "private source S3 message", nil),
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
				awserr.New(strings.Repeat("X", 5000),
					strings.Repeat("€", 5000), errors.New("private AWS cause")),
				400, strings.Repeat("R", 5000),
			),
			"PutObject", strings.Repeat("B", 5000), strings.Repeat("K", 5000),
		),
	)
	require.Less(t, len(grpc_status.Convert(err).Message()), 2048)
	require.NotContains(t, err.Error(), "private AWS cause")
	var state tasks_storage.TaskState
	state.SetError(err)
	_, marshalErr := proto.Marshal(state.ErrorDetails)
	require.NoError(t, marshalErr)
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
