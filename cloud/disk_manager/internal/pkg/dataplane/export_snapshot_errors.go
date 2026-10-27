package dataplane

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"

	"github.com/aws/aws-sdk-go/aws/awserr"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"

	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/client/codes"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

type snapshotExportStage string

const (
	snapshotExportReadStage        snapshotExportStage = "read"
	snapshotExportCredentialsStage snapshotExportStage = "credentials"
)

type snapshotExportStageError struct {
	err   error
	stage snapshotExportStage
}

func (e *snapshotExportStageError) Error() string {
	return fmt.Sprintf("export %v failed: %v", e.stage, e.err)
}

func (e *snapshotExportStageError) Unwrap() error {
	return e.err
}

func wrapSnapshotExportStageError(
	err error,
	stage snapshotExportStage,
) error {

	if err == nil {
		return nil
	}

	return &snapshotExportStageError{err: err, stage: stage}
}

// Retain the cause for retry decisions, but never serialize it to clients.
type snapshotExportPublicError struct {
	err     error
	message string
}

func (e *snapshotExportPublicError) Error() string {
	return e.message
}

func (e *snapshotExportPublicError) Unwrap() error {
	return e.err
}

func (e *snapshotExportPublicError) GRPCStatus() *grpc_status.Status {
	return grpc_status.New(grpc_codes.Aborted, e.message)
}

func publicSnapshotExportError(
	ctx context.Context,
	resourceKind string,
	err error,
) error {

	if err == nil || ctx.Err() != nil ||
		errors.Is(err, errors.NewWrongGenerationError()) ||
		errors.Is(err, errors.NewInterruptExecutionError()) {

		return err
	}

	logging.Error(ctx, "%v export failed: %v", resourceKind, err)
	message := fmt.Sprintf("Failed to export %v.", resourceKind)
	var target *snapshotExportTargetError
	var source *snapshotExportSourceError
	var stage *snapshotExportStageError
	switch {
	case errors.As(err, &target):
		message = snapshotExportTargetMessage(target)
	case errors.As(err, &stage):
		if stage.stage == snapshotExportCredentialsStage {
			message = "Failed to obtain export access parameters."
		} else {
			message = fmt.Sprintf("Failed to read %v data.", resourceKind)
		}
	case errors.As(err, &source):
		message = fmt.Sprintf("Failed to read %v data.", resourceKind)
	}

	return errors.NewDetailedErrorFull(
		&snapshotExportPublicError{err: err, message: message},
		&errors.ErrorDetails{
			Code:     codes.Aborted,
			Message:  message,
			Internal: false,
		},
		errors.IsSilent(err),
	)
}

func snapshotExportTargetMessage(target *snapshotExportTargetError) string {
	message := "Target S3 request failed"
	if target.operation != "" {
		message = fmt.Sprintf("Target S3 %v failed", target.operation)
	}

	if target.bucket != "" {
		message += fmt.Sprintf(
			" for bucket %q", exportErrorValue(target.bucket, 256),
		)
	}

	if target.key != "" {
		message += fmt.Sprintf(
			", key %q", exportErrorValue(target.key, 1024),
		)
	}

	if target.missingBucket {
		return message + ": the destination bucket does not exist."
	}

	message += ": " + snapshotExportTargetReason(target.err)
	var details []string
	var awsError awserr.Error
	if errors.As(target.err, &awsError) {
		details = append(details, fmt.Sprintf(
			"AWS code %q", exportErrorValue(awsError.Code(), 64),
		))
	}

	var failure awserr.RequestFailure
	if errors.As(target.err, &failure) {
		if failure.StatusCode() != 0 {
			details = append(details, fmt.Sprintf(
				"HTTP %v", failure.StatusCode(),
			))
		}

		if failure.RequestID() != "" {
			details = append(details, fmt.Sprintf(
				"request ID %q", exportErrorValue(failure.RequestID(), 128),
			))
		}
	}

	if len(details) != 0 {
		message += " (" + strings.Join(details, ", ") + ")"
	}

	return message + "."
}

func snapshotExportTargetReason(err error) string {
	var awsError awserr.Error
	if errors.As(err, &awsError) {
		switch awsError.Code() {
		case "AccessDenied":
			return "access to the destination was denied"
		case "InvalidAccessKeyId":
			return "the S3 access key is invalid"
		case "SignatureDoesNotMatch", "AuthorizationHeaderMalformed":
			return "the destination rejected the request signature"
		case "ExpiredToken", "InvalidToken", "TokenRefreshRequired":
			return "the S3 authentication token is invalid or expired"
		case "NoSuchBucket":
			return "the destination bucket does not exist"
		case "NoSuchKey", "NotFound":
			return "the requested destination object was not found"
		case "EntityTooLarge":
			return "the destination rejected the object size"
		case "BadDigest", "InvalidDigest":
			return "the destination rejected the object checksum"
		case "QuotaLimitExceeded":
			return "the destination storage quota was exceeded"
		case "SlowDown", "Throttling", "ThrottlingException":
			return "the destination is throttling requests"
		case "InternalError", "InternalServerError", "ServiceUnavailable":
			return "the destination storage service is unavailable"
		case "InvalidRequest", "InvalidArgument", "InvalidBucketName":
			return "the destination rejected the request parameters"
		case "PermanentRedirect", "IncorrectEndpoint":
			return "the destination endpoint or region is incorrect"
		case "SerializationError", "UnmarshalError":
			return "the destination returned an invalid error response"
		}
	}

	if isSnapshotExportSourceTimeout(err) {
		return "the destination request timed out"
	}

	var failure awserr.RequestFailure
	if errors.As(err, &failure) {
		switch status := failure.StatusCode(); {
		case status == http.StatusUnauthorized ||
			status == http.StatusForbidden:

			return "access to the destination was denied"
		case status == http.StatusNotFound:
			return "the requested destination resource was not found"
		case status == http.StatusRequestEntityTooLarge:
			return "the destination rejected the object size"
		case status == http.StatusTooManyRequests:
			return "the destination is throttling requests"
		case status >= 500:
			return "the destination storage service is unavailable"
		}
	}

	// AWS SDK v1 retains transport errors in OrigErr, not Unwrap.
	for errors.As(err, &awsError) && awsError.OrigErr() != nil {
		err = awsError.OrigErr()
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "the destination connection closed unexpectedly"
	}

	var networkError net.Error
	if errors.As(err, &networkError) {
		return "could not connect to the destination"
	}

	return "the destination request could not be completed"
}

func exportErrorValue(value string, limit int) string {
	if len(value) > limit {
		return value[:limit] + "..."
	}

	return value
}
