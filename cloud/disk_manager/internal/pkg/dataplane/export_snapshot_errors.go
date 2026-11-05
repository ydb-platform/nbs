package dataplane

import (
	"context"
	"fmt"
	"io"
	"net"
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
	var awsError awserr.Error
	if errors.As(target.err, &awsError) {
		message := exportErrorValue(awsError.Code(), 64)
		if text := awsError.Message(); text != "" {
			if message != "" {
				message += ": "
			}
			message += exportErrorValue(text, 1024)
		}
		if message == "" {
			message = "Target S3 request failed"
		}

		var details []string
		var failure awserr.RequestFailure
		if errors.As(target.err, &failure) {
			if failure.StatusCode() != 0 {
				details = append(details, fmt.Sprintf(
					"HTTP %v", failure.StatusCode(),
				))
			}
			if failure.RequestID() != "" {
				details = append(details,
					"request ID: "+exportErrorValue(failure.RequestID(), 128),
				)
			}
		}
		if len(details) != 0 {
			message += " (" + strings.Join(details, ", ") + ")"
		}

		return message
	}

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

	return message + ": " + snapshotExportTargetReason(target.err) + "."
}

func snapshotExportTargetReason(err error) string {
	if isSnapshotExportSourceTimeout(err) {
		return "the destination request timed out"
	}

	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "the destination connection closed unexpectedly"
	}

	var networkError net.Error
	if errors.As(err, &networkError) {
		return "could not connect to the destination endpoint; " +
			"check the endpoint address and network accessibility"
	}

	return "the destination request could not be completed"
}

func exportErrorValue(value string, limit int) string {
	if len(value) > limit {
		return strings.ToValidUTF8(value[:limit], "\uFFFD") + "..."
	}

	return strings.ToValidUTF8(value, "\uFFFD")
}
