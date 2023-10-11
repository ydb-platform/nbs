package logging

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/headers"
	"github.com/ydb-platform/nbs/library/go/core/log"
	"github.com/ydb-platform/nbs/library/go/core/log/ctxlog"
)

////////////////////////////////////////////////////////////////////////////////

func SetLoggingFields(ctx context.Context) context.Context {
	fields := make([]log.Field, 0)

	idempotencyKey := headers.GetIdempotencyKey(ctx)
	if len(idempotencyKey) != 0 {
		fields = append(fields, log.String("IDEMPOTENCY_KEY", idempotencyKey))
	}

	requestID := headers.GetRequestID(ctx)
	if len(requestID) != 0 {
		fields = append(fields, log.String("REQUEST_ID", requestID))
	}

	operationID := headers.GetOperationID(ctx)
	if len(operationID) != 0 {
		fields = append(fields, log.String("OPERATION_ID", operationID))
	}

	fields = append(fields, log.String("SYSLOG_IDENTIFIER", "yc-disk-manager"))

	return ctxlog.WithFields(
		ctx,
		fields...,
	)
}
