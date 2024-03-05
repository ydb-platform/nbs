package logging

import (
	"context"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/library/go/core/log"
	"github.com/ydb-platform/nbs/library/go/core/log/ctxlog"
)

////////////////////////////////////////////////////////////////////////////////

type Field = log.Field

////////////////////////////////////////////////////////////////////////////////

const ComponentYDB = "YDB"
const ComponentS3 = "S3"
const ComponentNbs = "NBS"
const ComponentNbsDiscovery = "NBS_DISCOVERY" // TODO:_ maybe just "Discovery"? Btw, how nfs discovery work?
const ComponentNfs = "NFS"
const ComponentClient = "CLIENT"
const ComponentRunners = "RUNNERS"
const ComponentScheduler = "SCHEDULER"
const ComponentApp = "APP"
const ComponentTasks = "TASKS" // TODO:_ naming. This name is confusing: looks like it is related to 'tasks' folder

////////////////////////////////////////////////////////////////////////////////

func String(key, value string) Field {
	return log.String(key, value)
}

func Strings(key string, value []string) Field {
	return log.Strings(key, value)
}

func Bool(key string, value bool) Field {
	return log.Bool(key, value)
}

func Int(key string, value int) Field {
	return log.Int(key, value)
}

func Int64(key string, value int64) Field {
	return log.Int64(key, value)
}

func Duration(key string, value time.Duration) Field {
	return log.Duration(key, value)
}

func ErrorValue(value error) Field {
	return log.Error(value)
}

func Any(key string, value interface{}) Field {
	return log.Any(key, value)
}

////////////////////////////////////////////////////////////////////////////////

func NewComponentField(value string) Field {
	return String("COMPONENT", value)
}

func NewTaskIDField(value string) Field {
	return String("TASK_ID", value)
}

func NewDiskIDField(value string) Field {
	return String("DISK_ID", value)
}

func NewSnapshotIDField(value string) Field {
	return String("SNAPSHOT_ID", value)
}

func NewImageIDField(value string) Field {
	return String("IMAGE_ID", value)
}

////////////////////////////////////////////////////////////////////////////////

func WithFields(ctx context.Context, fields ...Field) context.Context {
	return ctxlog.WithFields(ctx, fields...)
}

func WithCommonFields(ctx context.Context) context.Context {
	fields := make([]log.Field, 0)

	idempotencyKey := headers.GetIdempotencyKey(ctx)
	if len(idempotencyKey) != 0 {
		fields = append(fields, log.String(idempotencyKeyKey, idempotencyKey))
	}

	requestID := headers.GetRequestID(ctx)
	if len(requestID) != 0 {
		fields = append(fields, log.String(requestIDKey, requestID))
	}

	operationID := headers.GetOperationID(ctx)
	if len(operationID) != 0 {
		fields = append(fields, log.String(operationIDKey, operationID))
	}

	fields = append(fields, log.String(syslogIdentifierKey, "disk-manager"))

	return ctxlog.WithFields(ctx, fields...)
}
