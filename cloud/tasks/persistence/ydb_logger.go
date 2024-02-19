package persistence

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	ydb_log "github.com/ydb-platform/ydb-go-sdk/v3/log"
)

////////////////////////////////////////////////////////////////////////////////

func fieldToField(field ydb_log.Field) logging.Field {
	switch field.Type() {
	case ydb_log.IntType:
		return logging.Int(field.Key(), field.IntValue())
	case ydb_log.Int64Type:
		return logging.Int64(field.Key(), field.Int64Value())
	case ydb_log.StringType:
		return logging.String(field.Key(), field.StringValue())
	case ydb_log.BoolType:
		return logging.Bool(field.Key(), field.BoolValue())
	case ydb_log.DurationType:
		return logging.Duration(field.Key(), field.DurationValue())
	case ydb_log.StringsType:
		return logging.Strings(field.Key(), field.StringsValue())
	case ydb_log.ErrorType:
		return logging.ErrorValue(field.ErrorValue())
	case ydb_log.StringerType:
		return logging.String(field.Key(), field.Stringer().String())
	default:
		return logging.Any(field.Key(), field.AnyValue())
	}
}

func toFields(fields []ydb_log.Field) []logging.Field {
	ff := make([]logging.Field, len(fields))
	for i, f := range fields {
		ff[i] = fieldToField(f)
	}
	return ff
}

////////////////////////////////////////////////////////////////////////////////

var _ ydb_log.Logger = (*logger)(nil)

type logger struct {
	logger logging.Logger
}

func (l *logger) Log(ctx context.Context, msg string, fields ...ydb_log.Field) {
	if logging.GetLogger(ctx) == nil {
		logging.SetLogger(ctx, l.logger)
	}

	ctx = logging.WithFields(
		ctx,
		logging.NewComponentField(logging.ComponentYDB),
	)
	ctx = logging.WithFields(ctx, toFields(fields)...)

	// TODO:_ what to do with name provided by ydb sdk?
	// for _, name := range ydb_log.NamesFromContext(ctx) {
	// 	ll = ll.WithName(name)
	// }

	switch ydb_log.LevelFromContext(ctx) {
	case ydb_log.TRACE:
		logging.Trace(ctx, msg)
	case ydb_log.DEBUG:
		logging.Debug(ctx, msg)
	case ydb_log.INFO:
		logging.Info(ctx, msg)
	case ydb_log.WARN:
		logging.Warn(ctx, msg)
	case ydb_log.ERROR:
		logging.Error(ctx, msg)
	case ydb_log.FATAL:
		logging.Fatal(ctx, msg)
	default:
	}
}
