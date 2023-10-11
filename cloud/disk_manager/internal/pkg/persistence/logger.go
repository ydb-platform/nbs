package persistence

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	ydb_core_log "github.com/ydb-platform/nbs/library/go/yandex/ydb/log"
	ydb_log "github.com/ydb-platform/ydb-go-sdk/v3/log"
)

////////////////////////////////////////////////////////////////////////////////

var _ ydb_log.Logger = (*logger)(nil)

type logger struct {
	logger logging.Logger
}

func (l *logger) Log(ctx context.Context, msg string, fields ...ydb_log.Field) {
	ll := logging.GetLogger(ctx)

	if ll == nil {
		ll = l.logger
	}

	for _, name := range ydb_log.NamesFromContext(ctx) {
		ll = ll.WithName(name)
	}

	switch ydb_log.LevelFromContext(ctx) {
	case ydb_log.TRACE:
		ll.Trace(msg, ydb_core_log.ToCoreFields(fields)...)
	case ydb_log.DEBUG:
		ll.Debug(msg, ydb_core_log.ToCoreFields(fields)...)
	case ydb_log.INFO:
		ll.Info(msg, ydb_core_log.ToCoreFields(fields)...)
	case ydb_log.WARN:
		ll.Warn(msg, ydb_core_log.ToCoreFields(fields)...)
	case ydb_log.ERROR:
		ll.Error(msg, ydb_core_log.ToCoreFields(fields)...)
	case ydb_log.FATAL:
		ll.Fatal(msg, ydb_core_log.ToCoreFields(fields)...)
	default:
	}
}
