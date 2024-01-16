package logging

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/library/go/core/log"
	"github.com/ydb-platform/nbs/library/go/core/log/ctxlog"
)

////////////////////////////////////////////////////////////////////////////////

func Trace(ctx context.Context, format string, args ...interface{}) {
	l := log.AddCallerSkip(GetLogger(ctx), 1)
	ctxlog.Trace(ctx, l, fmt.Sprintf(format, args...))
}

func Debug(ctx context.Context, format string, args ...interface{}) {
	l := log.AddCallerSkip(GetLogger(ctx), 1)
	ctxlog.Debug(ctx, l, fmt.Sprintf(format, args...))
}

func Info(ctx context.Context, format string, args ...interface{}) {
	l := log.AddCallerSkip(GetLogger(ctx), 1)
	ctxlog.Info(ctx, l, fmt.Sprintf(format, args...))
}

func Warn(ctx context.Context, format string, args ...interface{}) {
	l := log.AddCallerSkip(GetLogger(ctx), 1)
	ctxlog.Warn(ctx, l, fmt.Sprintf(format, args...))
}

func Error(ctx context.Context, format string, args ...interface{}) {
	l := log.AddCallerSkip(GetLogger(ctx), 1)
	ctxlog.Error(ctx, l, fmt.Sprintf(format, args...))
}

func Fatal(ctx context.Context, format string, args ...interface{}) {
	l := log.AddCallerSkip(GetLogger(ctx), 1)
	ctxlog.Fatal(ctx, l, fmt.Sprintf(format, args...))
}
