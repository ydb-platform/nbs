package logging

import (
	"context"

	"github.com/ydb-platform/nbs/library/go/core/log"
	"github.com/ydb-platform/nbs/library/go/core/log/ctxlog"
)

////////////////////////////////////////////////////////////////////////////////

// contextLogger wraps a logger with a context, automatically including
// context fields in all log calls. It implements log.Logger interface.
type contextLogger struct {
	ctx    context.Context
	logger log.Logger
}

// WithContext returns a new Logger that includes context fields in all log
// calls. This allows passing the logger to code that doesn't accept context.
func WithContext(ctx context.Context) Logger {
	return &contextLogger{
		ctx:    ctx,
		logger: GetLogger(ctx),
	}
}

func (l *contextLogger) Trace(msg string, fields ...log.Field) {
	ctxlog.Trace(l.ctx, log.AddCallerSkip(l.logger, 1), msg, fields...)
}

func (l *contextLogger) Debug(msg string, fields ...log.Field) {
	ctxlog.Debug(l.ctx, log.AddCallerSkip(l.logger, 1), msg, fields...)
}

func (l *contextLogger) Info(msg string, fields ...log.Field) {
	ctxlog.Info(l.ctx, log.AddCallerSkip(l.logger, 1), msg, fields...)
}

func (l *contextLogger) Warn(msg string, fields ...log.Field) {
	ctxlog.Warn(l.ctx, log.AddCallerSkip(l.logger, 1), msg, fields...)
}

func (l *contextLogger) Error(msg string, fields ...log.Field) {
	ctxlog.Error(l.ctx, log.AddCallerSkip(l.logger, 1), msg, fields...)
}

func (l *contextLogger) Fatal(msg string, fields ...log.Field) {
	ctxlog.Fatal(l.ctx, log.AddCallerSkip(l.logger, 1), msg, fields...)
}

func (l *contextLogger) Tracef(format string, args ...interface{}) {
	ctxlog.Tracef(l.ctx, log.AddCallerSkip(l.logger, 1), format, args...)
}

func (l *contextLogger) Debugf(format string, args ...interface{}) {
	ctxlog.Debugf(l.ctx, log.AddCallerSkip(l.logger, 1), format, args...)
}

func (l *contextLogger) Infof(format string, args ...interface{}) {
	ctxlog.Infof(l.ctx, log.AddCallerSkip(l.logger, 1), format, args...)
}

func (l *contextLogger) Warnf(format string, args ...interface{}) {
	ctxlog.Warnf(l.ctx, log.AddCallerSkip(l.logger, 1), format, args...)
}

func (l *contextLogger) Errorf(format string, args ...interface{}) {
	ctxlog.Errorf(l.ctx, log.AddCallerSkip(l.logger, 1), format, args...)
}

func (l *contextLogger) Fatalf(format string, args ...interface{}) {
	ctxlog.Fatalf(l.ctx, log.AddCallerSkip(l.logger, 1), format, args...)
}

func (l *contextLogger) Logger() log.Logger {
	return l
}

func (l *contextLogger) Structured() log.Structured {
	return l
}

func (l *contextLogger) Fmt() log.Fmt {
	return l
}

func (l *contextLogger) WithName(name string) log.Logger {
	return &contextLogger{
		ctx:    l.ctx,
		logger: l.logger.WithName(name),
	}
}

func (l *contextLogger) AddCallerSkip(skip int) log.Logger {
	return &contextLogger{
		ctx:    l.ctx,
		logger: log.AddCallerSkip(l.logger, skip),
	}
}
