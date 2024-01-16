package logging

import (
	"context"
	"fmt"

	logging_config "github.com/ydb-platform/nbs/cloud/tasks/logging/config"
	"github.com/ydb-platform/nbs/library/go/core/log"
)

////////////////////////////////////////////////////////////////////////////////

type Logger interface {
	log.Logger
}

////////////////////////////////////////////////////////////////////////////////

type Level = log.Level

const (
	TraceLevel = log.TraceLevel
	DebugLevel = log.DebugLevel
	InfoLevel  = log.InfoLevel
	WarnLevel  = log.WarnLevel
	ErrorLevel = log.ErrorLevel
	FatalLevel = log.FatalLevel
)

////////////////////////////////////////////////////////////////////////////////

type loggerKey struct{}
type loggerNameKey struct{}

func SetLogger(ctx context.Context, logger Logger) context.Context {
	return SetLoggingFields(context.WithValue(ctx, loggerKey{}, logger))
}

func GetLogger(ctx context.Context) Logger {
	logger, _ := ctx.Value(loggerKey{}).(Logger)
	if logger == nil {
		return nil
	}

	if name, ok := ctx.Value(loggerNameKey{}).(string); ok {
		return logger.WithName(name)
	}

	return logger
}

func AddCallerSkip(ctx context.Context, skip int) context.Context {
	return SetLogger(ctx, log.AddCallerSkip(GetLogger(ctx), 1))
}

////////////////////////////////////////////////////////////////////////////////

func NewLogger(config *logging_config.LoggingConfig) Logger {
	switch logging := config.GetLogging().(type) {
	case *logging_config.LoggingConfig_LoggingStderr:
		return NewStderrLogger(Level(config.GetLevel()))
	case *logging_config.LoggingConfig_LoggingJournald:
		return NewJournaldLogger(Level(config.GetLevel()))
	default:
		panic(fmt.Errorf("unknown logger %v", logging))
	}
}
