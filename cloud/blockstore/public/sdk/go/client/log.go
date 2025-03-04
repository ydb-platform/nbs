package client

import (
	"log"
	"os"

	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

type LogLevel uint32

const (
	LOG_ERROR LogLevel = iota
	LOG_WARN
	LOG_INFO
	LOG_DEBUG
	LOG_TRACE
)

type Log interface {
	Logger(level LogLevel) Logger
}

type Logger interface {
	Trace(ctx context.Context, format string, args ...interface{})
	Debug(ctx context.Context, format string, args ...interface{})
	Info(ctx context.Context, format string, args ...interface{})
	Warn(ctx context.Context, format string, args ...interface{})
	Error(ctx context.Context, format string, args ...interface{})
	Fatal(ctx context.Context, format string, args ...interface{})
}

////////////////////////////////////////////////////////////////////////////////

type defaultLogger struct {
	logger *log.Logger
	level  LogLevel
}

func (e *defaultLogger) write(level LogLevel, format string, args ...interface{}) {
	if e.level < level {
		return
	}

	e.logger.Printf(format, args...)
}

func (e *defaultLogger) Trace(ctx context.Context, format string, args ...interface{}) {
	e.write(LOG_TRACE, format, args...)
}

func (e *defaultLogger) Debug(ctx context.Context, format string, args ...interface{}) {
	e.write(LOG_DEBUG, format, args...)
}

func (e *defaultLogger) Info(ctx context.Context, format string, args ...interface{}) {
	e.write(LOG_INFO, format, args...)
}

func (e *defaultLogger) Warn(ctx context.Context, format string, args ...interface{}) {
	e.write(LOG_WARN, format, args...)
}

func (e *defaultLogger) Error(ctx context.Context, format string, args ...interface{}) {
	e.write(LOG_ERROR, format, args...)
}

func (e *defaultLogger) Fatal(ctx context.Context, format string, args ...interface{}) {
	log.Fatalf(format, args...)
}

func NewLog(logger *log.Logger, level LogLevel) Logger {
	return &defaultLogger{logger: logger, level: level}
}

func NewStderrLog(level LogLevel) Logger {
	logger := log.New(os.Stderr, "", 0)
	return NewLog(logger, level)
}
