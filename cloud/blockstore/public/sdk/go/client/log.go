package client

import (
	"log"
	"os"
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

type Logger interface {
	Tracef(format string, args ...interface{})
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
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

func (e *defaultLogger) Tracef(format string, args ...interface{}) {
	e.write(LOG_TRACE, format, args...)
}

func (e *defaultLogger) Debugf(format string, args ...interface{}) {
	e.write(LOG_DEBUG, format, args...)
}

func (e *defaultLogger) Infof(format string, args ...interface{}) {
	e.write(LOG_INFO, format, args...)
}

func (e *defaultLogger) Warnf(format string, args ...interface{}) {
	e.write(LOG_WARN, format, args...)
}

func (e *defaultLogger) Errorf(format string, args ...interface{}) {
	e.write(LOG_ERROR, format, args...)
}

func (e *defaultLogger) Fatalf(format string, args ...interface{}) {
	log.Fatalf(format, args...)
}

func NewLog(logger *log.Logger, level LogLevel) Logger {
	return &defaultLogger{logger: logger, level: level}
}

func NewStderrLog(level LogLevel) Logger {
	logger := log.New(os.Stderr, "", 0)
	return NewLog(logger, level)
}
