package client

import (
	"fmt"
	"log"
	"log/syslog"
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
)

type Log interface {
	Logger(level LogLevel) Logger
}

type Logger interface {
	Print(ctx context.Context, v ...interface{})
	Printf(ctx context.Context, format string, v ...interface{})
}

////////////////////////////////////////////////////////////////////////////////

type errorLogger struct {
	writer *syslog.Writer
}

func (l *errorLogger) Print(ctx context.Context, v ...interface{}) {
	_ = l.writer.Err(fmt.Sprint(v...))
}

func (l *errorLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	_ = l.writer.Err(fmt.Sprintf(format, v...))
}

////////////////////////////////////////////////////////////////////////////////

type warnLogger struct {
	writer *syslog.Writer
}

func (l *warnLogger) Print(ctx context.Context, v ...interface{}) {
	_ = l.writer.Warning(fmt.Sprint(v...))
}

func (l *warnLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	_ = l.writer.Warning(fmt.Sprintf(format, v...))
}

////////////////////////////////////////////////////////////////////////////////

type infoLogger struct {
	writer *syslog.Writer
}

func (l *infoLogger) Print(ctx context.Context, v ...interface{}) {
	_ = l.writer.Info(fmt.Sprint(v...))
}

func (l *infoLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	_ = l.writer.Info(fmt.Sprintf(format, v...))
}

////////////////////////////////////////////////////////////////////////////////

type debugLogger struct {
	writer *syslog.Writer
}

func (l *debugLogger) Print(ctx context.Context, v ...interface{}) {
	_ = l.writer.Debug(fmt.Sprint(v...))
}

func (l *debugLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	_ = l.writer.Debug(fmt.Sprintf(format, v...))
}

////////////////////////////////////////////////////////////////////////////////

type stdLoggerWrapper struct {
	logger *log.Logger
}

func (w *stdLoggerWrapper) Print(ctx context.Context, v ...interface{}) {
	w.logger.Print(v...)
}

func (w *stdLoggerWrapper) Printf(ctx context.Context, format string, v ...interface{}) {
	w.logger.Printf(format, v...)
}

////////////////////////////////////////////////////////////////////////////////

type logWrapper struct {
	level  LogLevel
	logger Logger
}

func (w *logWrapper) Logger(level LogLevel) Logger {
	if level <= w.level {
		return w.logger
	}
	return nil
}

func NewLog(logger *log.Logger, level LogLevel) Log {
	if logger == nil {
		return &logWrapper{level, nil}
	} else {
		return &logWrapper{level, &stdLoggerWrapper{logger}}
	}
}

func NewStderrLog(level LogLevel) Log {
	logger := log.New(os.Stderr, "", 0)
	return NewLog(logger, level)
}

////////////////////////////////////////////////////////////////////////////////

type syslogWrapper struct {
	level   LogLevel
	loggers []Logger
}

func (w *syslogWrapper) Logger(level LogLevel) Logger {
	if w.loggers == nil {
		return nil
	}
	if level <= w.level {
		return w.loggers[level]
	}
	return nil
}

func NewSysLog(writer *syslog.Writer, level LogLevel) Log {
	var loggers []Logger
	if writer != nil {
		loggers = []Logger{
			&errorLogger{writer},
			&warnLogger{writer},
			&infoLogger{writer},
			&debugLogger{writer},
		}
	}
	return &syslogWrapper{level, loggers}
}
