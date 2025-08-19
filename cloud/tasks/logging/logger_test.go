package logging

import (
	"context"
	"testing"
    "regexp"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/library/go/core/log"
)

////////////////////////////////////////////////////////////////////////////////

type mockLogger struct {
	mock.Mock
}

func (l *mockLogger) Trace(msg string, fields ...log.Field) {
	l.Called(msg, fields)
}

func (l *mockLogger) Debug(msg string, fields ...log.Field) {
	l.Called(msg, fields)
}

func (l *mockLogger) Info(msg string, fields ...log.Field) {
	l.Called(msg, fields)
}

func (l *mockLogger) Warn(msg string, fields ...log.Field) {
	l.Called(msg, fields)
}

func (l *mockLogger) Error(msg string, fields ...log.Field) {
	l.Called(msg, fields)
}

func (l *mockLogger) Fatal(msg string, fields ...log.Field) {
	l.Called(msg, fields)
}

func (l *mockLogger) Tracef(format string, args ...interface{}) {
	l.Called(format, args)
}

func (l *mockLogger) Debugf(format string, args ...interface{}) {
	l.Called(format, args)
}

func (l *mockLogger) Infof(format string, args ...interface{}) {
	l.Called(format, args)
}

func (l *mockLogger) Warnf(format string, args ...interface{}) {
	l.Called(format, args)
}

func (l *mockLogger) Errorf(format string, args ...interface{}) {
	l.Called(format, args)
}

func (l *mockLogger) Fatalf(format string, args ...interface{}) {
	l.Called(format, args)
}

func (l *mockLogger) Logger() log.Logger {
	return l
}

func (l *mockLogger) Structured() log.Structured {
	return l
}

func (l *mockLogger) Fmt() log.Fmt {
	return l
}

func (l *mockLogger) WithName(name string) log.Logger {
	return l
}

func createMockLogger() *mockLogger {
	return &mockLogger{}
}

////////////////////////////////////////////////////////////////////////////////

func newContext(
	idempotencyKey string,
	requestID string,
	operationID string,
) context.Context {

	ctx := context.Background()
	ctx = headers.SetIncomingOperationID(ctx, operationID)
	ctx = headers.SetIncomingIdempotencyKey(ctx, idempotencyKey)
	return headers.SetIncomingRequestID(ctx, requestID)
}

////////////////////////////////////////////////////////////////////////////////

func TestLogTrace(t *testing.T) {
	logger := createMockLogger()
	ctx := SetLogger(newContext("idempID", "reqID", "opID"), logger)

	logger.On(
		"Trace",
		"Stuff happened",
		[]log.Field{
			log.String(idempotencyKeyKey, "idempID"),
			log.String(requestIDKey, "reqID"),
			log.String(operationIDKey, "opID"),
			log.String(syslogIdentifierKey, "disk-manager"),
		},
	)

	Trace(ctx, "Stuff happened")
}

func TestLogDebug(t *testing.T) {
	logger := createMockLogger()
	ctx := SetLogger(newContext("idempID", "reqID", "opID"), logger)

	logger.On(
		"Debug",
		"Stuff happened",
		[]log.Field{
			log.String(idempotencyKeyKey, "idempID"),
			log.String(requestIDKey, "reqID"),
			log.String(operationIDKey, "opID"),
			log.String(syslogIdentifierKey, "disk-manager"),
		},
	)

	Debug(ctx, "Stuff happened")
}

func TestLogInfo(t *testing.T) {
	logger := createMockLogger()
	ctx := SetLogger(newContext("idempID", "reqID", "opID"), logger)

	logger.On(
		"Info",
		"Stuff happened",
		[]log.Field{
			log.String(idempotencyKeyKey, "idempID"),
			log.String(requestIDKey, "reqID"),
			log.String(operationIDKey, "opID"),
			log.String(syslogIdentifierKey, "disk-manager"),
		},
	)

	Info(ctx, "Stuff happened")
}

func TestLogWarn(t *testing.T) {
	logger := createMockLogger()
	ctx := SetLogger(newContext("idempID", "reqID", "opID"), logger)

	logger.On(
		"Warn",
		"Stuff happened",
		[]log.Field{
			log.String(idempotencyKeyKey, "idempID"),
			log.String(requestIDKey, "reqID"),
			log.String(operationIDKey, "opID"),
			log.String(syslogIdentifierKey, "disk-manager"),
		},
	)

	Warn(ctx, "Stuff happened")
}

func TestLogError(t *testing.T) {
	logger := createMockLogger()
	ctx := SetLogger(newContext("idempID", "reqID", "opID"), logger)

	logger.On(
		"Error",
		"Stuff happened",
		[]log.Field{
			log.String(idempotencyKeyKey, "idempID"),
			log.String(requestIDKey, "reqID"),
			log.String(operationIDKey, "opID"),
			log.String(syslogIdentifierKey, "disk-manager"),
		},
	)

	Error(ctx, "Stuff happened")
}

func TestLogFatal(t *testing.T) {
	logger := createMockLogger()
	ctx := SetLogger(newContext("idempID", "reqID", "opID"), logger)

	logger.On(
		"Fatal",
		"Stuff happened",
		[]log.Field{
			log.String(idempotencyKeyKey, "idempID"),
			log.String(requestIDKey, "reqID"),
			log.String(operationIDKey, "opID"),
			log.String(syslogIdentifierKey, "disk-manager"),
		},
	)

	Fatal(ctx, "Stuff happened")
}

func TestLogWithFields(t *testing.T) {
	logger := createMockLogger()
	ctx := SetLogger(newContext("idempID", "reqID", "opID"), logger)

	ctx = WithComponent(ctx, "component")
	ctx = WithTaskID(ctx, "taskID")

	ctx = WithFields(
		ctx,
		log.String("field_1", "value_1"),
		log.Int("field_2", 713),
	)

	logger.On(
		"Info",
		"Stuff happened",
		[]log.Field{
			log.String(idempotencyKeyKey, "idempID"),
			log.String(requestIDKey, "reqID"),
			log.String(operationIDKey, "opID"),
			log.String(syslogIdentifierKey, "disk-manager"),
			log.String("COMPONENT", "component"),
			log.String("TASK_ID", "taskID"),
			log.String("field_1", "value_1"),
			log.Int("field_2", 713),
		},
	)

	Info(ctx, "Stuff happened")
}
