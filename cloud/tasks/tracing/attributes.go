package tracing

import (
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

////////////////////////////////////////////////////////////////////////////////

func WithAttributes(
	attributes ...attribute.KeyValue,
) trace.SpanStartEventOption {

	return trace.WithAttributes(attributes...)
}

////////////////////////////////////////////////////////////////////////////////

func AttributeBool(key string, value bool) attribute.KeyValue {
	return attribute.Bool(key, value)
}

func AttributeInt(key string, value int) attribute.KeyValue {
	return attribute.Int(key, value)
}

func AttributeInt64(key string, value int64) attribute.KeyValue {
	return attribute.Int64(key, value)
}

func AttributeString(key string, value string) attribute.KeyValue {
	return attribute.String(key, value)
}

func AttributeError(err error) attribute.KeyValue {
	return attribute.String(
		"error",
		errors.ErrorForTracing(err),
	)
}
