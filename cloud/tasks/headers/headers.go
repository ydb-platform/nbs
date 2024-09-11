package headers

import (
	"context"

	grpc_metadata "google.golang.org/grpc/metadata"
)

////////////////////////////////////////////////////////////////////////////////

func appendToIncomingContext(
	ctx context.Context,
	metadata grpc_metadata.MD,
) context.Context {

	existingMetadata, ok := grpc_metadata.FromIncomingContext(ctx)
	if ok {
		metadata = grpc_metadata.Join(existingMetadata, metadata)
	}
	return grpc_metadata.NewIncomingContext(ctx, metadata)
}

func appendToOutgoingContext(
	ctx context.Context,
	metadata grpc_metadata.MD,
) context.Context {

	existingMetadata, ok := grpc_metadata.FromOutgoingContext(ctx)
	if ok {
		metadata = grpc_metadata.Join(existingMetadata, metadata)
	}
	return grpc_metadata.NewOutgoingContext(ctx, metadata)
}

func Append(ctx context.Context, headers map[string]string) context.Context {
	metadata := grpc_metadata.New(headers)
	return appendToOutgoingContext(
		appendToIncomingContext(ctx, metadata),
		metadata,
	)
}

////////////////////////////////////////////////////////////////////////////////

func deleteFromIncomingContext(
	ctx context.Context,
	headers map[string]string,
) context.Context {

	existingMetadata, ok := grpc_metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}

	for key := range headers {
		existingMetadata.Delete(key)
	}
	return grpc_metadata.NewIncomingContext(ctx, existingMetadata)
}

func deleteFromOutgoingContext(
	ctx context.Context,
	headers map[string]string,
) context.Context {

	existingMetadata, ok := grpc_metadata.FromOutgoingContext(ctx)
	if !ok {
		return ctx
	}

	for key := range headers {
		existingMetadata.Delete(key)
	}
	return grpc_metadata.NewOutgoingContext(ctx, existingMetadata)
}

func Replace(ctx context.Context, headers map[string]string) context.Context {
	ctx = deleteFromOutgoingContext(ctx, headers)
	ctx = deleteFromIncomingContext(ctx, headers)
	return Append(ctx, headers)
}

////////////////////////////////////////////////////////////////////////////////

func getFromMetadata(
	metadata grpc_metadata.MD,
	allowedKeys []string,
) map[string]string {

	headers := make(map[string]string)

	for _, key := range allowedKeys {
		vals := metadata.Get(key)
		if len(vals) != 0 {
			headers[key] = vals[0]
		}
	}

	return headers
}

func GetFromIncomingContext(
	ctx context.Context,
	allowedKeys []string,
) map[string]string {

	metadata, ok := grpc_metadata.FromIncomingContext(ctx)
	if !ok {
		return map[string]string{}
	}
	return getFromMetadata(metadata, allowedKeys)
}

func GetFromOutgoingContext(
	ctx context.Context,
	allowedKeys []string,
) map[string]string {

	metadata, ok := grpc_metadata.FromOutgoingContext(ctx)
	if !ok {
		return map[string]string{}
	}
	return getFromMetadata(metadata, allowedKeys)
}

////////////////////////////////////////////////////////////////////////////////

func GetTracingHeaders(ctx context.Context) map[string]string {
	allowedKeys := []string{
		"x-operation-id",
		"x-request-id",
		"x-request-uid",
		"traceparent",
		"tracestate",
	}
	return GetFromIncomingContext(ctx, allowedKeys)
}

////////////////////////////////////////////////////////////////////////////////

type accountIDKey struct{}

func GetAccountID(ctx context.Context) string {
	res := ctx.Value(accountIDKey{})
	if res == nil {
		return ""
	}

	return res.(string)
}

func SetAccountID(ctx context.Context, accountID string) context.Context {
	return context.WithValue(ctx, accountIDKey{}, accountID)
}

////////////////////////////////////////////////////////////////////////////////

func GetIdempotencyKey(ctx context.Context) string {
	metadata, ok := grpc_metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	vals := metadata.Get("idempotency-key")
	if len(vals) == 0 {
		return ""
	}

	return vals[0]
}

func SetIncomingIdempotencyKey(ctx context.Context, key string) context.Context {
	return appendToIncomingContext(
		ctx,
		grpc_metadata.Pairs("idempotency-key", key),
	)
}

func SetOutgoingIdempotencyKey(ctx context.Context, key string) context.Context {
	return appendToOutgoingContext(
		ctx,
		grpc_metadata.Pairs("idempotency-key", key),
	)
}

////////////////////////////////////////////////////////////////////////////////

func GetRequestID(ctx context.Context) string {
	metadata, ok := grpc_metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	vals := metadata.Get("x-request-id")
	if len(vals) == 0 {
		return ""
	}

	return vals[0]
}

func SetIncomingRequestID(ctx context.Context, id string) context.Context {
	return appendToIncomingContext(
		ctx,
		grpc_metadata.Pairs("x-request-id", id),
	)
}

func SetOutgoingRequestID(ctx context.Context, id string) context.Context {
	return appendToOutgoingContext(
		ctx,
		grpc_metadata.Pairs("x-request-id", id),
	)
}

////////////////////////////////////////////////////////////////////////////////

func GetOperationID(ctx context.Context) string {
	metadata, ok := grpc_metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	vals := metadata.Get("x-operation-id")
	if len(vals) == 0 {
		return ""
	}

	return vals[0]
}

func SetIncomingOperationID(ctx context.Context, id string) context.Context {
	return appendToIncomingContext(
		ctx,
		grpc_metadata.Pairs("x-operation-id", id),
	)
}
