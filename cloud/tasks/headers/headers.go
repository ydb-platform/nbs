package headers

import (
	"context"

	grpc_metadata "google.golang.org/grpc/metadata"
)

////////////////////////////////////////////////////////////////////////////////

func appendToIncomingContext(
	ctx context.Context,
	md grpc_metadata.MD,
) context.Context {

	existingMd, ok := grpc_metadata.FromIncomingContext(ctx)
	if ok {
		md = grpc_metadata.Join(existingMd, md)
	}

	return grpc_metadata.NewIncomingContext(ctx, md)
}

func appendToOutgoingContext(
	ctx context.Context,
	md grpc_metadata.MD,
) context.Context {

	existingMd, ok := grpc_metadata.FromOutgoingContext(ctx)
	if ok {
		md = grpc_metadata.Join(existingMd, md)
	}

	return grpc_metadata.NewOutgoingContext(ctx, md)
}

func Append(ctx context.Context, headers map[string]string) context.Context {
	md := grpc_metadata.New(headers)
	return appendToOutgoingContext(appendToIncomingContext(ctx, md), md)
}

////////////////////////////////////////////////////////////////////////////////

func getFromIncomingContext(
	ctx context.Context,
	allowedKeys []string,
) map[string]string {

	md, ok := grpc_metadata.FromIncomingContext(ctx)
	if !ok {
		return map[string]string{}
	}

	headers := make(map[string]string)

	for _, key := range allowedKeys {
		vals := md.Get(key)
		if len(vals) != 0 {
			headers[key] = vals[0]
		}
	}

	return headers
}

////////////////////////////////////////////////////////////////////////////////

func GetTracingHeaders(ctx context.Context) map[string]string {
	allowedKeys := []string{
		"x-operation-id",
		"x-request-id",
		"x-request-uid",
	}
	return getFromIncomingContext(ctx, allowedKeys)
}

////////////////////////////////////////////////////////////////////////////////

func GetTraceContext(
	ctx context.Context,
	traceContextKeys []string,
) map[string]string {

	return getFromIncomingContext(ctx, traceContextKeys)
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
