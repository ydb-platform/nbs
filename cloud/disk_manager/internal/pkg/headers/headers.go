package headers

import (
	"context"
	"fmt"
	"strings"

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

////////////////////////////////////////////////////////////////////////////////

func getAuthorizationHeader(ctx context.Context) string {
	metadata, ok := grpc_metadata.FromIncomingContext(ctx)
	if !ok {
		return ""
	}

	vals := metadata.Get("authorization")
	if len(vals) == 0 {
		return ""
	}

	return vals[0]
}

const (
	tokenPrefix = "Bearer "
)

func GetAccessToken(ctx context.Context) (string, error) {
	token := getAuthorizationHeader(ctx)

	if len(token) == 0 {
		return "", fmt.Errorf("failed to find auth token in the request context")
	}

	if !strings.HasPrefix(token, tokenPrefix) {
		return "", fmt.Errorf(
			"expected token to start with \"%s\", found \"%.7s\"",
			tokenPrefix,
			token,
		)
	}

	return token[len(tokenPrefix):], nil
}

func SetOutgoingAccessToken(ctx context.Context, token string) context.Context {
	return appendToOutgoingContext(
		ctx,
		grpc_metadata.Pairs(
			"authorization",
			fmt.Sprintf("Bearer %v", token),
		),
	)
}
