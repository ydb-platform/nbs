package auth

import (
	"context"
)

////////////////////////////////////////////////////////////////////////////////

type AccessServiceClient interface {
	Authorize(ctx context.Context, permission string) (string, error)
}

////////////////////////////////////////////////////////////////////////////////

func NewStubClient() AccessServiceClient {
	return &stubClient{}
}

////////////////////////////////////////////////////////////////////////////////

type stubClient struct{}

func (c *stubClient) Authorize(
	ctx context.Context,
	permission string,
) (string, error) {

	return "unauthorized", nil
}
