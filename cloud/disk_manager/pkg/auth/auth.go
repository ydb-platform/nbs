package auth

import (
	"context"
)

////////////////////////////////////////////////////////////////////////////////

type Authorizer interface {
	Authorize(ctx context.Context, permission string) (string, error)
}

////////////////////////////////////////////////////////////////////////////////

func NewStubAuthorizer() Authorizer {
	return &stubAuthorizer{}
}

////////////////////////////////////////////////////////////////////////////////

type stubAuthorizer struct{}

func (a *stubAuthorizer) Authorize(
	ctx context.Context,
	permission string,
) (string, error) {

	return "unauthorized", nil
}
