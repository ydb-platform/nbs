package internal_auth

import (
	"context"
	"crypto/tls"
	"fmt"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	grpc_status "google.golang.org/grpc/status"

	auth_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	iamv1 "github.com/ydb-platform/nbs/cloud/disk_manager/pkg/internal_auth/access_service"
	auth_credentials "github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

////////////////////////////////////////////////////////////////////////////////

func tryParseUser(acc *iamv1.Account) (string, bool) {
	if userID := acc.GetUserAccount().GetId(); userID != "" {
		return userID, true
	}

	if saID := acc.GetServiceAccount().GetId(); saID != "" {
		return saID, true
	}

	return "", false
}

////////////////////////////////////////////////////////////////////////////////

func NewAuthenticator(config *auth_config.AuthConfig, creds auth_credentials.Credentials) (auth.Authorizer, error) {

	duration, err := time.ParseDuration(config.GetAuthorizationCacheLifetime())
	if err != nil {
		return nil, err
	}

	common.Assert(err == nil, fmt.Sprintf("Error while parsing duration from config %v", err))
	authorizer, err := newIamAuthorizer(config, creds)
	if err != nil {
		return nil, err
	}

	return auth.NewAuthorizerWithCache(
		authorizer,
		duration,
	), nil
}

type iamAuthorizer struct {
	client  iamv1.AccessServiceClient
	iamRoot string
}

////////////////////////////////////////////////////////////////////////////////

func newIamAuthorizer(config *auth_config.AuthConfig, _ auth_credentials.Credentials) (auth.Authorizer, error) {
	ctx := context.Background()
	conn, err := grpc.DialContext(
		ctx,
		config.GetAccessServiceEndpoint(),
		grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})),
	)
	if err != nil {
		return nil, err
	}

	client := iamv1.NewAccessServiceClient(conn)
	return &iamAuthorizer{
		client:  client,
		iamRoot: config.GetServiceAccount().GetRoot(),
	}, nil
}

func (a *iamAuthorizer) Authorize(ctx context.Context, permission string) (string, error) {
	token, err := auth.GetAccessToken(ctx)
	if err != nil {
		return "", err
	}

	response, err := a.client.Authorize(ctx, &iamv1.AuthorizeRequest{
		Checks: map[int64]*iamv1.AuthorizeCheck{
			0: {
				Permission: &iamv1.Permission{
					Name: permission,
				},
				ContainerId: a.iamRoot,
				ResourcePath: &iamv1.ResourcePath{
					Path: []*iamv1.Resource{},
				},
				Identifier: &iamv1.AuthorizeCheck_IamToken{
					IamToken: token,
				},
			},
		},
	})
	if err != nil {
		return "", grpc_status.Errorf(
			codes.Unauthenticated,
			"Error while authorizing client: %s",
			err.Error(),
		)
	}

	result, ok := response.Results[0]
	if !ok {
		return "", grpc_status.Error(
			codes.Unauthenticated,
			"Authorize result missing",
		)
	}

	user, ok := tryParseUser(result.Account)
	if !ok {
		return "", grpc_status.Error(codes.Unauthenticated, "invalid account")
	}

	if result.ResultCode != iamv1.AuthorizeResult_OK {
		return "", grpc_status.Error(codes.PermissionDenied, "unauthorized")
	}

	return user, nil
}
