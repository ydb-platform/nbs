package auth

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"time"

	"github.com/karlseguin/ccache/v2"
	auth_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/headers"
	tasks_headers "github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

////////////////////////////////////////////////////////////////////////////////

type AuthConfig = auth_config.AuthConfig

type Credentials = credentials.Credentials

////////////////////////////////////////////////////////////////////////////////

type Authorizer interface {
	Authorize(ctx context.Context, permission string) (string, error)
}

////////////////////////////////////////////////////////////////////////////////

type NewAuthorizer = func(config *AuthConfig, creds Credentials) (Authorizer, error)

func NewAuthorizerWithCache(
	authorizer Authorizer,
	cacheLifetime time.Duration,
) Authorizer {

	return &authorizerWithCache{
		authorizer:    authorizer,
		cacheLifetime: cacheLifetime,
		cache:         ccache.New(ccache.Configure()),
	}
}

func NewStubAuthorizer() Authorizer {
	return &stubAuthorizer{}
}

////////////////////////////////////////////////////////////////////////////////

func GetRequestID(ctx context.Context) string {
	return tasks_headers.GetRequestID(ctx)
}

func GetAccessToken(ctx context.Context) (string, error) {
	return headers.GetAccessToken(ctx)
}

func SetOutgoingAccessToken(ctx context.Context, token string) context.Context {
	return headers.SetOutgoingAccessToken(ctx, token)
}

////////////////////////////////////////////////////////////////////////////////

type cacheKey struct {
	token      string
	permission string
}

func (k *cacheKey) Hash() (hash string, ok bool) {
	h := md5.New()

	_, err := h.Write([]byte(k.token))
	if err != nil {
		return "", false
	}

	_, err = h.Write([]byte(k.permission))
	if err != nil {
		return "", false
	}

	return hex.EncodeToString(h.Sum(nil)), true
}

////////////////////////////////////////////////////////////////////////////////

type authorizerWithCache struct {
	authorizer    Authorizer
	cacheLifetime time.Duration
	cache         *ccache.Cache
}

func (a *authorizerWithCache) Authorize(
	ctx context.Context,
	permission string,
) (string, error) {

	token, err := GetAccessToken(ctx)
	if err != nil {
		return "", err
	}

	cacheKey := cacheKey{
		token:      token,
		permission: permission,
	}

	hash, hashOK := cacheKey.Hash()
	if hashOK {
		item := a.cache.Get(hash)
		if item != nil && !item.Expired() {
			return item.Value().(string), nil
		}
	}

	accountID, err := a.authorizer.Authorize(ctx, permission)
	if err != nil {
		return "", err
	}

	if hashOK {
		a.cache.Set(hash, accountID, a.cacheLifetime)
	}

	return accountID, nil
}

////////////////////////////////////////////////////////////////////////////////

type stubAuthorizer struct{}

func (a *stubAuthorizer) Authorize(
	ctx context.Context,
	permission string,
) (string, error) {

	return "unauthorized", nil
}
