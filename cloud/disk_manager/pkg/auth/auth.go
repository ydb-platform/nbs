package auth

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"time"

	"github.com/karlseguin/ccache/v2"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/headers"
	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"
)

////////////////////////////////////////////////////////////////////////////////

type Credentials = credentials.Credentials

////////////////////////////////////////////////////////////////////////////////

type Authorizer interface {
	Authorize(ctx context.Context, permission string) (string, error)
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

func NewStubAuthorizer() Authorizer {
	return &stubAuthorizer{}
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

	token, err := headers.GetAccessToken(ctx)
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
