package oauth2_jwt

import (
	"context"
	"sync"
	"time"

	auth_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/pkg/auth"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

const defaultPollingInterval = time.Minute

type tokenProvider struct {
	fetcher      TokenFetcher
	mutex        sync.RWMutex
	ticker       *time.Ticker
	done         chan struct{}
	currentToken Token
	err          error
}

func (t *tokenProvider) getToken() (bool, string, error) {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if t.currentToken != nil {
		return true, t.currentToken.Token(), nil
	}

	if t.err != nil {
		return true, "", errors.NewRetriableError(t.err)
	}
	return false, "", nil
}

func (t *tokenProvider) Token(ctx context.Context) (string, error) {
	// check token before launching loop
	if ok, tokenString, err := t.getToken(); ok {
		return tokenString, err
	}

	for {
		select {
		case <-ctx.Done():
			return "", errors.NewRetriableErrorf(
				"Context deadline exceeded")
		default:
			if ok, tokenString, err := t.getToken(); ok {
				return tokenString, err
			}
		}
	}
}

func (t *tokenProvider) fetchTokenLoop() {
	defer t.ticker.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	t.refreshOnce(ctx)
	for {
		select {
		case <-t.done:
			t.mutex.Lock()
			t.currentToken, t.err = nil, errors.NewRetriableErrorf(
				"Context deadline exceeded")
			t.mutex.Unlock()
			return
		case <-t.ticker.C:
			t.refreshOnce(ctx)
		}
	}
}

func (t *tokenProvider) shouldFetchToken() bool {
	t.mutex.RLock()
	defer t.mutex.RUnlock()

	if t.currentToken == nil {
		return true
	}

	return t.currentToken.NeedToUpdate()
}

func (t *tokenProvider) refreshOnce(ctx context.Context) {
	if !t.shouldFetchToken() {
		return
	}
	token, err := t.fetcher.Fetch(ctx)

	t.mutex.Lock()
	defer t.mutex.Unlock()

	t.err = err
	if t.err == nil {
		t.currentToken = token
	}
}

func NewOauth2JWTTokenProvider(
	config *auth_config.AuthConfig,
) (auth.Credentials, error) {
	return newOauth2JWTTokenProvider(config, time.Now)
}

func newOauth2JWTTokenProvider(
	config *auth_config.AuthConfig,
	now nowFunc,
) (auth.Credentials, error) {
	fetcher, err := newOauth2JWTTokenFetcher(config, now)
	if err != nil {
		return nil, err
	}

	pollingInterval, err := time.ParseDuration(config.GetPerRetryTimeout())
	if err != nil {
		return nil, err
	}

	if pollingInterval == 0 {
		pollingInterval = defaultPollingInterval
	}
	provider := &tokenProvider{
		fetcher:      fetcher,
		mutex:        sync.RWMutex{},
		ticker:       time.NewTicker(pollingInterval),
		done:         make(chan struct{}),
		currentToken: nil,
		err:          nil,
	}
	go provider.fetchTokenLoop()
	return provider, nil
}
