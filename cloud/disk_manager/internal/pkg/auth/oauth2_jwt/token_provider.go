package oauth2_jwt

import (
	"context"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth"
	auth_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/auth/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"sync"
	"time"
)

const defaultPollingInterval = time.Minute

type tokenProvider struct {
	fetcher             TokenFetcher
	maxRetryTimeout     time.Duration
	currentRetryTimeout time.Duration

	mutex        sync.RWMutex
	timer        *time.Timer
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
			return "", errors.NewRetriableErrorf("Context deadline exceeded")
		default:
			if ok, tokenString, err := t.getToken(); ok {
				return tokenString, err
			}
		}
	}
}

func (t *tokenProvider) fetchTokenLoop() {
	defer t.timer.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		select {
		case <-t.done:
			t.mutex.Lock()
			t.currentToken, t.err = nil, errors.NewRetriableErrorf("Context deadline exceeded")
			t.mutex.Unlock()
			return
		case <-t.timer.C:
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
	defer func() {
		t.mutex.Lock()
		defer t.mutex.Unlock()

		if t.err != nil {
			t.timer.Reset(t.currentRetryTimeout)
			if t.currentRetryTimeout*2 <= t.maxRetryTimeout {
				t.currentRetryTimeout *= 2
			}
			return
		}
		t.timer.Reset(defaultPollingInterval)
	}()
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

func NewOauth2JWTTokenProvider(config *auth_config.AuthConfig) (auth.Credentials, error) {
	return newOauth2JWTTokenProvider(config, time.Now)
}

func newOauth2JWTTokenProvider(config *auth_config.AuthConfig, now nowFunc) (auth.Credentials, error) {
	fetcher, err := newOauth2JWTTokenFetcher(config, now)
	if err != nil {
		return nil, err
	}

	retryTimeout, err := time.ParseDuration(config.GetPerRetryTimeout())
	if err != nil {
		return nil, err
	}
	retryMultiplier := 2 << config.GetRetryCount()
	maxRetryTimeout := retryTimeout * time.Duration(retryMultiplier)

	if maxRetryTimeout != 0 {
		maxRetryTimeout = min(maxRetryTimeout, defaultPollingInterval)
	}

	provider := &tokenProvider{
		fetcher:             fetcher,
		maxRetryTimeout:     maxRetryTimeout,
		currentRetryTimeout: retryTimeout,
		mutex:               sync.RWMutex{},
		timer:               time.NewTimer(0),
		done:                make(chan struct{}),
		currentToken:        nil,
		err:                 nil,
	}

	return provider, nil
}
