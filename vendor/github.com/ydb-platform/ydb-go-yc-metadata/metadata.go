package yc

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/credentials"

	"github.com/ydb-platform/ydb-go-yc-metadata/trace"
)

var (
	// check compatibility with ydb-go-sdk credentials interface
	_ credentials.Credentials = (*InstanceServiceAccountCredentials)(nil)

	errClosed = errors.New("instance service account client closed")
)

const metadataURL = "http://169.254.169.254/computeMetadata/v1/instance/service-accounts/default/token"

type InstanceServiceAccountCredentials struct {
	mu *sync.RWMutex

	token string
	err   error

	expiry time.Time
	timer  *time.Timer

	done chan struct{}

	metadataURL string

	caller string

	retryNotFound bool

	trace trace.Trace
}

// Token returns cached token if it is valid. Otherwise, will try to renew.
func (m *InstanceServiceAccountCredentials) Token(ctx context.Context) (token string, err error) {
	onDone := trace.TraceOnGetToken(m.trace, &ctx)
	defer func() {
		onDone(token, err)
	}()

	// check token before launching loop
	m.mu.RLock()
	token, err = m.token, m.err
	m.mu.RUnlock()
	if token != "" || err != nil {
		return token, err
	}

	for {
		select {
		case <-ctx.Done():
			return "", &createTokenError{
				Cause:  ctx.Err(),
				Reason: ctx.Err().Error(),
			}
		default:
			m.mu.RLock()
			token, err = m.token, m.err
			m.mu.RUnlock()
			if token != "" || err != nil {
				return token, err
			}
			// not yet initialized, wait
		}
	}
}

func (m *InstanceServiceAccountCredentials) Stop() {
	close(m.done)
}

func (m *InstanceServiceAccountCredentials) String() string {
	if m.caller == "" {
		return "InstanceServiceAccountCredentials (metadataURL=" + m.metadataURL + ")"
	}
	return "InstanceServiceAccountCredentials created from " + m.caller + " (metadataURL=" + m.metadataURL + ")"
}

func (m *InstanceServiceAccountCredentials) refreshLoop() {
	defer m.timer.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		select {
		case <-m.done:
			// Set up error
			m.mu.Lock()
			m.token, m.err = "", &createTokenError{
				Cause:  errClosed,
				Reason: errClosed.Error(),
			}
			m.mu.Unlock()
			return
		case <-m.timer.C:
			m.refreshOnce(ctx)
		}
	}
}

// Perform single refresh iteration.
// If token was obtained:
// 1. Clear current err;
// 2. Set up new token and expiration;
// Otherwise, if current token has expired, clear it and set up err.
func (m *InstanceServiceAccountCredentials) refreshOnce(ctx context.Context) {
	now := time.Now()
	tok, err := m.metaCall(ctx, m.metadataURL, m.retryNotFound)

	// Call has been performed, now updating fields
	m.mu.Lock()
	defer m.mu.Unlock()

	defer func() {
		const minInterval = 5 * time.Second
		// Reset timer: trigger after 10% of expiry.
		// NB: we are guaranteed to have drained timer here.
		interval := time.Until(m.expiry) / 10
		if interval < minInterval {
			interval = minInterval
		}
		m.timer.Reset(interval)
	}()

	if err != nil {
		// Check if current value is still good.
		if m.expiry.After(now) {
			// Will leave old token in place
			return
		}
		// Clear token and set up err
		m.token = ""
		m.err = err
		return
	}
	// Renew values.
	m.token, m.expiry, m.err = tok.Token, now.Add(tok.ExpiresIn), nil
}

type InstanceServiceAccountCredentialsOption func(c *InstanceServiceAccountCredentials)

func WithInstanceServiceAccountURL(url string) InstanceServiceAccountCredentialsOption {
	return func(c *InstanceServiceAccountCredentials) {
		c.metadataURL = url
	}
}

func WithTrace(t trace.Trace) InstanceServiceAccountCredentialsOption {
	return func(c *InstanceServiceAccountCredentials) {
		c.trace = c.trace.Compose(t)
	}
}

func WithInstanceServiceAccountCredentialsSourceInfo(sourceInfo string) InstanceServiceAccountCredentialsOption {
	return func(c *InstanceServiceAccountCredentials) {
		c.caller = sourceInfo
	}
}

func WithRetryNotFound() InstanceServiceAccountCredentialsOption {
	return func(c *InstanceServiceAccountCredentials) {
		c.retryNotFound = true
	}
}

// instanceServiceAccount makes credentials provider that uses instance metadata url to obtain
// token for service account attached to instance. Cancelling context will lead to credentials
// refresh halt. It should be used during application stop or credentials recreation.
func instanceServiceAccount(opts ...InstanceServiceAccountCredentialsOption) *InstanceServiceAccountCredentials {
	credentials := &InstanceServiceAccountCredentials{
		metadataURL: metadataURL,
		mu:          &sync.RWMutex{},
		timer:       time.NewTimer(0), // Allocate expired
		done:        make(chan struct{}),
	}
	for _, o := range opts {
		o(credentials)
	}
	// Start refresh loop.
	go credentials.refreshLoop()
	return credentials
}
