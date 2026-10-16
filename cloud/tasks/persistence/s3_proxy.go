package persistence

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

const s3ProxyHostnameTimeout = 5 * time.Second

// Sends requests past the L3 balancer of the storage, straight to one of its
// proxies. The proxy comes from the hostname handle of the storage; it is
// kept for refreshInterval and then asked for again, so that no proxy is
// used for long and the storage can take one out of rotation. A proxy that
// answered 5xx or did not answer is dropped at once. When the handle does
// not answer, requests go through the balancer.
type s3ProxyChooser struct {
	hostnameURL     string
	httpPort        uint32
	httpsPort       uint32
	refreshInterval time.Duration
	// Without keep-alive: a kept connection would be pinned to one proxy
	// and the handle would name that proxy every time.
	client  *http.Client
	metrics *s3Metrics

	mutex    sync.Mutex
	host     string
	chosenAt time.Time
}

func newS3ProxyChooser(
	hostnameURL string,
	httpPort uint32,
	httpsPort uint32,
	refreshInterval time.Duration,
	metrics *s3Metrics,
) *s3ProxyChooser {

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DisableKeepAlives = true

	return &s3ProxyChooser{
		hostnameURL:     hostnameURL,
		httpPort:        httpPort,
		httpsPort:       httpsPort,
		refreshInterval: refreshInterval,
		client: &http.Client{
			Transport: transport,
			Timeout:   s3ProxyHostnameTimeout,
		},
		metrics: metrics,
	}
}

// The proxy for a request, as http.Transport.Proxy expects it; nil sends
// the request through the balancer.
func (c *s3ProxyChooser) proxy(request *http.Request) (*url.URL, error) {
	host := c.currentHost(request.Context())
	if len(host) == 0 {
		return nil, nil
	}

	port := c.httpPort
	if request.URL.Scheme == "https" {
		port = c.httpsPort
	}

	return &url.URL{
		Scheme: "http",
		Host:   fmt.Sprintf("%v:%v", host, port),
	}, nil
}

// Drops the current proxy: the next request asks the handle for another.
func (c *s3ProxyChooser) discard() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.host = ""
}

func (c *s3ProxyChooser) currentHost(ctx context.Context) string {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	now := time.Now()
	if len(c.host) != 0 && now.Sub(c.chosenAt) < c.refreshInterval {
		return c.host
	}

	host, err := c.fetchHost(ctx)
	if err != nil {
		c.metrics.OnProxyRefreshError()
		logging.Warn(
			ctx,
			"failed to choose an S3 proxy, requests go through the balancer: %v",
			err,
		)
		// Not asked again before the interval passes.
		c.host = ""
		c.chosenAt = now
		return ""
	}

	c.metrics.OnProxyRefresh()
	c.host = host
	c.chosenAt = now
	return host
}

func (c *s3ProxyChooser) fetchHost(ctx context.Context) (string, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		c.hostnameURL,
		nil,
	)
	if err != nil {
		return "", err
	}

	response, err := c.client.Do(request)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	if response.StatusCode != http.StatusOK {
		return "", errors.NewRetriableErrorf(
			"%v answered %v",
			c.hostnameURL,
			response.Status,
		)
	}

	body, err := io.ReadAll(io.LimitReader(response.Body, 1024))
	if err != nil {
		return "", err
	}

	host := strings.TrimSpace(string(body))
	if len(host) == 0 || strings.ContainsAny(host, " /\\:") {
		return "", errors.NewRetriableErrorf(
			"%v answered an invalid host %q",
			c.hostnameURL,
			host,
		)
	}

	return host, nil
}
