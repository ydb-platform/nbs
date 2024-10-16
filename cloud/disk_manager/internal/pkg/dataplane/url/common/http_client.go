package common

import (
	"context"
	"fmt"
	"io"
	"net/http"
	net_url "net/url"
	"strings"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

const urlReplacement = "XXXXXX"

////////////////////////////////////////////////////////////////////////////////

type errorWithURLReplaced struct {
	Err                error
	substringToReplace string
}

func (e *errorWithURLReplaced) Error() string {
	return replaceURLSubstring(e.Err.Error(), e.substringToReplace)
}

func (e *errorWithURLReplaced) Unwrap() error {
	return e.Err
}

func (e *errorWithURLReplaced) Is(target error) bool {
	t, ok := target.(*errorWithURLReplaced)
	if !ok {
		return false
	}

	return t.Err == nil || (e.Err == t.Err)
}

func newErrorWithURLReplaced(err error, substringToReplace string) error {
	if err == nil {
		return nil
	}

	return &errorWithURLReplaced{
		Err:                err,
		substringToReplace: substringToReplace,
	}
}

////////////////////////////////////////////////////////////////////////////////

type loggerWithURLReplaced struct {
	ctx                context.Context
	substringToReplace string
}

func newLoggerWithURLReplaced(
	ctx context.Context,
	substringToReplace string,
) *loggerWithURLReplaced {

	return &loggerWithURLReplaced{
		ctx:                logging.AddCallerSkip(ctx, 1),
		substringToReplace: substringToReplace,
	}
}

func (l *loggerWithURLReplaced) getKeysAndValuesFormat(itemsCount int) string {
	format := ""

	for i := 0; i < itemsCount; i++ {
		if i%2 == 0 {
			format += ", %v:"
		} else {
			format += " %v"
		}
	}

	return format
}

func (l *loggerWithURLReplaced) replaceURLSubstring(
	msg string,
	keysAndValues ...interface{},
) string {

	msg += fmt.Sprintf(l.getKeysAndValuesFormat(len(keysAndValues)), keysAndValues...)
	return replaceURLSubstring(msg, l.substringToReplace)
}

func (l *loggerWithURLReplaced) Debug(msg string, keysAndValues ...interface{}) {
	logging.Debug(l.ctx, l.replaceURLSubstring(msg, keysAndValues))
}

func (l *loggerWithURLReplaced) Info(msg string, keysAndValues ...interface{}) {
	logging.Info(l.ctx, l.replaceURLSubstring(msg, keysAndValues))
}

func (l *loggerWithURLReplaced) Warn(msg string, keysAndValues ...interface{}) {
	logging.Warn(l.ctx, l.replaceURLSubstring(msg, keysAndValues))
}

func (l *loggerWithURLReplaced) Error(msg string, keysAndValues ...interface{}) {
	logging.Error(l.ctx, l.replaceURLSubstring(msg, keysAndValues))
}

////////////////////////////////////////////////////////////////////////////////

func checkHTTPStatus(statusCode int) error {
	if statusCode >= 200 && statusCode <= 299 {
		return nil
	}

	errorMessage := fmt.Sprintf("http code %d", statusCode)

	// 4xx statuses
	if statusCode == http.StatusTooManyRequests ||
		statusCode == http.StatusLocked ||
		statusCode == http.StatusRequestTimeout {
		return errors.NewRetriableErrorf(errorMessage)
	}
	if statusCode == http.StatusRequestedRangeNotSatisfiable {
		return errors.NewNonRetriableErrorf(errorMessage)
	}
	if statusCode == http.StatusForbidden {
		return NewSourceForbiddenError(errorMessage)
	}
	if statusCode >= 400 && statusCode <= 499 {
		return NewSourceNotFoundError(errorMessage)
	}

	if statusCode >= 500 && statusCode <= 599 {
		return errors.NewRetriableErrorf(errorMessage)
	}

	return errors.NewNonRetriableErrorf("http code %d", statusCode)
}

////////////////////////////////////////////////////////////////////////////////

type httpClientInterface interface {
	Head(ctx context.Context) (*http.Response, error)
	Body(
		ctx context.Context,
		start, end uint64, // Half-open interval [start:end).
		etag string,
	) (io.ReadCloser, error)
	RequestsCount() uint64
}

////////////////////////////////////////////////////////////////////////////////

type httpClient struct {
	client        *http.Client
	url           string
	requestsCount uint64
}

func (c *httpClient) Head(ctx context.Context) (*http.Response, error) {
	head, err := c.head(ctx)
	return head, newErrorWithURLReplaced(err, getParametersFromURL(c.url))
}

func (c *httpClient) Body(
	ctx context.Context,
	start, end uint64, // Half-open interval [start:end).
	etag string,
) (io.ReadCloser, error) {

	body, err := c.body(ctx, start, end, etag)
	c.requestsCount++
	return body, newErrorWithURLReplaced(err, getParametersFromURL(c.url))
}

func (c *httpClient) RequestsCount() uint64 {
	return c.requestsCount
}

////////////////////////////////////////////////////////////////////////////////

func (c *httpClient) head(
	ctx context.Context,
) (*http.Response, error) {

	logging.Debug(
		ctx,
		"Sending http get header request to %q",
		removeParametersFromURL(c.url),
	)

	resp, err := c.client.Head(c.url)
	if err != nil {
		// NBS-3324: should it be retriable?
		return nil, errors.NewRetriableError(err)
	}

	err = resp.Body.Close()
	if err != nil {
		// NBS-3324: should it be retriable?
		return nil, errors.NewRetriableError(err)
	}

	err = checkHTTPStatus(resp.StatusCode)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (c *httpClient) body(
	ctx context.Context,
	start, end uint64, // Half-open interval [start:end).
	etag string,
) (io.ReadCloser, error) {

	req, err := http.NewRequest(http.MethodGet, c.url, nil)
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	reqCtx, cancelReqCtx := context.WithCancel(ctx)

	req = req.WithContext(reqCtx)
	defer func() {
		if err != nil {
			cancelReqCtx()
		}
	}()

	// Use closed interval [start, last] for range request.
	last := end - 1
	req.Header.Set("RANGE", fmt.Sprintf("bytes=%v-%v", start, last))
	var resp *http.Response

	logging.Debug(
		ctx,
		"Sending http get body request to %q, range [%v, %v), etag %q",
		removeParametersFromURL(c.url),
		start,
		end,
		etag,
	)

	// TODO: try to use http2 streams NBS-3253.
	resp, err = c.client.Do(req)
	if err != nil {
		return nil, errors.NewRetriableErrorf(
			"range [%v:%v]: %v",
			start,
			end,
			err,
		)
	}

	err = checkHTTPStatus(resp.StatusCode)
	if err != nil {
		return nil, err
	}

	if resp.Header.Get("Etag") != etag {
		return nil, NewWrongETagError(
			"wrong ETag: requested %v, actual %v",
			etag,
			resp.Header.Get("Etag"),
		)
	}

	requestedContentLength := end - start
	if resp.ContentLength != int64(requestedContentLength) {
		return nil, errors.NewNonRetriableErrorf(
			"bad content length: requested %d, actual %d",
			requestedContentLength,
			resp.ContentLength,
		)
	}

	return httpReadCloser{
		cancel:     cancelReqCtx,
		readCloser: resp.Body,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

type httpReadCloser struct {
	cancel     func()
	readCloser io.ReadCloser
}

func (r httpReadCloser) Read(data []byte) (int, error) {
	return r.readCloser.Read(data)
}

func (r httpReadCloser) Close() error {
	r.cancel()
	return r.readCloser.Close()
}

////////////////////////////////////////////////////////////////////////////////

func newHTTPClient(
	ctx context.Context,
	timeout time.Duration,
	minRetryTimeout time.Duration,
	maxRetryTimeout time.Duration,
	maxRetries uint32,
	url string,
) *httpClient {

	retryableClient := retryablehttp.NewClient()

	retryableClient.HTTPClient.Timeout = timeout
	retryableClient.RetryWaitMin = minRetryTimeout
	retryableClient.RetryWaitMax = maxRetryTimeout
	retryableClient.RetryMax = int(maxRetries)
	retryableClient.Logger = newLoggerWithURLReplaced(
		ctx,
		getParametersFromURL(url),
	)

	return &httpClient{
		client: retryableClient.StandardClient(),
		url:    url,
	}
}

////////////////////////////////////////////////////////////////////////////////

func removeParametersFromURL(url string) string {
	parsedURL, err := net_url.Parse(url)
	if err != nil {
		return ""
	}

	return fmt.Sprintf(
		"%v://%v%v",
		parsedURL.Scheme,
		parsedURL.Host,
		parsedURL.Path,
	)
}

func getParametersFromURL(url string) string {
	suffix, _ := strings.CutPrefix(url, removeParametersFromURL(url))
	return suffix
}

func replaceURLSubstring(message string, substring string) string {
	if len(substring) == 0 {
		return message
	}

	return strings.Replace(message, substring, urlReplacement, -1)
}
