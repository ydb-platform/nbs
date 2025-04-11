package common

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

////////////////////////////////////////////////////////////////////////////////

func TestHTTPClientURLReplacement(t *testing.T) {
	invalidURL := "InvalidURL"

	notFoundURLPrefix := "https://foo/bar"
	notFoundURLParameters := "?aa=bb&cc=dd"
	notFoundURL := notFoundURLPrefix + notFoundURLParameters

	noParametersURL := "https://xxx/yyy"

	type TestCase struct {
		prefix     string
		parameters string
		url        string
	}

	testCases := []TestCase{
		{"", invalidURL, invalidURL},
		{notFoundURLPrefix, notFoundURLParameters, notFoundURL},
		{noParametersURL, "", noParametersURL},
	}

	checkError := func(err error, urlPrefix, parameters string) {
		require.Error(t, err)
		if len(parameters) > 0 {
			require.NotContains(t, err.Error(), parameters)
			require.Contains(t, err.Error(), urlReplacement)
		} else {
			require.NotContains(t, err.Error(), urlReplacement)
		}
		require.Contains(t, err.Error(), urlPrefix)
	}

	ctx := newContext()

	for _, testCase := range testCases {
		httpClient := newHTTPClient(
			ctx,
			time.Millisecond, // timeout
			time.Millisecond, // minRetryTimeout
			time.Millisecond, // maxRetryTimeout
			1,                // maxRetries
			testCase.url,
		)

		_, err := httpClient.Head(ctx)
		checkError(err, testCase.prefix, testCase.parameters)

		_, err = httpClient.Body(ctx, 0, 1, "")
		checkError(err, testCase.prefix, testCase.parameters)
	}
}

func TestHTTPClientLogger(t *testing.T) {
	urlPrefix := "https://foo.com/bar"
	urlParameters := "?aa=bb&cc=dd"
	url := urlPrefix + urlParameters

	logger := &loggerWithURLReplaced{
		ctx:                context.Background(),
		substringToReplace: urlParameters,
	}

	result := logger.replaceURLSubstring(
		"message",
		"key_1",
		"value_1",
		"url",
		url,
	)

	expected := fmt.Sprintf(
		"message, key_1: value_1, url: %v%v",
		urlPrefix,
		urlReplacement,
	)
	require.Equal(t, expected, result)
}

func TestHTTPClientTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(10 * time.Second)
		},
	))
	defer server.Close()

	ctx := newContext()

	httpClient := newHTTPClient(
		ctx,
		time.Millisecond, // timeout
		time.Millisecond, // minRetryTimeout
		time.Millisecond, // maxRetryTimeout
		1,                // maxRetries
		server.URL,
	)

	_, err := httpClient.Head(ctx)
	require.Error(t, err)
	require.ErrorContains(t, err, "Timeout exceeded")
}
