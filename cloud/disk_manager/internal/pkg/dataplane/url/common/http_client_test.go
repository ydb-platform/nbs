package common

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
)

////////////////////////////////////////////////////////////////////////////////

func TestURLReplacementInHTTPClientErrors(t *testing.T) {
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

	ctx := context.Background()
	logger := logging.NewStderrLogger(logging.DebugLevel)
	ctx = logging.SetLogger(ctx, logger)

	for _, testCase := range testCases {
		httpClient := newHTTPClient(
			ctx,
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

func TestLogger(t *testing.T) {
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
