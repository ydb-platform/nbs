package yc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/ydb-platform/ydb-go-sdk/v3/retry"
	
	metadataTrace "github.com/ydb-platform/ydb-go-yc-metadata/trace"
)

type metadataIAMResponse struct {
	Token     string
	ExpiresIn time.Duration
}

func (m *InstanceServiceAccountCredentials) metaCall(ctx context.Context, metadataURL string, retryNotFound bool) (res *metadataIAMResponse, err error) {
	onDone := metadataTrace.TraceOnRefreshToken(m.trace, &ctx)
	defer func() {
		if err != nil {
			onDone("", 0, err)
		} else {
			onDone(res.Token, res.ExpiresIn, nil)
		}
	}()

	defer func() {
		if e := recover(); e != nil {
			// Don't lose err
			if err == nil {
				err = &createTokenError{
					Cause:  fmt.Errorf("panic: %#v", e),
					Reason: "panic in metaCall",
				}
			}
		}
	}()

	var resp *http.Response
	resp, err = metaClient.Get(metadataURL)
	if err != nil {
		return nil, &createTokenError{
			Cause:  err,
			Reason: "failed to create HTTP request",
		}
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	switch resp.StatusCode {
	case http.StatusOK:
		// nop, will read outside switch
	case http.StatusNotFound:
		err = &createTokenError{
			Cause: fmt.Errorf("%s: possibly missing service_account_id in instance spec",
				resp.Status,
			),
			Reason: "possibly missing service_account_id in instance spec",
		}
		if retryNotFound {
			return nil, retry.RetryableError(err, retry.WithBackoff(retry.TypeFastBackoff))
		}
		return nil, err
	default:
		return nil, fmt.Errorf("%s", resp.Status)
	}
	var body []byte
	body, err = io.ReadAll(resp.Body)
	if err != nil {
		return nil, &createTokenError{
			Cause:  err,
			Reason: "response body read failed",
		}
	}

	var tokenResponse struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int64  `json:"expires_in"` // seconds
	}

	err = json.Unmarshal(body, &tokenResponse)
	if err != nil {
		return nil, &createTokenError{
			Cause:  err,
			Reason: "failed to unmarshal response body",
		}
	}
	return &metadataIAMResponse{
		Token:     tokenResponse.AccessToken,
		ExpiresIn: time.Duration(tokenResponse.ExpiresIn) * time.Second,
	}, nil
}

var metaClient = &http.Client{
	Transport: &rTripper{
		inner: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   time.Second, // One second should be enough for localhost connection.
				KeepAlive: -1,          // No keep alive. Near token per hour requested.
			}).DialContext,
		},
	},
	Timeout: 10 * time.Second,
}

type rTripper struct {
	inner *http.Transport
}

func (r *rTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	request.Header.Set("Metadata-Flavor", "Google") // from YC go-sdk
	return r.inner.RoundTrip(request)
}
