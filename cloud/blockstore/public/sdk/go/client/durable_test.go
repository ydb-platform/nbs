package client

import (
	"testing"
	"time"

	"golang.org/x/net/context"

	protos "a.yandex-team.ru/cloud/blockstore/public/api/protos"
)

////////////////////////////////////////////////////////////////////////////////

func TestRetryUndeliveredRequests(t *testing.T) {
	maxRequestsCount := 3
	requestCount := 1

	client := &testClient{
		PingHandler: func(
			ctx context.Context,
			req *protos.TPingRequest,
		) (*protos.TPingResponse, error) {
			if requestCount < maxRequestsCount {
				requestCount++
				return nil, &ClientError{
					Code: E_REJECTED,
				}
			}

			return &protos.TPingResponse{}, nil
		},
	}

	timeout := 1 * time.Second
	timeoutIncrement := 0 * time.Second

	opts := &DurableClientOpts{
		Timeout:          &timeout,
		TimeoutIncrement: &timeoutIncrement,
	}

	log := NewStderrLog(LOG_DEBUG)
	durable := NewDurableClient(client, opts, log)

	_, err := durable.Ping(context.TODO(), &protos.TPingRequest{})
	if err != nil {
		t.Error(err)
	}

	if requestCount != maxRequestsCount {
		t.Errorf(
			"Error: rejected requests count mismatch: expected %d, got %d",
			maxRequestsCount,
			requestCount)
	}
}

func TestNoRetryUnretriableRequests(t *testing.T) {
	requestsCount := 0

	client := &testClient{
		PingHandler: func(
			ctx context.Context,
			req *protos.TPingRequest,
		) (*protos.TPingResponse, error) {
			requestsCount++
			return nil, &ClientError{
				Code: E_FAIL,
			}
		},
	}

	log := NewStderrLog(LOG_DEBUG)
	durable := NewDurableClient(client, &DurableClientOpts{}, log)

	_, err := durable.Ping(context.TODO(), &protos.TPingRequest{})
	if err == nil {
		t.Fatal("Unexpectedly missing error")
	}

	if requestsCount != 1 {
		t.Fatalf("Unexpected requests count: %d. Expected 1", requestsCount)
	}
}
