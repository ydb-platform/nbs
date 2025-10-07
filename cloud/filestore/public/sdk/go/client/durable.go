package client

import (
	"time"

	protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

const (
	defaultRetryTimeout          = 5 * time.Minute
	defaultRetryTimeoutIncrement = 500 * time.Millisecond
)

////////////////////////////////////////////////////////////////////////////////

type durableClient struct {
	impl             ClientIface
	timeout          time.Duration
	timeoutIncrement time.Duration
	onError          func(ClientError)
	log              Log
}

func (client *durableClient) executeRequest(
	ctx context.Context,
	req request,
	call func(ctx context.Context) (response, error),
) (response, error) {

	ctx, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()

	retryCount := 1
	started := time.Now()
	for delay := client.timeoutIncrement; ; delay += client.timeoutIncrement {
		resp, err := call(ctx)
		if err == nil && retryCount > 1 {
			if logger := client.log.Logger(LOG_INFO); logger != nil {
				duration := time.Since(started)
				logger.Printf(
					ctx,
					"%s%s completed (retries: %d, duration: %v)",
					requestName(req),
					requestDetails(req),
					retryCount,
					duration,
				)
			}
		}

		cerr := GetClientError(err)
		if cerr.Succeeded() {
			return resp, nil
		}

		client.onError(cerr)

		if !cerr.IsRetriable() {
			if logger := client.log.Logger(LOG_ERROR); logger != nil {
				logger.Printf(
					ctx,
					"%s%s request failed: %v",
					requestName(req),
					requestDetails(req),
					err,
				)
			}
			return resp, err
		}

		if logger := client.log.Logger(LOG_WARN); logger != nil {
			logger.Printf(
				ctx,
				"%s%s retry request (retries: %d, timeout: %v, error: %v)",
				requestName(req),
				requestDetails(req),
				retryCount,
				client.timeout,
				err,
			)
		}

		retryCount++
		select {
		case <-ctx.Done():
			return resp, err
		case <-time.After(delay):
		}
	}
}

func (client *durableClient) Close() error {
	return client.impl.Close()
}

func (client *durableClient) Ping(
	ctx context.Context,
	req *protos.TPingRequest,
) (*protos.TPingResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.Ping(ctx, req)
		},
	)

	return resp.(*protos.TPingResponse), err
}

func (client *durableClient) CreateFileStore(
	ctx context.Context,
	req *protos.TCreateFileStoreRequest,
) (*protos.TCreateFileStoreResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CreateFileStore(ctx, req)
		},
	)

	return resp.(*protos.TCreateFileStoreResponse), err
}

func (client *durableClient) AlterFileStore(
	ctx context.Context,
	req *protos.TAlterFileStoreRequest,
) (*protos.TAlterFileStoreResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.AlterFileStore(ctx, req)
		},
	)

	return resp.(*protos.TAlterFileStoreResponse), err
}

func (client *durableClient) ResizeFileStore(
	ctx context.Context,
	req *protos.TResizeFileStoreRequest,
) (*protos.TResizeFileStoreResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ResizeFileStore(ctx, req)
		},
	)

	return resp.(*protos.TResizeFileStoreResponse), err
}

func (client *durableClient) DestroyFileStore(
	ctx context.Context,
	req *protos.TDestroyFileStoreRequest,
) (*protos.TDestroyFileStoreResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DestroyFileStore(ctx, req)
		},
	)

	return resp.(*protos.TDestroyFileStoreResponse), err
}

func (client *durableClient) GetFileStoreInfo(
	ctx context.Context,
	req *protos.TGetFileStoreInfoRequest,
) (*protos.TGetFileStoreInfoResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.GetFileStoreInfo(ctx, req)
		},
	)

	return resp.(*protos.TGetFileStoreInfoResponse), err
}

func (client *durableClient) CreateCheckpoint(
	ctx context.Context,
	req *protos.TCreateCheckpointRequest,
) (*protos.TCreateCheckpointResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CreateCheckpoint(ctx, req)
		},
	)

	return resp.(*protos.TCreateCheckpointResponse), err
}

func (client *durableClient) DestroyCheckpoint(
	ctx context.Context,
	req *protos.TDestroyCheckpointRequest,
) (*protos.TDestroyCheckpointResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DestroyCheckpoint(ctx, req)
		},
	)

	return resp.(*protos.TDestroyCheckpointResponse), err
}

func (client *durableClient) DescribeFileStoreModel(
	ctx context.Context,
	req *protos.TDescribeFileStoreModelRequest,
) (*protos.TDescribeFileStoreModelResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DescribeFileStoreModel(ctx, req)
		},
	)

	return resp.(*protos.TDescribeFileStoreModelResponse), err
}

func (client *durableClient) CreateSession(
	ctx context.Context,
	req *protos.TCreateSessionRequest,
) (*protos.TCreateSessionResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CreateSession(ctx, req)
		},
	)

	return resp.(*protos.TCreateSessionResponse), err
}

func (client *durableClient) DestroySession(
	ctx context.Context,
	req *protos.TDestroySessionRequest,
) (*protos.TDestroySessionResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DestroySession(ctx, req)
		},
	)

	return resp.(*protos.TDestroySessionResponse), err
}

func (client *durableClient) ListNodes(
	ctx context.Context,
	req *protos.TListNodesRequest,
) (*protos.TListNodesResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ListNodes(ctx, req)
		},
	)

	return resp.(*protos.TListNodesResponse), err
}

func (client *durableClient) CreateNode(
	ctx context.Context,
	req *protos.TCreateNodeRequest,
) (*protos.TCreateNodeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CreateNode(ctx, req)
		},
	)

	return resp.(*protos.TCreateNodeResponse), err
}

func (client *durableClient) ReadLink(
	ctx context.Context,
	req *protos.TReadLinkRequest,
) (*protos.TReadLinkResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ReadLink(ctx, req)
		},
	)

	return resp.(*protos.TReadLinkResponse), err
}

////////////////////////////////////////////////////////////////////////////////

type durableEndpointClient struct {
	impl             EndpointClientIface
	timeout          time.Duration
	timeoutIncrement time.Duration
	onError          func(ClientError)
	log              Log
}

func (client *durableEndpointClient) executeRequest(
	ctx context.Context,
	req request,
	call func(ctx context.Context) (response, error),
) (response, error) {

	ctx, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()

	retryCount := 1
	started := time.Now()
	for delay := client.timeoutIncrement; ; delay += client.timeoutIncrement {
		resp, err := call(ctx)
		if err == nil && retryCount > 1 {
			if logger := client.log.Logger(LOG_INFO); logger != nil {
				duration := time.Since(started)
				logger.Printf(
					ctx,
					"%s%s completed (retries: %d, duration: %v)",
					requestName(req),
					requestDetails(req),
					retryCount,
					duration,
				)
			}
		}

		cerr := GetClientError(err)
		if cerr.Succeeded() {
			return resp, nil
		}

		client.onError(cerr)

		if !cerr.IsRetriable() {
			if logger := client.log.Logger(LOG_ERROR); logger != nil {
				logger.Printf(
					ctx,
					"%s%s request failed: %v",
					requestName(req),
					requestDetails(req),
					err,
				)
			}
			return resp, err
		}

		if logger := client.log.Logger(LOG_WARN); logger != nil {
			logger.Printf(
				ctx,
				"%s%s retry request (retries: %d, timeout: %v, error: %v)",
				requestName(req),
				requestDetails(req),
				retryCount,
				client.timeout,
				err,
			)
		}

		retryCount++
		select {
		case <-ctx.Done():
			return resp, err
		case <-time.After(delay):
		}
	}
}

func (client *durableEndpointClient) Close() error {
	return client.impl.Close()
}

func (client *durableEndpointClient) StartEndpoint(
	ctx context.Context,
	req *protos.TStartEndpointRequest,
) (*protos.TStartEndpointResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.StartEndpoint(ctx, req)
		},
	)

	return resp.(*protos.TStartEndpointResponse), err
}

func (client *durableEndpointClient) StopEndpoint(
	ctx context.Context,
	req *protos.TStopEndpointRequest,
) (*protos.TStopEndpointResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.StopEndpoint(ctx, req)
		},
	)

	return resp.(*protos.TStopEndpointResponse), err
}

func (client *durableEndpointClient) ListEndpoints(
	ctx context.Context,
	req *protos.TListEndpointsRequest,
) (*protos.TListEndpointsResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ListEndpoints(ctx, req)
		},
	)

	return resp.(*protos.TListEndpointsResponse), err
}

func (client *durableEndpointClient) KickEndpoint(
	ctx context.Context,
	req *protos.TKickEndpointRequest,
) (*protos.TKickEndpointResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.KickEndpoint(ctx, req)
		},
	)

	return resp.(*protos.TKickEndpointResponse), err
}

func (client *durableEndpointClient) Ping(
	ctx context.Context,
	req *protos.TPingRequest,
) (*protos.TPingResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.Ping(ctx, req)
		},
	)

	return resp.(*protos.TPingResponse), err
}

////////////////////////////////////////////////////////////////////////////////

type DurableClientOpts struct {
	Timeout          *time.Duration
	TimeoutIncrement *time.Duration
	OnError          func(ClientError)
}

func NewDurableClient(
	impl ClientIface,
	opts *DurableClientOpts,
	log Log,
) ClientIface {

	retryTimeout := defaultRetryTimeout
	if opts.Timeout != nil {
		retryTimeout = *opts.Timeout
	}

	retryTimeoutIncrement := defaultRetryTimeoutIncrement
	if opts.TimeoutIncrement != nil {
		retryTimeoutIncrement = *opts.TimeoutIncrement
	}

	onError := func(ClientError) {}
	if opts.OnError != nil {
		onError = opts.OnError
	}

	return &durableClient{
		impl,
		retryTimeout,
		retryTimeoutIncrement,
		onError,
		log,
	}
}

func NewDurableEndpointClient(
	impl EndpointClientIface,
	opts *DurableClientOpts,
	log Log,
) EndpointClientIface {

	retryTimeout := defaultRetryTimeout
	if opts.Timeout != nil {
		retryTimeout = *opts.Timeout
	}

	retryTimeoutIncrement := defaultRetryTimeoutIncrement
	if opts.TimeoutIncrement != nil {
		retryTimeoutIncrement = *opts.TimeoutIncrement
	}

	onError := func(ClientError) {}
	if opts.OnError != nil {
		onError = opts.OnError
	}

	return &durableEndpointClient{
		impl,
		retryTimeout,
		retryTimeoutIncrement,
		onError,
		log,
	}
}
