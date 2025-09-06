package client

import (
	"time"

	api "github.com/ydb-platform/nbs/cloud/filestore/public/api/grpc"
	protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

const (
	defaultRequestTimeout    = 30 * time.Second
	maxMsgSize               = 8 * 1024 * 1024
	requestTimeWarnThreshold = 10 * time.Second

	IdempotenceIDHeaderKey  = "x-nfs-idempotence-id"
	TimestampHeaderKey      = "x-nfs-timestamp"
	TraceIDHeaderKey        = "x-nfs-trace-id"
	RequestTimeoutHeaderKey = "x-nfs-request-timeout"
)

////////////////////////////////////////////////////////////////////////////////

type grpcClient struct {
	log      Log
	impl     api.TFileStoreServiceClient
	conn     *grpc.ClientConn
	timeout  time.Duration
	clientID string
}

func (client *grpcClient) setupHeaders(ctx context.Context, req request) {
	headers := req.GetHeaders()
	headers.ClientId = []byte(client.clientID)

	if val := ctx.Value(IdempotenceIDHeaderKey); val != nil {
		if idempotenceID, ok := val.([]byte); ok {
			headers.IdempotenceId = idempotenceID
		}
	}

	var timestamp int64
	if val := ctx.Value(TimestampHeaderKey); val != nil {
		if t, ok := val.(time.Time); ok {
			timestamp = timeToTimestamp(t)
		}
	}

	now := timeToTimestamp(time.Now())
	if timestamp <= 0 || timestamp > now || now-timestamp > 1*int64(time.Second) {
		// fix request timestamp
		timestamp = now
	}

	headers.Timestamp = uint64(timestamp)

	if val := ctx.Value(TraceIDHeaderKey); val != nil {
		if traceID, ok := val.([]byte); ok {
			headers.TraceId = traceID
		}
	}

	var timeout time.Duration
	if val := ctx.Value(RequestTimeoutHeaderKey); val != nil {
		if d, ok := val.(time.Duration); ok {
			timeout = d
		}
	}

	if timeout == 0 {
		timeout = client.timeout
	}

	headers.RequestTimeout = uint32(durationToMsec(timeout))
}

func (client *grpcClient) executeRequest(
	ctx context.Context,
	req request,
	call func(ctx context.Context) (response, error),
) (response, error) {

	requestID := nextRequestID()
	client.setupHeaders(ctx, req)

	if logger := client.log.Logger(LOG_DEBUG); logger != nil {
		logger.Printf(ctx, "%s #%d sending request", requestName(req), requestID)
	}

	ctx, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()

	started := time.Now()
	resp, err := call(ctx)
	requestTime := time.Since(started)

	if err == nil {
		perr := resp.GetError()
		if perr != nil && failed(perr.GetCode()) {
			err = &ClientError{perr.Code, perr.Message}
		}
	} else {
		err = NewClientError(err)
	}

	if requestTime < requestTimeWarnThreshold {
		if logger := client.log.Logger(LOG_DEBUG); logger != nil {
			logger.Printf(
				ctx,
				"%s #%d request completed (time: %v, size: %d, error: %v)",
				requestName(req),
				requestID,
				requestTime,
				requestSize(req),
				err)
		}
	} else {
		if logger := client.log.Logger(LOG_WARN); logger != nil {
			logger.Printf(
				ctx,
				"%s #%d request too slow (time: %v, size: %d, error: %v)",
				requestName(req),
				requestID,
				requestTime,
				requestSize(req),
				err)
		}
	}

	return resp, err
}

func (client *grpcClient) Close() error {
	return client.conn.Close()
}

func (client *grpcClient) Ping(
	ctx context.Context,
	req *protos.TPingRequest,
) (*protos.TPingResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.Ping(ctx, req)
		})

	return resp.(*protos.TPingResponse), err
}

func (client *grpcClient) CreateFileStore(
	ctx context.Context,
	req *protos.TCreateFileStoreRequest,
) (*protos.TCreateFileStoreResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CreateFileStore(ctx, req)
		})

	return resp.(*protos.TCreateFileStoreResponse), err
}

func (client *grpcClient) AlterFileStore(
	ctx context.Context,
	req *protos.TAlterFileStoreRequest,
) (*protos.TAlterFileStoreResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.AlterFileStore(ctx, req)
		})

	return resp.(*protos.TAlterFileStoreResponse), err
}

func (client *grpcClient) ResizeFileStore(
	ctx context.Context,
	req *protos.TResizeFileStoreRequest,
) (*protos.TResizeFileStoreResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ResizeFileStore(ctx, req)
		})

	return resp.(*protos.TResizeFileStoreResponse), err
}

func (client *grpcClient) DestroyFileStore(
	ctx context.Context,
	req *protos.TDestroyFileStoreRequest,
) (*protos.TDestroyFileStoreResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DestroyFileStore(ctx, req)
		})

	return resp.(*protos.TDestroyFileStoreResponse), err
}

func (client *grpcClient) GetFileStoreInfo(
	ctx context.Context,
	req *protos.TGetFileStoreInfoRequest,
) (*protos.TGetFileStoreInfoResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.GetFileStoreInfo(ctx, req)
		})

	return resp.(*protos.TGetFileStoreInfoResponse), err
}

func (client *grpcClient) CreateCheckpoint(
	ctx context.Context,
	req *protos.TCreateCheckpointRequest,
) (*protos.TCreateCheckpointResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CreateCheckpoint(ctx, req)
		})

	return resp.(*protos.TCreateCheckpointResponse), err
}

func (client *grpcClient) DestroyCheckpoint(
	ctx context.Context,
	req *protos.TDestroyCheckpointRequest,
) (*protos.TDestroyCheckpointResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DestroyCheckpoint(ctx, req)
		})

	return resp.(*protos.TDestroyCheckpointResponse), err
}

func (client *grpcClient) DescribeFileStoreModel(
	ctx context.Context,
	req *protos.TDescribeFileStoreModelRequest,
) (*protos.TDescribeFileStoreModelResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DescribeFileStoreModel(ctx, req)
		})

	return resp.(*protos.TDescribeFileStoreModelResponse), err
}

func (client *grpcClient) CreateSession(
	ctx context.Context,
	req *protos.TCreateSessionRequest,
) (*protos.TCreateSessionResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CreateSession(ctx, req)
		},
	)

	return resp.(*protos.TCreateSessionResponse), err
}

func (client *grpcClient) DestroySession(
	ctx context.Context,
	req *protos.TDestroySessionRequest,
) (*protos.TDestroySessionResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DestroySession(ctx, req)
		},
	)

	return resp.(*protos.TDestroySessionResponse), err
}

func (client *grpcClient) ListNodes(
	ctx context.Context,
	req *protos.TListNodesRequest,
) (*protos.TListNodesResponse, error) {

	// The headers MUST be not nill, since we need a session
	// which is passed in headers.
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ListNodes(ctx, req)
		},
	)

	return resp.(*protos.TListNodesResponse), err
}

func (client *grpcClient) CreateNode(
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

func (client *grpcClient) ReadLink(
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

type grpcEndpointClient struct {
	log      Log
	impl     api.TEndpointManagerServiceClient
	conn     *grpc.ClientConn
	timeout  time.Duration
	clientID string
}

func (client *grpcEndpointClient) setupHeaders(ctx context.Context, req request) {
	headers := req.GetHeaders()
	headers.ClientId = []byte(client.clientID)

	if val := ctx.Value(IdempotenceIDHeaderKey); val != nil {
		if idempotenceID, ok := val.([]byte); ok {
			headers.IdempotenceId = idempotenceID
		}
	}

	var timestamp int64
	if val := ctx.Value(TimestampHeaderKey); val != nil {
		if t, ok := val.(time.Time); ok {
			timestamp = timeToTimestamp(t)
		}
	}

	now := timeToTimestamp(time.Now())
	if timestamp <= 0 || timestamp > now || now-timestamp > 1*int64(time.Second) {
		// fix request timestamp
		timestamp = now
	}

	headers.Timestamp = uint64(timestamp)

	if val := ctx.Value(TraceIDHeaderKey); val != nil {
		if traceID, ok := val.([]byte); ok {
			headers.TraceId = traceID
		}
	}

	var timeout time.Duration
	if val := ctx.Value(RequestTimeoutHeaderKey); val != nil {
		if d, ok := val.(time.Duration); ok {
			timeout = d
		}
	}

	if timeout == 0 {
		timeout = client.timeout
	}

	headers.RequestTimeout = uint32(durationToMsec(timeout))
}

func (client *grpcEndpointClient) executeRequest(
	ctx context.Context,
	req request,
	call func(ctx context.Context) (response, error),
) (response, error) {

	requestID := nextRequestID()
	client.setupHeaders(ctx, req)

	if logger := client.log.Logger(LOG_DEBUG); logger != nil {
		logger.Printf(ctx, "%s #%d sending request", requestName(req), requestID)
	}

	ctx, cancel := context.WithTimeout(ctx, client.timeout)
	defer cancel()

	started := time.Now()
	resp, err := call(ctx)
	requestTime := time.Since(started)

	if err == nil {
		perr := resp.GetError()
		if perr != nil && failed(perr.GetCode()) {
			err = &ClientError{perr.Code, perr.Message}
		}
	} else {
		err = NewClientError(err)
	}

	if requestTime < requestTimeWarnThreshold {
		if logger := client.log.Logger(LOG_DEBUG); logger != nil {
			logger.Printf(
				ctx,
				"%s #%d request completed (time: %v, size: %d, error: %v)",
				requestName(req),
				requestID,
				requestTime,
				requestSize(req),
				err)
		}
	} else {
		if logger := client.log.Logger(LOG_WARN); logger != nil {
			logger.Printf(
				ctx,
				"%s #%d request too slow (time: %v, size: %d, error: %v)",
				requestName(req),
				requestID,
				requestTime,
				requestSize(req),
				err)
		}
	}

	return resp, err
}

func (client *grpcEndpointClient) Close() error {
	return client.conn.Close()
}

func (client *grpcEndpointClient) StartEndpoint(
	ctx context.Context,
	req *protos.TStartEndpointRequest,
) (*protos.TStartEndpointResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.StartEndpoint(ctx, req)
		})

	return resp.(*protos.TStartEndpointResponse), err
}

func (client *grpcEndpointClient) StopEndpoint(
	ctx context.Context,
	req *protos.TStopEndpointRequest,
) (*protos.TStopEndpointResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.StopEndpoint(ctx, req)
		})

	return resp.(*protos.TStopEndpointResponse), err
}

func (client *grpcEndpointClient) ListEndpoints(
	ctx context.Context,
	req *protos.TListEndpointsRequest,
) (*protos.TListEndpointsResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ListEndpoints(ctx, req)
		})

	return resp.(*protos.TListEndpointsResponse), err
}

func (client *grpcEndpointClient) KickEndpoint(
	ctx context.Context,
	req *protos.TKickEndpointRequest,
) (*protos.TKickEndpointResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.KickEndpoint(ctx, req)
		})

	return resp.(*protos.TKickEndpointResponse), err
}

func (client *grpcEndpointClient) Ping(
	ctx context.Context,
	req *protos.TPingRequest,
) (*protos.TPingResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.Ping(ctx, req)
		})

	return resp.(*protos.TPingResponse), err
}

////////////////////////////////////////////////////////////////////////////////

type GrpcClientOpts struct {
	Endpoint    string
	Credentials *ClientCredentials
	Timeout     *time.Duration
	DialContext func(
		ctx context.Context,
		target string,
		opts ...grpc.DialOption) (conn *grpc.ClientConn, err error)
}

func NewGrpcClient(opts *GrpcClientOpts, log Log) (ClientIface, error) {
	requestTimeout := defaultRequestTimeout
	if opts.Timeout != nil {
		requestTimeout = *opts.Timeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(maxMsgSize),
			grpc.MaxCallRecvMsgSize(maxMsgSize),
		),
	}

	if opts.Credentials != nil {
		credOpts, err := opts.Credentials.GetSslChannelCredentials()
		if err != nil {
			return nil, NewClientError(err)
		}

		dialOpts = append(dialOpts, credOpts...)
	} else {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}

	dialer := opts.DialContext
	if opts.DialContext == nil {
		dialer = grpc.DialContext
	}
	conn, err := dialer(ctx, opts.Endpoint, dialOpts...)
	if err != nil {
		return nil, NewClientError(err)
	}

	clientID, err := createUUID()
	if err != nil {
		return nil, NewClientError(err)
	}

	client := &grpcClient{
		log,
		api.NewTFileStoreServiceClient(conn),
		conn,
		requestTimeout,
		clientID,
	}

	return client, nil
}

func NewGrpcEndpointClient(opts *GrpcClientOpts, log Log) (EndpointClientIface, error) {
	requestTimeout := defaultRequestTimeout
	if opts.Timeout != nil {
		requestTimeout = *opts.Timeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallSendMsgSize(maxMsgSize),
			grpc.MaxCallRecvMsgSize(maxMsgSize),
		),
	}

	if opts.Credentials != nil {
		credOpts, err := opts.Credentials.GetSslChannelCredentials()
		if err != nil {
			return nil, NewClientError(err)
		}

		dialOpts = append(dialOpts, credOpts...)
	} else {
		dialOpts = append(dialOpts, grpc.WithInsecure())
	}

	dialer := opts.DialContext
	if opts.DialContext == nil {
		dialer = grpc.DialContext
	}
	conn, err := dialer(ctx, opts.Endpoint, dialOpts...)
	if err != nil {
		return nil, NewClientError(err)
	}

	clientID, err := createUUID()
	if err != nil {
		return nil, NewClientError(err)
	}

	client := &grpcEndpointClient{
		log,
		api.NewTEndpointManagerServiceClient(conn),
		conn,
		requestTimeout,
		clientID,
	}

	return client, nil
}
