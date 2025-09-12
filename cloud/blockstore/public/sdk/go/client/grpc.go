package client

import (
	"time"

	api "github.com/ydb-platform/nbs/cloud/blockstore/public/api/grpc"
	protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding/gzip"
)

////////////////////////////////////////////////////////////////////////////////

const (
	defaultRequestTimeout    = 30 * time.Second
	maxMsgSize               = 8 * 1024 * 1024
	requestTimeWarnThreshold = 10 * time.Second

	IdempotenceIdHeaderKey  = "x-nbs-idempotence-id"
	TimestampHeaderKey      = "x-nbs-timestamp"
	TraceIdHeaderKey        = "x-nbs-trace-id"
	RequestTimeoutHeaderKey = "x-nbs-request-timeout"
	ClientIdHeaderKey       = "x-nbs-client-id"
)

////////////////////////////////////////////////////////////////////////////////

func WithClientID(ctx context.Context, clientId string) context.Context {
	return context.WithValue(ctx, ClientIdHeaderKey, clientId)
}

////////////////////////////////////////////////////////////////////////////////

type grpcClient struct {
	log      Log
	impl     api.TBlockStoreServiceClient
	conn     *grpc.ClientConn
	timeout  time.Duration
	clientId string
}

func (client *grpcClient) setupHeaders(ctx context.Context, req request) {
	headers := req.GetHeaders()

	if val := ctx.Value(IdempotenceIdHeaderKey); val != nil {
		if str, ok := val.(string); ok {
			headers.IdempotenceId = str
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

	if val := ctx.Value(TraceIdHeaderKey); val != nil {
		if str, ok := val.(string); ok {
			headers.TraceId = str
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

	clientId := ""
	if val := ctx.Value(ClientIdHeaderKey); val != nil {
		if str, ok := val.(string); ok {
			clientId = str
		}
	}

	if len(clientId) == 0 {
		clientId = client.clientId
	}

	headers.ClientId = clientId
}

func (client *grpcClient) executeRequest(
	ctx context.Context,
	req request,
	call func(ctx context.Context) (response, error),
) (response, error) {

	requestId := nextRequestId()
	client.setupHeaders(ctx, req)

	if logger := client.log.Logger(requestLogLevel(req)); logger != nil {
		logger.Printf(ctx, "%s #%d sending request", requestName(req), requestId)
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
		if logger := client.log.Logger(requestLogLevel(req)); logger != nil {
			logger.Printf(
				ctx,
				"%s%s #%d request completed (time: %v, size: %d, error: %v)",
				requestName(req),
				requestDetails(req),
				requestId,
				requestTime,
				requestSize(req),
				err,
			)
		}
	} else {
		if logger := client.log.Logger(LOG_WARN); logger != nil {
			logger.Printf(
				ctx,
				"%s%s #%d request too slow (time: %v, size: %d, error: %v)",
				requestName(req),
				requestDetails(req),
				requestId,
				requestTime,
				requestSize(req),
				err,
			)
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

func (client *grpcClient) CreateVolume(
	ctx context.Context,
	req *protos.TCreateVolumeRequest,
) (*protos.TCreateVolumeResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CreateVolume(ctx, req)
		})

	return resp.(*protos.TCreateVolumeResponse), err
}

func (client *grpcClient) DestroyVolume(
	ctx context.Context,
	req *protos.TDestroyVolumeRequest,
) (*protos.TDestroyVolumeResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DestroyVolume(ctx, req)
		})

	return resp.(*protos.TDestroyVolumeResponse), err
}

func (client *grpcClient) ResizeVolume(
	ctx context.Context,
	req *protos.TResizeVolumeRequest,
) (*protos.TResizeVolumeResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ResizeVolume(ctx, req)
		})

	return resp.(*protos.TResizeVolumeResponse), err
}

func (client *grpcClient) AlterVolume(
	ctx context.Context,
	req *protos.TAlterVolumeRequest,
) (*protos.TAlterVolumeResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.AlterVolume(ctx, req)
		})

	return resp.(*protos.TAlterVolumeResponse), err
}

func (client *grpcClient) AssignVolume(
	ctx context.Context,
	req *protos.TAssignVolumeRequest,
) (*protos.TAssignVolumeResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.AssignVolume(ctx, req)
		})

	return resp.(*protos.TAssignVolumeResponse), err
}

func (client *grpcClient) StatVolume(
	ctx context.Context,
	req *protos.TStatVolumeRequest,
) (*protos.TStatVolumeResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.StatVolume(ctx, req)
		})

	return resp.(*protos.TStatVolumeResponse), err
}

func (client *grpcClient) QueryAvailableStorage(
	ctx context.Context,
	req *protos.TQueryAvailableStorageRequest,
) (*protos.TQueryAvailableStorageResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.QueryAvailableStorage(ctx, req)
		})

	return resp.(*protos.TQueryAvailableStorageResponse), err
}

func (client *grpcClient) ResumeDevice(
	ctx context.Context,
	req *protos.TResumeDeviceRequest,
) (*protos.TResumeDeviceResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ResumeDevice(ctx, req)
		})

	return resp.(*protos.TResumeDeviceResponse), err
}

func (client *grpcClient) CreateVolumeFromDevice(
	ctx context.Context,
	req *protos.TCreateVolumeFromDeviceRequest,
) (*protos.TCreateVolumeFromDeviceResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CreateVolumeFromDevice(ctx, req)
		})

	return resp.(*protos.TCreateVolumeFromDeviceResponse), err
}

func (client *grpcClient) MountVolume(
	ctx context.Context,
	req *protos.TMountVolumeRequest,
) (*protos.TMountVolumeResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.MountVolume(ctx, req)
		})

	return resp.(*protos.TMountVolumeResponse), err
}

func (client *grpcClient) UnmountVolume(
	ctx context.Context,
	req *protos.TUnmountVolumeRequest,
) (*protos.TUnmountVolumeResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.UnmountVolume(ctx, req)
		})

	return resp.(*protos.TUnmountVolumeResponse), err
}

func (client *grpcClient) ReadBlocks(
	ctx context.Context,
	req *protos.TReadBlocksRequest,
) (*protos.TReadBlocksResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ReadBlocks(ctx, req)
		})

	return resp.(*protos.TReadBlocksResponse), err
}

func (client *grpcClient) WriteBlocks(
	ctx context.Context,
	req *protos.TWriteBlocksRequest,
) (*protos.TWriteBlocksResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.WriteBlocks(ctx, req)
		})

	return resp.(*protos.TWriteBlocksResponse), err
}

func (client *grpcClient) ZeroBlocks(
	ctx context.Context,
	req *protos.TZeroBlocksRequest,
) (*protos.TZeroBlocksResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ZeroBlocks(ctx, req)
		})

	return resp.(*protos.TZeroBlocksResponse), err
}

func (client *grpcClient) StartEndpoint(
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

func (client *grpcClient) StopEndpoint(
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

func (client *grpcClient) ListEndpoints(
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

func (client *grpcClient) KickEndpoint(
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

func (client *grpcClient) ListKeyrings(
	ctx context.Context,
	req *protos.TListKeyringsRequest,
) (*protos.TListKeyringsResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ListKeyrings(ctx, req)
		})

	return resp.(*protos.TListKeyringsResponse), err
}

func (client *grpcClient) DescribeEndpoint(
	ctx context.Context,
	req *protos.TDescribeEndpointRequest,
) (*protos.TDescribeEndpointResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DescribeEndpoint(ctx, req)
		})

	return resp.(*protos.TDescribeEndpointResponse), err
}

func (client *grpcClient) RefreshEndpoint(
	ctx context.Context,
	req *protos.TRefreshEndpointRequest,
) (*protos.TRefreshEndpointResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.RefreshEndpoint(ctx, req)
		})

	return resp.(*protos.TRefreshEndpointResponse), err
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

func (client *grpcClient) GetCheckpointStatus(
	ctx context.Context,
	req *protos.TGetCheckpointStatusRequest,
) (*protos.TGetCheckpointStatusResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.GetCheckpointStatus(ctx, req)
		})

	return resp.(*protos.TGetCheckpointStatusResponse), err
}

func (client *grpcClient) DeleteCheckpoint(
	ctx context.Context,
	req *protos.TDeleteCheckpointRequest,
) (*protos.TDeleteCheckpointResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DeleteCheckpoint(ctx, req)
		})

	return resp.(*protos.TDeleteCheckpointResponse), err
}

func (client *grpcClient) GetChangedBlocks(
	ctx context.Context,
	req *protos.TGetChangedBlocksRequest,
) (*protos.TGetChangedBlocksResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.GetChangedBlocks(ctx, req)
		})

	return resp.(*protos.TGetChangedBlocksResponse), err
}

func (client *grpcClient) DescribeVolume(
	ctx context.Context,
	req *protos.TDescribeVolumeRequest,
) (*protos.TDescribeVolumeResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DescribeVolume(ctx, req)
		})

	return resp.(*protos.TDescribeVolumeResponse), err
}

func (client *grpcClient) DescribeVolumeModel(
	ctx context.Context,
	req *protos.TDescribeVolumeModelRequest,
) (*protos.TDescribeVolumeModelResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DescribeVolumeModel(ctx, req)
		})

	return resp.(*protos.TDescribeVolumeModelResponse), err
}

func (client *grpcClient) ListVolumes(
	ctx context.Context,
	req *protos.TListVolumesRequest,
) (*protos.TListVolumesResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ListVolumes(ctx, req)
		})

	return resp.(*protos.TListVolumesResponse), err
}

func (client *grpcClient) ListDiskStates(
	ctx context.Context,
	req *protos.TListDiskStatesRequest,
) (*protos.TListDiskStatesResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ListDiskStates(ctx, req)
		})

	return resp.(*protos.TListDiskStatesResponse), err
}

func (client *grpcClient) DiscoverInstances(
	ctx context.Context,
	req *protos.TDiscoverInstancesRequest,
) (*protos.TDiscoverInstancesResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DiscoverInstances(ctx, req)
		})

	return resp.(*protos.TDiscoverInstancesResponse), err
}

func (client *grpcClient) ExecuteAction(
	ctx context.Context,
	req *protos.TExecuteActionRequest,
) (*protos.TExecuteActionResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ExecuteAction(ctx, req)
		})

	return resp.(*protos.TExecuteActionResponse), err
}

func (client *grpcClient) CreatePlacementGroup(
	ctx context.Context,
	req *protos.TCreatePlacementGroupRequest,
) (*protos.TCreatePlacementGroupResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CreatePlacementGroup(ctx, req)
		})

	return resp.(*protos.TCreatePlacementGroupResponse), err
}

func (client *grpcClient) DestroyPlacementGroup(
	ctx context.Context,
	req *protos.TDestroyPlacementGroupRequest,
) (*protos.TDestroyPlacementGroupResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DestroyPlacementGroup(ctx, req)
		})

	return resp.(*protos.TDestroyPlacementGroupResponse), err
}

func (client *grpcClient) DescribePlacementGroup(
	ctx context.Context,
	req *protos.TDescribePlacementGroupRequest,
) (*protos.TDescribePlacementGroupResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DescribePlacementGroup(ctx, req)
		})

	return resp.(*protos.TDescribePlacementGroupResponse), err
}

func (client *grpcClient) AlterPlacementGroupMembership(
	ctx context.Context,
	req *protos.TAlterPlacementGroupMembershipRequest,
) (*protos.TAlterPlacementGroupMembershipResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.AlterPlacementGroupMembership(ctx, req)
		})

	return resp.(*protos.TAlterPlacementGroupMembershipResponse), err
}

func (client *grpcClient) ListPlacementGroups(
	ctx context.Context,
	req *protos.TListPlacementGroupsRequest,
) (*protos.TListPlacementGroupsResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ListPlacementGroups(ctx, req)
		})

	return resp.(*protos.TListPlacementGroupsResponse), err
}

func (client *grpcClient) CmsAction(
	ctx context.Context,
	req *protos.TCmsActionRequest,
) (*protos.TCmsActionResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}
	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CmsAction(ctx, req)
		})

	return resp.(*protos.TCmsActionResponse), err
}

func (client *grpcClient) QueryAgentsInfo(
	ctx context.Context,
	req *protos.TQueryAgentsInfoRequest,
) (*protos.TQueryAgentsInfoResponse, error) {

	if req.Headers == nil {
		req.Headers = &protos.THeaders{}
	}

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.QueryAgentsInfo(ctx, req)
		})

	return resp.(*protos.TQueryAgentsInfoResponse), err
}

////////////////////////////////////////////////////////////////////////////////

type GrpcClientOpts struct {
	Endpoint    string
	Credentials *ClientCredentials
	Timeout     *time.Duration
	DialOptions []grpc.DialOption
	DialContext func(
		ctx context.Context,
		target string,
		opts ...grpc.DialOption) (conn *grpc.ClientConn, err error)
	ClientId           string
	UseGZIPCompression bool
}

func NewGrpcClient(opts *GrpcClientOpts, log Log) (ClientIface, error) {
	requestTimeout := defaultRequestTimeout
	if opts.Timeout != nil {
		requestTimeout = *opts.Timeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), requestTimeout)
	defer cancel()

	callOpts := []grpc.CallOption{
		grpc.MaxCallSendMsgSize(maxMsgSize),
		grpc.MaxCallRecvMsgSize(maxMsgSize),
	}
	if opts.UseGZIPCompression {
		callOpts = append(callOpts, grpc.UseCompressor(gzip.Name))
	}

	dialOpts := []grpc.DialOption{
		grpc.WithDefaultCallOptions(callOpts...),
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

	dialOpts = append(dialOpts, opts.DialOptions...)

	dialer := opts.DialContext
	if opts.DialContext == nil {
		dialer = grpc.DialContext
	}
	conn, err := dialer(ctx, opts.Endpoint, dialOpts...)
	if err != nil {
		return nil, NewClientError(err)
	}

	clientId := opts.ClientId
	if len(clientId) == 0 {
		clientId, err = createUuid()

		if err != nil {
			return nil, NewClientError(err)
		}
	}

	client := &grpcClient{
		log,
		api.NewTBlockStoreServiceClient(conn),
		conn,
		requestTimeout,
		clientId,
	}

	return client, nil
}
