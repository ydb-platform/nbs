package client

import (
	"time"

	protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
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
	onError          func(context.Context, ClientError)
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

		client.onError(ctx, cerr)

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

func (client *durableClient) CreateVolume(
	ctx context.Context,
	req *protos.TCreateVolumeRequest,
) (*protos.TCreateVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CreateVolume(ctx, req)
		},
	)

	return resp.(*protos.TCreateVolumeResponse), err
}

func (client *durableClient) DestroyVolume(
	ctx context.Context,
	req *protos.TDestroyVolumeRequest,
) (*protos.TDestroyVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DestroyVolume(ctx, req)
		},
	)

	return resp.(*protos.TDestroyVolumeResponse), err
}

func (client *durableClient) ResizeVolume(
	ctx context.Context,
	req *protos.TResizeVolumeRequest,
) (*protos.TResizeVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ResizeVolume(ctx, req)
		},
	)

	return resp.(*protos.TResizeVolumeResponse), err
}

func (client *durableClient) AlterVolume(
	ctx context.Context,
	req *protos.TAlterVolumeRequest,
) (*protos.TAlterVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.AlterVolume(ctx, req)
		},
	)

	return resp.(*protos.TAlterVolumeResponse), err
}

func (client *durableClient) AssignVolume(
	ctx context.Context,
	req *protos.TAssignVolumeRequest,
) (*protos.TAssignVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.AssignVolume(ctx, req)
		},
	)

	return resp.(*protos.TAssignVolumeResponse), err
}

func (client *durableClient) StatVolume(
	ctx context.Context,
	req *protos.TStatVolumeRequest,
) (*protos.TStatVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.StatVolume(ctx, req)
		},
	)

	return resp.(*protos.TStatVolumeResponse), err
}

func (client *durableClient) QueryAvailableStorage(
	ctx context.Context,
	req *protos.TQueryAvailableStorageRequest,
) (*protos.TQueryAvailableStorageResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.QueryAvailableStorage(ctx, req)
		},
	)

	return resp.(*protos.TQueryAvailableStorageResponse), err
}

func (client *durableClient) ResumeDevice(
	ctx context.Context,
	req *protos.TResumeDeviceRequest,
) (*protos.TResumeDeviceResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ResumeDevice(ctx, req)
		},
	)

	return resp.(*protos.TResumeDeviceResponse), err
}

func (client *durableClient) CreateVolumeFromDevice(
	ctx context.Context,
	req *protos.TCreateVolumeFromDeviceRequest,
) (*protos.TCreateVolumeFromDeviceResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CreateVolumeFromDevice(ctx, req)
		},
	)

	return resp.(*protos.TCreateVolumeFromDeviceResponse), err
}

func (client *durableClient) MountVolume(
	ctx context.Context,
	req *protos.TMountVolumeRequest,
) (*protos.TMountVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.MountVolume(ctx, req)
		},
	)

	return resp.(*protos.TMountVolumeResponse), err
}

func (client *durableClient) UnmountVolume(
	ctx context.Context,
	req *protos.TUnmountVolumeRequest,
) (*protos.TUnmountVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.UnmountVolume(ctx, req)
		},
	)

	return resp.(*protos.TUnmountVolumeResponse), err
}

func (client *durableClient) ReadBlocks(
	ctx context.Context,
	req *protos.TReadBlocksRequest,
) (*protos.TReadBlocksResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ReadBlocks(ctx, req)
		},
	)

	return resp.(*protos.TReadBlocksResponse), err
}

func (client *durableClient) WriteBlocks(
	ctx context.Context,
	req *protos.TWriteBlocksRequest,
) (*protos.TWriteBlocksResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.WriteBlocks(ctx, req)
		},
	)

	return resp.(*protos.TWriteBlocksResponse), err
}

func (client *durableClient) ZeroBlocks(
	ctx context.Context,
	req *protos.TZeroBlocksRequest,
) (*protos.TZeroBlocksResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ZeroBlocks(ctx, req)
		},
	)

	return resp.(*protos.TZeroBlocksResponse), err
}

func (client *durableClient) StartEndpoint(
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

func (client *durableClient) StopEndpoint(
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

func (client *durableClient) ListEndpoints(
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

func (client *durableClient) KickEndpoint(
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

func (client *durableClient) ListKeyrings(
	ctx context.Context,
	req *protos.TListKeyringsRequest,
) (*protos.TListKeyringsResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ListKeyrings(ctx, req)
		},
	)

	return resp.(*protos.TListKeyringsResponse), err
}

func (client *durableClient) DescribeEndpoint(
	ctx context.Context,
	req *protos.TDescribeEndpointRequest,
) (*protos.TDescribeEndpointResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DescribeEndpoint(ctx, req)
		},
	)

	return resp.(*protos.TDescribeEndpointResponse), err
}

func (client *durableClient) RefreshEndpoint(
	ctx context.Context,
	req *protos.TRefreshEndpointRequest,
) (*protos.TRefreshEndpointResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.RefreshEndpoint(ctx, req)
		},
	)

	return resp.(*protos.TRefreshEndpointResponse), err
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

func (client *durableClient) GetCheckpointStatus(
	ctx context.Context,
	req *protos.TGetCheckpointStatusRequest,
) (*protos.TGetCheckpointStatusResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.GetCheckpointStatus(ctx, req)
		},
	)

	return resp.(*protos.TGetCheckpointStatusResponse), err
}

func (client *durableClient) DeleteCheckpoint(
	ctx context.Context,
	req *protos.TDeleteCheckpointRequest,
) (*protos.TDeleteCheckpointResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DeleteCheckpoint(ctx, req)
		},
	)

	return resp.(*protos.TDeleteCheckpointResponse), err
}

func (client *durableClient) GetChangedBlocks(
	ctx context.Context,
	req *protos.TGetChangedBlocksRequest,
) (*protos.TGetChangedBlocksResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.GetChangedBlocks(ctx, req)
		},
	)

	return resp.(*protos.TGetChangedBlocksResponse), err
}

func (client *durableClient) DescribeVolume(
	ctx context.Context,
	req *protos.TDescribeVolumeRequest,
) (*protos.TDescribeVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DescribeVolume(ctx, req)
		},
	)

	return resp.(*protos.TDescribeVolumeResponse), err
}

func (client *durableClient) DescribeVolumeModel(
	ctx context.Context,
	req *protos.TDescribeVolumeModelRequest,
) (*protos.TDescribeVolumeModelResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DescribeVolumeModel(ctx, req)
		},
	)

	return resp.(*protos.TDescribeVolumeModelResponse), err
}

func (client *durableClient) ListVolumes(
	ctx context.Context,
	req *protos.TListVolumesRequest,
) (*protos.TListVolumesResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ListVolumes(ctx, req)
		},
	)

	return resp.(*protos.TListVolumesResponse), err
}

func (client *durableClient) ListDiskStates(
	ctx context.Context,
	req *protos.TListDiskStatesRequest,
) (*protos.TListDiskStatesResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ListDiskStates(ctx, req)
		},
	)

	return resp.(*protos.TListDiskStatesResponse), err
}

func (client *durableClient) DiscoverInstances(
	ctx context.Context,
	req *protos.TDiscoverInstancesRequest,
) (*protos.TDiscoverInstancesResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DiscoverInstances(ctx, req)
		},
	)

	return resp.(*protos.TDiscoverInstancesResponse), err
}

func (client *durableClient) ExecuteAction(
	ctx context.Context,
	req *protos.TExecuteActionRequest,
) (*protos.TExecuteActionResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ExecuteAction(ctx, req)
		},
	)

	return resp.(*protos.TExecuteActionResponse), err
}

func (client *durableClient) CreatePlacementGroup(
	ctx context.Context,
	req *protos.TCreatePlacementGroupRequest,
) (*protos.TCreatePlacementGroupResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CreatePlacementGroup(ctx, req)
		},
	)

	return resp.(*protos.TCreatePlacementGroupResponse), err
}

func (client *durableClient) DestroyPlacementGroup(
	ctx context.Context,
	req *protos.TDestroyPlacementGroupRequest,
) (*protos.TDestroyPlacementGroupResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DestroyPlacementGroup(ctx, req)
		},
	)

	return resp.(*protos.TDestroyPlacementGroupResponse), err
}

func (client *durableClient) DescribePlacementGroup(
	ctx context.Context,
	req *protos.TDescribePlacementGroupRequest,
) (*protos.TDescribePlacementGroupResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.DescribePlacementGroup(ctx, req)
		},
	)

	return resp.(*protos.TDescribePlacementGroupResponse), err
}

func (client *durableClient) AlterPlacementGroupMembership(
	ctx context.Context,
	req *protos.TAlterPlacementGroupMembershipRequest,
) (*protos.TAlterPlacementGroupMembershipResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.AlterPlacementGroupMembership(ctx, req)
		},
	)

	return resp.(*protos.TAlterPlacementGroupMembershipResponse), err
}

func (client *durableClient) ListPlacementGroups(
	ctx context.Context,
	req *protos.TListPlacementGroupsRequest,
) (*protos.TListPlacementGroupsResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.ListPlacementGroups(ctx, req)
		},
	)

	return resp.(*protos.TListPlacementGroupsResponse), err
}

func (client *durableClient) CmsAction(
	ctx context.Context,
	req *protos.TCmsActionRequest,
) (*protos.TCmsActionResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.CmsAction(ctx, req)
		},
	)

	return resp.(*protos.TCmsActionResponse), err
}

func (client *durableClient) QueryAgentsInfo(
	ctx context.Context,
	req *protos.TQueryAgentsInfoRequest,
) (*protos.TQueryAgentsInfoResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		req,
		func(ctx context.Context) (response, error) {
			return client.impl.QueryAgentsInfo(ctx, req)
		},
	)

	return resp.(*protos.TQueryAgentsInfoResponse), err
}

////////////////////////////////////////////////////////////////////////////////

type DurableClientOpts struct {
	Timeout          *time.Duration
	TimeoutIncrement *time.Duration
	OnError          func(context.Context, ClientError)
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

	onError := func(context.Context, ClientError) {}
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
