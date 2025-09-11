package client

import (
	"errors"
	"fmt"
	"sync"
	"time"

	protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

const (
	defaultHardTimeout = 8 * time.Minute
	defaultSoftTimeout = 15 * time.Second
)

////////////////////////////////////////////////////////////////////////////////

func isRetriable(err error) bool {
	cerr := GetClientError(err)

	return cerr.IsRetriable()
}

func wrapError(err error) error {
	switch err {
	case context.Canceled:
		return &ClientError{
			Code:    E_CANCELLED,
			Message: "context canceled",
		}
	case context.DeadlineExceeded:
		return &ClientError{
			Code:    E_TIMEOUT,
			Message: "context deadline exceeded",
		}
	}
	return err
}

////////////////////////////////////////////////////////////////////////////////

type CreateClientFunc func(host string, port uint32) (ClientIface, error)

type DiscoveryClientOpts struct {
	Limit       uint32
	HardTimeout time.Duration
	SoftTimeout time.Duration
}

////////////////////////////////////////////////////////////////////////////////

type instanceAddress = protos.TDiscoveredInstance

type discoveryClient struct {
	endpoints     []ClientIface
	clientFactory CreateClientFunc
	log           Log
	limit         uint32
	hardTimeout   time.Duration
	softTimeout   time.Duration
	secure        bool
	cachedImpl    ClientIface
	mtx           sync.Mutex
}

type shootResult struct {
	resp response
	addr *instanceAddress
	err  error
}

func (client *discoveryClient) closeImpl(
	ctx context.Context,
	impl ClientIface,
	addr *instanceAddress,
) {

	dbg := client.log.Logger(LOG_DEBUG)

	if dbg != nil {
		dbg.Printf(
			ctx,
			"calling client.Close for %v, %s:%d",
			impl,
			addr.Host,
			addr.Port,
		)
	}

	err := impl.Close()
	if err != nil && dbg != nil {
		dbg.Printf(
			ctx,
			"error on client.Close: %v for %s:%d",
			err,
			addr.Host,
			addr.Port,
		)
	}
}

func (client *discoveryClient) getCachedImpl() ClientIface {
	client.mtx.Lock()
	defer client.mtx.Unlock()

	return client.cachedImpl
}

func (client *discoveryClient) onSuccess(
	ctx context.Context,
	impl ClientIface,
) {

	client.mtx.Lock()
	defer client.mtx.Unlock()

	if client.cachedImpl == nil {
		client.cachedImpl = impl
	}
}

func (client *discoveryClient) onFailure(
	ctx context.Context,
	impl ClientIface,
) {
	client.mtx.Lock()
	defer client.mtx.Unlock()

	if client.cachedImpl == impl {
		client.cachedImpl = nil
	}
}

func (client *discoveryClient) shoot(
	ctx context.Context,
	addr *instanceAddress,
	op func(ctx context.Context, impl ClientIface) (response, error),
) (response, error) {

	impl, err := client.createImpl(ctx, addr)
	if err != nil {
		return nil, err
	}

	dbg := client.log.Logger(LOG_DEBUG)

	if dbg != nil {
		dbg.Printf(
			ctx,
			"shooting for %v, %s:%d",
			impl,
			addr.Host,
			addr.Port,
		)
	}

	resp, err := op(ctx, impl)
	if err != nil && dbg != nil {
		dbg.Printf(ctx, "%s:%d request error: %v", addr.Host, addr.Port, err)
	}

	client.closeImpl(ctx, impl, addr)

	return resp, err
}

func (client *discoveryClient) createImpl(
	ctx context.Context,
	addr *instanceAddress,
) (ClientIface, error) {

	client.logDebug(ctx, "create client %s:%d", addr.Host, addr.Port)

	impl, err := client.clientFactory(addr.Host, addr.Port)
	if err != nil {
		client.logError(
			ctx,
			"can't create client for %s:%d, error '%v'",
			addr.Host,
			addr.Port,
			err,
		)

		return nil, &ClientError{
			Code:    E_REJECTED,
			Message: "can't create client",
		}
	}

	return impl, nil
}

func (client *discoveryClient) logError(
	ctx context.Context,
	fmt string,
	args ...interface{},
) {

	logger := client.log.Logger(LOG_ERROR)

	if logger != nil {
		logger.Printf(ctx, fmt, args...)
	}
}

func (client *discoveryClient) logDebug(
	ctx context.Context,
	fmt string,
	args ...interface{},
) {

	logger := client.log.Logger(LOG_DEBUG)

	if logger != nil {
		logger.Printf(ctx, fmt, args...)
	}
}

func (client *discoveryClient) invokeDiscoverInstances(
	ctx context.Context,
	endpoint ClientIface,
	req *protos.TDiscoverInstancesRequest,
) (response, error) {

	if client.softTimeout != 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, client.softTimeout)
		defer cancel()
	}

	return endpoint.DiscoverInstances(ctx, req)
}

func (client *discoveryClient) tryCachedEndpoint(
	ctx context.Context,
	req *protos.TDiscoverInstancesRequest,
) response {
	impl := client.getCachedImpl()

	if impl == nil {
		return nil
	}

	resp, err := client.invokeDiscoverInstances(ctx, impl, req)
	if err != nil {
		client.onFailure(ctx, impl)
		return nil
	}

	client.onSuccess(ctx, impl)

	return resp
}

func (client *discoveryClient) discoverInstancesImpl(
	ctx context.Context,
	req *protos.TDiscoverInstancesRequest,
) (response, error) {

	resp := client.tryCachedEndpoint(ctx, req)

	if resp != nil {
		return resp, nil
	}

	impl, err := findClosest(ctx, client.endpoints)

	if err != nil {
		return nil, err
	}

	client.onSuccess(ctx, impl)

	return client.invokeDiscoverInstances(ctx, impl, req)
}

func (client *discoveryClient) newDiscoverRequest() *protos.TDiscoverInstancesRequest {
	filter := protos.EDiscoveryPortFilter_DISCOVERY_INSECURE_PORT

	if client.secure {
		filter = protos.EDiscoveryPortFilter_DISCOVERY_SECURE_PORT
	}

	return &protos.TDiscoverInstancesRequest{
		Limit:          client.limit,
		InstanceFilter: filter,
	}
}

func (client *discoveryClient) discoverInstances(
	ctx context.Context,
) ([]*instanceAddress, error) {

	req := client.newDiscoverRequest()

	res, err := client.discoverInstancesImpl(ctx, req)
	if err != nil {
		client.logError(ctx, "discovery error: %v", err)
		return nil, err
	}

	instances := res.(*protos.TDiscoverInstancesResponse).Instances
	client.logDebug(ctx, "discovered instances: %v", instances)
	return instances, nil
}

func (client *discoveryClient) mainShoot(
	ctx context.Context,
	op func(ctx context.Context, impl ClientIface) (response, error),
) *shootResult {

	var instances []*instanceAddress

	for {
		if len(instances) == 0 {
			var err error
			instances, err = client.discoverInstances(ctx)
			if err != nil {
				return &shootResult{nil, nil, err}
			}
		}

		for _, addr := range instances {
			resp, err := client.shoot(ctx, addr, op)

			if err == nil || !isRetriable(err) {
				return &shootResult{resp, addr, err}
			}
		}

		instances = nil
	}
}

func (client *discoveryClient) hedgedShoot(
	ctx context.Context,
	op func(ctx context.Context, impl ClientIface) (response, error),
) *shootResult {

	instances, err := client.discoverInstances(ctx)
	if err != nil {
		return &shootResult{nil, nil, err}
	}

	if len(instances) == 0 {
		return nil
	}

	addr := instances[0]

	resp, err := client.shoot(ctx, addr, op)
	if err == nil || !isRetriable(err) {
		return &shootResult{resp, addr, err}
	}

	return nil
}

func (client *discoveryClient) executeRequest(
	ctx context.Context,
	op func(ctx context.Context, impl ClientIface) (response, error),
) (response, error) {

	r := client.tryExecuteRequest(ctx, op)
	return r.resp, wrapError(r.err)
}

func (client *discoveryClient) tryExecuteRequest(
	ctx context.Context,
	op func(ctx context.Context, impl ClientIface) (response, error),
) *shootResult {

	channel := make(chan *shootResult, 2)
	defer close(channel)

	var wg sync.WaitGroup
	defer wg.Wait()

	ctx, cancel := context.WithTimeout(ctx, client.hardTimeout)
	defer cancel()

	if client.softTimeout != 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()

			select {
			case <-ctx.Done():
			case <-time.After(client.softTimeout):
				r := client.hedgedShoot(ctx, op)
				if r != nil {
					channel <- r
				}
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		channel <- client.mainShoot(ctx, op)
	}()

	return <-channel
}

func (client *discoveryClient) discoverInstance(
	ctx context.Context,
) (*Client, string, error) {

	op := func(ctx context.Context, impl ClientIface) (response, error) {
		return impl.Ping(ctx, &protos.TPingRequest{})
	}

	r := client.tryExecuteRequest(ctx, op)
	if r.err != nil {
		return nil, "", wrapError(r.err)
	}

	impl, err := client.createImpl(ctx, r.addr)
	if err != nil {
		return nil, "", err
	}

	return &Client{safeClient{impl}}, r.addr.Host, nil
}

func (client *discoveryClient) Close() error {
	var res error

	for _, endpoint := range client.endpoints {
		err := endpoint.Close()
		if err != nil {
			res = err
		}
	}
	client.endpoints = nil

	client.mtx.Lock()
	defer client.mtx.Unlock()

	client.cachedImpl = nil

	return res
}

func (client *discoveryClient) Ping(
	ctx context.Context,
	req *protos.TPingRequest,
) (*protos.TPingResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.Ping(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TPingResponse), nil
}

func (client *discoveryClient) DiscoverInstances(
	ctx context.Context,
	req *protos.TDiscoverInstancesRequest,
) (*protos.TDiscoverInstancesResponse, error) {

	resp, err := client.discoverInstancesImpl(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.(*protos.TDiscoverInstancesResponse), nil
}

func (client *discoveryClient) CreateVolume(
	ctx context.Context,
	req *protos.TCreateVolumeRequest,
) (*protos.TCreateVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.CreateVolume(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TCreateVolumeResponse), err
}

func (client *discoveryClient) DestroyVolume(
	ctx context.Context,
	req *protos.TDestroyVolumeRequest,
) (*protos.TDestroyVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.DestroyVolume(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TDestroyVolumeResponse), err
}

func (client *discoveryClient) ResizeVolume(
	ctx context.Context,
	req *protos.TResizeVolumeRequest,
) (*protos.TResizeVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.ResizeVolume(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TResizeVolumeResponse), err
}

func (client *discoveryClient) AlterVolume(
	ctx context.Context,
	req *protos.TAlterVolumeRequest,
) (*protos.TAlterVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.AlterVolume(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TAlterVolumeResponse), err
}

func (client *discoveryClient) AssignVolume(
	ctx context.Context,
	req *protos.TAssignVolumeRequest,
) (*protos.TAssignVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.AssignVolume(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TAssignVolumeResponse), err
}

func (client *discoveryClient) StatVolume(
	ctx context.Context,
	req *protos.TStatVolumeRequest,
) (*protos.TStatVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.StatVolume(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TStatVolumeResponse), err
}

func (client *discoveryClient) QueryAvailableStorage(
	ctx context.Context,
	req *protos.TQueryAvailableStorageRequest,
) (*protos.TQueryAvailableStorageResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.QueryAvailableStorage(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TQueryAvailableStorageResponse), err
}

func (client *discoveryClient) ResumeDevice(
	ctx context.Context,
	req *protos.TResumeDeviceRequest,
) (*protos.TResumeDeviceResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.ResumeDevice(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TResumeDeviceResponse), err
}

func (client *discoveryClient) CreateVolumeFromDevice(
	ctx context.Context,
	req *protos.TCreateVolumeFromDeviceRequest,
) (*protos.TCreateVolumeFromDeviceResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.CreateVolumeFromDevice(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TCreateVolumeFromDeviceResponse), err
}

func (client *discoveryClient) MountVolume(
	ctx context.Context,
	req *protos.TMountVolumeRequest,
) (*protos.TMountVolumeResponse, error) {

	panic("not implemented")
}

func (client *discoveryClient) UnmountVolume(
	ctx context.Context,
	req *protos.TUnmountVolumeRequest,
) (*protos.TUnmountVolumeResponse, error) {

	panic("not implemented")
}

func (client *discoveryClient) ReadBlocks(
	ctx context.Context,
	req *protos.TReadBlocksRequest,
) (*protos.TReadBlocksResponse, error) {

	panic("not implemented")
}

func (client *discoveryClient) WriteBlocks(
	ctx context.Context,
	req *protos.TWriteBlocksRequest,
) (*protos.TWriteBlocksResponse, error) {

	panic("not implemented")
}

func (client *discoveryClient) ZeroBlocks(
	ctx context.Context,
	req *protos.TZeroBlocksRequest,
) (*protos.TZeroBlocksResponse, error) {

	panic("not implemented")
}

func (client *discoveryClient) StartEndpoint(
	ctx context.Context,
	req *protos.TStartEndpointRequest,
) (*protos.TStartEndpointResponse, error) {

	panic("not implemented")
}

func (client *discoveryClient) StopEndpoint(
	ctx context.Context,
	req *protos.TStopEndpointRequest,
) (*protos.TStopEndpointResponse, error) {

	panic("not implemented")
}

func (client *discoveryClient) ListEndpoints(
	ctx context.Context,
	req *protos.TListEndpointsRequest,
) (*protos.TListEndpointsResponse, error) {

	panic("not implemented")
}

func (client *discoveryClient) KickEndpoint(
	ctx context.Context,
	req *protos.TKickEndpointRequest,
) (*protos.TKickEndpointResponse, error) {

	panic("not implemented")
}

func (client *discoveryClient) ListKeyrings(
	ctx context.Context,
	req *protos.TListKeyringsRequest,
) (*protos.TListKeyringsResponse, error) {

	panic("not implemented")
}

func (client *discoveryClient) DescribeEndpoint(
	ctx context.Context,
	req *protos.TDescribeEndpointRequest,
) (*protos.TDescribeEndpointResponse, error) {

	panic("not implemented")
}

func (client *discoveryClient) RefreshEndpoint(
	ctx context.Context,
	req *protos.TRefreshEndpointRequest,
) (*protos.TRefreshEndpointResponse, error) {

	panic("not implemented")
}

func (client *discoveryClient) CreateCheckpoint(
	ctx context.Context,
	req *protos.TCreateCheckpointRequest,
) (*protos.TCreateCheckpointResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.CreateCheckpoint(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TCreateCheckpointResponse), err
}

func (client *discoveryClient) GetCheckpointStatus(
	ctx context.Context,
	req *protos.TGetCheckpointStatusRequest,
) (*protos.TGetCheckpointStatusResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.GetCheckpointStatus(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TGetCheckpointStatusResponse), err
}

func (client *discoveryClient) DeleteCheckpoint(
	ctx context.Context,
	req *protos.TDeleteCheckpointRequest,
) (*protos.TDeleteCheckpointResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.DeleteCheckpoint(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TDeleteCheckpointResponse), err
}

func (client *discoveryClient) GetChangedBlocks(
	ctx context.Context,
	req *protos.TGetChangedBlocksRequest,
) (*protos.TGetChangedBlocksResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.GetChangedBlocks(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TGetChangedBlocksResponse), err
}

func (client *discoveryClient) DescribeVolume(
	ctx context.Context,
	req *protos.TDescribeVolumeRequest,
) (*protos.TDescribeVolumeResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.DescribeVolume(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TDescribeVolumeResponse), err
}

func (client *discoveryClient) DescribeVolumeModel(
	ctx context.Context,
	req *protos.TDescribeVolumeModelRequest,
) (*protos.TDescribeVolumeModelResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.DescribeVolumeModel(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TDescribeVolumeModelResponse), err
}

func (client *discoveryClient) ListVolumes(
	ctx context.Context,
	req *protos.TListVolumesRequest,
) (*protos.TListVolumesResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.ListVolumes(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TListVolumesResponse), err
}

func (client *discoveryClient) ListDiskStates(
	ctx context.Context,
	req *protos.TListDiskStatesRequest,
) (*protos.TListDiskStatesResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.ListDiskStates(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TListDiskStatesResponse), err
}

func (client *discoveryClient) ExecuteAction(
	ctx context.Context,
	req *protos.TExecuteActionRequest,
) (*protos.TExecuteActionResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.ExecuteAction(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TExecuteActionResponse), err
}

func (client *discoveryClient) CreatePlacementGroup(
	ctx context.Context,
	req *protos.TCreatePlacementGroupRequest,
) (*protos.TCreatePlacementGroupResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.CreatePlacementGroup(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TCreatePlacementGroupResponse), err
}

func (client *discoveryClient) DestroyPlacementGroup(
	ctx context.Context,
	req *protos.TDestroyPlacementGroupRequest,
) (*protos.TDestroyPlacementGroupResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.DestroyPlacementGroup(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TDestroyPlacementGroupResponse), err
}

func (client *discoveryClient) DescribePlacementGroup(
	ctx context.Context,
	req *protos.TDescribePlacementGroupRequest,
) (*protos.TDescribePlacementGroupResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.DescribePlacementGroup(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TDescribePlacementGroupResponse), err
}

func (client *discoveryClient) AlterPlacementGroupMembership(
	ctx context.Context,
	req *protos.TAlterPlacementGroupMembershipRequest,
) (*protos.TAlterPlacementGroupMembershipResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.AlterPlacementGroupMembership(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TAlterPlacementGroupMembershipResponse), err
}

func (client *discoveryClient) ListPlacementGroups(
	ctx context.Context,
	req *protos.TListPlacementGroupsRequest,
) (*protos.TListPlacementGroupsResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.ListPlacementGroups(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TListPlacementGroupsResponse), err
}

func (client *discoveryClient) CmsAction(
	ctx context.Context,
	req *protos.TCmsActionRequest,
) (*protos.TCmsActionResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.CmsAction(ctx, req)
		})
	if err != nil {
		return nil, err
	}

	return resp.(*protos.TCmsActionResponse), nil
}

func (client *discoveryClient) QueryAgentsInfo(
	ctx context.Context,
	req *protos.TQueryAgentsInfoRequest,
) (*protos.TQueryAgentsInfoResponse, error) {

	resp, err := client.executeRequest(
		ctx,
		func(ctx context.Context, impl ClientIface) (response, error) {
			return impl.QueryAgentsInfo(ctx, req)
		})

	if err != nil {
		return nil, err
	}

	return resp.(*protos.TQueryAgentsInfoResponse), err
}

////////////////////////////////////////////////////////////////////////////////

func createDurableClient(
	endpoint string,
	grpcOpts *GrpcClientOpts,
	durableOpts *DurableClientOpts,
	log Log,
) (ClientIface, error) {

	newGrpcOpts := *grpcOpts
	newGrpcOpts.Endpoint = endpoint

	grpcClient, err := NewGrpcClient(&newGrpcOpts, log)
	if err != nil {
		return nil, err
	}

	durableClient := NewDurableClient(grpcClient, durableOpts, log)
	return durableClient, nil
}

func newDiscoveryClient(
	endpoints []ClientIface,
	discoveryOpts *DiscoveryClientOpts,
	factory CreateClientFunc,
	log Log,
	secure bool,
) ClientIface {

	limit := discoveryOpts.Limit
	if limit == 0 {
		// a reasonable default
		limit = 3
	}

	hardTimeout := defaultHardTimeout

	if discoveryOpts.HardTimeout != 0 {
		hardTimeout = discoveryOpts.HardTimeout
	}

	softTimeout := defaultSoftTimeout

	if discoveryOpts.SoftTimeout != 0 {
		softTimeout = discoveryOpts.SoftTimeout
	}

	return &discoveryClient{
		endpoints:     endpoints,
		clientFactory: factory,
		log:           log,
		limit:         limit,
		hardTimeout:   hardTimeout,
		softTimeout:   softTimeout,
		secure:        secure,
	}
}

////////////////////////////////////////////////////////////////////////////////

type DiscoveryClient struct {
	safeClient
}

// Returns (client, host, error).
func (client *DiscoveryClient) DiscoverInstance(
	ctx context.Context,
) (*Client, string, error) {

	balancer := client.Impl.(*discoveryClient)

	return balancer.discoverInstance(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func findClosest(
	ctx context.Context,
	clients []ClientIface,
) (ClientIface, error) {

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	val_chan := make(chan ClientIface, len(clients))
	err_chan := make(chan error, len(clients))

	ping := func(cli ClientIface) {
		req := &protos.TPingRequest{}

		_, err := cli.Ping(ctx, req)
		if err != nil {
			err_chan <- err
			return
		}

		val_chan <- cli
	}

	running := 0

	var cli ClientIface
	err := errors.New("no clients specified")

	for _, client := range clients {
		go ping(client)
		running++
	}

	for running > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case cli = <-val_chan:
			return cli, nil

		case err = <-err_chan:
			running--
		}
	}

	return nil, err
}

func NewDiscoveryClient(
	endpoints []string,
	grpcOpts *GrpcClientOpts,
	durableOpts *DurableClientOpts,
	discoveryOpts *DiscoveryClientOpts,
	log Log,
) (*DiscoveryClient, error) {

	_factory := func(endpoint string, log Log) (ClientIface, error) {
		return createDurableClient(
			endpoint,
			grpcOpts,
			durableOpts,
			log,
		)
	}

	factory := func(host string, port uint32) (ClientIface, error) {
		return _factory(fmt.Sprintf("%s:%d", host, port), log)
	}

	clients := make([]ClientIface, len(endpoints))
	discard := NewLog(nil, LOG_ERROR)

	for i, endpoint := range endpoints {
		client, err := _factory(endpoint, discard)
		if err != nil {
			return nil, err
		}
		clients[i] = client
	}

	impl := newDiscoveryClient(
		clients,
		discoveryOpts,
		factory,
		log,
		grpcOpts.Credentials != nil,
	)

	return &DiscoveryClient{
		safeClient{impl},
	}, nil
}
