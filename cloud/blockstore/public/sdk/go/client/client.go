package client

import (
	"errors"
	"strings"

	protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

type Client struct {
	safeClient
}

func (client *Client) MountVolume(
	ctx context.Context,
	diskId string,
	opts *MountVolumeOpts,
) (*protos.TVolume, *SessionInfo, error) {

	req := &protos.TMountVolumeRequest{
		DiskId: diskId,
	}

	if opts != nil {
		req.Token = opts.Token
		req.VolumeAccessMode = opts.AccessMode
		req.VolumeMountMode = opts.MountMode
		req.MountFlags = opts.MountFlags
		req.EncryptionSpec = opts.EncryptionSpec
		req.FillGeneration = opts.FillGeneration
		req.FillSeqNumber = opts.FillSeqNumber
	}

	resp, err := client.Impl.MountVolume(ctx, req)
	if err != nil {
		return nil, nil, err
	}

	timeout := durationFromMsec(int64(resp.GetInactiveClientsTimeout()))

	session := &SessionInfo{
		SessionId:              resp.GetSessionId(),
		InactiveClientsTimeout: timeout,
	}

	return resp.GetVolume(), session, nil
}

func (client *Client) UnmountVolume(
	ctx context.Context,
	diskId string,
	sessionId string,
) error {
	req := &protos.TUnmountVolumeRequest{
		DiskId:    diskId,
		SessionId: sessionId,
	}

	_, err := client.Impl.UnmountVolume(ctx, req)
	return err
}

func (client *Client) ReadBlocks(
	ctx context.Context,
	diskId string,
	startIndex uint64,
	blocksCount uint32,
	checkpointId string,
	sessionId string,
) ([][]byte, error) {

	req := &protos.TReadBlocksRequest{
		DiskId:       diskId,
		StartIndex:   startIndex,
		BlocksCount:  blocksCount,
		CheckpointId: checkpointId,
		SessionId:    sessionId,
	}

	resp, err := client.Impl.ReadBlocks(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.Blocks.Buffers, nil
}

func (client *Client) WriteBlocks(
	ctx context.Context,
	diskId string,
	startIndex uint64,
	blocks [][]byte,
	sessionId string,
) error {
	req := &protos.TWriteBlocksRequest{
		DiskId:     diskId,
		StartIndex: startIndex,
		Blocks: &protos.TIOVector{
			Buffers: blocks,
		},
		SessionId: sessionId,
	}

	_, err := client.Impl.WriteBlocks(ctx, req)
	return err
}

func (client *Client) ZeroBlocks(
	ctx context.Context,
	diskId string,
	startIndex uint64,
	blocksCount uint32,
	sessionId string,
) error {
	req := &protos.TZeroBlocksRequest{
		DiskId:      diskId,
		StartIndex:  startIndex,
		BlocksCount: blocksCount,
		SessionId:   sessionId,
	}

	_, err := client.Impl.ZeroBlocks(ctx, req)
	return err
}

func (client *Client) StartEndpoint(
	ctx context.Context,
	unixSocketPath string,
	diskId string,
	ipcType protos.EClientIpcType,
	clientId string,
	instanceId string,
	accessMode protos.EVolumeAccessMode,
	mountMode protos.EVolumeMountMode,
	mountSeqNumber uint64,
	vhostQueuesCount uint32,
	unalignedRequestsDisabled bool,
) (*protos.TVolume, error) {
	req := &protos.TStartEndpointRequest{
		UnixSocketPath:            unixSocketPath,
		DiskId:                    diskId,
		IpcType:                   ipcType,
		ClientId:                  clientId,
		InstanceId:                instanceId,
		VolumeAccessMode:          accessMode,
		VolumeMountMode:           mountMode,
		MountSeqNumber:            mountSeqNumber,
		VhostQueuesCount:          vhostQueuesCount,
		UnalignedRequestsDisabled: unalignedRequestsDisabled,
	}

	resp, err := client.Impl.StartEndpoint(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetVolume(), err
}

func (client *Client) StopEndpoint(
	ctx context.Context,
	unixSocketPath string,
) error {
	req := &protos.TStopEndpointRequest{
		UnixSocketPath: unixSocketPath,
	}

	_, err := client.Impl.StopEndpoint(ctx, req)
	return err
}

func (client *Client) ListEndpoints(
	ctx context.Context,
) ([]*protos.TStartEndpointRequest, error) {
	req := &protos.TListEndpointsRequest{}

	resp, err := client.Impl.ListEndpoints(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetEndpoints(), nil
}

func (client *Client) KickEndpoint(
	ctx context.Context,
	keyringId uint32,
) error {
	req := &protos.TKickEndpointRequest{
		KeyringId: keyringId,
	}

	_, err := client.Impl.KickEndpoint(ctx, req)
	return err
}

func (client *Client) ListKeyrings(
	ctx context.Context,
) ([]*protos.TListKeyringsResponse_TKeyringEndpoint, error) {
	req := &protos.TListKeyringsRequest{}

	resp, err := client.Impl.ListKeyrings(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetEndpoints(), nil
}

func (client *Client) DescribeEndpoint(
	ctx context.Context,
	unixSocketPath string,
) (*protos.TClientPerformanceProfile, error) {
	req := &protos.TDescribeEndpointRequest{
		UnixSocketPath: unixSocketPath,
	}

	resp, err := client.Impl.DescribeEndpoint(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetPerformanceProfile(), nil
}

func (client *Client) RefreshEndpoint(
	ctx context.Context,
	unixSocketPath string,
) error {
	req := &protos.TRefreshEndpointRequest{
		UnixSocketPath: unixSocketPath,
	}

	_, err := client.Impl.RefreshEndpoint(ctx, req)
	return err
}

func (client *Client) CancelEndpointInFlightRequests(
	ctx context.Context,
	unixSocketPath string,
) error {
	req := &protos.TCancelEndpointInFlightRequestsRequest{
		UnixSocketPath: unixSocketPath,
	}

	_, err := client.Impl.CancelEndpointInFlightRequests(ctx, req)
	return err
}

////////////////////////////////////////////////////////////////////////////////

func IsDiskNotFoundError(e error) bool {
	var clientErr *ClientError
	if errors.As(e, &clientErr) {
		if clientErr.Facility() == FACILITY_SCHEMESHARD {
			// TODO: remove support for PathDoesNotExist.
			if clientErr.Status() == 2 {
				return true
			}

			// Hack for NBS-3162.
			if strings.Contains(clientErr.Error(), "Another drop in progress") {
				return true
			}
		}
	}

	return false
}

////////////////////////////////////////////////////////////////////////////////

func NewClient(
	grpcOpts *GrpcClientOpts,
	durableOpts *DurableClientOpts,
	log Log,
) (*Client, error) {

	grpcClient, err := NewGrpcClient(grpcOpts, log)
	if err != nil {
		return nil, err
	}

	durableClient := NewDurableClient(grpcClient, durableOpts, log)

	return &Client{
		safeClient{durableClient},
	}, nil
}
