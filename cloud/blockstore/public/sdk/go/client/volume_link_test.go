package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
)

func TestVolumeLinkRequestsPreserveCell(t *testing.T) {
	ctx := context.Background()
	created := false
	destroyed := false
	client := &safeClient{Impl: &testClient{
		CreateVolumeLinkHandler: func(_ context.Context, request *protos.TCreateVolumeLinkRequest) (*protos.TCreateVolumeLinkResponse, error) {
			require.Equal(t, "leader", request.LeaderDiskId)
			require.Equal(t, "follower", request.FollowerDiskId)
			require.Equal(t, "target-cell", request.FollowerCellId)
			require.True(t, request.GetHeaders().GetExactDiskIdMatch())
			created = true
			return &protos.TCreateVolumeLinkResponse{}, nil
		},
		DestroyVolumeLinkHandler: func(_ context.Context, request *protos.TDestroyVolumeLinkRequest) (*protos.TDestroyVolumeLinkResponse, error) {
			require.Equal(t, "leader", request.LeaderDiskId)
			require.Equal(t, "follower", request.FollowerDiskId)
			require.Equal(t, "target-cell", request.FollowerCellId)
			require.True(t, request.GetHeaders().GetExactDiskIdMatch())
			destroyed = true
			return &protos.TDestroyVolumeLinkResponse{}, nil
		},
	}}
	require.NoError(t, client.CreateVolumeLink(ctx, "leader", "follower", "target-cell"))
	require.NoError(t, client.DestroyVolumeLink(ctx, "leader", "follower", "target-cell"))
	require.True(t, created)
	require.True(t, destroyed)
}

func TestCreateVolumeLinkRetriesTransportErrors(t *testing.T) {
	requests := 0
	impl := &testClient{
		CreateVolumeLinkHandler: func(_ context.Context, request *protos.TCreateVolumeLinkRequest) (*protos.TCreateVolumeLinkResponse, error) {
			require.Equal(t, "target-cell", request.FollowerCellId)
			requests++
			if requests == 1 {
				return nil, &ClientError{Code: E_REJECTED}
			}
			return &protos.TCreateVolumeLinkResponse{}, nil
		},
	}
	timeout := time.Second
	delay := time.Duration(0)
	client := NewDurableClient(impl, &DurableClientOpts{
		Timeout: &timeout, TimeoutIncrement: &delay,
	}, NewStderrLog(LOG_DEBUG))
	_, err := client.CreateVolumeLink(context.Background(), &protos.TCreateVolumeLinkRequest{
		LeaderDiskId: "leader", FollowerDiskId: "follower", FollowerCellId: "target-cell",
	})
	require.NoError(t, err)
	require.Equal(t, 2, requests)
}
