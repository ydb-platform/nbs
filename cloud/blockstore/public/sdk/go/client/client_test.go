package client

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
)

////////////////////////////////////////////////////////////////////////////////

const (
	DefaultUnixSocket = "./test.sock"
)

func createTestClient(port string) (*Client, error) {
	return NewClient(
		&GrpcClientOpts{
			Endpoint: fmt.Sprintf("localhost:%v", port),
		},
		&DurableClientOpts{},
		NewStderrLog(LOG_DEBUG),
	)
}

func checkEndpointsCount(client *Client, expectedCount int, t *testing.T) {
	endpoints, err := client.ListEndpoints(context.Background())

	if err != nil {
		t.Fatal(err)
	}

	if len(endpoints) != expectedCount {
		err = fmt.Errorf("Size mismatch: (expected: %v, actual: %v)",
			expectedCount, len(endpoints))
		t.Fatal(err)
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestEndpointRequests(t *testing.T) {
	ctx := context.Background()
	port := os.Getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT")

	client, err := createTestClient(port)
	require.NoError(t, err)

	checkEndpointsCount(client, 0, t)

	_, err = client.StartEndpoint(
		ctx,
		DefaultUnixSocket,
		"diskId",
		protos.EClientIpcType_IPC_GRPC,
		"clientId",
		"instanceId",
		protos.EVolumeAccessMode_VOLUME_ACCESS_READ_WRITE,
		protos.EVolumeMountMode_VOLUME_MOUNT_LOCAL,
		3,    // mountSeqNumber
		1,    // vhostQueuesCount
		true) // unalignedRequestsDisabled
	require.NoError(t, err)

	checkEndpointsCount(client, 1, t)

	err = client.StopEndpoint(ctx, DefaultUnixSocket)
	require.NoError(t, err)

	checkEndpointsCount(client, 0, t)
}

func TestQueryAvailableStorage(t *testing.T) {
	ctx := context.Background()
	port := os.Getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT")

	client, err := createTestClient(port)
	require.NoError(t, err)

	_, err = client.QueryAvailableStorage(
		ctx,
		[]string{"node"},
	)
	require.NoError(t, err)
}

func TestCreateVolumeFromDevice(t *testing.T) {
	ctx := context.Background()
	port := os.Getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT")

	client, err := createTestClient(port)
	require.NoError(t, err)

	err = client.CreateVolumeFromDevice(
		ctx,
		"diskId",
		"agentId",
		"path",
		&CreateVolumeOpts{
			FolderId: "folder",
			CloudId:  "cloud",
		},
	)
	require.NoError(t, err)
}

func TestResumeDevice(t *testing.T) {
	ctx := context.Background()
	port := os.Getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT")

	client, err := createTestClient(port)
	require.NoError(t, err)

	err = client.ResumeDevice(
		ctx,
		"agentId",
		"path",
		false, // DryRun
	)
	require.NoError(t, err)
}

func TestLocalNVMeMethods(t *testing.T) {
	ctx := context.Background()
	port := os.Getenv("LOCAL_NULL_INSECURE_NBS_SERVER_PORT")

	client, err := createTestClient(port)
	require.NoError(t, err)

	_, err = client.ListNVMeDevices(ctx)
	require.NoError(t, err)

	_, err = client.AcquireNVMeDevice(ctx, "sn")
	require.NoError(t, err)

	err = client.ReleaseNVMeDevice(ctx, "sn")
	require.NoError(t, err)
}

func TestReadBlocksSnapshotCreationRead(t *testing.T) {
	ctx := context.Background()
	snapshotCtx := WithSnapshotCreationRead(ctx)
	derivedCtx, cancel := context.WithCancel(
		WithClientID(snapshotCtx, "snapshot-client"),
	)
	defer cancel()

	for _, testCase := range []struct {
		name                 string
		ctx                  context.Context
		snapshotCreationRead bool
	}{
		{"ordinary", ctx, false},
		{"snapshot", snapshotCtx, true},
		{"derivedContext", derivedCtx, true},
		{"ordinaryAfterSnapshot", ctx, false},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			for _, checkpointID := range []string{"", "checkpoint"} {
				calls := 0
				buffers := [][]byte{[]byte("block-data")}
				impl := &testClient{
					ReadBlocksHandler: func(
						ctx context.Context,
						req *protos.TReadBlocksRequest,
					) (*protos.TReadBlocksResponse, error) {
						calls++
						require.Equal(
							t, testCase.snapshotCreationRead, req.GetSnapshotCreationRead(),
						)
						require.Equal(t, checkpointID, req.GetCheckpointId())
						req.Headers = &protos.THeaders{}
						(&grpcClient{}).setupHeaders(ctx, req)
						require.False(t, req.GetHeaders().GetIsBackgroundRequest())
						return &protos.TReadBlocksResponse{
							Blocks: &protos.TIOVector{Buffers: buffers},
						}, nil
					},
				}
				client := &Client{safeClient{impl}}
				blocks, err := client.ReadBlocks(
					testCase.ctx, "disk", 0, 1, checkpointID, "session",
				)
				require.NoError(t, err)
				require.Equal(t, buffers, blocks)
				require.Equal(t, 1, calls)
			}
		})
	}
}

func TestSnapshotCreationReadDoesNotAffectWrites(t *testing.T) {
	ctx := context.Background()
	buffers := [][]byte{[]byte("block-data")}
	calls := 0
	impl := &testClient{
		WriteBlocksHandler: func(
			ctx context.Context,
			req *protos.TWriteBlocksRequest,
		) (*protos.TWriteBlocksResponse, error) {
			calls++
			require.Equal(t, &protos.TWriteBlocksRequest{
				DiskId:     "disk",
				StartIndex: 1,
				Blocks:     &protos.TIOVector{Buffers: buffers},
				SessionId:  "session",
			}, req)
			req.Headers = &protos.THeaders{}
			(&grpcClient{}).setupHeaders(ctx, req)
			require.False(t, req.GetHeaders().GetIsBackgroundRequest())
			return &protos.TWriteBlocksResponse{}, nil
		},
	}
	client := &Client{safeClient{impl}}
	for _, writeCtx := range []context.Context{ctx, WithSnapshotCreationRead(ctx)} {
		require.NoError(t, client.WriteBlocks(writeCtx, "disk", 1, buffers, "session"))
	}
	require.Equal(t, 2, calls)
}
