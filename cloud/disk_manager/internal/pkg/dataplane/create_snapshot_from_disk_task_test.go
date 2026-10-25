package dataplane

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	blockstore_protos "github.com/ydb-platform/nbs/cloud/blockstore/public/api/protos"
	blockstore_client "github.com/ydb-platform/nbs/cloud/blockstore/public/sdk/go/client"
	nbs_client "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	performance_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/performance/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"google.golang.org/grpc"
)

////////////////////////////////////////////////////////////////////////////////

func TestCreateSnapshotFromDiskMarksReadBlocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(newContext(), 10*time.Second)
	defer cancel()

	const blockSize = uint32(4096)
	const blockCount = chunkSize / blockSize
	const successfulReadMsg = "stop after observing ReadBlocks"
	reads := make(chan *blockstore_protos.TReadBlocksRequest, 1)

	intercept := func(
		ctx context.Context,
		method string,
		request, response interface{},
		conn *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {

		switch request := request.(type) {
		case *blockstore_protos.TPingRequest, *blockstore_protos.TUnmountVolumeRequest:
			return nil
		case *blockstore_protos.TDiscoverInstancesRequest:
			response.(*blockstore_protos.TDiscoverInstancesResponse).Instances =
				[]*blockstore_protos.TDiscoveredInstance{{Host: "localhost"}}
		case *blockstore_protos.TMountVolumeRequest:
			mount := response.(*blockstore_protos.TMountVolumeResponse)
			mount.SessionId = "session"
			mount.Volume = &blockstore_protos.TVolume{
				DiskId:      "disk",
				BlockSize:   blockSize,
				BlocksCount: uint64(blockCount),
			}
		case *blockstore_protos.TReadBlocksRequest:
			reads <- request
			// We're using this error to stop the interceptor after the read.
			return fmt.Errorf(successfulReadMsg)
		default:
			return fmt.Errorf("unexpected RPC: %v", method)
		}
		return nil
	}

	discovery, err := blockstore_client.NewDiscoveryClient(
		[]string{"localhost:0"},
		&blockstore_client.GrpcClientOpts{
			DialOptions: []grpc.DialOption{
				grpc.WithUnaryInterceptor(intercept),
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
					return nil, fmt.Errorf("RPCs are handled by the test interceptor")
				}),
			},
		},
		&blockstore_client.DurableClientOpts{},
		&blockstore_client.DiscoveryClientOpts{},
		blockstore_client.NewStderrLog(blockstore_client.LOG_ERROR),
	)
	require.NoError(t, err)
	defer discovery.Close()

	session, err := nbs_client.NewROSession(
		ctx, discovery, metrics.NewEmptyRegistry(), "disk", 0, nil,
		time.Hour, 2*time.Hour,
	)
	require.NoError(t, err)
	defer session.Close(ctx)

	nbsClient := nbs_mocks.NewClientMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	snapshotStorage := storage_mocks.NewStorageMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	nbsFactory.On("GetClient", mock.Anything, "zone").Return(nbsClient, nil)
	nbsClient.On("Describe", mock.Anything, "disk").Return(nbs_client.DiskParams{}, nil)
	nbsClient.On("MountRO", mock.Anything, "disk", mock.Anything).Return(session, nil).Once()
	nbsClient.On("GetChangedBytes", mock.Anything, "disk", "", "checkpoint", false).
		Return(uint64(chunkSize), nil).Once()
	changedBlocks := make([]byte, blockCount/8)
	changedBlocks[0] = 1
	nbsClient.On("GetChangedBlocks", mock.Anything, "disk", uint64(0), blockCount, "", "checkpoint").
		Return(changedBlocks, nil).Once()
	snapshotStorage.On("CreateSnapshot", mock.Anything, mock.Anything).
		Return(&storage.SnapshotMeta{ID: "snapshot"}, nil).Once()
	snapshotStorage.On("CheckSnapshotAlive", mock.Anything, "snapshot").Return(nil).Maybe()
	execCtx.On("GetTaskID").Return("task")
	execCtx.On("SetEstimatedInflightDuration", mock.Anything).Once()
	execCtx.On("SaveState", mock.Anything).Return(nil).Maybe()

	task := createSnapshotFromDiskTask{
		config: &config.DataplaneConfig{
			ReaderCount:         proto.Uint32(1),
			WriterCount:         proto.Uint32(1),
			ChunksInflightLimit: proto.Uint32(1),
		},
		performanceConfig: &performance_config.PerformanceConfig{},
		nbsFactory:        nbsFactory,
		storage:           snapshotStorage,
		request: &protos.CreateSnapshotFromDiskRequest{
			SrcDisk:             &types.Disk{ZoneId: "zone", DiskId: "disk"},
			SrcDiskCheckpointId: "checkpoint",
			DstSnapshotId:       "snapshot",
		},
		state: &protos.CreateSnapshotFromDiskTaskState{},
	}

	err = task.Run(ctx, execCtx)
	require.Error(t, err)
	require.Contains(t, err.Error(), successfulReadMsg)
	require.Len(t, reads, 1)
	request := <-reads
	require.Equal(t, "disk", request.GetDiskId())
	require.Equal(t, "checkpoint", request.GetCheckpointId())
	require.True(t, request.GetSnapshotCreationRead())
	mock.AssertExpectationsForObjects(t, nbsFactory, nbsClient, snapshotStorage, execCtx)
}
