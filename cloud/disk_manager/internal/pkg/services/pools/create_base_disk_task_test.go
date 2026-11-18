package pools

import (
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/pools/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

// The transfer from the source base disk is bounded by the size of the base
// disk being created: the source may be a default sized (4 TB) base disk
// while an image sized base disk is being created from it.
func TestCreateBaseDiskTaskFromSrcDiskBoundsTransferByBaseDiskSize(
	t *testing.T,
) {

	for _, tc := range []struct {
		name         string
		baseDiskSize uint64
		// Size of the base disk being created.
		expectedSize uint64
	}{
		{
			name:         "image sized",
			baseDiskSize: 32 << 30,
			expectedSize: 32 << 30,
		},
		{
			name:         "default sized",
			baseDiskSize: 0,
			expectedSize: storage.DefaultBaseDiskSize,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := newContext()
			s := storage_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			nbsFactory := nbs_mocks.NewFactoryMock()
			nbsClient := nbs_mocks.NewClientMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			execCtx.On("GetTaskID").Return("create")
			execCtx.On("SaveState", ctx).Return(nil)

			baseDisk := &types.Disk{ZoneId: "zone", DiskId: "base"}
			srcDisk := &types.Disk{ZoneId: "zone", DiskId: "src"}

			task := &createBaseDiskTask{
				cloudID:    "cloud",
				folderID:   "folder",
				scheduler:  scheduler,
				storage:    s,
				nbsFactory: nbsFactory,
				request: &protos.CreateBaseDiskRequest{
					SrcImageId:           "image",
					SrcDisk:              srcDisk,
					SrcDiskCheckpointId:  "image",
					BaseDisk:             baseDisk,
					BaseDiskCheckpointId: "image",
					BaseDiskSize:         tc.baseDiskSize,
				},
				state: &protos.CreateBaseDiskTaskState{},
			}

			nbsFactory.On("GetClient", ctx, "zone").Return(nbsClient, nil)
			nbsClient.On("Create", ctx, nbs.CreateDiskParams{
				ID:              "base",
				BlocksCount:     tc.expectedSize / baseDiskBlockSize,
				BlockSize:       baseDiskBlockSize,
				Kind:            types.DiskKind_DISK_KIND_SSD,
				CloudID:         "cloud",
				FolderID:        "folder",
				PartitionsCount: 1,
				IsSystem:        true,
			}).Return(nil)

			scheduler.On(
				"ScheduleZonalTask",
				headers.SetIncomingIdempotencyKey(ctx, "create"),
				"dataplane.TransferFromDiskToDisk",
				"",
				"zone",
				&dataplane_protos.TransferFromDiskToDiskRequest{
					SrcDisk:                 srcDisk,
					SrcDiskBaseCheckpointId: "",
					SrcDiskCheckpointId:     "image",
					DstDisk:                 baseDisk,
					MaxSizeBytes:            tc.expectedSize,
				},
			).Return("transfer", nil)
			scheduler.On("WaitTask", ctx, execCtx, "transfer").Return(
				&empty.Empty{},
				nil,
			)

			nbsClient.On("CreateCheckpoint", ctx, nbs.CheckpointParams{
				DiskID:       "base",
				CheckpointID: "image",
			}).Return(nil)
			s.On("BaseDiskCreated", ctx, storage.BaseDisk{
				ID:      "base",
				ImageID: "image",
				ZoneID:  "zone",
			}).Return(nil)

			require.NoError(t, task.Run(ctx, execCtx))
			mock.AssertExpectationsForObjects(
				t,
				s,
				scheduler,
				nbsFactory,
				nbsClient,
				execCtx,
			)
		})
	}
}
