package snapshots

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	cells_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	nbs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/bucket"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

const snapshotTestBucket = "snapshot"

// The snapshot bucket of the tests, under the prefix of the test.
func newTestSnapshotS3(t *testing.T, ctx context.Context) *bucket.S3 {
	s3, err := test.NewS3Client()
	require.NoError(t, err)

	exists, err := s3.BucketExists(ctx, snapshotTestBucket)
	require.NoError(t, err)
	if !exists {
		err = s3.CreateBucket(ctx, snapshotTestBucket)
		require.NoError(t, err)
	}

	return bucket.NewS3(s3, snapshotTestBucket, t.Name())
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateSnapshotFromDiskTaskPutsMeta(t *testing.T) {
	ctx := test.NewContext()

	snapshotS3 := newTestSnapshotS3(t, ctx)
	metaKey := bucket.SnapshotMetaKey("disk1", "snap1")

	disk := &types.Disk{ZoneId: "zone", DiskId: "disk1"}
	creatingAt := time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC)
	encryption := &types.EncryptionDesc{
		Mode: types.EncryptionMode_ENCRYPTION_AES_XTS,
		Key:  &types.EncryptionDesc_KeyHash{KeyHash: []byte("hash")},
	}

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	nbsFactory := nbs_mocks.NewFactoryMock()
	nbsClient := nbs_mocks.NewClientMock()
	cellSelector := cells_mocks.NewCellSelectorMock()

	cellSelector.On(
		"ReplaceZoneIdWithCellIdInDiskMeta",
		mock.Anything,
		storage,
		disk,
	).Return(disk, nil)
	nbsFactory.On("GetClient", mock.Anything, "zone").Return(nbsClient, nil)
	nbsClient.On("Describe", mock.Anything, "disk1").Return(
		nbs.DiskParams{EncryptionDesc: encryption},
		nil,
	)
	nbsClient.On(
		"CreateCheckpoint",
		mock.Anything,
		nbs.CheckpointParams{DiskID: "disk1", CheckpointID: "snap1"},
	).Return(nil)
	nbsClient.On("DeleteCheckpointData", mock.Anything, "disk1", "snap1").Return(nil)
	execCtx.On("GetTaskID").Return("create1")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	storage.On(
		"CreateSnapshot",
		mock.Anything,
		mock.MatchedBy(func(meta resources.SnapshotMeta) bool {
			return meta.ID == "snap1"
		}),
	).Return(resources.SnapshotMeta{
		ID:           "snap1",
		FolderID:     "folder",
		Disk:         disk,
		CreateTaskID: "create1",
		CreatingAt:   creatingAt,
		CreatedBy:    "user",
		Encryption:   encryption,
	}, nil)
	scheduler.On(
		"ScheduleZonalTask",
		mock.Anything,
		"dataplane.CreateSnapshotFromDisk",
		"",
		"zone",
		mock.MatchedBy(func(request *dataplane_protos.CreateSnapshotFromDiskRequest) bool {
			return request.DstSnapshotId == "snap1" &&
				request.SrcDiskCheckpointId == "snap1" &&
				request.UseS3
		}),
	).Return("dataplane1", nil)
	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"dataplane1",
	).Return(&dataplane_protos.CreateSnapshotFromDiskResponse{
		SnapshotSize:        8192,
		SnapshotStorageSize: 4096,
	}, nil)

	// The meta is in the bucket by the time the snapshot is marked ready.
	storage.On(
		"SnapshotCreated",
		mock.Anything,
		"snap1",
		"snap1",
		mock.Anything,
		uint64(8192),
		uint64(4096),
	).Run(func(mock.Arguments) {
		_, err := snapshotS3.GetObject(ctx, metaKey)
		require.NoError(t, err)
	}).Return(nil)

	task := &createSnapshotFromDiskTask{
		scheduler:  scheduler,
		storage:    storage,
		nbsFactory: nbsFactory,
		snapshotS3: snapshotS3,
		request: &protos.CreateSnapshotFromDiskRequest{
			SrcDisk:       disk,
			DstSnapshotId: "snap1",
			FolderId:      "folder",
			UseS3:         true,
		},
		state:        &protos.CreateSnapshotFromDiskTaskState{},
		cellSelector: cellSelector,
	}

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(
		t,
		storage,
		scheduler,
		execCtx,
		nbsFactory,
		nbsClient,
		cellSelector,
	)

	object, err := snapshotS3.GetObject(ctx, metaKey)
	require.NoError(t, err)

	var meta bucket.SnapshotMeta
	require.NoError(t, json.Unmarshal(object.Data, &meta))
	require.Equal(
		t,
		bucket.SnapshotMeta{
			ID:                "snap1",
			FolderID:          "folder",
			ZoneID:            "zone",
			DiskID:            "disk1",
			CheckpointID:      "snap1",
			CreateTaskID:      "create1",
			CreatingAt:        creatingAt,
			CreatedBy:         "user",
			Size:              8192,
			StorageSize:       4096,
			EncryptionMode:    uint32(types.EncryptionMode_ENCRYPTION_AES_XTS),
			EncryptionKeyHash: []byte("hash"),
		},
		meta,
	)
}
