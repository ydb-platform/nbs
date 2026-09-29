package snapshots

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

const backupTestBucket = "snapshots-backup"

func TestBackupSnapshotTask(t *testing.T) {
	ctx := test.NewContext()

	s3, err := test.NewS3Client()
	require.NoError(t, err)

	exists, err := s3.BucketExists(ctx, backupTestBucket)
	require.NoError(t, err)
	if !exists {
		err = s3.CreateBucket(ctx, backupTestBucket)
		require.NoError(t, err)
	}

	creatingAt := time.Date(2026, 8, 31, 10, 0, 0, 0, time.UTC)
	snapshot := &resources.SnapshotMeta{
		ID:           "snap1",
		FolderID:     "folder",
		Disk:         &types.Disk{ZoneId: "zone", DiskId: "disk1"},
		CheckpointID: "cp1",
		CreateTaskID: "task1",
		CreatingAt:   creatingAt,
		CreatedBy:    "user",
		Size:         8192,
		StorageSize:  4096,
		Encryption: &types.EncryptionDesc{
			Mode: types.EncryptionMode_ENCRYPTION_AES_XTS,
			Key:  &types.EncryptionDesc_KeyHash{KeyHash: []byte("hash")},
		},
		Ready: true,
	}

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	storage.On("GetSnapshotMeta", mock.Anything, "snap1").Return(snapshot, nil)
	storage.On("SnapshotBackupScheduled", mock.Anything, "snap1").Return(nil)
	execCtx.On("GetTaskID").Return("backup1")
	execCtx.On("SaveState", mock.Anything).Return(nil)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.ScheduleBackupChunksTasks",
		"",
		mock.MatchedBy(func(request *dataplane_protos.ScheduleBackupChunksTasksRequest) bool {
			return request.SnapshotId == "snap1"
		}),
	).Return("dataplane1", nil)
	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"dataplane1",
	).Return(&empty.Empty{}, nil)

	followerS3 := backup.NewFollowerS3(s3, backupTestBucket, t.Name())

	task := &backupSnapshotTask{
		scheduler:  scheduler,
		storage:    storage,
		followerS3: followerS3,
		request:    &protos.BackupSnapshotRequest{SnapshotId: "snap1"},
		state:      &protos.BackupSnapshotTaskState{},
	}

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, "dataplane1", task.state.DataplaneTaskID)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)

	object, err := s3.GetObject(
		ctx,
		backupTestBucket,
		followerS3.Key(backup.SnapshotMetaKey("disk1", "snap1")),
	)
	require.NoError(t, err)

	var meta backup.SnapshotMeta
	require.NoError(t, json.Unmarshal(object.Data, &meta))
	require.Equal(
		t,
		backup.SnapshotMeta{
			ID:                "snap1",
			FolderID:          "folder",
			ZoneID:            "zone",
			DiskID:            "disk1",
			CheckpointID:      "cp1",
			CreateTaskID:      "task1",
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
