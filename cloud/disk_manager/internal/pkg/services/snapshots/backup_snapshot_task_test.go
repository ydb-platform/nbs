package snapshots

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
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
	var scheduledDEK []byte
	var scheduledMetaKey string
	var scheduledMeta []byte

	storage.On("GetSnapshotMeta", mock.Anything, "snap1").Return(snapshot, nil)
	storage.On("SnapshotBackupCompleted", mock.Anything, "snap1").Return(nil)
	storage.On(
		"RemoveSnapshotFromBackupQueue",
		mock.Anything,
		"snap1",
		"attempt1",
	).Return(nil)
	execCtx.On("GetTaskID").Return("backup1")
	execCtx.On("SaveState", mock.Anything).Return(nil)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.BackupSnapshotData",
		"",
		mock.MatchedBy(func(request *dataplane_protos.BackupSnapshotDataRequest) bool {
			dek := request.EncryptedDek
			scheduledDEK = append([]byte(nil), dek...)
			scheduledMetaKey = request.MetaKey
			scheduledMeta = append([]byte(nil), request.Meta...)
			return request.SnapshotId == "snap1" &&
				len(request.EncryptedDek) != 0
		}),
	).Return("dataplane1", nil)
	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"dataplane1",
	).Return(&empty.Empty{}, nil)

	backupS3, err := backup.NewS3(
		s3,
		backupTestBucket,
		t.Name(),
		"kek1",
		make([]byte, 32),
		0, // uploadBytesPerSecond
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	task := &backupSnapshotTask{
		scheduler: scheduler,
		storage:   storage,
		backupS3:  backupS3,
		request: &protos.BackupSnapshotRequest{
			SnapshotId: "snap1",
			BackupId:   "attempt1",
		},
		state: &protos.BackupSnapshotTaskState{},
	}

	// What the copy does last: the meta is in the follower before the wait
	// ends.
	putMeta(t, ctx, backupS3, backup.SnapshotMetaKey("disk1", "snap1"), "snap1")

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, "dataplane1", task.state.DataplaneTaskID)
	require.Equal(t, task.state.EncryptedDek, scheduledDEK)
	execCtx.AssertNumberOfCalls(t, "SaveState", 2)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)

	// The task only passes the meta to the copy.
	require.Equal(t, backup.SnapshotMetaKey("disk1", "snap1"), scheduledMetaKey)

	var meta backup.SnapshotMeta
	require.NoError(t, json.Unmarshal(scheduledMeta, &meta))
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

func TestBackupSnapshotTaskReusesEncryptedDEK(t *testing.T) {
	ctx := test.NewContext()

	s3, err := test.NewS3Client()
	require.NoError(t, err)

	exists, err := s3.BucketExists(ctx, backupTestBucket)
	require.NoError(t, err)
	if !exists {
		err = s3.CreateBucket(ctx, backupTestBucket)
		require.NoError(t, err)
	}

	snapshot := &resources.SnapshotMeta{
		ID:    "snap1",
		Disk:  &types.Disk{ZoneId: "zone", DiskId: "disk1"},
		Ready: true,
	}

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	backupS3, err := backup.NewS3(
		s3,
		backupTestBucket,
		t.Name(),
		"kek1",
		make([]byte, 32),
		0, // uploadBytesPerSecond
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	preset, err := backupS3.NewEncryptedDEK()
	require.NoError(t, err)

	storage.On(
		"GetSnapshotMeta",
		mock.Anything,
		"snap1",
	).Return(snapshot, nil)
	storage.On(
		"SnapshotBackupCompleted",
		mock.Anything,
		"snap1",
	).Return(nil)
	storage.On(
		"RemoveSnapshotFromBackupQueue",
		mock.Anything,
		"snap1",
		"attempt1",
	).Return(nil)
	execCtx.On("GetTaskID").Return("backup1")
	execCtx.On("SaveState", mock.Anything).Return(nil)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.BackupSnapshotData",
		"",
		mock.MatchedBy(func(
			request *dataplane_protos.BackupSnapshotDataRequest,
		) bool {
			return request.SnapshotId == "snap1" &&
				bytes.Equal(request.EncryptedDek, preset)
		}),
	).Return("dataplane1", nil)
	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"dataplane1",
	).Return(&empty.Empty{}, nil)

	task := &backupSnapshotTask{
		scheduler: scheduler,
		storage:   storage,
		backupS3:  backupS3,
		request: &protos.BackupSnapshotRequest{
			SnapshotId: "snap1",
			BackupId:   "attempt1",
		},
		state: &protos.BackupSnapshotTaskState{
			EncryptedDek: preset,
		},
	}

	// What the copy does last: the meta is in the follower before the wait
	// ends.
	putMeta(t, ctx, backupS3, backup.SnapshotMetaKey("disk1", "snap1"), "snap1")

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, preset, task.state.EncryptedDek)
	execCtx.AssertNumberOfCalls(t, "SaveState", 1)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}

func TestBackupSnapshotTaskWithoutEncryption(t *testing.T) {
	ctx := test.NewContext()

	s3, err := test.NewS3Client()
	require.NoError(t, err)

	exists, err := s3.BucketExists(ctx, backupTestBucket)
	require.NoError(t, err)
	if !exists {
		err = s3.CreateBucket(ctx, backupTestBucket)
		require.NoError(t, err)
	}

	snapshot := &resources.SnapshotMeta{
		ID:    "snap1",
		Disk:  &types.Disk{ZoneId: "zone", DiskId: "disk1"},
		Ready: true,
	}

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	backupS3, err := backup.NewS3(
		s3,
		backupTestBucket,
		t.Name(),
		"",  // kekID
		nil, // kek
		0,   // uploadBytesPerSecond
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	storage.On(
		"GetSnapshotMeta",
		mock.Anything,
		"snap1",
	).Return(snapshot, nil)
	storage.On(
		"SnapshotBackupCompleted",
		mock.Anything,
		"snap1",
	).Return(nil)
	storage.On(
		"RemoveSnapshotFromBackupQueue",
		mock.Anything,
		"snap1",
		"attempt1",
	).Return(nil)
	execCtx.On("GetTaskID").Return("backup1")
	execCtx.On("SaveState", mock.Anything).Return(nil)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.BackupSnapshotData",
		"",
		mock.MatchedBy(func(
			request *dataplane_protos.BackupSnapshotDataRequest,
		) bool {
			return request.SnapshotId == "snap1" &&
				len(request.EncryptedDek) == 0 &&
				request.MetaKey == backup.SnapshotMetaKey("disk1", "snap1") &&
				len(request.Meta) != 0
		}),
	).Return("dataplane1", nil)
	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"dataplane1",
	).Return(&empty.Empty{}, nil)

	task := &backupSnapshotTask{
		scheduler: scheduler,
		storage:   storage,
		backupS3:  backupS3,
		request: &protos.BackupSnapshotRequest{
			SnapshotId: "snap1",
			BackupId:   "attempt1",
		},
		state: &protos.BackupSnapshotTaskState{},
	}

	// What the copy does last: the meta is in the follower before the wait
	// ends.
	putMeta(t, ctx, backupS3, backup.SnapshotMetaKey("disk1", "snap1"), "snap1")

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Empty(t, task.state.EncryptedDek)
	execCtx.AssertNumberOfCalls(t, "SaveState", 1)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)

	object, err := backupS3.GetObject(ctx, backup.SnapshotMetaKey("disk1", "snap1"))
	require.NoError(t, err)

	var meta backup.SnapshotMeta
	require.NoError(t, json.Unmarshal(object.Data, &meta))
	require.Equal(t, "snap1", meta.ID)
	require.Equal(t, "disk1", meta.DiskID)
}

func TestBackupSnapshotTaskSkipsSnapshotWithoutCopy(t *testing.T) {
	for _, snapshot := range []*resources.SnapshotMeta{
		nil,
		{ID: "snap1", Ready: false},
		{ID: "snap1", Ready: true, BackupCompleted: true},
	} {
		ctx := test.NewContext()
		storage := resources_mocks.NewStorageMock()
		scheduler := tasks_mocks.NewSchedulerMock()
		execCtx := tasks_mocks.NewExecutionContextMock()

		storage.On("GetSnapshotMeta", mock.Anything, "snap1").Return(snapshot, nil)
		storage.On(
			"RemoveSnapshotFromBackupQueue",
			mock.Anything,
			"snap1",
			"attempt1",
		).Return(nil)

		task := &backupSnapshotTask{
			scheduler: scheduler,
			storage:   storage,
			request: &protos.BackupSnapshotRequest{
				SnapshotId: "snap1",
				BackupId:   "attempt1",
			},
			state: &protos.BackupSnapshotTaskState{},
		}

		err := task.Run(ctx, execCtx)
		require.NoError(t, err)
		mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
	}
}

func TestBackupSnapshotTaskCancel(t *testing.T) {
	for _, dataplaneTaskID := range []string{"", "dataplane1"} {
		ctx := test.NewContext()
		storage := resources_mocks.NewStorageMock()
		scheduler := tasks_mocks.NewSchedulerMock()
		execCtx := tasks_mocks.NewExecutionContextMock()

		if len(dataplaneTaskID) != 0 {
			scheduler.On(
				"CancelTask",
				mock.Anything,
				dataplaneTaskID,
			).Return(true, nil)
		}
		storage.On(
			"RemoveSnapshotFromBackupQueue",
			mock.Anything,
			"snap1",
			"attempt1",
		).Return(nil)

		task := &backupSnapshotTask{
			scheduler: scheduler,
			storage:   storage,
			request: &protos.BackupSnapshotRequest{
				SnapshotId: "snap1",
				BackupId:   "attempt1",
			},
			state: &protos.BackupSnapshotTaskState{
				DataplaneTaskID: dataplaneTaskID,
			},
		}

		err := task.Cancel(ctx, execCtx)
		require.NoError(t, err)
		mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
	}
}

func TestBackupSnapshotTaskDoesNotMarkBackupWithoutMeta(t *testing.T) {
	ctx := test.NewContext()

	s3, err := test.NewS3Client()
	require.NoError(t, err)

	exists, err := s3.BucketExists(ctx, backupTestBucket)
	require.NoError(t, err)
	if !exists {
		err = s3.CreateBucket(ctx, backupTestBucket)
		require.NoError(t, err)
	}

	snapshot := &resources.SnapshotMeta{
		ID:    "snap1",
		Disk:  &types.Disk{ZoneId: "zone", DiskId: "disk1"},
		Ready: true,
	}

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	storage.On("GetSnapshotMeta", mock.Anything, "snap1").Return(snapshot, nil)
	execCtx.On("GetTaskID").Return("backup1")
	execCtx.On("SaveState", mock.Anything).Return(nil)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.BackupSnapshotData",
		"",
		mock.Anything,
	).Return("dataplane1", nil)
	// The copy ends well, but leaves no meta: a copy of an older build.
	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"dataplane1",
	).Return(&empty.Empty{}, nil)

	backupS3, err := backup.NewS3(
		s3,
		backupTestBucket,
		t.Name(),
		"kek1",
		make([]byte, 32),
		0, // uploadBytesPerSecond
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	task := &backupSnapshotTask{
		scheduler: scheduler,
		storage:   storage,
		backupS3:  backupS3,
		request: &protos.BackupSnapshotRequest{
			SnapshotId: "snap1",
			BackupId:   "attempt1",
		},
		state: &protos.BackupSnapshotTaskState{},
	}

	// Not marked and not removed from the queue: the storage mock expects
	// neither.
	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}

////////////////////////////////////////////////////////////////////////////////

// Writes the meta of a snapshot to the follower, as the copy does last: under a
// DEK of its own, the kek of backupS3 opens it.
func putMeta(
	t *testing.T,
	ctx context.Context,
	backupS3 *backup.S3,
	key string,
	id string,
) {

	dek, err := backupS3.EnsureEncryptedDEK(nil)
	require.NoError(t, err)

	data, err := json.Marshal(backup.SnapshotMeta{ID: id, DiskID: "disk1"})
	require.NoError(t, err)

	err = backupS3.PutObject(ctx, key, dek, persistence.S3Object{Data: data})
	require.NoError(t, err)
}
