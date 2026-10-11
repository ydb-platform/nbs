package snapshots

import (
	"bytes"
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

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, "dataplane1", task.state.DataplaneTaskID)
	require.Equal(t, task.state.EncryptedDek, scheduledDEK)
	execCtx.AssertNumberOfCalls(t, "SaveState", 2)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)

	// The copy writes the meta while it holds the source; the task only
	// passes it.
	require.Equal(t, backup.SnapshotMetaKey("disk1", "snap1"), scheduledMetaKey)
	_, err = backupS3.GetObject(ctx, scheduledMetaKey)
	require.Error(t, err)

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

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Empty(t, task.state.EncryptedDek)
	execCtx.AssertNumberOfCalls(t, "SaveState", 1)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)

	_, err = backupS3.GetObject(ctx, backup.SnapshotMetaKey("disk1", "snap1"))
	require.Error(t, err)
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
