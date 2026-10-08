package images

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
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

const backupTestBucket = "images-backup"

func TestBackupImageTask(t *testing.T) {
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
	image := &resources.ImageMeta{
		ID:            "image1",
		FolderID:      "folder",
		SrcSnapshotID: "snap1",
		CreateTaskID:  "task1",
		CreatingAt:    creatingAt,
		Size:          8192,
		StorageSize:   4096,
		Ready:         true,
	}

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	var scheduledDEK []byte

	storage.On("GetImageMeta", mock.Anything, "image1").Return(image, nil)
	storage.On("ImageBackupScheduled", mock.Anything, "image1").Return(nil)
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
			return request.SnapshotId == "image1" &&
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
	)
	require.NoError(t, err)

	task := &backupImageTask{
		scheduler: scheduler,
		storage:   storage,
		backupS3:  backupS3,
		request:   &protos.BackupImageRequest{ImageId: "image1"},
		state:     &protos.BackupImageTaskState{},
	}

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, "dataplane1", task.state.DataplaneTaskID)
	require.Equal(t, task.state.EncryptedDek, scheduledDEK)
	execCtx.AssertNumberOfCalls(t, "SaveState", 2)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)

	object, err := backupS3.GetObject(
		ctx,
		backup.ImageMetaKey("image1"),
	)
	require.NoError(t, err)

	var meta backup.ImageMeta
	require.NoError(t, json.Unmarshal(object.Data, &meta))
	require.Equal(
		t,
		backup.ImageMeta{
			ID:            "image1",
			FolderID:      "folder",
			SrcSnapshotID: "snap1",
			CreateTaskID:  "task1",
			CreatingAt:    creatingAt,
			Size:          8192,
			StorageSize:   4096,
		},
		meta,
	)
}

func TestBackupImageTaskReusesEncryptedDEK(t *testing.T) {
	ctx := test.NewContext()

	s3, err := test.NewS3Client()
	require.NoError(t, err)

	exists, err := s3.BucketExists(ctx, backupTestBucket)
	require.NoError(t, err)
	if !exists {
		err = s3.CreateBucket(ctx, backupTestBucket)
		require.NoError(t, err)
	}

	image := &resources.ImageMeta{
		ID:    "image1",
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
	)
	require.NoError(t, err)

	preset, err := backupS3.NewEncryptedDEK()
	require.NoError(t, err)

	storage.On("GetImageMeta", mock.Anything, "image1").Return(image, nil)
	storage.On(
		"ImageBackupScheduled",
		mock.Anything,
		"image1",
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
			return request.SnapshotId == "image1" &&
				bytes.Equal(request.EncryptedDek, preset)
		}),
	).Return("dataplane1", nil)
	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"dataplane1",
	).Return(&empty.Empty{}, nil)

	task := &backupImageTask{
		scheduler: scheduler,
		storage:   storage,
		backupS3:  backupS3,
		request:   &protos.BackupImageRequest{ImageId: "image1"},
		state: &protos.BackupImageTaskState{
			EncryptedDek: preset,
		},
	}

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, preset, task.state.EncryptedDek)
	execCtx.AssertNumberOfCalls(t, "SaveState", 1)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}

func TestBackupImageTaskWithoutEncryption(t *testing.T) {
	ctx := test.NewContext()

	s3, err := test.NewS3Client()
	require.NoError(t, err)

	exists, err := s3.BucketExists(ctx, backupTestBucket)
	require.NoError(t, err)
	if !exists {
		err = s3.CreateBucket(ctx, backupTestBucket)
		require.NoError(t, err)
	}

	image := &resources.ImageMeta{
		ID:    "image1",
		Ready: true,
	}

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	backupS3, err := backup.NewS3(s3, backupTestBucket, t.Name(), "", nil)
	require.NoError(t, err)

	storage.On("GetImageMeta", mock.Anything, "image1").Return(image, nil)
	storage.On(
		"ImageBackupScheduled",
		mock.Anything,
		"image1",
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
			return request.SnapshotId == "image1" &&
				len(request.EncryptedDek) == 0
		}),
	).Return("dataplane1", nil)
	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"dataplane1",
	).Return(&empty.Empty{}, nil)

	task := &backupImageTask{
		scheduler: scheduler,
		storage:   storage,
		backupS3:  backupS3,
		request:   &protos.BackupImageRequest{ImageId: "image1"},
		state:     &protos.BackupImageTaskState{},
	}

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Empty(t, task.state.EncryptedDek)
	execCtx.AssertNumberOfCalls(t, "SaveState", 1)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)

	key := backup.ImageMetaKey("image1")
	object, err := backupS3.GetObject(ctx, key)
	require.NoError(t, err)

	raw, err := s3.GetObject(ctx, backupTestBucket, backupS3.Key(key))
	require.NoError(t, err)
	require.Equal(t, object.Data, raw.Data)
	require.Nil(t, raw.Metadata["Key-Id"])
	require.Nil(t, raw.Metadata["Encrypted-Dek"])
}
