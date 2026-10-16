package images

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
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
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
	var scheduledMetaKey string
	var scheduledMeta []byte

	storage.On("GetImageMeta", mock.Anything, "image1").Return(image, nil)
	storage.On("ImageBackupCompleted", mock.Anything, "image1").Return(nil)
	storage.On(
		"RemoveImageFromBackupQueue",
		mock.Anything,
		"image1",
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
		0, // uploadBytesPerSecond
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	task := &backupImageTask{
		scheduler: scheduler,
		storage:   storage,
		backupS3:  backupS3,
		request: &protos.BackupImageRequest{
			ImageId:  "image1",
			BackupId: "attempt1",
		},
		state: &protos.BackupImageTaskState{},
	}

	// What the copy does last: the meta is in the follower before the wait
	// ends.
	putMeta(t, ctx, backupS3, backup.ImageMetaKey("image1"), "image1")

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, "dataplane1", task.state.DataplaneTaskID)
	require.Equal(t, task.state.EncryptedDek, scheduledDEK)
	execCtx.AssertNumberOfCalls(t, "SaveState", 2)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)

	// The task only passes the meta to the copy.
	require.Equal(t, backup.ImageMetaKey("image1"), scheduledMetaKey)

	var meta backup.ImageMeta
	require.NoError(t, json.Unmarshal(scheduledMeta, &meta))
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
		0, // uploadBytesPerSecond
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	preset, err := backupS3.NewEncryptedDEK()
	require.NoError(t, err)

	storage.On("GetImageMeta", mock.Anything, "image1").Return(image, nil)
	storage.On("ImageBackupCompleted", mock.Anything, "image1").Return(nil)
	storage.On(
		"RemoveImageFromBackupQueue",
		mock.Anything,
		"image1",
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
		request: &protos.BackupImageRequest{
			ImageId:  "image1",
			BackupId: "attempt1",
		},
		state: &protos.BackupImageTaskState{
			EncryptedDek: preset,
		},
	}

	// What the copy does last: the meta is in the follower before the wait
	// ends.
	putMeta(t, ctx, backupS3, backup.ImageMetaKey("image1"), "image1")

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

	storage.On("GetImageMeta", mock.Anything, "image1").Return(image, nil)
	storage.On("ImageBackupCompleted", mock.Anything, "image1").Return(nil)
	storage.On(
		"RemoveImageFromBackupQueue",
		mock.Anything,
		"image1",
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
			return request.SnapshotId == "image1" &&
				len(request.EncryptedDek) == 0 &&
				request.MetaKey == backup.ImageMetaKey("image1") &&
				len(request.Meta) != 0
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
		request: &protos.BackupImageRequest{
			ImageId:  "image1",
			BackupId: "attempt1",
		},
		state: &protos.BackupImageTaskState{},
	}

	// What the copy does last: the meta is in the follower before the wait
	// ends.
	putMeta(t, ctx, backupS3, backup.ImageMetaKey("image1"), "image1")

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Empty(t, task.state.EncryptedDek)
	execCtx.AssertNumberOfCalls(t, "SaveState", 1)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)

}

func TestBackupImageTaskCancelsItsCopy(t *testing.T) {
	ctx := test.NewContext()

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	// The copy clears its chunks in its own Cancel; the image task only
	// cancels it and leaves the queue.
	scheduler.On("CancelTask", mock.Anything, "dataplane1").Return(true, nil)
	storage.On(
		"RemoveImageFromBackupQueue",
		mock.Anything,
		"image1",
		"attempt1",
	).Return(nil)

	task := &backupImageTask{
		scheduler: scheduler,
		storage:   storage,
		request: &protos.BackupImageRequest{
			ImageId:  "image1",
			BackupId: "attempt1",
		},
		state: &protos.BackupImageTaskState{DataplaneTaskID: "dataplane1"},
	}

	err := task.Cancel(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
}

func TestBackupImageTaskLeavesQueueForMissingImage(t *testing.T) {
	ctx := test.NewContext()

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	storage.On("GetImageMeta", mock.Anything, "image1").Return(
		(*resources.ImageMeta)(nil),
		nil,
	)
	storage.On(
		"RemoveImageFromBackupQueue",
		mock.Anything,
		"image1",
		"attempt1",
	).Return(nil)

	task := &backupImageTask{
		scheduler: scheduler,
		storage:   storage,
		request: &protos.BackupImageRequest{
			ImageId:  "image1",
			BackupId: "attempt1",
		},
		state: &protos.BackupImageTaskState{},
	}

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
	scheduler.AssertNotCalled(
		t,
		"ScheduleTask",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	)
}

func TestBackupImageTaskSkipsBackedUpImage(t *testing.T) {
	ctx := test.NewContext()

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	storage.On("GetImageMeta", mock.Anything, "image1").Return(
		&resources.ImageMeta{ID: "image1", Ready: true, BackupCompleted: true},
		nil,
	)
	storage.On(
		"RemoveImageFromBackupQueue",
		mock.Anything,
		"image1",
		"attempt1",
	).Return(nil)

	task := &backupImageTask{
		scheduler: scheduler,
		storage:   storage,
		request: &protos.BackupImageRequest{
			ImageId:  "image1",
			BackupId: "attempt1",
		},
		state: &protos.BackupImageTaskState{},
	}

	err := task.Run(ctx, execCtx)
	require.NoError(t, err)
	mock.AssertExpectationsForObjects(t, storage, scheduler)
	scheduler.AssertNotCalled(
		t,
		"ScheduleTask",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	)
}

func TestBackupImageTaskDoesNotMarkBackupWithoutMeta(t *testing.T) {
	ctx := test.NewContext()

	s3, err := test.NewS3Client()
	require.NoError(t, err)

	exists, err := s3.BucketExists(ctx, backupTestBucket)
	require.NoError(t, err)
	if !exists {
		err = s3.CreateBucket(ctx, backupTestBucket)
		require.NoError(t, err)
	}

	image := &resources.ImageMeta{ID: "image1", Ready: true}

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()

	storage.On("GetImageMeta", mock.Anything, "image1").Return(image, nil)
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

	task := &backupImageTask{
		scheduler: scheduler,
		storage:   storage,
		backupS3:  backupS3,
		request: &protos.BackupImageRequest{
			ImageId:  "image1",
			BackupId: "attempt1",
		},
		state: &protos.BackupImageTaskState{},
	}

	// Not marked and not removed from the queue: the storage mock expects
	// neither.
	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
}

////////////////////////////////////////////////////////////////////////////////

// Writes the meta of an image to the follower, as the copy does last: under a
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

	data, err := json.Marshal(backup.ImageMeta{ID: id})
	require.NoError(t, err)

	err = backupS3.PutObject(ctx, key, dek, persistence.S3Object{Data: data})
	require.NoError(t, err)
}
