package images

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

	storage.On("GetImageMeta", mock.Anything, "image1").Return(image, nil)
	storage.On("ImageBackupScheduled", mock.Anything, "image1").Return(nil)
	execCtx.On("GetTaskID").Return("backup1")
	execCtx.On("SaveState", mock.Anything).Return(nil)
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.ScheduleBackupChunksTasks",
		"",
		mock.MatchedBy(func(request *dataplane_protos.ScheduleBackupChunksTasksRequest) bool {
			return request.SnapshotId == "image1"
		}),
	).Return("dataplane1", nil)
	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"dataplane1",
	).Return(&empty.Empty{}, nil)

	followerS3 := backup.NewFollowerS3(s3, backupTestBucket, t.Name())

	task := &backupImageTask{
		scheduler:  scheduler,
		storage:    storage,
		followerS3: followerS3,
		request:    &protos.BackupImageRequest{ImageId: "image1"},
		state:      &protos.BackupImageTaskState{},
	}

	err = task.Run(ctx, execCtx)
	require.NoError(t, err)
	require.Equal(t, "dataplane1", task.state.DataplaneTaskID)
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)

	object, err := s3.GetObject(
		ctx,
		backupTestBucket,
		followerS3.Key(backup.ImageMetaKey("image1")),
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
