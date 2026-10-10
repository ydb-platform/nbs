package images

import (
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteBackupMetaTask(t *testing.T) {
	ctx := test.NewContext()

	s3, err := test.NewS3Client()
	require.NoError(t, err)

	exists, err := s3.BucketExists(ctx, backupTestBucket)
	require.NoError(t, err)
	if !exists {
		err = s3.CreateBucket(ctx, backupTestBucket)
		require.NoError(t, err)
	}

	backupS3, err := backup.NewS3(
		s3,
		backupTestBucket,
		t.Name(),
		"kek1",
		make([]byte, 32),
	)
	require.NoError(t, err)

	encryptedDEK, err := backupS3.NewEncryptedDEK()
	require.NoError(t, err)
	metaKey := backup.ImageMetaKey("image1")

	err = backupS3.PutObject(
		ctx,
		metaKey,
		encryptedDEK,
		persistence.S3Object{Data: []byte("{}")},
	)
	require.NoError(t, err)

	storage := resources_mocks.NewStorageMock()
	scheduler := tasks_mocks.NewSchedulerMock()
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("delete-meta")
	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.DeleteBackupSnapshotData",
		"",
		mock.MatchedBy(func(request *dataplane_protos.DeleteBackupSnapshotDataRequest) bool {
			return request.SnapshotId == "image1"
		}),
	).Return("dataplane1", nil)
	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"dataplane1",
	).Return(&empty.Empty{}, nil)
	storage.On(
		"GetImageBackupDeleteQueue",
		mock.Anything,
		10,
	).Return([]string{"image1"}, nil).Once()
	storage.On(
		"GetImageBackupDeleteQueue",
		mock.Anything,
		10,
	).Return([]string{}, nil).Once()
	storage.On(
		"ImageBackupDeletionsCompleted",
		mock.Anything,
		[]string{"image1"},
	).Return(nil)

	task := &deleteBackupMetaTask{
		scheduler: scheduler,
		storage:   storage,
		backupS3:  backupS3,
		batchSize: 10,
	}

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)

	_, err = s3.GetObject(ctx, backupTestBucket, backupS3.Key(metaKey))
	require.Error(t, err)
}
