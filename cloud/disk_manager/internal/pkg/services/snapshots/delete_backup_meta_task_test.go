package snapshots

import (
	"testing"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type deleteBackupMetaTestCase struct {
	name     string
	backupID string
}

func TestDeleteBackupMetaTask(t *testing.T) {
	for _, testCase := range []deleteBackupMetaTestCase{
		{name: "no queued backup"},
		{name: "queued backup is cancelled", backupID: "backup1"},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			testDeleteBackupMetaTask(t, testCase.backupID)
		})
	}
}

func testDeleteBackupMetaTask(t *testing.T, backupID string) {
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

	metaKey := backup.SnapshotMetaKey("disk1", "snap1")
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

	var calls []string
	if len(backupID) != 0 {
		scheduler.On(
			"ScheduleTask",
			mock.Anything,
			"snapshots.BackupSnapshot",
			"",
			mock.MatchedBy(func(request *protos.BackupSnapshotRequest) bool {
				return request.SnapshotId == "snap1" &&
					request.BackupId == backupID
			}),
		).Return("backup-task", nil)
		scheduler.On(
			"CancelTask",
			mock.Anything,
			"backup-task",
		).Return(true, nil).Run(func(mock.Arguments) {
			calls = append(calls, "cancel")
		})
		scheduler.On(
			"WaitTaskEnded",
			mock.Anything,
			"backup-task",
		).Return(nil).Run(func(mock.Arguments) {
			calls = append(calls, "wait")
		})
	}

	scheduler.On(
		"ScheduleTask",
		mock.Anything,
		"dataplane.DeleteBackupSnapshotData",
		"",
		mock.MatchedBy(func(request *dataplane_protos.DeleteBackupSnapshotDataRequest) bool {
			return request.SnapshotId == "snap1"
		}),
	).Return("dataplane1", nil).Run(func(mock.Arguments) {
		calls = append(calls, "delete")
	})
	scheduler.On(
		"WaitTask",
		mock.Anything,
		execCtx,
		"dataplane1",
	).Return(&empty.Empty{}, nil)
	storage.On(
		"GetSnapshotBackupDeleteQueue",
		mock.Anything,
		10,
	).Return([]resources.SnapshotBackupID{{
		DiskID:     "disk1",
		SnapshotID: "snap1",
		BackupID:   backupID,
	}}, nil).Once()
	storage.On(
		"GetSnapshotBackupDeleteQueue",
		mock.Anything,
		10,
	).Return([]resources.SnapshotBackupID{}, nil).Once()
	storage.On(
		"SnapshotBackupDeletionsCompleted",
		mock.Anything,
		[]string{"snap1"},
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

	if len(backupID) != 0 {
		require.Equal(t, []string{"cancel", "wait", "delete"}, calls)
	} else {
		require.Equal(t, []string{"delete"}, calls)
	}

	_, err = s3.GetObject(ctx, backupTestBucket, backupS3.Key(metaKey))
	require.Error(t, err)
}
