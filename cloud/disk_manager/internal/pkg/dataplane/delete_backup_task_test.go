package dataplane

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func TestDeleteBackupTask(t *testing.T) {
	ctx := test.NewContext()

	storage, closeFunc := newStorage(t, ctx)
	defer closeFunc()

	slave := newBackupTestSlave(t, ctx)

	err := slave.S3.PutObject(
		ctx,
		slave.Bucket,
		slave.Key("chunks/task.snap1.0"),
		persistence.S3Object{Data: []byte("abc")},
	)
	require.NoError(t, err)

	entries := []snapshot_storage.BackupDeletingEntry{
		{Object: "chunks/task.snap1.0", Slave: slave.ID},
		// Deleting a missing object is not an error.
		{Object: "snapshots/-/snap1/map.bin", Slave: slave.ID},
	}
	err = storage.EnqueueBackupDeleting(ctx, entries)
	require.NoError(t, err)

	task := &deleteBackupTask{
		storage:   storage,
		slaves:    backup.Slaves{slave.ID: slave},
		batchSize: 10,
		state:     &protos.DeleteBackupTaskState{},
	}

	execCtx := mocks.NewExecutionContextMock()

	err = task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))

	_, err = slave.S3.GetObject(ctx, slave.Bucket, slave.Key("chunks/task.snap1.0"))
	require.Error(t, err)

	deleting, err := storage.GetBackupDeleting(ctx, 10)
	require.NoError(t, err)
	require.Empty(t, deleting)
}
