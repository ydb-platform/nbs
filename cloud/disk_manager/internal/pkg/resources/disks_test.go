package resources

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/common"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	task_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

func setupTest(
	t *testing.T,
) (context.Context, *persistence.YDBClient, Storage) {

	ctx := newContext()
	db, err := newYDB(ctx)
	require.NoError(t, err)
	storage := newStorage(t, ctx, db)
	return ctx, db, storage
}

func requireDisksAreEqual(t *testing.T, expected DiskMeta, actual DiskMeta) {
	// TODO: Get rid of boilerplate.
	require.Equal(t, expected.ID, actual.ID)
	require.Equal(t, expected.ZoneID, actual.ZoneID)
	require.Equal(t, expected.SrcImageID, actual.SrcImageID)
	require.Equal(t, expected.SrcSnapshotID, actual.SrcSnapshotID)
	require.Equal(t, expected.BlocksCount, actual.BlocksCount)
	require.Equal(t, expected.BlockSize, actual.BlockSize)
	require.Equal(t, expected.Kind, actual.Kind)
	require.Equal(t, expected.CloudID, actual.CloudID)
	require.Equal(t, expected.FolderID, actual.FolderID)
	require.Equal(t, expected.PlacementGroupID, actual.PlacementGroupID)

	require.True(t, proto.Equal(expected.CreateRequest, actual.CreateRequest))
	require.Equal(t, expected.CreateTaskID, actual.CreateTaskID)
	if !expected.CreatingAt.IsZero() {
		require.WithinDuration(t, expected.CreatingAt, actual.CreatingAt, time.Microsecond)
	}
	if !expected.CreatedAt.IsZero() {
		require.WithinDuration(t, expected.CreatedAt, actual.CreatedAt, time.Microsecond)
	}
	require.Equal(t, expected.CreatedBy, actual.CreatedBy)
	require.Equal(t, expected.DeleteTaskID, actual.DeleteTaskID)
}

////////////////////////////////////////////////////////////////////////////////

func TestDisksCreateDisk(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	disk := DiskMeta{
		ID:               "disk",
		ZoneID:           "zone",
		SrcImageID:       "image",
		SrcSnapshotID:    "snapshot",
		BlocksCount:      1000000,
		BlockSize:        4096,
		Kind:             "ssd",
		CloudID:          "cloud",
		FolderID:         "folder",
		PlacementGroupID: "group",

		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)

	expected := disk
	expected.CreateRequest = nil
	requireDisksAreEqual(t, expected, *actual)
	createRequestData, err := proto.Marshal(disk.CreateRequest)
	require.NoError(t, err)
	require.Equal(t, createRequestData, actual.CreateRequestData)

	loaded, err := storage.GetDiskMeta(ctx, disk.ID)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	require.Equal(t, createRequestData, loaded.CreateRequestData)

	// Check idempotency.
	actual, err = storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)

	disk.CreatedAt = time.Now()
	err = storage.DiskCreated(ctx, disk)
	require.NoError(t, err)

	// Check idempotency.
	err = storage.DiskCreated(ctx, disk)
	require.NoError(t, err)

	// Check idempotency.
	actual, err = storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)

	expected = disk
	expected.CreateRequest = nil
	requireDisksAreEqual(t, expected, *actual)

	disk.CreateTaskID = "other"
	actual, err = storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.Nil(t, actual)
}

func TestDisksDeleteDisk(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	disk := DiskMeta{
		ID: "disk",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)

	expected := disk
	expected.CreateRequest = nil
	expected.DeleteTaskID = "delete"

	actual, err = storage.DeleteDisk(ctx, disk.ID, "delete", time.Now())
	require.NoError(t, err)
	requireDisksAreEqual(t, expected, *actual)

	disk.CreatedAt = time.Now()
	err = storage.DiskCreated(ctx, disk)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	// Check idempotency.
	actual, err = storage.DeleteDisk(ctx, disk.ID, "delete", time.Now())
	require.NoError(t, err)
	requireDisksAreEqual(t, expected, *actual)

	_, err = storage.CreateDisk(ctx, disk)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.DiskDeleted(ctx, disk.ID, time.Now())
	require.NoError(t, err)

	// Check that DeleteDisk returns nil for already deleted disk.
	actual, err = storage.DeleteDisk(ctx, disk.ID, "delete", time.Now())
	require.NoError(t, err)
	require.Nil(t, actual)

	_, err = storage.CreateDisk(ctx, disk)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.DiskCreated(ctx, disk)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
}

func TestDisksDeleteNonexistentDisk(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	disk := DiskMeta{
		ID:           "disk",
		DeleteTaskID: "delete",
	}

	deletingAt := time.Now()
	actual, err := storage.DeleteDisk(ctx, disk.ID, "delete", deletingAt)
	require.NoError(t, err)
	require.Nil(t, actual)

	created := disk
	created.CreatedAt = time.Now()
	err = storage.DiskCreated(ctx, created)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	// Check idempotency.
	deletingAt = deletingAt.Add(time.Second)
	actual, err = storage.DeleteDisk(ctx, disk.ID, "delete", deletingAt)
	require.NoError(t, err)
	require.Nil(t, actual)

	_, err = storage.CreateDisk(ctx, disk)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))
}

func TestDisksClearDeletedDisks(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	deletedAt := time.Now()
	deletedBefore := deletedAt.Add(-time.Microsecond)

	err := storage.ClearDeletedDisks(ctx, deletedBefore, 10)
	require.NoError(t, err)

	disk := DiskMeta{
		ID: "disk",
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)

	_, err = storage.DeleteDisk(ctx, disk.ID, "delete", deletedAt)
	require.NoError(t, err)

	err = storage.DiskDeleted(ctx, disk.ID, deletedAt)
	require.NoError(t, err)

	err = storage.ClearDeletedDisks(ctx, deletedBefore, 10)
	require.NoError(t, err)

	_, err = storage.CreateDisk(ctx, disk)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	deletedBefore = deletedAt.Add(time.Microsecond)
	err = storage.ClearDeletedDisks(ctx, deletedBefore, 10)
	require.NoError(t, err)

	actual, err = storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)
}

func TestDisksGetDisk(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	diskID := t.Name()

	d, err := storage.GetDiskMeta(ctx, diskID)
	require.NoError(t, err)
	require.Nil(t, d)

	disk := DiskMeta{
		ID:               diskID,
		ZoneID:           "zone",
		SrcImageID:       "image",
		SrcSnapshotID:    "snapshot",
		BlocksCount:      1000000,
		BlockSize:        4096,
		Kind:             "ssd",
		CloudID:          "cloud",
		FolderID:         "folder",
		PlacementGroupID: "group",

		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)

	disk.CreateRequest = nil

	d, err = storage.GetDiskMeta(ctx, diskID)
	require.NoError(t, err)
	require.NotNil(t, d)
	requireDisksAreEqual(t, disk, *d)
}

func TestDiskScanned(t *testing.T) {
	ctx, _, storage := setupTest(t)

	diskID := "disk"

	err := storage.DiskScanned(ctx, diskID, time.Now(), false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	disk := DiskMeta{
		ID: diskID,
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
	}

	actual, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)

	err = storage.DiskScanned(ctx, diskID, time.Now(), false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()))

	err = storage.DiskCreated(ctx, *actual)
	require.NoError(t, err)

	require.False(t, actual.ScanFoundBrokenBlobs)

	scannedAt := time.Now()
	err = storage.DiskScanned(ctx, diskID, scannedAt, true)
	require.NoError(t, err)

	actual, err = storage.GetDiskMeta(ctx, diskID)
	require.NoError(t, err)
	require.NotNil(t, actual)

	require.WithinDuration(t, scannedAt, actual.ScannedAt, time.Microsecond)
	require.True(t, actual.ScanFoundBrokenBlobs)

	scannedAt = time.Now()
	err = storage.DiskScanned(ctx, diskID, scannedAt, false)
	require.NoError(t, err)

	actual, err = storage.GetDiskMeta(ctx, diskID)
	require.NoError(t, err)
	require.NotNil(t, actual)

	require.WithinDuration(t, scannedAt, actual.ScannedAt, time.Microsecond)
	require.False(t, actual.ScanFoundBrokenBlobs)
}

func TestDiskRelocated(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	diskID := t.Name()
	oldZoneID := "zone"
	dstZoneID := "other"

	diskRelocated := func(ctx context.Context, diskID string, srcZoneID, dstZoneID string) error {
		return db.Execute(
			ctx,
			func(ctx context.Context, session *persistence.Session) error {
				tx, err := session.BeginRWTransaction(ctx)
				if err != nil {
					return err
				}
				defer tx.Rollback(ctx)

				err = storage.DiskRelocated(
					ctx,
					tx,
					diskID,
					srcZoneID,
					dstZoneID,
					"",
				)
				if err != nil {
					return err
				}

				return tx.Commit(ctx)
			},
		)
	}

	err := diskRelocated(ctx, diskID, oldZoneID, dstZoneID)
	require.Error(t, err)
	require.ErrorContains(t, err, "unable to find disk")

	diskMeta := DiskMeta{
		ID:     diskID,
		ZoneID: oldZoneID,
		CreateRequest: &wrappers.UInt64Value{
			Value: 1,
		},
		CreateTaskID: "create",
		CreatingAt:   time.Now(),
		CreatedBy:    "user",
	}

	actual, err := storage.CreateDisk(ctx, diskMeta)
	require.NoError(t, err)
	require.NotNil(t, actual)

	err = diskRelocated(ctx, diskID, "another", dstZoneID)
	require.Error(t, err)
	require.ErrorContains(
		t,
		err,
		"Disk has been already relocated by another competing task.",
	)

	err = diskRelocated(ctx, diskID, oldZoneID, dstZoneID)
	require.NoError(t, err)

	actual, err = storage.GetDiskMeta(ctx, diskID)
	require.NoError(t, err)
	require.Equal(t, dstZoneID, actual.ZoneID)
}

func TestExclusiveDiskOperationBlocksConflictingChanges(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	disk := DiskMeta{
		ID:            t.Name(),
		ZoneID:        "cell",
		CreateTaskID:  "create-task",
		CreatingAt:    time.Now(),
		CreateRequest: &wrappers.UInt64Value{Value: 1},
	}

	_, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	disk.CreatedAt = time.Now()
	require.NoError(t, storage.DiskCreated(ctx, disk))
	operation := LockDiskInput{
		DiskID: disk.ID,
		TaskID: "exclusive-task",
	}

	meta, err := lockDisk(t, ctx, db, storage, operation)
	require.NoError(t, err)
	require.Equal(t, operation.TaskID, meta.LockTaskID)

	_, err = lockDisk(t, ctx, db, storage, operation)
	require.NoError(t, err)
	_, err = lockDisk(t, ctx, db, storage, LockDiskInput{
		DiskID: disk.ID,
		TaskID: "another-task",
	})
	require.ErrorIs(t, err, errors.NewInterruptExecutionError())

	_, err = storage.IncrementFillGeneration(ctx, disk.ID, "another-task")
	require.ErrorIs(t, err, ErrDiskOperationInProgress)
	_, err = storage.DeleteDisk(ctx, disk.ID, "delete-task", time.Now())
	require.ErrorIs(t, err, errors.NewInterruptExecutionError())
	relocate := func(taskID string) error {
		return db.Execute(
			ctx,
			func(ctx context.Context, session *persistence.Session) error {
				tx, err := session.BeginRWTransaction(ctx)
				if err != nil {
					return err
				}

				defer tx.Rollback(ctx)
				err = storage.DiskRelocated(
					ctx, tx, disk.ID, "cell", "target-cell", taskID,
				)
				if err != nil {
					return err
				}

				return tx.Commit(ctx)
			},
		)
	}

	err = relocate("another-task")
	require.ErrorIs(t, err, errors.NewInterruptExecutionError())
	meta, err = storage.GetDiskMeta(ctx, disk.ID)
	require.NoError(t, err)
	require.Empty(t, meta.DeleteTaskID)
	require.Equal(t, "cell", meta.ZoneID)

	require.NoError(t, unlockDisk(
		ctx, db, storage, disk.ID, "another-task",
	))
	meta, err = storage.GetDiskMeta(ctx, disk.ID)
	require.NoError(t, err)
	require.Equal(t, operation.TaskID, meta.LockTaskID)
	require.NoError(t, unlockDisk(
		ctx, db, storage, disk.ID, operation.TaskID,
	))
	next := LockDiskInput{DiskID: disk.ID, TaskID: "another-task"}
	_, err = lockDisk(t, ctx, db, storage, next)
	require.NoError(t, err)
	require.NoError(t, relocate(next.TaskID))
	require.NoError(t, unlockDisk(
		ctx, db, storage, disk.ID, next.TaskID,
	))
	meta, err = storage.GetDiskMeta(ctx, disk.ID)
	require.NoError(t, err)
	require.Equal(t, "target-cell", meta.ZoneID)
	_, err = lockDisk(t, ctx, db, storage, operation)
	require.NoError(t, err)
	require.NoError(t, unlockDisk(
		ctx, db, storage, disk.ID, operation.TaskID,
	))
	_, err = storage.DeleteDisk(ctx, disk.ID, "delete-task", time.Now())
	require.NoError(t, err)
	_, err = lockDisk(t, ctx, db, storage, LockDiskInput{
		DiskID: disk.ID,
		TaskID: "new-task",
	})
	require.Error(t, err)
}

func lockDisk(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	storage Storage,
	operation LockDiskInput,
) (*DiskMeta, error) {

	t.Helper()
	var meta *DiskMeta
	err := db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			tx, err := session.BeginRWTransaction(ctx)
			if err != nil {
				return err
			}

			defer tx.Rollback(ctx)
			meta, err = storage.LockDisk(ctx, tx, operation)
			if err != nil {
				return err
			}

			return tx.Commit(ctx)
		},
	)
	return meta, err
}

func TestDiskOperationReservationRollsBackAfterTaskCancellation(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	disk := DiskMeta{
		ID:            t.Name(),
		ZoneID:        "cell",
		CreateTaskID:  "create-task",
		CreatingAt:    time.Now(),
		CreateRequest: &wrappers.UInt64Value{Value: 1},
	}

	_, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	disk.CreatedAt = time.Now()
	require.NoError(t, storage.DiskCreated(ctx, disk))
	folder := "tasks/" + t.Name()
	config := &tasks_config.TasksConfig{StorageFolder: &folder}
	require.NoError(t, task_storage.CreateYDBTables(
		ctx, config, db, false,
	))
	tasks, err := task_storage.NewStorage(config, empty.NewRegistry(), db)
	require.NoError(t, err)
	now := time.Now()
	taskID, err := tasks.CreateTask(ctx, task_storage.TaskState{
		IdempotencyKey: t.Name(),
		TaskType:       "disks.ResizeDisk",
		CreatedAt:      now,
		ModifiedAt:     now,
		Status:         task_storage.TaskStatusReadyToRun,
		Dependencies:   common.NewStringSet(),
	})
	require.NoError(t, err)
	stale, err := tasks.LockTaskToRun(
		ctx,
		task_storage.TaskInfo{ID: taskID},
		now,
		"worker-a",
		"run",
	)
	require.NoError(t, err)
	_, err = tasks.MarkForCancellation(ctx, taskID, now)
	require.NoError(t, err)
	current, err := tasks.GetTask(ctx, taskID)
	require.NoError(t, err)
	cancel, err := tasks.LockTaskToCancel(
		ctx,
		task_storage.TaskInfo{
			ID:           taskID,
			GenerationID: current.GenerationID,
		},
		now,
		"worker-b",
		"cancel",
	)
	require.NoError(t, err)
	cancel.Status = task_storage.TaskStatusCancelled
	_, err = tasks.UpdateTaskWithPreparation(
		ctx,
		cancel,
		func(ctx context.Context, tx *persistence.Transaction) error {
			return storage.UnlockDisk(ctx, tx, disk.ID, taskID)
		},
	)
	require.NoError(t, err)
	prepared := false
	_, err = tasks.UpdateTaskWithPreparation(
		ctx,
		stale,
		func(ctx context.Context, tx *persistence.Transaction) error {
			_, err := storage.LockDisk(ctx, tx, LockDiskInput{
				DiskID: disk.ID,
				TaskID: taskID,
			})
			prepared = err == nil
			return err
		},
	)
	require.True(t, prepared)
	require.ErrorIs(t, err, errors.NewWrongGenerationError())
	meta, err := storage.GetDiskMeta(ctx, disk.ID)
	require.NoError(t, err)
	require.Empty(t, meta.LockTaskID)
	current, err = tasks.GetTask(ctx, taskID)
	require.NoError(t, err)
	require.Equal(t, task_storage.TaskStatusCancelled, current.Status)
}

func unlockDisk(
	ctx context.Context,
	db *persistence.YDBClient,
	storage Storage,
	diskID, taskID string,
) error {

	return db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			tx, err := session.BeginRWTransaction(ctx)
			if err != nil {
				return err
			}

			defer tx.Rollback(ctx)
			if err := storage.UnlockDisk(
				ctx, tx, diskID, taskID,
			); err != nil {

				return err
			}

			return tx.Commit(ctx)
		},
	)
}

func TestDiskOperationsAreExclusive(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	disk := DiskMeta{
		ID:            t.Name(),
		ZoneID:        "cell",
		CreateTaskID:  "create-task",
		CreatingAt:    time.Now(),
		CreateRequest: &wrappers.UInt64Value{Value: 1},
	}

	_, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	disk.CreatedAt = time.Now()
	require.NoError(t, storage.DiskCreated(ctx, disk))
	operations := []LockDiskInput{
		{DiskID: disk.ID, TaskID: "resize"},
		{DiskID: disk.ID, TaskID: "alter"},
		{DiskID: disk.ID, TaskID: "migrate"},
	}

	previousTaskID := ""
	for i, operation := range operations {
		meta, err := lockDisk(t, ctx, db, storage, operation)
		require.NoError(t, err)
		require.Equal(t, operation.TaskID, meta.LockTaskID)
		_, err = lockDisk(t, ctx, db, storage, operation)
		require.NoError(t, err)

		if previousTaskID != "" {
			// A repeated old cleanup must not release the new owner's lock.
			require.NoError(t, unlockDisk(
				ctx, db, storage, disk.ID, previousTaskID,
			))
		}

		next := operations[(i+1)%len(operations)]
		_, err = lockDisk(t, ctx, db, storage, next)
		require.ErrorIs(t, err, errors.NewInterruptExecutionError())
		meta, err = storage.GetDiskMeta(ctx, disk.ID)
		require.NoError(t, err)
		require.Equal(t, operation.TaskID, meta.LockTaskID)
		require.NoError(t, unlockDisk(
			ctx, db, storage, disk.ID, operation.TaskID,
		))
		require.NoError(t, unlockDisk(
			ctx, db, storage, disk.ID, operation.TaskID,
		))
		previousTaskID = operation.TaskID
	}

	meta, err := storage.GetDiskMeta(ctx, disk.ID)
	require.NoError(t, err)
	require.Empty(t, meta.LockTaskID)
}

func TestDiskLockAllowsOwnerMigrationUpdates(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	disk := DiskMeta{
		ID:            t.Name(),
		ZoneID:        "cell",
		CreateTaskID:  "create-task",
		CreatingAt:    time.Now(),
		CreateRequest: &wrappers.UInt64Value{Value: 1},
	}

	_, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	disk.CreatedAt = time.Now()
	require.NoError(t, storage.DiskCreated(ctx, disk))
	operation := LockDiskInput{
		DiskID: disk.ID,
		TaskID: "migration-task",
	}

	_, err = lockDisk(t, ctx, db, storage, operation)
	require.NoError(t, err)
	generation, err := storage.IncrementFillGeneration(
		ctx, disk.ID, operation.TaskID,
	)
	require.NoError(t, err)
	require.EqualValues(t, 1, generation)
	meta, err := storage.GetDiskMeta(ctx, disk.ID)
	require.NoError(t, err)
	require.Equal(t, operation.TaskID, meta.LockTaskID)

	err = db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			tx, err := session.BeginRWTransaction(ctx)
			if err != nil {
				return err
			}

			defer tx.Rollback(ctx)
			err = storage.DiskRelocated(
				ctx, tx, disk.ID, "cell", "target-cell", operation.TaskID,
			)
			if err != nil {
				return err
			}

			err = storage.UnlockDisk(ctx, tx, disk.ID, operation.TaskID)
			if err != nil {
				return err
			}

			return tx.Commit(ctx)
		},
	)
	require.NoError(t, err)
	meta, err = storage.GetDiskMeta(ctx, disk.ID)
	require.NoError(t, err)
	require.Equal(t, "target-cell", meta.ZoneID)
	require.Empty(t, meta.LockTaskID)
	require.EqualValues(t, 1, meta.FillGeneration)

	// Legacy execution remains allowed when no task holds a lock.
	generation, err = storage.IncrementFillGeneration(ctx, disk.ID, "")
	require.NoError(t, err)
	require.EqualValues(t, 2, generation)
}

func TestLegacyDiskUpdatesIgnoreExclusiveLock(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	disk := DiskMeta{
		ID:            t.Name(),
		ZoneID:        "cell",
		CreateTaskID:  "create-task",
		CreatingAt:    time.Now(),
		CreateRequest: &wrappers.UInt64Value{Value: 1},
	}

	_, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	disk.CreatedAt = time.Now()
	require.NoError(t, storage.DiskCreated(ctx, disk))
	owner := LockDiskInput{DiskID: disk.ID, TaskID: "guarded-task"}
	_, err = lockDisk(t, ctx, db, storage, owner)
	require.NoError(t, err)

	generation, err := storage.IncrementFillGeneration(ctx, disk.ID, "")
	require.NoError(t, err)
	require.EqualValues(t, 1, generation)
	err = db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			tx, err := session.BeginRWTransaction(ctx)
			if err != nil {
				return err
			}

			defer tx.Rollback(ctx)
			err = storage.DiskRelocated(
				ctx, tx, disk.ID, "cell", "target-cell", "",
			)
			if err != nil {
				return err
			}

			return tx.Commit(ctx)
		},
	)
	require.NoError(t, err)
	meta, err := storage.GetDiskMeta(ctx, disk.ID)
	require.NoError(t, err)
	require.Equal(t, "target-cell", meta.ZoneID)
	require.EqualValues(t, 1, meta.FillGeneration)
	require.Equal(t, owner.TaskID, meta.LockTaskID)
	require.NoError(t, unlockDisk(
		ctx, db, storage, disk.ID, owner.TaskID,
	))
}

func TestGuardedDiskUpdatesRequireLock(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	disk := DiskMeta{
		ID:            t.Name(),
		ZoneID:        "cell",
		CreateTaskID:  "create-task",
		CreatingAt:    time.Now(),
		CreateRequest: &wrappers.UInt64Value{Value: 1},
	}

	_, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	disk.CreatedAt = time.Now()
	require.NoError(t, storage.DiskCreated(ctx, disk))

	_, err = storage.IncrementFillGeneration(
		ctx, disk.ID, "guarded-task",
	)
	require.ErrorIs(t, err, ErrDiskOperationInProgress)
	err = db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			tx, err := session.BeginRWTransaction(ctx)
			if err != nil {
				return err
			}

			defer tx.Rollback(ctx)
			return storage.DiskRelocated(
				ctx, tx, disk.ID, "cell", "target-cell", "guarded-task",
			)
		},
	)
	require.ErrorIs(t, err, errors.NewInterruptExecutionError())
	meta, err := storage.GetDiskMeta(ctx, disk.ID)
	require.NoError(t, err)
	require.Equal(t, "cell", meta.ZoneID)
	require.Zero(t, meta.FillGeneration)
	require.Empty(t, meta.LockTaskID)
}
