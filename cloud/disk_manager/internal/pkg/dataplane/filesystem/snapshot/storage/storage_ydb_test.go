package storage

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

////////////////////////////////////////////////////////////////////////////////

func newYDB(ctx context.Context) (*persistence.YDBClient, error) {
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
	)
	database := "/Root"
	rootPath := "disk_manager"
	connectionTimeout := "10s"

	return persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint:          &endpoint,
			Database:          &database,
			RootPath:          &rootPath,
			ConnectionTimeout: &connectionTimeout,
		},
		metrics.NewEmptyRegistry(),
	)
}

func newStorage(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	storageFolder string,
) Storage {

	err := schema.Create(ctx, storageFolder, db, false)
	require.NoError(t, err)

	storage := NewStorage(db, storageFolder)
	require.NotNil(t, storage)

	return storage
}

////////////////////////////////////////////////////////////////////////////////

type fixture struct {
	t             *testing.T
	ctx           context.Context
	cancel        context.CancelFunc
	db            *persistence.YDBClient
	storageFolder string
	storage       Storage
}

func (f *fixture) teardown() {
	err := f.db.Close(f.ctx)
	require.NoError(f.t, err)
	f.cancel()
}

func createFixture(t *testing.T) *fixture {
	ctx, cancel := context.WithCancel(test.NewContext())

	db, err := newYDB(ctx)
	require.NoError(t, err)

	storageFolder := fmt.Sprintf(
		"filesystem_snapshot_storage_ydb_test/%v",
		t.Name(),
	)
	storage := newStorage(t, ctx, db, storageFolder)

	return &fixture{
		t:             t,
		ctx:           ctx,
		cancel:        cancel,
		db:            db,
		storageFolder: storageFolder,
		storage:       storage,
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateFilesystemSnapshot(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	filesystem := &types.Filesystem{
		ZoneId:       "zone",
		FilesystemId: "filesystem",
	}

	snapshotMeta := FilesystemSnapshotMeta{
		ID:           "snapshot",
		Filesystem:   filesystem,
		CreateTaskID: "create",
	}

	created, err := f.storage.CreateFilesystemSnapshot(f.ctx, snapshotMeta)
	require.NoError(t, err)
	require.NotNil(t, created)
	require.False(t, created.Ready)
	require.Equal(t, snapshotMeta.ID, created.ID)
	require.Equal(t, snapshotMeta.Filesystem.ZoneId, created.Filesystem.ZoneId)
	require.Equal(
		t,
		snapshotMeta.Filesystem.FilesystemId, created.Filesystem.FilesystemId,
	)
	require.Equal(t, snapshotMeta.CreateTaskID, created.CreateTaskID)

	// Check idempotency of Create - calling again should succeed
	created2, err := f.storage.CreateFilesystemSnapshot(f.ctx, snapshotMeta)
	require.NoError(t, err)
	require.NotNil(t, created2)
	require.False(t, created2.Ready)
	require.Equal(t, created.ID, created2.ID)

	err = f.storage.FilesystemSnapshotCreated(f.ctx, "snapshot", 100, 200, 5)
	require.NoError(t, err)

	// Check idempotency of FilesystemSnapshotCreated - calling again should succeed
	err = f.storage.FilesystemSnapshotCreated(f.ctx, "snapshot", 100, 200, 5)
	require.NoError(t, err)

	retrieved, err := f.storage.GetFilesystemSnapshotMeta(f.ctx, "snapshot")
	require.NoError(t, err)
	require.NotNil(t, retrieved)
	require.True(t, retrieved.Ready)
	require.Equal(t, uint64(100), retrieved.Size)
	require.Equal(t, uint64(200), retrieved.StorageSize)
	require.Equal(t, uint32(5), retrieved.ChunkCount)

	// Check idempotency of Create after snapshot is ready
	created3, err := f.storage.CreateFilesystemSnapshot(f.ctx, snapshotMeta)
	require.NoError(t, err)
	require.NotNil(t, created3)
	require.True(t, created3.Ready)
	require.Equal(t, created.ID, created3.ID)
}

func TestDeleteFilesystemSnapshot(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	snapshotMeta := FilesystemSnapshotMeta{
		ID:           "snapshot",
		CreateTaskID: "create",
		Filesystem: &types.Filesystem{
			ZoneId:       "zone",
			FilesystemId: "fs",
		},
	}

	created, err := f.storage.CreateFilesystemSnapshot(f.ctx, snapshotMeta)
	require.NoError(t, err)
	require.Equal(t, snapshotMeta.ID, created.ID)

	deleting, err := f.storage.DeletingFilesystemSnapshot(
		f.ctx,
		snapshotMeta.ID,
	)
	require.NoError(t, err)
	require.NotNil(t, deleting)
	require.Equal(t, snapshotMeta.ID, deleting.ID)

	// Check idempotency of DeletingFilesystemSnapshot
	deleting2, err := f.storage.DeletingFilesystemSnapshot(
		f.ctx,
		snapshotMeta.ID,
	)
	require.NoError(t, err)
	require.NotNil(t, deleting2)
	require.Equal(t, snapshotMeta.ID, deleting2.ID)

	// Check that we can't create after deleting
	_, err = f.storage.CreateFilesystemSnapshot(f.ctx, snapshotMeta)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())
}

func TestDeleteNonexistingFilesystemSnapshot(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	snapshotID := "nonexisting"

	deleting, err := f.storage.DeletingFilesystemSnapshot(
		f.ctx,
		snapshotID,
	)
	require.NoError(t, err)
	require.NotNil(t, deleting)
	require.Equal(t, snapshotID, deleting.ID)
}

func TestGetFilesystemSnapshotsToDelete(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	// Create and mark for deletion
	_, err := f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           "snapshot1",
			CreateTaskID: "create1",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "filesystem",
			},
		},
	)
	require.NoError(t, err)

	_, err = f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           "snapshot2",
			CreateTaskID: "create2",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "filesystem2",
			},
		},
	)
	require.NoError(t, err)

	_, err = f.storage.DeletingFilesystemSnapshot(f.ctx, "snapshot1")
	require.NoError(t, err)

	_, err = f.storage.DeletingFilesystemSnapshot(f.ctx, "snapshot2")
	require.NoError(t, err)

	// Get snapshots to delete
	keys, err := f.storage.GetFilesystemSnapshotsToDelete(
		f.ctx,
		time.Now().Add(time.Hour),
		10,
	)
	require.NoError(t, err)
	require.Len(t, keys, 2)

	// Clear deleting snapshots
	err = f.storage.ClearDeletingFilesystemSnapshots(f.ctx, keys)
	require.NoError(t, err)

	// Verify they're gone
	keys, err = f.storage.GetFilesystemSnapshotsToDelete(
		f.ctx,
		time.Now().Add(time.Hour),
		10,
	)
	require.NoError(t, err)
	require.Len(t, keys, 0)
}

func TestCheckFilesystemSnapshotAlive(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	// Non-existing snapshot
	err := f.storage.CheckFilesystemSnapshotAlive(f.ctx, "nonexisting")
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())

	// Create snapshot
	_, err = f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           "snapshot",
			CreateTaskID: "create",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "fs",
			},
		},
	)
	require.NoError(t, err)

	// Should be alive
	err = f.storage.CheckFilesystemSnapshotAlive(f.ctx, "snapshot")
	require.NoError(t, err)

	// Mark for deletion
	_, err = f.storage.DeletingFilesystemSnapshot(f.ctx, "snapshot")
	require.NoError(t, err)

	// Should not be alive
	err = f.storage.CheckFilesystemSnapshotAlive(f.ctx, "snapshot")
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())
}

func TestCheckFilesystemSnapshotReady(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	err := f.storage.CheckFilesystemSnapshotReady(f.ctx, "nonexisting")
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())

	_, err = f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           "snapshot",
			CreateTaskID: "create",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "fs",
			},
		},
	)
	require.NoError(t, err)

	err = f.storage.CheckFilesystemSnapshotReady(f.ctx, "snapshot")
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())

	err = f.storage.FilesystemSnapshotCreated(f.ctx, "snapshot", 100, 200, 5)
	require.NoError(t, err)

	err = f.storage.CheckFilesystemSnapshotReady(f.ctx, "snapshot")
	require.NoError(t, err)

	_, err = f.storage.DeletingFilesystemSnapshot(f.ctx, "snapshot")
	require.NoError(t, err)

	err = f.storage.CheckFilesystemSnapshotReady(f.ctx, "snapshot")
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())
}

func TestCheckFilesystemSnapshotReadyWithCancellationDoesNotReturnNonRetriableError(
	t *testing.T,
) {
	f := createFixture(t)
	defer f.teardown()

	snapshotID := "snapshot"
	created, err := f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           snapshotID,
			CreateTaskID: "create",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "fs",
			},
		},
	)
	require.NoError(t, err)
	require.NotNil(t, created)
	require.False(t, created.Ready)

	err = f.storage.FilesystemSnapshotCreated(f.ctx, snapshotID, 100, 200, 5)
	require.NoError(t, err)

	err = f.storage.CheckFilesystemSnapshotReady(f.ctx, snapshotID)
	require.NoError(t, err)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < 200; i++ {
		runCtx, cancel := context.WithCancel(f.ctx)
		delay := time.Duration(rng.Intn(50)+1) * time.Millisecond
		timer := time.AfterFunc(delay, cancel)

		err = f.storage.CheckFilesystemSnapshotReady(runCtx, snapshotID)

		timer.Stop()
		cancel()

		if err == nil {
			continue
		}

		require.False(
			t,
			errors.Is(err, errors.NewEmptyNonRetriableError()),
			"iteration %v returned non-retriable error after cancelling after %v: %v",
			i,
			delay,
			err,
		)
		require.True(
			t,
			errors.Is(err, context.Canceled) ||
				errors.Is(err, errors.NewEmptyRetriableError()),
			"iteration %v returned unexpected error after cancelling after %v: %v",
			i,
			delay,
			err,
		)
	}
}

func TestLockUnlockFilesystemSnapshot(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	snapshotID := t.Name()
	_, err := f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           snapshotID,
			CreateTaskID: "create",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "fs",
			},
		},
	)
	require.NoError(t, err)

	err = f.storage.LockFilesystemSnapshot(f.ctx, snapshotID, "lock1")
	require.NoError(t, err)

	err = f.storage.LockFilesystemSnapshot(f.ctx, snapshotID, "lock2")
	require.NoError(t, err)

	_, err = f.storage.DeletingFilesystemSnapshot(f.ctx, snapshotID)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyRetriableError())

	err = f.storage.UnlockFilesystemSnapshot(f.ctx, snapshotID, "lock2")
	require.NoError(t, err)

	// Unlocking one task does not remove the other task's lock.
	_, err = f.storage.DeletingFilesystemSnapshot(f.ctx, snapshotID)
	require.Error(t, err)
	require.ErrorIs(t, err, errors.NewEmptyRetriableError())

	err = f.storage.UnlockFilesystemSnapshot(f.ctx, snapshotID, "lock1")
	require.NoError(t, err)

	deleting, err := f.storage.DeletingFilesystemSnapshot(
		f.ctx,
		snapshotID,
	)
	require.NoError(t, err)
	require.NotNil(t, deleting)

	err = f.storage.LockFilesystemSnapshot(f.ctx, snapshotID, "lock3")
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())

	err = f.storage.UnlockFilesystemSnapshot(f.ctx, snapshotID, "lock3")
	require.NoError(t, err)
}

func TestLockUnlockFilesystemSnapshotAreIdempotent(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	snapshotID := t.Name()
	const lockTaskID = "lock"
	_, err := f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           snapshotID,
			CreateTaskID: "create",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "fs",
			},
		},
	)
	require.NoError(t, err)

	err = f.storage.LockFilesystemSnapshot(
		f.ctx,
		snapshotID,
		lockTaskID,
	)
	require.NoError(t, err)

	err = f.storage.LockFilesystemSnapshot(
		f.ctx,
		snapshotID,
		lockTaskID,
	)
	require.NoError(t, err)

	err = f.storage.UnlockFilesystemSnapshot(
		f.ctx,
		snapshotID,
		lockTaskID,
	)
	require.NoError(t, err)

	err = f.storage.UnlockFilesystemSnapshot(
		f.ctx,
		snapshotID,
		lockTaskID,
	)
	require.NoError(t, err)

	// Repeated locking by the same task creates one lock, and repeated unlocking
	// is a no-op, so deletion is allowed.
	_, err = f.storage.DeletingFilesystemSnapshot(f.ctx, snapshotID)
	require.NoError(t, err)
}

func TestFilesystemSnapshotAdditionalLockIsIdempotent(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	snapshotID := t.Name()
	const firstLockTaskID = "lock-1"
	const secondLockTaskID = "lock-2"
	_, err := f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           snapshotID,
			CreateTaskID: "create",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "fs",
			},
		},
	)
	require.NoError(t, err)

	err = f.storage.LockFilesystemSnapshot(
		f.ctx,
		snapshotID,
		firstLockTaskID,
	)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		err = f.storage.LockFilesystemSnapshot(
			f.ctx,
			snapshotID,
			secondLockTaskID,
		)
		require.NoError(t, err)
	}

	err = f.storage.UnlockFilesystemSnapshot(
		f.ctx,
		snapshotID,
		firstLockTaskID,
	)
	require.NoError(t, err)

	deleting, err := f.storage.DeletingFilesystemSnapshot(
		f.ctx,
		snapshotID,
	)
	require.Nil(t, deleting)
	require.ErrorIs(t, err, errors.NewEmptyRetriableError())

	err = f.storage.UnlockFilesystemSnapshot(
		f.ctx,
		snapshotID,
		secondLockTaskID,
	)
	require.NoError(t, err)

	// Repeated locking by the second task must require only one unlock before
	// the snapshot is unlocked.
	deleting, err = f.storage.DeletingFilesystemSnapshot(
		f.ctx,
		snapshotID,
	)
	require.NoError(t, err)
	require.NotNil(t, deleting)
}

func TestEmptyFilesystemSnapshotLockTaskIDIsRejected(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	snapshotID := t.Name()
	_, err := f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           snapshotID,
			CreateTaskID: "create",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "fs",
			},
		},
	)
	require.NoError(t, err)

	err = f.storage.LockFilesystemSnapshot(f.ctx, snapshotID, "")
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())

	err = f.storage.UnlockFilesystemSnapshot(f.ctx, snapshotID, "")
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())

	// Invalid lock operations must not create a lock.
	_, err = f.storage.DeletingFilesystemSnapshot(f.ctx, snapshotID)
	require.NoError(t, err)
}

func TestConcurrentFilesystemSnapshotLockPromotionAndIdempotentLock(
	t *testing.T,
) {
	f := createFixture(t)
	defer f.teardown()

	const attempts = 10
	for i := 0; i < attempts; i++ {
		snapshotID := fmt.Sprintf("%s-%d", t.Name(), i)
		firstLockTaskID := fmt.Sprintf("lock-1-%d", i)
		secondLockTaskID := fmt.Sprintf("lock-2-%d", i)

		_, err := f.storage.CreateFilesystemSnapshot(
			f.ctx,
			FilesystemSnapshotMeta{
				ID:           snapshotID,
				CreateTaskID: "create",
				Filesystem: &types.Filesystem{
					ZoneId:       "zone",
					FilesystemId: "fs",
				},
			},
		)
		require.NoError(t, err, "attempt %v", i)

		err = f.storage.LockFilesystemSnapshot(
			f.ctx,
			snapshotID,
			firstLockTaskID,
		)
		require.NoError(t, err, "attempt %v", i)

		err = f.storage.LockFilesystemSnapshot(
			f.ctx,
			snapshotID,
			secondLockTaskID,
		)
		require.NoError(t, err, "attempt %v", i)

		start := make(chan struct{})
		unlockResultCh := make(chan error, 1)
		lockResultCh := make(chan error, 1)

		go func() {
			<-start
			unlockResultCh <- f.storage.UnlockFilesystemSnapshot(
				f.ctx,
				snapshotID,
				firstLockTaskID,
			)
		}()
		go func() {
			<-start
			lockResultCh <- f.storage.LockFilesystemSnapshot(
				f.ctx,
				snapshotID,
				secondLockTaskID,
			)
		}()

		close(start)
		require.NoError(t, <-unlockResultCh, "attempt %v", i)
		require.NoError(t, <-lockResultCh, "attempt %v", i)

		deleting, err := f.storage.DeletingFilesystemSnapshot(
			f.ctx,
			snapshotID,
		)
		require.Nil(t, deleting, "attempt %v", i)
		require.ErrorIs(
			t,
			err,
			errors.NewEmptyRetriableError(),
			"attempt %v",
			i,
		)

		// A concurrent idempotent lock must not duplicate the promoted lock. One
		// unlock removes it.
		err = f.storage.UnlockFilesystemSnapshot(
			f.ctx,
			snapshotID,
			secondLockTaskID,
		)
		require.NoError(t, err, "attempt %v", i)

		deleting, err = f.storage.DeletingFilesystemSnapshot(
			f.ctx,
			snapshotID,
		)
		require.NoError(t, err, "attempt %v", i)
		require.NotNil(t, deleting, "attempt %v", i)
	}
}

func TestDeleteFilesystemSnapshotAfterLastUnlock(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	snapshotID := t.Name()
	const firstLockTaskID = "lock-1"
	const secondLockTaskID = "lock-2"
	_, err := f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           snapshotID,
			CreateTaskID: "create",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "fs",
			},
		},
	)
	require.NoError(t, err)

	err = f.storage.LockFilesystemSnapshot(
		f.ctx,
		snapshotID,
		firstLockTaskID,
	)
	require.NoError(t, err)

	err = f.storage.LockFilesystemSnapshot(
		f.ctx,
		snapshotID,
		secondLockTaskID,
	)
	require.NoError(t, err)

	err = f.storage.UnlockFilesystemSnapshot(
		f.ctx,
		snapshotID,
		firstLockTaskID,
	)
	require.NoError(t, err)

	// Deletion remains blocked until every task unlocks the snapshot.
	deleting, err := f.storage.DeletingFilesystemSnapshot(
		f.ctx,
		snapshotID,
	)
	require.Nil(t, deleting)
	require.ErrorIs(t, err, errors.NewEmptyRetriableError())

	err = f.storage.UnlockFilesystemSnapshot(
		f.ctx,
		snapshotID,
		secondLockTaskID,
	)
	require.NoError(t, err)

	deleting, err = f.storage.DeletingFilesystemSnapshot(
		f.ctx,
		snapshotID,
	)
	require.NoError(t, err)
	require.NotNil(t, deleting)
}

func TestConcurrentLockFilesystemSnapshotAndDelete(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	type deleteResult struct {
		meta *FilesystemSnapshotMeta
		err  error
	}

	const attempts = 10
	for i := 0; i < attempts; i++ {
		snapshotID := fmt.Sprintf("%s-%d", t.Name(), i)
		lockTaskID := fmt.Sprintf("lock-%v", i)
		_, err := f.storage.CreateFilesystemSnapshot(
			f.ctx,
			FilesystemSnapshotMeta{
				ID:           snapshotID,
				CreateTaskID: "create",
				Filesystem: &types.Filesystem{
					ZoneId:       "zone",
					FilesystemId: "fs",
				},
			},
		)
		require.NoError(t, err, "attempt %v", i)

		start := make(chan struct{})
		lockResultCh := make(chan error, 1)
		deleteResultCh := make(chan deleteResult, 1)

		go func() {
			<-start
			lockResultCh <- f.storage.LockFilesystemSnapshot(
				f.ctx,
				snapshotID,
				lockTaskID,
			)
		}()
		go func() {
			<-start
			meta, err := f.storage.DeletingFilesystemSnapshot(
				f.ctx,
				snapshotID,
			)
			deleteResultCh <- deleteResult{meta: meta, err: err}
		}()

		close(start)
		lockErr := <-lockResultCh
		deleteResult := <-deleteResultCh

		if deleteResult.err == nil {
			// Deletion serialized first, so the snapshot can no longer be locked.
			// Unlocking it remains an idempotent no-op.
			require.NotNil(t, deleteResult.meta, "attempt %v", i)
			require.ErrorIs(
				t,
				lockErr,
				errors.NewEmptyNonRetriableError(),
				"attempt %v",
				i,
			)

			err = f.storage.UnlockFilesystemSnapshot(
				f.ctx,
				snapshotID,
				lockTaskID,
			)
			require.NoError(t, err, "attempt %v", i)
			continue
		}

		// Locking serialized first, so deletion must wait for that lock.
		require.Nil(t, deleteResult.meta, "attempt %v", i)
		require.ErrorIs(
			t,
			deleteResult.err,
			errors.NewEmptyRetriableError(),
			"attempt %v",
			i,
		)
		require.NoError(t, lockErr, "attempt %v", i)

		err = f.storage.UnlockFilesystemSnapshot(
			f.ctx,
			snapshotID,
			lockTaskID,
		)
		require.NoError(t, err, "attempt %v", i)

		deleting, err := f.storage.DeletingFilesystemSnapshot(
			f.ctx,
			snapshotID,
		)
		require.NoError(t, err, "attempt %v", i)
		require.NotNil(t, deleting, "attempt %v", i)
	}
}

func TestConcurrentUnlockAndDeleteFilesystemSnapshotWithSecondLockHolder(
	t *testing.T,
) {
	f := createFixture(t)
	defer f.teardown()

	const attempts = 10
	for i := 0; i < attempts; i++ {
		snapshotID := fmt.Sprintf("%s-%d", t.Name(), i)
		firstLockTaskID := fmt.Sprintf("lock-1-%v", i)
		secondLockTaskID := fmt.Sprintf("lock-2-%v", i)
		_, err := f.storage.CreateFilesystemSnapshot(
			f.ctx,
			FilesystemSnapshotMeta{
				ID:           snapshotID,
				CreateTaskID: "create",
				Filesystem: &types.Filesystem{
					ZoneId:       "zone",
					FilesystemId: "fs",
				},
			},
		)
		require.NoError(t, err, "attempt %v", i)

		err = f.storage.LockFilesystemSnapshot(
			f.ctx,
			snapshotID,
			firstLockTaskID,
		)
		require.NoError(t, err)

		err = f.storage.LockFilesystemSnapshot(
			f.ctx,
			snapshotID,
			secondLockTaskID,
		)
		require.NoError(t, err, "attempt %v", i)

		start := make(chan struct{})
		unlockResultCh := make(chan error, 1)
		deleteResultCh := make(chan error, 1)

		go func() {
			<-start
			unlockResultCh <- f.storage.UnlockFilesystemSnapshot(
				f.ctx,
				snapshotID,
				firstLockTaskID,
			)
		}()
		go func() {
			<-start
			_, err := f.storage.DeletingFilesystemSnapshot(
				f.ctx,
				snapshotID,
			)
			deleteResultCh <- err
		}()

		close(start)
		require.NoError(t, <-unlockResultCh, "attempt %v", i)
		require.ErrorIs(
			t,
			<-deleteResultCh,
			errors.NewEmptyRetriableError(),
			"attempt %v",
			i,
		)

		err = f.storage.UnlockFilesystemSnapshot(
			f.ctx,
			snapshotID,
			secondLockTaskID,
		)
		require.NoError(t, err, "attempt %v", i)

		deleting, err := f.storage.DeletingFilesystemSnapshot(
			f.ctx,
			snapshotID,
		)
		require.NoError(t, err, "attempt %v", i)
		require.NotNil(t, deleting, "attempt %v", i)
	}
}

func TestLockUnlockNonexistingFilesystemSnapshot(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	snapshotID := t.Name()

	err := f.storage.LockFilesystemSnapshot(
		f.ctx,
		snapshotID,
		"lock",
	)
	require.ErrorIs(t, err, errors.NewEmptyNonRetriableError())

	err = f.storage.UnlockFilesystemSnapshot(
		f.ctx,
		snapshotID,
		"lock",
	)
	require.NoError(t, err)
}

func TestListFilesystemSnapshots(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	// Initially empty
	snapshots, err := f.storage.ListFilesystemSnapshots(f.ctx)
	require.NoError(t, err)
	require.Empty(t, snapshots)

	// Create snapshots
	_, err = f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           "snapshot1",
			CreateTaskID: "create1",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "filesystem1",
			},
		},
	)
	require.NoError(t, err)

	_, err = f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           "snapshot2",
			CreateTaskID: "create2",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "filesystem2",
			},
		},
	)
	require.NoError(t, err)

	// Not ready yet, should be empty
	snapshots, err = f.storage.ListFilesystemSnapshots(f.ctx)
	require.NoError(t, err)
	require.Empty(t, snapshots)

	// Mark as created
	err = f.storage.FilesystemSnapshotCreated(f.ctx, "snapshot1", 0, 0, 0)
	require.NoError(t, err)

	err = f.storage.FilesystemSnapshotCreated(f.ctx, "snapshot2", 0, 0, 0)
	require.NoError(t, err)

	// Should list both
	snapshots, err = f.storage.ListFilesystemSnapshots(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 2, snapshots.Size())
	require.True(t, snapshots.Has("snapshot1"))
	require.True(t, snapshots.Has("snapshot2"))

	// Mark one for deletion
	_, err = f.storage.DeletingFilesystemSnapshot(f.ctx, "snapshot1")
	require.NoError(t, err)

	// Should only list the ready one
	snapshots, err = f.storage.ListFilesystemSnapshots(f.ctx)
	require.NoError(t, err)
	require.Equal(t, 1, snapshots.Size())
	require.True(t, snapshots.Has("snapshot2"))
}

func TestGetFilesystemSnapshotCount(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	// Initially zero
	count, err := f.storage.GetFilesystemSnapshotCount(f.ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(0), count)

	// Create and mark as ready
	_, err = f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           "snapshot1",
			CreateTaskID: "create1",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "filesystem1",
			},
		},
	)
	require.NoError(t, err)

	err = f.storage.FilesystemSnapshotCreated(f.ctx, "snapshot1", 0, 0, 0)
	require.NoError(t, err)

	count, err = f.storage.GetFilesystemSnapshotCount(f.ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), count)

	// Create another
	_, err = f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           "snapshot2",
			CreateTaskID: "create2",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "filesystem2",
			},
		},
	)
	require.NoError(t, err)

	err = f.storage.FilesystemSnapshotCreated(f.ctx, "snapshot2", 0, 0, 0)
	require.NoError(t, err)

	count, err = f.storage.GetFilesystemSnapshotCount(f.ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), count)
}

func TestGetTotalFilesystemSnapshotSizes(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	// Initially zero
	size, err := f.storage.GetTotalFilesystemSnapshotSize(f.ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(0), size)

	storageSize, err := f.storage.GetTotalFilesystemSnapshotStorageSize(f.ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(0), storageSize)

	// Create snapshots with different sizes
	_, err = f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           "snapshot1",
			CreateTaskID: "create1",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "filesystem1",
			},
		},
	)
	require.NoError(t, err)

	err = f.storage.FilesystemSnapshotCreated(f.ctx, "snapshot1", 100, 50, 0)
	require.NoError(t, err)

	size, err = f.storage.GetTotalFilesystemSnapshotSize(f.ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(100), size)

	storageSize, err = f.storage.GetTotalFilesystemSnapshotStorageSize(f.ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(50), storageSize)

	// Add another
	_, err = f.storage.CreateFilesystemSnapshot(
		f.ctx,
		FilesystemSnapshotMeta{
			ID:           "snapshot2",
			CreateTaskID: "create2",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "filesystem2",
			},
		},
	)
	require.NoError(t, err)

	err = f.storage.FilesystemSnapshotCreated(f.ctx, "snapshot2", 200, 75, 0)
	require.NoError(t, err)

	size, err = f.storage.GetTotalFilesystemSnapshotSize(f.ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(300), size)

	storageSize, err = f.storage.GetTotalFilesystemSnapshotStorageSize(f.ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(125), storageSize)
}

func TestGetFilesystemSnapshotMeta(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	// Non-existing snapshot
	meta, err := f.storage.GetFilesystemSnapshotMeta(f.ctx, "nonexisting")
	require.NoError(t, err)
	require.Nil(t, meta)

	// Create snapshot
	filesystem := &types.Filesystem{
		ZoneId:       "zone",
		FilesystemId: "filesystem",
	}

	snapshotMeta := FilesystemSnapshotMeta{
		ID:           "snapshot",
		Filesystem:   filesystem,
		CreateTaskID: "create",
	}

	_, err = f.storage.CreateFilesystemSnapshot(f.ctx, snapshotMeta)
	require.NoError(t, err)

	// Get metadata
	meta, err = f.storage.GetFilesystemSnapshotMeta(f.ctx, "snapshot")
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.Equal(t, snapshotMeta.ID, meta.ID)
	require.Equal(t, snapshotMeta.Filesystem.ZoneId, meta.Filesystem.ZoneId)
	require.Equal(
		t,
		snapshotMeta.Filesystem.FilesystemId, meta.Filesystem.FilesystemId,
	)
	require.Equal(t, snapshotMeta.CreateTaskID, meta.CreateTaskID)
	require.False(t, meta.Ready)

	// Mark as created
	err = f.storage.FilesystemSnapshotCreated(f.ctx, "snapshot", 100, 50, 10)
	require.NoError(t, err)

	// Get updated metadata
	meta, err = f.storage.GetFilesystemSnapshotMeta(f.ctx, "snapshot")
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.True(t, meta.Ready)
	require.Equal(t, uint64(100), meta.Size)
	require.Equal(t, uint64(50), meta.StorageSize)
	require.Equal(t, uint32(10), meta.ChunkCount)
}
