package resources

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func readTrackedDisk(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	storage Storage,
	diskID string,
) diskState {

	t.Helper()

	var state diskState
	err := db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			tx, err := session.BeginRWTransaction(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)

			state, err = storage.(*storageYDB).getDiskState(ctx, tx, diskID)
			if err != nil {
				// Disk may already be collected by ClearDeletedDisks;
				// getDiskState commits the tx in that case.
				state = diskState{}
				return nil
			}

			return tx.Commit(ctx)
		},
	)
	require.NoError(t, err)
	return state
}

func completeDiskCleanup(
	ctx context.Context,
	db *persistence.YDBClient,
	storage Storage,
	diskID string,
	cleanupTaskID string,
	commit bool,
) error {

	return db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			tx, err := session.BeginRWTransaction(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)

			err = storage.CompleteDiskCleanup(ctx, tx, diskID, cleanupTaskID)
			if err != nil || !commit {
				return err
			}

			return tx.Commit(ctx)
		},
	)
}

func executeRW(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	run func(context.Context, *persistence.Session) (persistence.Result, error),
) {

	t.Helper()

	err := db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			_, err := run(ctx, session)
			return err
		},
	)
	require.NoError(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func TestTombstoneCompletesOnLastCleanupRelease(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	actual, err := storage.DeleteDisk(ctx, "disk", "delete", "delete", time.Now())
	require.NoError(t, err)
	require.Nil(t, actual)

	_, err = storage.CreateDisk(ctx, DiskMeta{
		ID:            "disk",
		CreateRequest: &wrappers.UInt64Value{Value: 1},
		CreateTaskID:  "create",
	})
	require.Error(t, err)

	// With cleanup holders, DiskDeleted must not finalize the tombstone.
	prepared, err := storage.PrepareDiskCleanup(ctx, "disk", "holder")
	require.NoError(t, err)
	require.True(t, prepared)

	err = storage.DiskDeleted(ctx, "disk", time.Now())
	require.NoError(t, err)
	require.True(t, diskIsTombstone(readTrackedDisk(t, ctx, db, storage, "disk")))

	for _, id := range []string{"first", "second", "first"} {
		prepared, err := storage.PrepareDiskCleanup(ctx, "disk", id)
		require.NoError(t, err)
		require.True(t, prepared)
	}

	state := readTrackedDisk(t, ctx, db, storage, "disk")
	require.True(t, diskIsTombstone(state))
	require.Equal(t, `["delete","holder","first","second"]`, state.cleanupTaskIDs)
	require.False(t, state.deletedAt.After(time.Unix(0, 0)))

	// Reservation is released only together with the transaction.
	err = completeDiskCleanup(ctx, db, storage, "disk", "first", false)
	require.NoError(t, err)
	require.Equal(
		t,
		`["delete","holder","first","second"]`,
		readTrackedDisk(t, ctx, db, storage, "disk").cleanupTaskIDs,
	)

	err = completeDiskCleanup(ctx, db, storage, "disk", "first", true)
	require.NoError(t, err)

	state = readTrackedDisk(t, ctx, db, storage, "disk")
	require.True(t, diskIsTombstone(state))
	require.Equal(t, `["delete","holder","second"]`, state.cleanupTaskIDs)

	beforeCompletion := time.Now()
	for _, id := range []string{"delete", "holder", "second", "unknown"} {
		err = completeDiskCleanup(ctx, db, storage, "disk", id, true)
		require.NoError(t, err)
	}

	state = readTrackedDisk(t, ctx, db, storage, "disk")
	require.Equal(t, diskStatusDeleted, state.status)
	require.True(t, state.tombstone)
	require.Equal(t, "[]", state.cleanupTaskIDs)
	require.False(t, state.deletedAt.Before(beforeCompletion))

	prepared, err = storage.PrepareDiskCleanup(ctx, "disk", "late")
	require.NoError(t, err)
	require.False(t, prepared)

	// The task which created the tombstone may resume after another task has
	// completed the deletion. It must not clean up a completed deletion.
	prepared, err = storage.PrepareDiskCleanup(ctx, "disk", "creator")
	require.NoError(t, err)
	require.False(t, prepared)
	require.Equal(
		t,
		"[]",
		readTrackedDisk(t, ctx, db, storage, "disk").cleanupTaskIDs,
	)

	// Completed deletion is put into the deleted queue and collected.
	err = storage.ClearDeletedDisks(ctx, time.Now().Add(time.Second), 100)
	require.NoError(t, err)
	require.Empty(t, readTrackedDisk(t, ctx, db, storage, "disk").id)
}

func TestDeleteDiskWithoutCleanupMarksDeleted(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	// Without cleanupTaskID (DeleteIfNonExistent off) keep the legacy path:
	// missing disk is marked Deleted immediately, not a Deleting tombstone.
	actual, err := storage.DeleteDisk(ctx, "disk", "delete", "", time.Now())
	require.NoError(t, err)
	require.Nil(t, actual)

	state := readTrackedDisk(t, ctx, db, storage, "disk")
	require.Equal(t, diskStatusDeleted, state.status)
	require.False(t, state.tombstone)
	require.Empty(t, state.cleanupTaskIDs)
}

func TestConcurrentCleanupHoldersArePreserved(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 2)

	// Two DeleteDisk calls race on a missing ID: both must end up as holders
	// on the same tombstone (serializable RW tx must not drop either append).
	for _, id := range []string{"first", "second"} {
		wg.Add(1)
		taskID := id
		go func() {
			defer wg.Done()
			<-start
			actual, err := storage.DeleteDisk(ctx, "disk", taskID, taskID, time.Now())
			if err != nil {
				errs <- err
				return
			}
			if actual != nil {
				errs <- fmt.Errorf("DeleteDisk(%q) returned meta %+v", taskID, actual)
				return
			}
			errs <- nil
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	state := readTrackedDisk(t, ctx, db, storage, "disk")
	require.True(t, diskIsTombstone(state))
	ids, err := diskCleanupIDs(state)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"first", "second"}, ids)

	start = make(chan struct{})
	wg = sync.WaitGroup{}
	errs = make(chan error, 2)
	for _, id := range []string{"third", "fourth"} {
		wg.Add(1)
		cleanupID := id
		go func() {
			defer wg.Done()
			<-start
			prepared, err := storage.PrepareDiskCleanup(ctx, "disk", cleanupID)
			if err != nil {
				errs <- err
				return
			}
			if !prepared {
				errs <- fmt.Errorf("PrepareDiskCleanup(%q) returned false", cleanupID)
				return
			}
			errs <- nil
		}()
	}

	close(start)
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}

	ids, err = diskCleanupIDs(readTrackedDisk(t, ctx, db, storage, "disk"))
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"first", "second", "third", "fourth"}, ids)

	for _, id := range []string{"first", "second", "third", "fourth"} {
		err = completeDiskCleanup(ctx, db, storage, "disk", id, true)
		require.NoError(t, err)
	}
	require.Equal(t, diskStatusDeleted, readTrackedDisk(t, ctx, db, storage, "disk").status)
}

func TestPrepareDiskCleanupRejectsCompletedDisk(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	_, err := storage.CreateDisk(ctx, DiskMeta{
		ID:            "disk",
		CreateRequest: &wrappers.UInt64Value{Value: 1},
		CreateTaskID:  "create",
	})
	require.NoError(t, err)

	_, err = storage.DeleteDisk(ctx, "disk", "delete", "delete", time.Now())
	require.NoError(t, err)

	err = storage.DiskDeleted(ctx, "disk", time.Now())
	require.NoError(t, err)

	prepared, err := storage.PrepareDiskCleanup(ctx, "disk", "late")
	require.NoError(t, err)
	require.False(t, prepared)

	state := readTrackedDisk(t, ctx, db, storage, "disk")
	require.False(t, state.tombstone)
	require.Empty(t, state.cleanupTaskIDs)
}

func TestDiskTombstoneClassification(t *testing.T) {
	for _, tc := range []struct {
		name     string
		state    diskState
		expected bool
	}{
		{"tombstone", diskState{status: diskStatusDeleting, tombstone: true}, true},
		{"deleting disk", diskState{status: diskStatusDeleting}, false},
		{"completed tombstone", diskState{status: diskStatusDeleted, tombstone: true, deletedAt: time.Now()}, false},
		{"completed or legacy placeholder", diskState{status: diskStatusDeleted, deletedAt: time.Now()}, false},
		{"live", diskState{status: diskStatusReady}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, diskIsTombstone(tc.state))
		})
	}
}

func TestMissingDiskCreatesTombstone(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	actual, err := storage.DeleteDisk(ctx, "disk", "delete", "delete", time.Now())
	require.NoError(t, err)
	require.Nil(t, actual)

	state := readTrackedDisk(t, ctx, db, storage, "disk")
	require.True(t, diskIsTombstone(state))
	require.Equal(t, "delete", state.deleteTaskID)

	prepared, err := storage.PrepareDiskCleanup(ctx, "disk", "cleanup")
	require.NoError(t, err)
	require.True(t, prepared)

	// DiskDeleted must not finalize while cleanup holders are present.
	err = storage.DiskDeleted(ctx, "disk", time.Now())
	require.NoError(t, err)
	require.True(t, diskIsTombstone(readTrackedDisk(t, ctx, db, storage, "disk")))

	err = completeDiskCleanup(ctx, db, storage, "disk", "cleanup", true)
	require.NoError(t, err)
	require.True(t, diskIsTombstone(readTrackedDisk(t, ctx, db, storage, "disk")))

	err = completeDiskCleanup(ctx, db, storage, "disk", "delete", true)
	require.NoError(t, err)
	require.Equal(t, diskStatusDeleted, readTrackedDisk(t, ctx, db, storage, "disk").status)

	actual, err = storage.DeleteDisk(ctx, "disk2", "delete2", "delete2", time.Now())
	require.NoError(t, err)
	require.Nil(t, actual)
	require.True(t, diskIsTombstone(readTrackedDisk(t, ctx, db, storage, "disk2")))

	err = completeDiskCleanup(ctx, db, storage, "disk2", "delete2", true)
	require.NoError(t, err)
	require.Equal(t, diskStatusDeleted, readTrackedDisk(t, ctx, db, storage, "disk2").status)
}

func TestConcurrentCleanupHolders(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	actual, err := storage.DeleteDisk(ctx, "disk", "delete", "delete", time.Now())
	require.NoError(t, err)
	require.Nil(t, actual)

	const n = 8
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("holder-%d", i)
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	prepared := make([]bool, n)
	for i, id := range ids {
		wg.Add(1)
		go func(i int, id string) {
			defer wg.Done()
			prepared[i], errs[i] = storage.PrepareDiskCleanup(ctx, "disk", id)
		}(i, id)
	}
	wg.Wait()

	for i := range ids {
		require.NoError(t, errs[i], "holder %s", ids[i])
		require.True(t, prepared[i], "holder %s", ids[i])
	}

	state := readTrackedDisk(t, ctx, db, storage, "disk")
	require.True(t, diskIsTombstone(state))

	got, err := diskCleanupIDs(state)
	require.NoError(t, err)
	require.ElementsMatch(t, append([]string{"delete"}, ids...), got)
}

func TestLegacyPlaceholderNotReopenedAfterCleanupEnabled(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	// Prior delete without cleanup leaves a Deleted placeholder.
	actual, err := storage.DeleteDisk(ctx, "disk", "first", "", time.Now())
	require.NoError(t, err)
	require.Nil(t, actual)
	require.Equal(t, diskStatusDeleted, readTrackedDisk(t, ctx, db, storage, "disk").status)

	// Enabling DeleteIfNonExistent later must not reopen cleanup for that ID
	// while the placeholder still exists.
	actual, err = storage.DeleteDisk(ctx, "disk", "second", "second", time.Now())
	require.NoError(t, err)
	require.Nil(t, actual)

	state := readTrackedDisk(t, ctx, db, storage, "disk")
	require.Equal(t, diskStatusDeleted, state.status)
	require.False(t, state.tombstone)
	require.Equal(t, "first", state.deleteTaskID)
	require.Empty(t, state.cleanupTaskIDs)
}

func TestDeletedPlaceholderRetryWithoutCleanupReturnsNil(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	actual, err := storage.DeleteDisk(ctx, "disk", "cancel", "", time.Now())
	require.NoError(t, err)
	require.Nil(t, actual)

	// create Cancel retry: same task, empty cleanupTaskID must stay a no-op
	// and must not return empty-zone meta.
	actual, err = storage.DeleteDisk(ctx, "disk", "cancel", "", time.Now())
	require.NoError(t, err)
	require.Nil(t, actual)

	state := readTrackedDisk(t, ctx, db, storage, "disk")
	require.Equal(t, diskStatusDeleted, state.status)
	require.False(t, state.tombstone)
	require.Equal(t, "cancel", state.deleteTaskID)
}

func TestCompletedDiskDeleteRetryReturnsNil(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	_, err := storage.CreateDisk(ctx, DiskMeta{
		ID:            "disk",
		ZoneID:        "zone",
		CreateRequest: &wrappers.UInt64Value{Value: 1},
		CreateTaskID:  "create",
	})
	require.NoError(t, err)

	actual, err := storage.DeleteDisk(ctx, "disk", "delete", "delete", time.Now())
	require.NoError(t, err)
	require.NotNil(t, actual)

	err = storage.DiskDeleted(ctx, "disk", time.Now())
	require.NoError(t, err)

	// Same task retry after completion must not return meta again (would
	// re-run external deletion).
	actual, err = storage.DeleteDisk(ctx, "disk", "delete", "delete", time.Now())
	require.NoError(t, err)
	require.Nil(t, actual)

	state := readTrackedDisk(t, ctx, db, storage, "disk")
	require.Equal(t, diskStatusDeleted, state.status)
	require.False(t, state.tombstone)
	require.Empty(t, state.cleanupTaskIDs)
}

func TestZoneLessTombstoneCompletesWithoutDiskDeleted(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	actual, err := storage.DeleteDisk(ctx, "disk", "delete", "delete", time.Now())
	require.NoError(t, err)
	require.Nil(t, actual)
	require.True(t, diskIsTombstone(readTrackedDisk(t, ctx, db, storage, "disk")))

	// Zone-less tracked path finishes via CompleteDiskCleanup together with
	// the task commit, not via an early DiskDeleted.
	err = completeDiskCleanup(ctx, db, storage, "disk", "delete", true)
	require.NoError(t, err)

	state := readTrackedDisk(t, ctx, db, storage, "disk")
	require.Equal(t, diskStatusDeleted, state.status)
	require.True(t, state.tombstone)
	require.Equal(t, "[]", state.cleanupTaskIDs)
}

// Rows written before the explicit tombstone flag was added: a placeholder
// and a completed deletion look the same. PrepareDiskCleanup must not treat
// them as tombstones even for the task which wrote the row.
func TestLegacyDeletedDiskCleanup(t *testing.T) {
	for _, tc := range []struct {
		name          string
		createDisk    bool
		nullTimestamp bool
	}{
		{name: "legacy placeholder"},
		{name: "legacy placeholder with NULL timestamp", nullTimestamp: true},
		{name: "completed disk", createDisk: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db, storage := setupTest(t)
			defer db.Close(ctx)

			if tc.createDisk {
				_, err := storage.CreateDisk(ctx, DiskMeta{
					ID:            "disk",
					ZoneID:        "zone",
					CreateRequest: &wrappers.UInt64Value{Value: 1},
					CreateTaskID:  "create",
				})
				require.NoError(t, err)
			}

			_, err := storage.DeleteDisk(ctx, "disk", "delete", "delete", time.Now())
			require.NoError(t, err)

			if tc.createDisk {
				err = storage.DiskDeleted(ctx, "disk", time.Now())
				require.NoError(t, err)
			}

			executeRW(t, ctx, db, func(
				ctx context.Context,
				session *persistence.Session,
			) (persistence.Result, error) {

				return session.ExecuteRW(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $id as Utf8;
				declare $timestamp as Timestamp;
				declare $null_timestamp as Bool;
				declare $status as Int64;

				update disks set
					status = $status,
					tombstone = null,
					cleanup_task_ids = null,
					deleted_at = if($null_timestamp, null, $timestamp)
				where id = $id;
			`, storage.(*storageYDB).disksPath),
					persistence.ValueParam("$id", persistence.UTF8Value("disk")),
					persistence.ValueParam("$timestamp", persistence.TimestampValue(time.Now())),
					persistence.ValueParam("$null_timestamp", persistence.BoolValue(tc.nullTimestamp)),
					persistence.ValueParam("$status", persistence.Int64Value(int64(diskStatusDeleted))),
				)
			})

			for _, id := range []string{"other", "cleanup"} {
				prepared, err := storage.PrepareDiskCleanup(ctx, "disk", id)
				require.NoError(t, err)
				require.False(t, prepared)
			}

			state := readTrackedDisk(t, ctx, db, storage, "disk")
			require.Equal(t, diskStatusDeleted, state.status)
			require.False(t, state.tombstone)
			require.Empty(t, state.cleanupTaskIDs)
		})
	}
}
