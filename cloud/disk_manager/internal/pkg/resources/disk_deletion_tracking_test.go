package resources

import (
	"context"
	"fmt"
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

			state, err = storage.(*storageYDB).loadDiskCleanupState(ctx, tx, diskID)
			if err != nil {
				return err
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
	cleanupID string,
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

			err = storage.CompleteDiskCleanup(ctx, tx, diskID, cleanupID)
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

func TestTrackedDiskDeletionCompletesOnLastRelease(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	actual, err := storage.DeleteDisk(ctx, "disk", "delete", time.Now())
	require.NoError(t, err)
	require.Nil(t, actual)

	_, err = storage.CreateDisk(ctx, DiskMeta{
		ID:            "disk",
		CreateRequest: &wrappers.UInt64Value{Value: 1},
		CreateTaskID:  "create",
	})
	require.Error(t, err)

	// Only the last cleanup reservation completes tombstone deletion.
	err = storage.DiskDeleted(ctx, "disk", time.Now())
	require.NoError(t, err)
	require.True(t, diskIsTombstone(readTrackedDisk(t, ctx, db, storage, "disk")))

	for _, id := range []string{"first", "second", "first"} {
		prepared, err := storage.PrepareDiskCleanup(ctx, "disk", "delete", id)
		require.NoError(t, err)
		require.True(t, prepared)
	}

	state := readTrackedDisk(t, ctx, db, storage, "disk")
	require.True(t, diskIsTombstone(state))
	require.Equal(t, `["first","second"]`, state.cleanupTaskIDs)
	require.False(t, state.deletedAt.After(time.Unix(0, 0)))

	// Reservation is released only together with the transaction.
	err = completeDiskCleanup(ctx, db, storage, "disk", "first", false)
	require.NoError(t, err)
	require.Equal(
		t,
		`["first","second"]`,
		readTrackedDisk(t, ctx, db, storage, "disk").cleanupTaskIDs,
	)

	err = completeDiskCleanup(ctx, db, storage, "disk", "first", true)
	require.NoError(t, err)

	state = readTrackedDisk(t, ctx, db, storage, "disk")
	require.True(t, diskIsTombstone(state))
	require.Equal(t, `["second"]`, state.cleanupTaskIDs)

	beforeCompletion := time.Now()
	for _, id := range []string{"second", "unknown"} {
		err = completeDiskCleanup(ctx, db, storage, "disk", id, true)
		require.NoError(t, err)
	}

	state = readTrackedDisk(t, ctx, db, storage, "disk")
	require.Equal(t, diskStatusDeleted, state.status)
	require.True(t, state.tombstone)
	require.Equal(t, "[]", state.cleanupTaskIDs)
	require.False(t, state.deletedAt.Before(beforeCompletion))

	prepared, err := storage.PrepareDiskCleanup(ctx, "disk", "late", "late")
	require.NoError(t, err)
	require.False(t, prepared)

	// The task which created the tombstone may resume after another task has
	// completed the deletion. It must not clean up a completed deletion.
	prepared, err = storage.PrepareDiskCleanup(ctx, "disk", "delete", "creator")
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

func TestTrackedDeletionRejectsCompletedDisk(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	_, err := storage.CreateDisk(ctx, DiskMeta{
		ID:            "disk",
		CreateRequest: &wrappers.UInt64Value{Value: 1},
		CreateTaskID:  "create",
	})
	require.NoError(t, err)

	_, err = storage.DeleteDisk(ctx, "disk", "delete", time.Now())
	require.NoError(t, err)

	err = storage.DiskDeleted(ctx, "disk", time.Now())
	require.NoError(t, err)

	prepared, err := storage.PrepareDiskCleanup(ctx, "disk", "late", "late")
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

// Rows written before the explicit tombstone flag was added: a placeholder
// and a completed deletion look the same. Only the task which wrote the row
// may still have unfinished external cleanup.
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

			_, err := storage.DeleteDisk(ctx, "disk", "delete", time.Now())
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

			prepared, err := storage.PrepareDiskCleanup(ctx, "disk", "other", "other")
			require.NoError(t, err)
			require.False(t, prepared)

			// Task which wrote the row before the upgrade must finish cleanup.
			prepared, err = storage.PrepareDiskCleanup(ctx, "disk", "delete", "cleanup")
			require.NoError(t, err)
			require.True(t, prepared)

			state := readTrackedDisk(t, ctx, db, storage, "disk")
			require.Equal(t, diskStatusDeleted, state.status)
			require.False(t, state.tombstone)
			require.Equal(t, `["cleanup"]`, state.cleanupTaskIDs)

			err = completeDiskCleanup(ctx, db, storage, "disk", "cleanup", true)
			require.NoError(t, err)
			require.Equal(
				t,
				"[]",
				readTrackedDisk(t, ctx, db, storage, "disk").cleanupTaskIDs,
			)
		})
	}
}
