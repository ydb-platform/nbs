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

func readTrackedDisk(t *testing.T, ctx context.Context, db *persistence.YDBClient, storage Storage, id string) diskState {
	t.Helper()
	var state diskState
	err := db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		tx, err := session.BeginRWTransaction(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		state, err = storage.(*storageYDB).loadDiskCleanupState(ctx, tx, id)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
	require.NoError(t, err)
	return state
}

func TestTrackedDiskDeletionKeepsTombstone(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	_, err := storage.DeleteDisk(ctx, "disk", "delete", time.Now())
	require.NoError(t, err)
	for _, id := range []string{"first", "second"} {
		prepared, err := storage.PrepareLegacyDiskCleanup(ctx, "disk", id)
		require.NoError(t, err)
		require.True(t, prepared)
	}
	state := readTrackedDisk(t, ctx, db, storage, "disk")
	require.True(t, state.tombstone)
	require.Equal(t, `["first","second"]`, state.cleanupTaskIDs)
	require.Equal(t, time.Unix(0, 0).UTC(), state.deletedAt)

	// A failed task transaction must retain its reservation.
	err = db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		tx, err := session.BeginRWTransaction(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		return storage.CompleteDiskCleanup(ctx, tx, "disk", "first")
	})
	require.NoError(t, err)
	require.Equal(t, `["first","second"]`, readTrackedDisk(t, ctx, db, storage, "disk").cleanupTaskIDs)

	for _, id := range []string{"first", "second"} {
		err = db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
			tx, err := session.BeginRWTransaction(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)
			if err := storage.CompleteDiskCleanup(ctx, tx, "disk", id); err != nil {
				return err
			}
			return tx.Commit(ctx)
		})
		require.NoError(t, err)
	}
	state = readTrackedDisk(t, ctx, db, storage, "disk")
	require.Equal(t, "[]", state.cleanupTaskIDs)
	require.True(t, state.tombstone)
	require.Equal(t, time.Unix(0, 0).UTC(), state.deletedAt)
	require.NoError(t, storage.ClearDeletedDisks(ctx, time.Now(), 100))
	require.Equal(t, "disk", readTrackedDisk(t, ctx, db, storage, "disk").id)
}

func TestTrackedDeletionRejectsCompletedDisk(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	_, err := storage.CreateDisk(ctx, DiskMeta{ID: "disk", CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: "create"})
	require.NoError(t, err)
	_, err = storage.DeleteDisk(ctx, "disk", "delete", time.Now())
	require.NoError(t, err)
	require.NoError(t, storage.DiskDeleted(ctx, "disk", time.Now()))
	prepared, err := storage.PrepareLegacyDiskCleanup(ctx, "disk", "late")
	require.NoError(t, err)
	require.False(t, prepared)
	require.False(t, readTrackedDisk(t, ctx, db, storage, "disk").tombstone)
}

func TestDiskTombstoneClassification(t *testing.T) {
	for _, tc := range []struct {
		name     string
		state    diskState
		expected bool
	}{
		{"explicit tombstone", diskState{status: diskStatusDeleted, tombstone: true}, true},
		{"explicit tombstone with timestamp", diskState{status: diskStatusDeleted, tombstone: true, deletedAt: time.Now()}, true},
		{"completed", diskState{status: diskStatusDeleted, deletedAt: time.Now()}, false},
		{"legacy NULL", diskState{status: diskStatusDeleted}, true},
		{"legacy epoch", diskState{status: diskStatusDeleted, deletedAt: time.Unix(0, 0)}, true},
		{"deleting", diskState{status: diskStatusDeleting, tombstone: true}, false},
		{"live", diskState{status: diskStatusReady}, false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.expected, diskIsTombstone(tc.state)) })
	}
}

func TestLegacyDiskTombstoneUsesDeletionTime(t *testing.T) {
	for _, tc := range []struct {
		name          string
		nullTimestamp bool
		timestamp     time.Time
		completed     bool
	}{
		{name: "legacy NULL", nullTimestamp: true},
		{name: "legacy epoch", timestamp: time.Unix(0, 0)},
		{name: "completed", timestamp: time.Now(), completed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, db, storage := setupTest(t)
			defer db.Close(ctx)
			disk := DiskMeta{ID: "disk", ZoneID: "cell", CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: "create"}
			if tc.completed {
				_, err := storage.CreateDisk(ctx, disk)
				require.NoError(t, err)
			}
			_, err := storage.DeleteDisk(ctx, disk.ID, "delete", time.Now())
			require.NoError(t, err)
			if tc.completed {
				require.NoError(t, storage.DiskDeleted(ctx, disk.ID, tc.timestamp))
			}
			// Simulate a row written before the explicit tombstone flag was added.
			err = db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
				_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
                    --!syntax_v1
                    pragma TablePathPrefix = "%v";
                    declare $id as Utf8;
                    declare $timestamp as Timestamp;
                    declare $null_timestamp as Bool;
                    update disks set tombstone = null,
                        deleted_at = if($null_timestamp, null, $timestamp)
                    where id = $id;
                `, storage.(*storageYDB).disksPath),
					persistence.ValueParam("$id", persistence.UTF8Value(disk.ID)),
					persistence.ValueParam("$timestamp", persistence.TimestampValue(tc.timestamp)),
					persistence.ValueParam("$null_timestamp", persistence.BoolValue(tc.nullTimestamp)),
				)
				return err
			})
			require.NoError(t, err)
			actual := diskDeletionCompleted(readTrackedDisk(t, ctx, db, storage, disk.ID))
			require.Equal(t, tc.completed, actual)
			require.Equal(t, !tc.completed, readTrackedDisk(t, ctx, db, storage, disk.ID).tombstone)
			prepared, err := storage.PrepareLegacyDiskCleanup(ctx, disk.ID, "legacy")
			require.NoError(t, err)
			require.Equal(t, !tc.completed, prepared)
		})
	}
}
