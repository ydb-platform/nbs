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

func TestDiskCleanupReservation(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	disk := DiskMeta{ID: "disk", ZoneID: "cell", CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: "create", CreatedBy: "user"}
	meta, err := storage.DeleteDisk(ctx, disk.ID, "delete", time.Now(), true /* onlyIfExists */)
	require.NoError(t, err)
	require.Nil(t, meta)
	completed, err := storage.IsDiskDeletionCompleted(ctx, disk.ID)
	require.NoError(t, err)
	require.False(t, completed)
	prepared, err := storage.PrepareDiskCleanup(ctx, disk.ID, []string{"cell-a", "cell-b"})
	require.NoError(t, err)
	require.True(t, prepared)
	active, err := storage.IsDiskCleanupInProgress(ctx, disk.ID)
	require.NoError(t, err)
	require.True(t, active)
	// A duplicate preparation must not increase the number of reservations.
	prepared, err = storage.PrepareDiskCleanup(ctx, disk.ID, []string{"cell-a", "cell-b"})
	require.NoError(t, err)
	require.True(t, prepared)
	meta, err = storage.DeleteDisk(ctx, disk.ID, "repeat", time.Now(), false /* onlyIfExists */)
	require.NoError(t, err)
	require.Nil(t, meta)
	require.NoError(t, storage.DiskDeleted(ctx, disk.ID, time.Now()))
	require.NoError(t, storage.ClearDeletedDisks(ctx, time.Now().Add(time.Hour), 100))
	_, err = storage.CreateDisk(ctx, disk)
	require.Error(t, err)
	require.NoError(t, finishDiskCleanup(ctx, db, storage, disk.ID, "cell-a"))
	pending, err := storage.IsDiskCleanupPending(ctx, disk.ID, "cell-a")
	require.NoError(t, err)
	require.False(t, pending)
	pending, err = storage.IsDiskCleanupPending(ctx, disk.ID, "cell-b")
	require.NoError(t, err)
	require.True(t, pending)
	require.NoError(t, storage.ClearDeletedDisks(ctx, time.Now().Add(time.Hour), 100))
	_, err = storage.CreateDisk(ctx, disk)
	require.Error(t, err)
	// A concurrent delete request has its own reservation and cannot be unpinned by this group.
	prepared, err = storage.PrepareDiskCleanup(ctx, disk.ID, []string{"another-parent"})
	require.NoError(t, err)
	require.True(t, prepared)
	require.NoError(t, finishDiskCleanup(ctx, db, storage, disk.ID, "cell-b"))
	require.NoError(t, storage.ClearDeletedDisks(ctx, time.Now().Add(time.Hour), 100))
	_, err = storage.CreateDisk(ctx, disk)
	require.Error(t, err)
	require.NoError(t, finishDiskCleanup(ctx, db, storage, disk.ID, "another-parent"))
	active, err = storage.IsDiskCleanupInProgress(ctx, disk.ID)
	require.NoError(t, err)
	require.False(t, active)
	completed, err = storage.IsDiskDeletionCompleted(ctx, disk.ID)
	require.NoError(t, err)
	require.True(t, completed)
	prepared, err = storage.PrepareDiskCleanup(ctx, disk.ID, []string{"late-parent"})
	require.NoError(t, err)
	require.False(t, prepared)
	require.NoError(t, storage.ClearDeletedDisks(ctx, time.Now().Add(time.Hour), 100))
	actual, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)
	// A late worker or preparation must not affect the new incarnation.
	pending, err = storage.IsDiskCleanupPending(ctx, disk.ID, "cell-b")
	require.NoError(t, err)
	require.False(t, pending)
	require.NoError(t, finishDiskCleanup(ctx, db, storage, disk.ID, "cell-b"))
	prepared, err = storage.PrepareDiskCleanup(ctx, disk.ID, []string{"late"})
	require.NoError(t, err)
	require.False(t, prepared)
}

// This is the same transaction boundary used by FinishWithPreparation.
func finishDiskCleanup(ctx context.Context, db *persistence.YDBClient, storage Storage, diskID, cleanupID string) error {
	return db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		tx, err := session.BeginRWTransaction(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		err = storage.CompleteDiskCleanup(ctx, tx, diskID, cleanupID)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
}

func TestDiskCleanupCompletionRollback(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	prepared, err := storage.PrepareDiskCleanup(ctx, "disk", []string{"cleanup"})
	require.NoError(t, err)
	require.True(t, prepared)
	err = db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		tx, err := session.BeginRWTransaction(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		// Simulate a failed task-result commit: all preparation writes are rolled back.
		return storage.CompleteDiskCleanup(ctx, tx, "disk", "cleanup")
	})
	require.NoError(t, err)
	pending, err := storage.IsDiskCleanupPending(ctx, "disk", "cleanup")
	require.NoError(t, err)
	require.True(t, pending)
	require.NoError(t, finishDiskCleanup(ctx, db, storage, "disk", "cleanup"))
	pending, err = storage.IsDiskCleanupPending(ctx, "disk", "cleanup")
	require.NoError(t, err)
	require.False(t, pending)
}

func TestDiskCleanupAlreadyCompletedDoesNotRestart(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	disk := DiskMeta{ID: "disk", ZoneID: "cell", CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: "create"}
	_, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	_, err = storage.DeleteDisk(ctx, disk.ID, "delete", time.Now(), false /* onlyIfExists */)
	require.NoError(t, err)
	deletedAt := time.Now().Add(-time.Hour)
	require.NoError(t, storage.DiskDeleted(ctx, disk.ID, deletedAt))
	completed, err := storage.IsDiskDeletionCompleted(ctx, disk.ID)
	require.NoError(t, err)
	require.True(t, completed)
	prepared, err := storage.PrepareDiskCleanup(ctx, disk.ID, []string{"cleanup"})
	require.NoError(t, err)
	require.False(t, prepared)
	require.NoError(t, finishDiskCleanup(ctx, db, storage, disk.ID, "cleanup"))
	pending, err := storage.IsDiskCleanupPending(ctx, disk.ID, "cleanup")
	require.NoError(t, err)
	require.False(t, pending)
	// A repeated delete must preserve the original expiration time.
	require.NoError(t, storage.ClearDeletedDisks(ctx, time.Now().Add(-time.Minute), 100))
	actual, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)
}

func TestDiskCleanupSetsDeletedAtOnlyAfterCompletion(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)

	readState := func() diskState {
		var state diskState
		err := db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
			tx, err := session.BeginRWTransaction(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)
			state, err = storage.(*storageYDB).loadDiskCleanupState(ctx, tx, "disk")
			if err != nil {
				return err
			}
			return tx.Commit(ctx)
		})
		require.NoError(t, err)
		return state
	}
	// The persistence layer represents an unset timestamp as the Unix epoch.
	unset := time.Unix(0, 0).UTC()
	_, err := storage.DeleteDisk(ctx, "disk", "delete", time.Now().Add(-time.Hour), true /* onlyIfExists */)
	require.NoError(t, err)
	require.Empty(t, readState().id)

	prepared, err := storage.PrepareDiskCleanup(ctx, "disk", []string{"cell-a", "cell-b"})
	require.NoError(t, err)
	require.True(t, prepared)
	require.Equal(t, diskStatusDeleting, readState().status)
	require.Equal(t, `["cell-a","cell-b"]`, readState().cleanupTaskIDs)
	require.Equal(t, unset, readState().deletedAt)

	require.NoError(t, finishDiskCleanup(ctx, db, storage, "disk", "cell-a"))
	state := readState()
	require.Equal(t, diskStatusDeleting, state.status)
	require.Equal(t, unset, state.deletedAt)

	err = db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		tx, err := session.BeginRWTransaction(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		// A failed completion commit must not persist the deletion timestamp.
		return storage.CompleteDiskCleanup(ctx, tx, "disk", "cell-b")
	})
	require.NoError(t, err)
	state = readState()
	require.Equal(t, diskStatusDeleting, state.status)
	require.Equal(t, unset, state.deletedAt)

	beforeCompletion := time.Now().Truncate(time.Microsecond)
	require.NoError(t, finishDiskCleanup(ctx, db, storage, "disk", "cell-b"))
	state = readState()
	require.Equal(t, diskStatusDeleted, state.status)
	require.False(t, state.deletedAt.Before(beforeCompletion))
	require.False(t, state.deletedAt.After(time.Now()))
	deletedAt := state.deletedAt
	require.NoError(t, finishDiskCleanup(ctx, db, storage, "disk", "cell-b"))
	require.Equal(t, deletedAt, readState().deletedAt)
	prepared, err = storage.PrepareDiskCleanup(ctx, "disk", []string{"new-cleanup"})
	require.NoError(t, err)
	require.False(t, prepared)
	require.Equal(t, deletedAt, readState().deletedAt)
}

func TestDiskDeletionCompleted(t *testing.T) {
	for _, tc := range []struct {
		name      string
		state     diskState
		completed bool
	}{
		{name: "missing"},
		{name: "unclassified old deleted row", state: diskState{status: diskStatusDeleted}},
		{name: "tombstone with no timestamp", state: diskState{status: diskStatusDeleted, deletionMarker: diskDeletionTombstone}},
		{name: "tombstone with recent timestamp", state: diskState{status: diskStatusDeleted, deletionMarker: diskDeletionTombstone, deletedAt: time.Now()}},
		{name: "completed without timestamp", state: diskState{status: diskStatusDeleted, deletionMarker: diskDeletionCompletedMarker}, completed: true},
		{name: "completed with epoch timestamp", state: diskState{status: diskStatusDeleted, deletionMarker: diskDeletionCompletedMarker, deletedAt: time.Unix(0, 0)}, completed: true},
		{name: "completed with real timestamp", state: diskState{status: diskStatusDeleted, deletionMarker: diskDeletionCompletedMarker, deletedAt: time.Now()}, completed: true},
		{name: "pending", state: diskState{status: diskStatusDeleting, deletionMarker: diskDeletionTombstone}},
		{name: "unknown marker", state: diskState{status: diskStatusDeleted, deletionMarker: "invalid"}},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.completed, diskDeletionCompleted(tc.state)) })
	}
}

func TestDiskCleanupDoesNotTakeOverOrdinaryDeletion(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	disk := DiskMeta{ID: "disk", ZoneID: "cell", CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: "create"}
	_, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	meta, err := storage.DeleteDisk(ctx, disk.ID, "delete", time.Now(), true /* onlyIfExists */)
	require.NoError(t, err)
	require.NotNil(t, meta)
	prepared, err := storage.PrepareDiskCleanup(ctx, disk.ID, []string{"cleanup"})
	require.NoError(t, err)
	require.False(t, prepared)
	pending, err := storage.IsDiskCleanupPending(ctx, disk.ID, "cleanup")
	require.NoError(t, err)
	require.False(t, pending)
	require.NoError(t, finishDiskCleanup(ctx, db, storage, disk.ID, "cleanup"))
	completed, err := storage.IsDiskDeletionCompleted(ctx, disk.ID)
	require.NoError(t, err)
	require.False(t, completed)
	meta, err = storage.DeleteDisk(ctx, disk.ID, "repeat", time.Now(), false /* onlyIfExists */)
	require.NoError(t, err)
	require.NotNil(t, meta)
	// Ordinary deletion still finalizes through DiskDeleted.
	require.NoError(t, storage.DiskDeleted(ctx, disk.ID, time.Now().Add(-time.Hour)))
	completed, err = storage.IsDiskDeletionCompleted(ctx, disk.ID)
	require.NoError(t, err)
	require.True(t, completed)
	require.NoError(t, storage.ClearDeletedDisks(ctx, time.Now(), 100))
	meta, err = storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, meta)
}

func TestDiskCleanupInProgress(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   diskState
		pending bool
	}{
		{name: "ordinary deleting", state: diskState{status: diskStatusDeleting}},
		{name: "initialized empty list stays protected", state: diskState{status: diskStatusDeleting, cleanupTaskIDs: "[]"}, pending: true},
		{name: "pending workers", state: diskState{status: diskStatusDeleting, cleanupTaskIDs: `["worker"]`}, pending: true},
		{name: "invalid list stays protected", state: diskState{status: diskStatusDeleting, cleanupTaskIDs: "invalid"}, pending: true},
		{name: "completed", state: diskState{status: diskStatusDeleted, cleanupTaskIDs: "[]"}},
		{name: "ready", state: diskState{status: diskStatusReady}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.pending, diskCleanupInProgress(tc.state))
		})
	}
}

func TestDiskCleanupPreparationDoesNotTakeOverNewDisk(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	meta, err := storage.DeleteDisk(ctx, "disk", "delete", time.Now(), true /* onlyIfExists */)
	require.NoError(t, err)
	require.Nil(t, meta)
	// Creation wins the race between the missing-record check and reservation.
	disk := DiskMeta{ID: "disk", ZoneID: "cell", CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: "new-create"}
	created, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, created)
	prepared, err := storage.PrepareDiskCleanup(ctx, disk.ID, []string{"cleanup"})
	require.NoError(t, err)
	require.False(t, prepared)
	pending, err := storage.IsDiskCleanupPending(ctx, disk.ID, "cleanup")
	require.NoError(t, err)
	require.False(t, pending)
	actual, err := storage.GetDiskMeta(ctx, disk.ID)
	require.NoError(t, err)
	require.Equal(t, "new-create", actual.CreateTaskID)
}

func TestDiskCleanupPreparationRejectsEmptyIDs(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	prepared, err := storage.PrepareDiskCleanup(ctx, "disk", nil)
	require.Error(t, err)
	require.False(t, prepared)
	// Failed preparation must not leave an unfinishable deleting reservation.
	disk := DiskMeta{ID: "disk", ZoneID: "cell", CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: "create"}
	meta, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, meta)
}

func TestLegacyTombstoneCanStartBackgroundCleanup(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	meta, err := storage.DeleteDisk(ctx, "disk", "legacy-delete", time.Now(), false /* onlyIfExists */)
	require.NoError(t, err)
	require.Nil(t, meta)
	prepared, err := storage.PrepareLegacyDiskCleanup(ctx, "disk", "legacy")
	require.NoError(t, err)
	require.True(t, prepared)
	require.NoError(t, finishDiskCleanup(ctx, db, storage, "disk", "legacy"))
	active, err := storage.IsDiskCleanupInProgress(ctx, "disk")
	require.NoError(t, err)
	require.False(t, active)
	completed, err := storage.IsDiskDeletionCompleted(ctx, "disk")
	require.NoError(t, err)
	require.False(t, completed)
	prepared, err = storage.PrepareDiskCleanup(ctx, "disk", []string{"cell-a", "cell-b"})
	require.NoError(t, err)
	require.True(t, prepared)
	require.NoError(t, finishDiskCleanup(ctx, db, storage, "disk", "cell-a"))
	require.NoError(t, finishDiskCleanup(ctx, db, storage, "disk", "cell-b"))
	completed, err = storage.IsDiskDeletionCompleted(ctx, "disk")
	require.NoError(t, err)
	require.True(t, completed)
	prepared, err = storage.PrepareDiskCleanup(ctx, "disk", []string{"another"})
	require.NoError(t, err)
	require.False(t, prepared)
	prepared, err = storage.PrepareLegacyDiskCleanup(ctx, "disk", "new-legacy")
	require.NoError(t, err)
	require.False(t, prepared)
}

func TestDiskCleanupWaitsForRunningLegacyDeletion(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	disk := DiskMeta{ID: "disk", ZoneID: "cell", CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: "new-create"}
	_, err := storage.DeleteDisk(ctx, disk.ID, "legacy-task", time.Now(), false /* onlyIfExists */)
	require.NoError(t, err)
	prepared, err := storage.PrepareLegacyDiskCleanup(ctx, disk.ID, "legacy")
	require.NoError(t, err)
	require.True(t, prepared)
	prepared, err = storage.PrepareDiskCleanup(ctx, disk.ID, []string{"cell-a", "cell-b"})
	require.NoError(t, err)
	require.True(t, prepared)
	ids, err := storage.GetDiskCleanupIDs(ctx, disk.ID)
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"legacy", "cell-a", "cell-b"}, ids)
	// A new legacy request skips active background cleanup; the original parent resumes.
	prepared, err = storage.PrepareLegacyDiskCleanup(ctx, disk.ID, "new-legacy")
	require.NoError(t, err)
	require.False(t, prepared)
	prepared, err = storage.PrepareLegacyDiskCleanup(ctx, disk.ID, "legacy")
	require.NoError(t, err)
	require.True(t, prepared)
	require.NoError(t, finishDiskCleanup(ctx, db, storage, disk.ID, "cell-a"))
	require.NoError(t, finishDiskCleanup(ctx, db, storage, disk.ID, "cell-b"))
	completed, err := storage.IsDiskDeletionCompleted(ctx, disk.ID)
	require.NoError(t, err)
	require.False(t, completed)
	require.NoError(t, storage.ClearDeletedDisks(ctx, time.Now().Add(time.Hour), 100))
	_, err = storage.CreateDisk(ctx, disk)
	require.Error(t, err)
	// Losing the legacy parent's task-result transaction must leave its ID pending.
	err = db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		tx, err := session.BeginRWTransaction(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		return storage.CompleteDiskCleanup(ctx, tx, disk.ID, "legacy")
	})
	require.NoError(t, err)
	pending, err := storage.IsDiskCleanupPending(ctx, disk.ID, "legacy")
	require.NoError(t, err)
	require.True(t, pending)
	completed, err = storage.IsDiskDeletionCompleted(ctx, disk.ID)
	require.NoError(t, err)
	require.False(t, completed)
	require.NoError(t, finishDiskCleanup(ctx, db, storage, disk.ID, "legacy"))
	completed, err = storage.IsDiskDeletionCompleted(ctx, disk.ID)
	require.NoError(t, err)
	require.True(t, completed)
	require.NoError(t, storage.ClearDeletedDisks(ctx, time.Now().Add(time.Hour), 100))
	meta, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, meta)
	prepared, err = storage.PrepareLegacyDiskCleanup(ctx, disk.ID, "legacy")
	require.NoError(t, err)
	require.False(t, prepared)
	require.NoError(t, finishDiskCleanup(ctx, db, storage, disk.ID, "legacy"))
}

func TestDiskDeletionMarkerResolvesOldRowsWithoutTimestamp(t *testing.T) {
	for _, completed := range []bool{false, true} {
		name := "tombstone"
		if completed {
			name = "completed"
		}
		t.Run(name, func(t *testing.T) {
			ctx, db, storage := setupTest(t)
			defer db.Close(ctx)
			disk := DiskMeta{ID: "disk", ZoneID: "cell", CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: "create"}
			if completed {
				_, err := storage.CreateDisk(ctx, disk)
				require.NoError(t, err)
			}
			_, err := storage.DeleteDisk(ctx, disk.ID, "delete", time.Now(), false /* onlyIfExists */)
			require.NoError(t, err)
			if completed {
				require.NoError(t, storage.DiskDeleted(ctx, disk.ID, time.Now()))
			}
			// Simulate rows written by a pre-marker binary. Deliberately make timestamps
			// contradict the old sentinel heuristic: completed=epoch, tombstone=now.
			timestamp := time.Now()
			if completed {
				timestamp = time.Unix(0, 0)
			}
			err = db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
				_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
     --!syntax_v1
     pragma TablePathPrefix = "%v";
     declare $id as Utf8;
     declare $timestamp as Timestamp;
     update disks set deletion_marker = null, deleted_at = $timestamp where id = $id;
    `, storage.(*storageYDB).disksPath), persistence.ValueParam("$id", persistence.UTF8Value(disk.ID)), persistence.ValueParam("$timestamp", persistence.TimestampValue(timestamp)))
				return err
			})
			require.NoError(t, err)
			actual, err := storage.IsDiskDeletionCompleted(ctx, disk.ID)
			require.NoError(t, err)
			require.Equal(t, completed, actual)
			// Classification is persisted, so subsequent operations use the explicit marker.
			err = db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
				tx, err := session.BeginRWTransaction(ctx)
				if err != nil {
					return err
				}
				defer tx.Rollback(ctx)
				state, err := storage.(*storageYDB).loadDiskCleanupState(ctx, tx, disk.ID)
				if err != nil {
					return err
				}
				marker := diskDeletionTombstone
				if completed {
					marker = diskDeletionCompletedMarker
				}
				require.Equal(t, marker, state.deletionMarker)
				return tx.Commit(ctx)
			})
			require.NoError(t, err)
			prepared, err := storage.PrepareDiskCleanup(ctx, disk.ID, []string{"cell"})
			require.NoError(t, err)
			require.Equal(t, !completed, prepared)
		})
	}
}

func TestDiskCollectorPreservesExplicitTombstone(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	_, err := storage.DeleteDisk(ctx, "disk", "legacy", time.Now(), false /* onlyIfExists */)
	require.NoError(t, err)
	prepared, err := storage.PrepareLegacyDiskCleanup(ctx, "disk", "legacy")
	require.NoError(t, err)
	require.True(t, prepared)
	// A stale expiry entry must not collect a tombstone with an active legacy call.
	err = db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
   --!syntax_v1
   pragma TablePathPrefix = "%v";
   declare $id as Utf8;
   declare $timestamp as Timestamp;
   upsert into deleted (disk_id,deleted_at) values ($id,$timestamp)
  `, storage.(*storageYDB).disksPath), persistence.ValueParam("$id", persistence.UTF8Value("disk")), persistence.ValueParam("$timestamp", persistence.TimestampValue(time.Now().Add(-time.Hour))))
		return err
	})
	require.NoError(t, err)
	require.NoError(t, storage.ClearDeletedDisks(ctx, time.Now(), 100))
	pending, err := storage.IsDiskCleanupPending(ctx, "disk", "legacy")
	require.NoError(t, err)
	require.True(t, pending)
	completed, err := storage.IsDiskDeletionCompleted(ctx, "disk")
	require.NoError(t, err)
	require.False(t, completed)
}

func TestDiskCleanupReserved(t *testing.T) {
	for _, tc := range []struct {
		name     string
		state    diskState
		reserved bool
	}{
		{name: "legacy parent", state: diskState{status: diskStatusDeleted, deletionMarker: diskDeletionTombstone, cleanupTaskIDs: `["legacy"]`}, reserved: true},
		{name: "background workers", state: diskState{status: diskStatusDeleting, cleanupTaskIDs: `["worker"]`}, reserved: true},
		{name: "completed", state: diskState{status: diskStatusDeleted, deletionMarker: diskDeletionCompletedMarker, cleanupTaskIDs: "[]"}},
		{name: "unclassified deleted row", state: diskState{status: diskStatusDeleted}},
		{name: "new live disk", state: diskState{status: diskStatusReady}},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.reserved, diskCleanupReserved(tc.state)) })
	}
}
