package resources

import (
	"context"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func TestDiskCleanupReservation(t *testing.T) {
	for _, existing := range []bool{false, true} {
		name := "missing"
		if existing {
			name = "previously_deleted"
		}
		t.Run(name, func(t *testing.T) {
			ctx, db, storage := setupTest(t)
			defer db.Close(ctx)
			disk := DiskMeta{ID: "disk", ZoneID: "cell", CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: "create", CreatedBy: "user"}
			if existing {
				_, err := storage.CreateDisk(ctx, disk)
				require.NoError(t, err)
			}
			_, err := storage.DeleteDisk(ctx, disk.ID, "delete", time.Now())
			require.NoError(t, err)
			if existing {
				require.NoError(t, storage.DiskDeleted(ctx, disk.ID, time.Now()))
			}
			prepared, err := storage.PrepareDiskCleanup(ctx, disk.ID, []string{"cell-a", "cell-b"})
			require.NoError(t, err)
			require.True(t, prepared)
			// A duplicate preparation must not increase the number of reservations.
			prepared, err = storage.PrepareDiskCleanup(ctx, disk.ID, []string{"cell-a", "cell-b"})
			require.NoError(t, err)
			require.True(t, prepared)
			_, err = storage.DeleteDisk(ctx, disk.ID, "repeat", time.Now())
			require.NoError(t, err)
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
		})
	}
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
	_, err := storage.DeleteDisk(ctx, "disk", "delete", time.Now())
	require.NoError(t, err)
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

func TestDiskCleanupKeepsFreshTombstone(t *testing.T) {
	ctx, db, storage := setupTest(t)
	defer db.Close(ctx)
	disk := DiskMeta{ID: "disk", ZoneID: "cell", CreateRequest: &wrappers.UInt64Value{Value: 1}, CreateTaskID: "create"}
	_, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	_, err = storage.DeleteDisk(ctx, disk.ID, "delete", time.Now())
	require.NoError(t, err)
	require.NoError(t, storage.DiskDeleted(ctx, disk.ID, time.Now().Add(-time.Hour)))
	prepared, err := storage.PrepareDiskCleanup(ctx, disk.ID, []string{"cleanup"})
	require.NoError(t, err)
	require.True(t, prepared)
	require.NoError(t, finishDiskCleanup(ctx, db, storage, disk.ID, "cleanup"))
	// The old expiry-queue entry must not collect the fresh post-cleanup tombstone.
	require.NoError(t, storage.ClearDeletedDisks(ctx, time.Now().Add(-time.Minute), 100))
	_, err = storage.CreateDisk(ctx, disk)
	require.Error(t, err)
	require.NoError(t, storage.ClearDeletedDisks(ctx, time.Now().Add(time.Hour), 100))
	actual, err := storage.CreateDisk(ctx, disk)
	require.NoError(t, err)
	require.NotNil(t, actual)
}
