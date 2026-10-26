package resources

import (
	"context"
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
	require.Equal(t, diskDeletionTombstone, state.deletionMarker)
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
	require.Equal(t, diskDeletionTombstone, state.deletionMarker)
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
	require.Equal(t, diskDeletionCompletedMarker, readTrackedDisk(t, ctx, db, storage, "disk").deletionMarker)
}

func TestDiskTombstoneClassification(t *testing.T) {
	for _, tc := range []struct {
		name     string
		state    diskState
		expected bool
	}{
		{"explicit tombstone", diskState{status: diskStatusDeleted, deletionMarker: diskDeletionTombstone}, true},
		{"completed", diskState{status: diskStatusDeleted, deletionMarker: diskDeletionCompletedMarker}, false},
		{"unclassified legacy", diskState{status: diskStatusDeleted}, false},
		{"deleting", diskState{status: diskStatusDeleting, deletionMarker: diskDeletionTombstone}, false},
		{"live", diskState{status: diskStatusReady}, false},
	} {
		t.Run(tc.name, func(t *testing.T) { require.Equal(t, tc.expected, diskIsTombstone(tc.state)) })
	}
}
