package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

// In deleting, cleanup_task_ids distinguishes background from ordinary deletion.
// Legacy tombstones also keep IDs of in-flight calls until those parents end.
func (s *storageYDB) loadDiskCleanupState(
	ctx context.Context,
	tx *persistence.Transaction,
	diskID string,
) (diskState, error) {
	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;
		select * from disks where id = $id
	`, s.disksPath), persistence.ValueParam("$id", persistence.UTF8Value(diskID)))
	if err != nil {
		return diskState{}, err
	}
	defer res.Close()
	states, err := scanDiskStates(ctx, res)
	if err != nil {
		return diskState{}, err
	}
	if len(states) == 0 {
		return diskState{}, nil
	}
	state := states[0]
	if state.status == diskStatusDeleted {
		switch state.deletionMarker {
		case "":
			// Old completed deletions were enqueued for collection; legacy
			// tombstones were not. Resolve this once without inspecting deleted_at.
			queued, err := s.diskHasDeletionQueueEntry(ctx, tx, diskID)
			if err != nil {
				return diskState{}, err
			}
			state.deletionMarker = diskDeletionTombstone
			if queued {
				state.deletionMarker = diskDeletionCompletedMarker
			}
			if err := s.updateDiskState(ctx, tx, state); err != nil {
				return diskState{}, err
			}
		case diskDeletionTombstone, diskDeletionCompletedMarker:
		default:
			return diskState{}, fmt.Errorf("unknown deletion marker %q for disk %q", state.deletionMarker, diskID)
		}
	}
	return state, nil
}

func (s *storageYDB) diskHasDeletionQueueEntry(
	ctx context.Context,
	tx *persistence.Transaction,
	diskID string,
) (bool, error) {
	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $id as Utf8;
		select disk_id from deleted where disk_id = $id limit 1
	`, s.disksPath), persistence.ValueParam("$id", persistence.UTF8Value(diskID)))
	if err != nil {
		return false, err
	}
	defer res.Close()
	found := false
	for res.NextResultSet(ctx) {
		if res.NextRow() {
			found = true
			break
		}
	}
	return found, res.Err()
}

func diskIsTombstone(state diskState) bool {
	return state.status == diskStatusDeleted && state.deletionMarker == diskDeletionTombstone
}

// Legacy parents keep their own ID reserved through their atomic task finish.
// Conversion to background cleanup retains those IDs alongside per-cell workers.
func diskCleanupReserved(state diskState) bool {
	return diskCleanupInProgress(state) || diskIsTombstone(state)
}

func diskCleanupIDs(state diskState) ([]string, error) {
	var ids []string
	if state.cleanupTaskIDs == "" {
		return ids, nil
	}
	err := json.Unmarshal([]byte(state.cleanupTaskIDs), &ids)
	return ids, err
}

func diskCleanupInProgress(state diskState) bool {
	// Preparation initializes the list together with the deleting status.
	// Treat any initialized value as background cleanup, including an invalid
	// value, so ordinary deletion cannot finalize the reservation prematurely.
	return state.status == diskStatusDeleting && state.cleanupTaskIDs != ""
}

func diskDeletionCompleted(state diskState) bool {
	return state.status == diskStatusDeleted && state.deletionMarker == diskDeletionCompletedMarker
}

func (s *storageYDB) IsDiskDeletionCompleted(
	ctx context.Context,
	diskID string,
) (completed bool, err error) {
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		tx, err := session.BeginRWTransaction(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		state, err := s.loadDiskCleanupState(ctx, tx, diskID)
		if err != nil {
			return err
		}
		completed = diskDeletionCompleted(state)
		return tx.Commit(ctx)
	})
	return completed, err
}

func (s *storageYDB) IsDiskCleanupInProgress(
	ctx context.Context,
	diskID string,
) (pending bool, err error) {
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		tx, err := session.BeginRWTransaction(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		state, err := s.loadDiskCleanupState(ctx, tx, diskID)
		if err != nil {
			return err
		}
		pending = diskCleanupInProgress(state)
		return tx.Commit(ctx)
	})
	return pending, err
}

// PrepareDiskCleanup reserves the ID before background tasks can be scheduled.
// A concurrently recreated disk must never be included in the old cleanup.
func (s *storageYDB) PrepareDiskCleanup(
	ctx context.Context,
	diskID string,
	cleanupIDs []string,
) (prepared bool, err error) {
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		tx, err := session.BeginRWTransaction(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		state, err := s.loadDiskCleanupState(ctx, tx, diskID)
		if err != nil {
			return err
		}
		if state.id != "" && !diskCleanupInProgress(state) && !diskIsTombstone(state) {
			// Missing records, explicit tombstones, and active cleanup can be
			// reserved. Never take over a live disk or restart completed deletion.
			prepared = false
			return tx.Commit(ctx)
		}
		if len(cleanupIDs) == 0 {
			return fmt.Errorf("cannot reserve disk %q without cleanup IDs", diskID)
		}
		if state.id == "" {
			state.id = diskID
			state.deletionMarker = diskDeletionTombstone
			state.deletingAt = time.Now()
		}
		ids, err := diskCleanupIDs(state)
		if err != nil {
			return err
		}
		for _, id := range cleanupIDs {
			if !slices.Contains(ids, id) {
				ids = append(ids, id)
			}
		}
		data, err := json.Marshal(ids)
		if err != nil {
			return err
		}
		state.cleanupTaskIDs = string(data)
		state.status = diskStatusDeleting
		err = s.updateDiskState(ctx, tx, state)
		if err != nil {
			return err
		}
		err = tx.Commit(ctx)
		prepared = err == nil
		return err
	})
	return prepared, err
}

// PrepareLegacyDiskCleanup pins a legacy missing-disk request before any external
// deletion. New legacy requests cannot join background cleanup, but an already
// pinned parent must resume and release its own reservation when it finishes.
func (s *storageYDB) PrepareLegacyDiskCleanup(
	ctx context.Context,
	diskID string,
	cleanupID string,
) (prepared bool, err error) {
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		tx, err := session.BeginRWTransaction(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		state, err := s.loadDiskCleanupState(ctx, tx, diskID)
		if err != nil {
			return err
		}
		ids, err := diskCleanupIDs(state)
		if err != nil {
			return err
		}
		if !diskIsTombstone(state) {
			prepared = diskCleanupInProgress(state) && slices.Contains(ids, cleanupID)
			return tx.Commit(ctx)
		}
		if cleanupID == "" {
			return fmt.Errorf("empty legacy cleanup ID for disk %q", diskID)
		}
		if !slices.Contains(ids, cleanupID) {
			ids = append(ids, cleanupID)
		}
		data, err := json.Marshal(ids)
		if err != nil {
			return err
		}
		state.cleanupTaskIDs = string(data)
		if err := s.updateDiskState(ctx, tx, state); err != nil {
			return err
		}
		err = tx.Commit(ctx)
		prepared = err == nil
		return err
	})
	return prepared, err
}

// GetDiskCleanupIDs also exposes legacy reservations that an old executor may
// have left behind, so the background parent can schedule independent observers.
func (s *storageYDB) GetDiskCleanupIDs(ctx context.Context, diskID string) (ids []string, err error) {
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		tx, err := session.BeginRWTransaction(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		state, err := s.loadDiskCleanupState(ctx, tx, diskID)
		if err != nil {
			return err
		}
		ids, err = diskCleanupIDs(state)
		if err != nil {
			return err
		}
		return tx.Commit(ctx)
	})
	return ids, err
}

func (s *storageYDB) IsDiskCleanupPending(
	ctx context.Context,
	diskID string,
	cleanupID string,
) (pending bool, err error) {
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		tx, err := session.BeginRWTransaction(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback(ctx)
		state, err := s.loadDiskCleanupState(ctx, tx, diskID)
		if err != nil {
			return err
		}
		ids, err := diskCleanupIDs(state)
		if err != nil {
			return err
		}
		pending = diskCleanupReserved(state) && slices.Contains(ids, cleanupID)
		return tx.Commit(ctx)
	})
	return pending, err
}

func (s *storageYDB) CompleteDiskCleanup(
	ctx context.Context,
	tx *persistence.Transaction,
	diskID string,
	cleanupID string,
) error {
	state, err := s.loadDiskCleanupState(ctx, tx, diskID)
	if err != nil {
		return err
	}
	ids, err := diskCleanupIDs(state)
	if err != nil {
		return err
	}
	if !diskCleanupReserved(state) || !slices.Contains(ids, cleanupID) {
		return nil
	}
	remaining := make([]string, 0, len(ids)-1)
	for _, id := range ids {
		if id != cleanupID {
			remaining = append(remaining, id)
		}
	}
	data, err := json.Marshal(remaining)
	if err != nil {
		return err
	}
	state.cleanupTaskIDs = string(data)
	// Legacy-only completion leaves a tombstone eligible for future all-cell cleanup.
	// Only the last reservation of background cleanup marks deletion completed.
	if len(remaining) == 0 && diskCleanupInProgress(state) {
		state.status = diskStatusDeleted
		state.deletionMarker = diskDeletionCompletedMarker
		state.deletedAt = time.Now()
		_, err = tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $id as Utf8;
			declare $deleted_at as Timestamp;
			upsert into deleted (deleted_at, disk_id) values ($deleted_at, $id)
		`, s.disksPath),
			persistence.ValueParam("$id", persistence.UTF8Value(diskID)),
			persistence.ValueParam("$deleted_at", persistence.TimestampValue(state.deletedAt)),
		)
		if err != nil {
			return err
		}
	}
	err = s.updateDiskState(ctx, tx, state)
	if err != nil {
		return err
	}
	return nil
}
