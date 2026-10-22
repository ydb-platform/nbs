package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

// cleanup_task_ids distinguishes background cleanup from ordinary deletion.
// Both use deleting, which prevents collection until all cleanup tasks finish.
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
	return states[0], nil
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
	return state.status == diskStatusDeleted
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
		if state.id != "" && !diskCleanupInProgress(state) {
			// Only a missing record or an existing background cleanup can be
			// reserved. Never take over a live disk or restart completed deletion.
			prepared = false
			return tx.Commit(ctx)
		}
		if len(cleanupIDs) == 0 {
			return fmt.Errorf("cannot reserve disk %q without cleanup IDs", diskID)
		}
		if state.id == "" {
			state.id = diskID
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
		pending = diskCleanupInProgress(state) && slices.Contains(ids, cleanupID)
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
	if !diskCleanupInProgress(state) || !slices.Contains(ids, cleanupID) {
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
	if len(remaining) == 0 {
		state.status = diskStatusDeleted
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
