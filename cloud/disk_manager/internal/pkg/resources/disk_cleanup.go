package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

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
	if !state.tombstone && diskIsTombstone(state) {
		// Older binaries left deleted_at unset for missing-disk tombstones.
		// Persist the explicit flag before reserving any legacy cleanup.
		state.tombstone = true
		if err := s.updateDiskState(ctx, tx, state); err != nil {
			return diskState{}, err
		}
	}
	return state, nil
}

func diskIsTombstone(state diskState) bool {
	return state.status == diskStatusDeleted && !diskDeletionCompleted(state)
}

// Unset timestamps are either Go zero (legacy NULL) or the persisted Unix epoch.
func diskDeletionCompleted(state diskState) bool {
	return state.status == diskStatusDeleted && !state.tombstone &&
		state.deletedAt.After(time.Unix(0, 0))
}

func diskCleanupIDs(state diskState) ([]string, error) {
	var ids []string
	if state.cleanupTaskIDs == "" {
		return ids, nil
	}
	err := json.Unmarshal([]byte(state.cleanupTaskIDs), &ids)
	return ids, err
}

func (s *storageYDB) PrepareDiskCleanup(
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
			prepared = false
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
	if !diskIsTombstone(state) || !slices.Contains(ids, cleanupID) {
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

	err = s.updateDiskState(ctx, tx, state)
	if err != nil {
		return err
	}
	return nil
}
