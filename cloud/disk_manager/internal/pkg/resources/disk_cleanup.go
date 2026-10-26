package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"

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

func diskCleanupIDs(state diskState) ([]string, error) {
	var ids []string
	if state.cleanupTaskIDs == "" {
		return ids, nil
	}
	err := json.Unmarshal([]byte(state.cleanupTaskIDs), &ids)
	return ids, err
}

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
