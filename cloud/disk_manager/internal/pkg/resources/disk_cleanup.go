package resources

import (
	"context"
	"encoding/json"
	"slices"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

// Tombstone is a placeholder of a disk without metadata, which deletion is
// not completed yet.
func diskIsTombstone(state diskState) bool {
	return state.status == diskStatusDeleting && state.tombstone
}

func diskCleanupIDs(state diskState) ([]string, error) {
	var ids []string
	if len(state.cleanupTaskIDs) == 0 {
		return ids, nil
	}

	err := json.Unmarshal([]byte(state.cleanupTaskIDs), &ids)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"failed to parse cleanup task ids %q of disk %q: %w",
			state.cleanupTaskIDs,
			state.id,
			err,
		)
	}

	return ids, nil
}

func encodeDiskCleanupIDs(ids []string) (string, error) {
	data, err := json.Marshal(ids)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) PrepareDiskCleanup(
	ctx context.Context,
	diskID string,
	cleanupID string,
) (bool, error) {

	var prepared bool

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			tx, err := session.BeginRWTransaction(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback(ctx)

			state, err := s.getDiskState(ctx, tx, diskID)
			if err != nil {
				return err
			}

			prepared = diskIsTombstone(state)
			if !prepared {
				return tx.Commit(ctx)
			}

			ids, err := diskCleanupIDs(state)
			if err != nil {
				return err
			}

			if slices.Contains(ids, cleanupID) {
				return tx.Commit(ctx)
			}

			state.cleanupTaskIDs, err = encodeDiskCleanupIDs(
				append(ids, cleanupID),
			)
			if err != nil {
				return err
			}

			err = s.updateDiskState(ctx, tx, state)
			if err != nil {
				return err
			}

			return tx.Commit(ctx)
		},
	)
	return prepared, err
}

func (s *storageYDB) CompleteDiskCleanup(
	ctx context.Context,
	tx *persistence.Transaction,
	diskID string,
	cleanupID string,
) error {

	state, err := s.getDiskState(ctx, tx, diskID)
	if err != nil {
		return err
	}

	ids, err := diskCleanupIDs(state)
	if err != nil {
		return err
	}

	if !slices.Contains(ids, cleanupID) {
		return nil
	}

	ids = slices.DeleteFunc(ids, func(id string) bool { return id == cleanupID })

	state.cleanupTaskIDs, err = encodeDiskCleanupIDs(ids)
	if err != nil {
		return err
	}

	if len(ids) == 0 && diskIsTombstone(state) {
		// The last cleanup reservation is released: deletion is completed.
		return s.markDiskDeleted(ctx, tx, diskID, state, time.Now())
	}

	return s.updateDiskState(ctx, tx, state)
}
