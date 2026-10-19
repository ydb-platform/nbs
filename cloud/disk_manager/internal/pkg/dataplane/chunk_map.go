package dataplane

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

func validateChunkMapEntry(
	entry storage.ChunkMapEntry,
	snapshotID string,
	chunkCount uint32,
) error {

	if entry.ChunkIndex >= chunkCount {
		return errors.NewNonRetriableErrorf(
			"chunk index %v of snapshot %v is out of range %v",
			entry.ChunkIndex,
			snapshotID,
			chunkCount,
		)
	}

	return nil
}

// Reads the whole chunk map of the snapshot in the form stored in a bucket.
func readChunkMap(
	ctx context.Context,
	storage storage.Storage,
	snapshotID string,
	chunkCount uint32,
) (*protos.BackupChunkMap, error) {

	chunkMap := &protos.BackupChunkMap{
		ChunkIds: make([]string, chunkCount),
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	entries, entriesErrors := storage.ReadChunkMap(
		readCtx,
		snapshotID,
		0, // milestoneChunkIndex
	)
	for entry := range entries {
		err := validateChunkMapEntry(entry, snapshotID, chunkCount)
		if err != nil {
			return nil, err
		}

		chunkMap.ChunkIds[entry.ChunkIndex] = entry.ChunkID
	}

	err := <-entriesErrors
	if err != nil {
		return nil, err
	}

	return chunkMap, nil
}
