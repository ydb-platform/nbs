package dataplane

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	s3_fault_proxy "github.com/ydb-platform/nbs/cloud/disk_manager/test/mocks/s3_fault_proxy"
)

// YDB does not promise an order for a query without ORDER BY. Make the fixture
// order deterministic, with all permanently missing entries ahead of healthy
// work. Every row and completion transaction still uses the real recipe YDB.
type orderedBackupQueue struct {
	snapshot_storage.Storage
}

func (s orderedBackupQueue) GetQueuedChunksToBackup(
	ctx context.Context,
	limit int,
) ([]snapshot_storage.BackupChunkQueueEntry, error) {
	entries, err := s.Storage.GetQueuedChunksToBackup(ctx, 2*backupChunkQueueWindowSize)
	if err != nil {
		return nil, err
	}
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].SnapshotID != entries[j].SnapshotID {
			return entries[i].SnapshotID < entries[j].SnapshotID
		}
		return entries[i].ChunkID < entries[j].ChunkID
	})
	if len(entries) > limit {
		entries = entries[:limit]
	}
	return entries, nil
}

func TestBackupFaultMissingQueueWindowDoesNotStarveHealthySnapshot(t *testing.T) {
	e := newBackupFaultEnvironment(t)
	const healthy = "z-healthy"
	chunkID := createSnapshotWithChunk(t, e.ctx, e.storage, healthy)
	dataTask := newBackupSnapshotDataTask(e.storage, e.follower, healthy)
	execCtx := newBackupExecutionContext(e.ctx)
	requireBackupWaiting(t, dataTask.Run(e.ctx, execCtx))
	missing := make([]snapshot_storage.BackupChunkQueueEntry, backupChunkQueueWindowSize+1)
	for i := range missing {
		missing[i] = snapshot_storage.BackupChunkQueueEntry{
			SnapshotID: "a-deleted", ChunkID: fmt.Sprintf("task.a-deleted.%06d", i),
			StoredInS3: true, EncryptedDEK: e.follower.encryptedDEK,
		}
	}
	require.NoError(t, e.storage.EnqueueBackupChunks(e.ctx, "a-deleted", missing))
	storage := orderedBackupQueue{e.storage}
	firstWindow, err := storage.GetQueuedChunksToBackup(e.ctx, backupChunkQueueWindowSize)
	require.NoError(t, err)
	require.Len(t, firstWindow, backupChunkQueueWindowSize)
	for _, entry := range firstWindow {
		require.NotEqual(t, healthy, entry.SnapshotID, "fixture must put healthy work after a full window")
	}
	// Retry the actual worker, retaining durable queue state. More random
	// shuffles of the same window cannot solve starvation across its boundary.
	for attempt := 0; attempt < 3; attempt++ {
		worker := newBackupChunksTask(storage, e.follower)
		err := worker.Run(e.ctx, execCtx)
		t.Logf("worker attempt %d returned %v", attempt+1, err)
		if _, err := e.follower.getObject(e.ctx, backup.ChunkKey(chunkID)); err == nil {
			break
		}
	}
	_, err = e.follower.getObject(e.ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err, "a full window of deleted chunks must not permanently block a healthy snapshot")
	require.NoError(t, dataTask.Run(e.ctx, execCtx))
	data, err := readBackupData(e.ctx, e.follower, healthy, 3)
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), data)
}

func TestBackupFaultChildWaitsForInheritedChunk(t *testing.T) {
	for _, parentState := range []string{"never_backed_up", "copy_delayed", "deleted_while_queued"} {
		t.Run(parentState, func(t *testing.T) {
			e := newBackupFaultEnvironment(t)
			parentChunk := createSnapshotWithChunk(t, e.ctx, e.storage, "parent")
			_, err := e.storage.CreateSnapshot(e.ctx, snapshot_storage.SnapshotMeta{ID: "child"})
			require.NoError(t, err)
			require.NoError(t, e.storage.ShallowCopyChunk(e.ctx, snapshot_storage.ChunkMapEntry{
				ChunkIndex: 0, ChunkID: parentChunk, StoredInS3: true,
			}, "child"))
			childChunk, err := e.storage.WriteChunk(e.ctx, "task", "child", dataplane_common.Chunk{
				Index: 1, Data: []byte("def"),
			}, true)
			require.NoError(t, err)
			require.NoError(t, e.storage.SnapshotCreated(e.ctx, "child", 8192, 8192, 2, nil))
			execCtx := newBackupExecutionContext(e.ctx)
			if parentState != "never_backed_up" {
				parent := newBackupSnapshotDataTask(e.storage, e.follower, "parent")
				requireBackupWaiting(t, parent.Run(e.ctx, execCtx))
			}
			child := newBackupSnapshotDataTask(e.storage, e.follower, "child")
			requireBackupWaiting(t, child.Run(e.ctx, execCtx))
			_, err = e.follower.getObject(e.ctx, backup.ChunkKey(parentChunk))
			require.Error(t, err, "shared chunk must not already exist in backup")
			if parentState == "deleted_while_queued" {
				_, err := e.storage.DeletingSnapshot(e.ctx, "parent", "delete-parent")
				require.NoError(t, err)
				require.NoError(t, e.storage.DeleteSnapshotData(e.ctx, "parent"))
				_, err = e.storage.ReadChunkBlob(e.ctx, parentChunk, true)
				require.NoError(t, err, "the child still references the source chunk")
			}
			e.proxy.Set(s3_fault_proxy.Fault{
				Method:     http.MethodPut,
				PathPrefix: "/" + backupTestBucket + "/" + e.follower.backupS3.Key(backup.ChunkKey(parentChunk)),
				StatusCode: http.StatusServiceUnavailable,
			})
			worker := newBackupChunksTask(e.storage, e.follower)
			_ = worker.Run(e.ctx, execCtx)
			_, err = e.follower.getObject(e.ctx, backup.ChunkKey(childChunk))
			require.NoError(t, err, "the child's own changed chunk should be copied")
			_, err = e.follower.getObject(e.ctx, backup.ChunkKey(parentChunk))
			require.Error(t, err, "the delayed inherited chunk must still be absent")
			requireBackupWaiting(t, child.Run(e.ctx, execCtx))
			e.requireNoChunkMap(t, "child")
			e.proxy.Clear()
			e.copyChunks(t)
			require.NoError(t, child.Run(e.ctx, execCtx))
			actual, err := readBackupData(e.ctx, e.follower, "child", 3)
			require.NoError(t, err)
			require.Equal(t, []byte("abcdef"), actual)
		})
	}
}
