package dataplane

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_common "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/common"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/compressor"
	storage_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	s3_fault_proxy "github.com/ydb-platform/nbs/cloud/disk_manager/test/mocks/s3_fault_proxy"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

// All injections are test-only: real task code, recipe YDB and recipe S3 are
// exercised without adding failure switches to production code.
type backupFaultEnvironment struct {
	ctx      context.Context
	storage  snapshot_storage.Storage
	follower testFollower
	proxy    *s3_fault_proxy.Proxy
}

func newBackupFaultEnvironment(t *testing.T) backupFaultEnvironment {
	t.Helper()
	ctx, cancel := context.WithTimeout(test.NewContext(), 2*time.Minute)
	t.Cleanup(cancel)
	storage, closeStorage := newStorage(t, ctx)
	t.Cleanup(closeStorage)
	follower := newTestFollower(t, ctx)
	s3, proxy := test.NewFaultyS3Client(t)
	// Reads bypass fault injection, including assertions after a lost response.
	var err error
	follower.backupS3, err = backup.NewS3(s3, backupTestBucket, t.Name(), "kek1", make([]byte, 32))
	require.NoError(t, err)
	return backupFaultEnvironment{ctx, storage, follower, proxy}
}

func requireBackupWaiting(t *testing.T, err error) {
	t.Helper()
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()), "%v", err)
}

func (e backupFaultEnvironment) requireNoChunkMap(t *testing.T, id string) {
	t.Helper()
	_, err := e.follower.getObject(e.ctx, backup.ChunkMapKey(id))
	require.Error(t, err)
	require.Contains(t, err.Error(), "s3 object not found")
}

func (e backupFaultEnvironment) copyChunks(t *testing.T) {
	t.Helper()
	task := newBackupChunksTask(e.storage, e.follower)
	requireBackupWaiting(t, task.Run(e.ctx, newBackupExecutionContext(e.ctx)))
}

func reloadBackupTask(
	t *testing.T,
	e backupFaultEnvironment,
	id string,
	state []byte,
) *backupSnapshotDataTask {
	t.Helper()
	request, err := proto.Marshal(&protos.BackupSnapshotDataRequest{
		SnapshotId: id, EncryptedDek: e.follower.encryptedDEK,
	})
	require.NoError(t, err)
	task := newBackupSnapshotDataTask(e.storage, e.follower, id)
	require.NoError(t, task.Load(request, state))
	return task
}

// Keep only successfully persisted bytes. Reloading the task discards all
// in-memory progress, as a worker crash would. This is not a process-kill test.
type backupCheckpoint struct {
	tasks.ExecutionContext
	task    tasks.Task
	durable []byte
	saves   int
	failAt  int
}

func (c *backupCheckpoint) SaveState(ctx context.Context) error {
	c.saves++
	if c.saves == c.failAt {
		return errors.NewRetriableErrorf("injected SaveState failure")
	}
	state, err := c.task.Save()
	if err == nil {
		c.durable = state
	}
	return err
}

type backupCompletionFailure struct {
	snapshot_storage.Storage
}

func (s backupCompletionFailure) ChunksBackupCompleted(
	ctx context.Context,
	entries []snapshot_storage.BackupChunkQueueEntry,
) error {
	return errors.NewRetriableErrorf("injected completion transaction failure")
}

func TestBackupFaultS3WriteRecovery(t *testing.T) {
	for _, failure := range []struct {
		name   string
		status int
		code   string
	}{
		{"forbidden", http.StatusForbidden, "AccessDenied"},
		{"too_many_requests", http.StatusTooManyRequests, "SlowDown"},
		{"service_unavailable", http.StatusServiceUnavailable, "ServiceUnavailable"},
		{"slow_down", http.StatusServiceUnavailable, "SlowDown"},
	} {
		t.Run(failure.name, func(t *testing.T) {
			e := newBackupFaultEnvironment(t)
			createSnapshotWithChunk(t, e.ctx, e.storage, "snap")
			dataTask := newBackupSnapshotDataTask(e.storage, e.follower, "snap")
			execCtx := newBackupExecutionContext(e.ctx)
			requireBackupWaiting(t, dataTask.Run(e.ctx, execCtx))
			e.proxy.Set(s3_fault_proxy.Fault{
				Method: http.MethodPut, StatusCode: failure.status, ErrorCode: failure.code,
			})
			worker := newBackupChunksTask(e.storage, e.follower)
			require.Error(t, worker.Run(e.ctx, execCtx))
			require.Positive(t, e.proxy.Hits())
			completed, err := e.storage.GetBackedUpChunkCount(e.ctx, "snap")
			require.NoError(t, err)
			require.Zero(t, completed)
			e.requireNoChunkMap(t, "snap")
			e.proxy.Clear()
			e.copyChunks(t)
			require.NoError(t, dataTask.Run(e.ctx, execCtx))
			require.Len(t, readBackupChunkMap(t, e.ctx, e.follower, "snap").ChunkIds, 1)
		})
	}
}

func TestBackupFaultLostChunkResponse(t *testing.T) {
	e := newBackupFaultEnvironment(t)
	chunkID := createSnapshotWithChunk(t, e.ctx, e.storage, "snap")
	dataTask := newBackupSnapshotDataTask(e.storage, e.follower, "snap")
	execCtx := newBackupExecutionContext(e.ctx)
	requireBackupWaiting(t, dataTask.Run(e.ctx, execCtx))
	e.proxy.Set(s3_fault_proxy.Fault{Method: http.MethodPut, DropResponse: true})
	worker := newBackupChunksTask(e.storage, e.follower)
	require.Error(t, worker.Run(e.ctx, execCtx))
	require.Positive(t, e.proxy.Hits())
	objectBefore, err := e.follower.getObject(e.ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err, "S3 must have accepted the write before the response was lost")
	completed, err := e.storage.GetBackedUpChunkCount(e.ctx, "snap")
	require.NoError(t, err)
	require.Zero(t, completed)
	e.requireNoChunkMap(t, "snap")
	e.proxy.Clear()
	e.copyChunks(t)
	objectAfter, err := e.follower.getObject(e.ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)
	require.Equal(t, objectBefore.Data, objectAfter.Data)
	require.Equal(t, objectBefore.Metadata, objectAfter.Metadata)
	completed, err = e.storage.GetBackedUpChunkCount(e.ctx, "snap")
	require.NoError(t, err)
	require.EqualValues(t, 1, completed)
	require.NoError(t, dataTask.Run(e.ctx, execCtx))
}

func TestBackupFaultCompletionTransaction(t *testing.T) {
	e := newBackupFaultEnvironment(t)
	chunkID := createSnapshotWithChunk(t, e.ctx, e.storage, "snap")
	dataTask := newBackupSnapshotDataTask(e.storage, e.follower, "snap")
	execCtx := newBackupExecutionContext(e.ctx)
	requireBackupWaiting(t, dataTask.Run(e.ctx, execCtx))
	worker := newBackupChunksTask(backupCompletionFailure{e.storage}, e.follower)
	require.Error(t, worker.Run(e.ctx, execCtx))
	_, err := e.follower.getObject(e.ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)
	requireBackupWaiting(t, dataTask.Run(e.ctx, execCtx))
	e.requireNoChunkMap(t, "snap")
	e.copyChunks(t)
	completed, err := e.storage.GetBackedUpChunkCount(e.ctx, "snap")
	require.NoError(t, err)
	require.EqualValues(t, 1, completed, "retry must not double-count the chunk")
	require.NoError(t, dataTask.Run(e.ctx, execCtx))
}

func TestBackupFaultPartialProgress(t *testing.T) {
	e := newBackupFaultEnvironment(t)
	badID := createSnapshotWithChunk(t, e.ctx, e.storage, "blocked")
	goodID := createSnapshotWithChunk(t, e.ctx, e.storage, "healthy")
	execCtx := newBackupExecutionContext(e.ctx)
	blocked := newBackupSnapshotDataTask(e.storage, e.follower, "blocked")
	healthy := newBackupSnapshotDataTask(e.storage, e.follower, "healthy")
	requireBackupWaiting(t, blocked.Run(e.ctx, execCtx))
	requireBackupWaiting(t, healthy.Run(e.ctx, execCtx))
	e.proxy.Set(s3_fault_proxy.Fault{
		Method:     http.MethodPut,
		PathPrefix: "/" + backupTestBucket + "/" + e.follower.backupS3.Key(backup.ChunkKey(badID)),
		StatusCode: http.StatusServiceUnavailable,
	})
	worker := newBackupChunksTask(e.storage, e.follower)
	require.Error(t, worker.Run(e.ctx, execCtx))
	require.Positive(t, e.proxy.Hits())
	_, err := e.follower.getObject(e.ctx, backup.ChunkKey(goodID))
	require.NoError(t, err)
	require.NoError(t, healthy.Run(e.ctx, execCtx))
	requireBackupWaiting(t, blocked.Run(e.ctx, execCtx))
	e.requireNoChunkMap(t, "blocked")
	queue, err := e.storage.GetQueuedChunksToBackup(e.ctx, 10)
	require.NoError(t, err)
	require.Len(t, queue, 1)
	require.Equal(t, badID, queue[0].ChunkID)
	e.proxy.Clear()
	e.copyChunks(t)
	require.NoError(t, blocked.Run(e.ctx, execCtx))
}

func TestBackupFaultCancelledS3Request(t *testing.T) {
	e := newBackupFaultEnvironment(t)
	createSnapshotWithChunk(t, e.ctx, e.storage, "snap")
	task := newBackupSnapshotDataTask(e.storage, e.follower, "snap")
	execCtx := newBackupExecutionContext(e.ctx)
	requireBackupWaiting(t, task.Run(e.ctx, execCtx))
	gate := make(chan struct{})
	defer close(gate)
	reached := make(chan struct{}, 1)
	e.proxy.Set(s3_fault_proxy.Fault{Method: http.MethodPut, WaitFor: gate, Reached: reached})
	ctx, cancel := context.WithCancel(e.ctx)
	defer cancel()
	worker := newBackupChunksTask(e.storage, e.follower)
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx, execCtx) }()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("request did not reach the barrier")
	}
	cancel()
	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
	completed, err := e.storage.GetBackedUpChunkCount(e.ctx, "snap")
	require.NoError(t, err)
	require.Zero(t, completed)
	e.requireNoChunkMap(t, "snap")
	e.proxy.Clear()
	e.copyChunks(t)
	require.NoError(t, task.Run(e.ctx, execCtx))
}

func TestBackupFaultEnqueueCheckpointReplay(t *testing.T) {
	e := newBackupFaultEnvironment(t)
	createSnapshotWithChunk(t, e.ctx, e.storage, "snap")
	task := newBackupSnapshotDataTask(e.storage, e.follower, "snap")
	checkpoint := &backupCheckpoint{task: task, failAt: 1}
	require.Error(t, task.Run(e.ctx, checkpoint))
	require.Empty(t, checkpoint.durable)
	queue, err := e.storage.GetQueuedChunksToBackup(e.ctx, 10)
	require.NoError(t, err)
	require.Len(t, queue, 1, "enqueue committed even though SaveState failed")
	task = reloadBackupTask(t, e, "snap", checkpoint.durable)
	checkpoint = &backupCheckpoint{task: task}
	requireBackupWaiting(t, task.Run(e.ctx, checkpoint))
	require.EqualValues(t, 1, task.state.EnqueuedChunkCount)
	queue, err = e.storage.GetQueuedChunksToBackup(e.ctx, 10)
	require.NoError(t, err)
	require.Len(t, queue, 1, "replay must not enqueue a duplicate")
	e.copyChunks(t)
	require.NoError(t, task.Run(e.ctx, checkpoint))
}

func TestBackupFaultChunkMapReplay(t *testing.T) {
	for _, loseResponse := range []bool{false, true} {
		t.Run(fmt.Sprintf("lost_response_%v", loseResponse), func(t *testing.T) {
			e := newBackupFaultEnvironment(t)
			createSnapshotWithChunk(t, e.ctx, e.storage, "snap")
			task := newBackupSnapshotDataTask(e.storage, e.follower, "snap")
			checkpoint := &backupCheckpoint{task: task}
			requireBackupWaiting(t, task.Run(e.ctx, checkpoint))
			e.copyChunks(t)
			if loseResponse {
				e.proxy.Set(s3_fault_proxy.Fault{Method: http.MethodPut, DropResponse: true})
			} else {
				checkpoint.failAt = checkpoint.saves + 1
			}
			require.Error(t, task.Run(e.ctx, checkpoint))
			before := readBackupChunkMap(t, e.ctx, e.follower, "snap")
			e.proxy.Clear()
			task = reloadBackupTask(t, e, "snap", checkpoint.durable)
			require.False(t, task.state.ChunkMapBackedUp)
			checkpoint = &backupCheckpoint{task: task}
			require.NoError(t, task.Run(e.ctx, checkpoint))
			after := readBackupChunkMap(t, e.ctx, e.follower, "snap")
			require.Equal(t, before.ChunkIds, after.ChunkIds)
			length, err := e.storage.GetBackupChunkQueueLength(e.ctx)
			require.NoError(t, err)
			require.Zero(t, length)
		})
	}
}

func TestBackupFaultDeleteWhileChunkPutIsBlocked(t *testing.T) {
	e := newBackupFaultEnvironment(t)
	createSnapshotWithChunk(t, e.ctx, e.storage, "snap")
	task := newBackupSnapshotDataTask(e.storage, e.follower, "snap")
	execCtx := newBackupExecutionContext(e.ctx)
	requireBackupWaiting(t, task.Run(e.ctx, execCtx))
	gate := make(chan struct{})
	reached := make(chan struct{}, 1)
	// Always release the handler, including when an assertion fails.
	defer func() {
		if gate != nil {
			close(gate)
		}
	}()
	e.proxy.Set(s3_fault_proxy.Fault{Method: http.MethodPut, WaitFor: gate, Reached: reached})
	worker := newBackupChunksTask(e.storage, e.follower)
	done := make(chan error, 1)
	go func() { done <- worker.Run(e.ctx, execCtx) }()
	select {
	case <-reached:
	case <-time.After(10 * time.Second):
		t.Fatal("chunk PUT did not reach the barrier")
	}
	_, err := e.storage.DeletingSnapshot(e.ctx, "snap", "delete-task")
	require.NoError(t, err)
	require.NoError(t, e.storage.DeleteSnapshotData(e.ctx, "snap"))
	close(gate)
	gate = nil
	select {
	case err := <-done:
		requireBackupWaiting(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not finish after releasing the barrier")
	}
	err = task.Run(e.ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()), "%v", err)
	e.requireNoChunkMap(t, "snap")
	// Retention/rollback of partial objects is deliberately not specified here.
}

// Read only the follower bucket: decrypt first, then decompress and validate
// the checksum of the plaintext. This is a component oracle, not the restore API.
func readBackupData(
	ctx context.Context,
	follower testFollower,
	id string,
	chunkSize int,
) ([]byte, error) {
	object, err := follower.getObject(ctx, backup.ChunkMapKey(id))
	if err != nil {
		return nil, err
	}
	chunkMap := &protos.BackupChunkMap{}
	if err := proto.Unmarshal(object.Data, chunkMap); err != nil {
		return nil, err
	}
	var data []byte
	for _, chunkID := range chunkMap.ChunkIds {
		chunk := dataplane_common.Chunk{ID: chunkID, Data: make([]byte, chunkSize)}
		if chunkID != "" {
			object, err := follower.getObject(ctx, backup.ChunkKey(chunkID))
			if err != nil {
				return nil, err
			}
			checksum := object.Metadata["Checksum"]
			if checksum == nil {
				return nil, errors.NewNonRetriableErrorf("backup chunk has no checksum")
			}
			expected, err := strconv.ParseUint(*checksum, 10, 32)
			if err != nil {
				return nil, err
			}
			compression := ""
			if value := object.Metadata["Compression"]; value != nil {
				compression = *value
			}
			err = compressor.Decompress(compression, object.Data, chunk.Data,
				storage_metrics.New(metrics.NewEmptyRegistry(), "backup-test"))
			if err != nil {
				return nil, err
			}
			if uint64(chunk.Checksum()) != expected {
				return nil, errors.NewNonRetriableErrorf("backup chunk checksum mismatch")
			}
		}
		data = append(data, chunk.Data...)
	}
	return data, nil
}

func TestBackupFaultSeededRecovery(t *testing.T) {
	for _, seed := range []int64{7, 42, 7764} {
		t.Run(fmt.Sprintf("seed_%d", seed), func(t *testing.T) {
			e := newBackupFaultEnvironment(t)
			rng := rand.New(rand.NewSource(seed))
			const size = 4096
			var expected []byte
			_, err := e.storage.CreateSnapshot(e.ctx, snapshot_storage.SnapshotMeta{ID: "snap"})
			require.NoError(t, err)
			for i := uint32(0); i < 8; i++ {
				data := make([]byte, size)
				zero := i%3 == 0
				if !zero {
					_, err := rng.Read(data)
					require.NoError(t, err)
				}
				expected = append(expected, data...)
				_, err := e.storage.WriteChunk(e.ctx, "task", "snap", dataplane_common.Chunk{
					Index: i, Data: data, Zero: zero, Compression: "gzip",
				}, i%2 == 0)
				require.NoError(t, err)
			}
			require.NoError(t, e.storage.SnapshotCreated(e.ctx, "snap", 8*size, 5*size, 8, nil))
			dataTask := newBackupSnapshotDataTask(e.storage, e.follower, "snap")
			checkpoint := &backupCheckpoint{task: dataTask}
			requireBackupWaiting(t, dataTask.Run(e.ctx, checkpoint))
			for round := 0; round < 6; round++ {
				fault := rng.Intn(3)
				t.Logf("seed=%d round=%d fault=%d", seed, round, fault)
				e.proxy.Clear()
				workerStorage := e.storage
				switch fault {
				case 0:
					e.proxy.Set(s3_fault_proxy.Fault{Method: http.MethodPut, StatusCode: 503})
				case 1:
					e.proxy.Set(s3_fault_proxy.Fault{Method: http.MethodPut, DropResponse: true})
				case 2:
					workerStorage = backupCompletionFailure{e.storage}
				}
				worker := newBackupChunksTask(workerStorage, e.follower)
				worker.inflightLimit = 1
				require.Error(t, worker.Run(e.ctx, newBackupExecutionContext(e.ctx)))
				dataTask = reloadBackupTask(t, e, "snap", checkpoint.durable)
				checkpoint.task = dataTask
				requireBackupWaiting(t, dataTask.Run(e.ctx, checkpoint))
				e.requireNoChunkMap(t, "snap")
			}
			e.proxy.Clear()
			e.copyChunks(t)
			require.NoError(t, dataTask.Run(e.ctx, checkpoint))
			// Destroy the original data before verifying the follower.
			_, err = e.storage.DeletingSnapshot(e.ctx, "snap", "delete-task")
			require.NoError(t, err)
			require.NoError(t, e.storage.DeleteSnapshotData(e.ctx, "snap"))
			actual, err := readBackupData(e.ctx, e.follower, "snap", size)
			require.NoError(t, err)
			require.True(t, bytes.Equal(expected, actual), "backup contents differ, seed=%d", seed)
		})
	}
}

func TestBackupFaultReaderDetectsMissingAndCorruptChunks(t *testing.T) {
	e := newBackupFaultEnvironment(t)
	chunkID := createSnapshotWithChunk(t, e.ctx, e.storage, "snap")
	task := newBackupSnapshotDataTask(e.storage, e.follower, "snap")
	execCtx := newBackupExecutionContext(e.ctx)
	requireBackupWaiting(t, task.Run(e.ctx, execCtx))
	e.copyChunks(t)
	require.NoError(t, task.Run(e.ctx, execCtx))
	data, err := readBackupData(e.ctx, e.follower, "snap", 3)
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), data)
	key := e.follower.backupS3.Key(backup.ChunkKey(chunkID))
	object, err := e.follower.getObject(e.ctx, backup.ChunkKey(chunkID))
	require.NoError(t, err)
	blob, err := e.storage.ReadChunkBlob(e.ctx, chunkID, true)
	require.NoError(t, err)
	require.NotEmpty(t, blob.Compression, "the fixture must use compressed chunks")

	t.Run("ciphertext_tampered", func(t *testing.T) {
		raw, err := e.follower.getRawObject(e.ctx, backup.ChunkKey(chunkID))
		require.NoError(t, err)
		require.NotEmpty(t, raw.Data)
		raw.Data[len(raw.Data)-1] ^= 1
		require.NoError(t, e.follower.s3.PutObject(e.ctx, backupTestBucket, key, raw))
		_, err = readBackupData(e.ctx, e.follower, "snap", 3)
		require.Error(t, err, "authenticated decryption must reject modified ciphertext")
	})

	t.Run("invalid_compressed_data", func(t *testing.T) {
		require.NoError(t, e.follower.backupS3.PutObject(e.ctx, backup.ChunkKey(chunkID), e.follower.encryptedDEK, persistence.S3Object{
			Data: []byte("bad"), Metadata: object.Metadata,
		}))
		_, err := readBackupData(e.ctx, e.follower, "snap", 3)
		require.Error(t, err)
		require.True(t, errors.Is(err, errors.NewEmptyNonRetriableError()), "%v", err)
	})

	t.Run("checksum_mismatch", func(t *testing.T) {
		// Keep a valid compression stream and the original checksum metadata.
		// Otherwise decompression fails before the checksum can be checked.
		corrupted, err := compressor.Compress(
			blob.Compression, []byte("bad"),
			storage_metrics.New(metrics.NewEmptyRegistry(), "backup-test"), nil,
		)
		require.NoError(t, err)
		require.NoError(t, e.follower.backupS3.PutObject(e.ctx, backup.ChunkKey(chunkID), e.follower.encryptedDEK, persistence.S3Object{
			Data: corrupted, Metadata: object.Metadata,
		}))
		_, err = readBackupData(e.ctx, e.follower, "snap", 3)
		require.Error(t, err)
		require.Contains(t, err.Error(), "checksum mismatch")
	})

	t.Run("missing_chunk", func(t *testing.T) {
		require.NoError(t, e.follower.s3.DeleteObject(e.ctx, backupTestBucket, key))
		_, err := readBackupData(e.ctx, e.follower, "snap", 3)
		require.Error(t, err)
		require.Contains(t, err.Error(), "s3 object not found")
	})
}

func TestBackupFaultEmptyAndZeroSnapshots(t *testing.T) {
	for _, count := range []uint32{0, 3} {
		t.Run(fmt.Sprintf("chunks_%d", count), func(t *testing.T) {
			e := newBackupFaultEnvironment(t)
			_, err := e.storage.CreateSnapshot(e.ctx, snapshot_storage.SnapshotMeta{ID: "snap"})
			require.NoError(t, err)
			for index := uint32(0); index < count; index++ {
				_, err := e.storage.WriteChunk(e.ctx, "task", "snap", dataplane_common.Chunk{
					Index: index, Zero: true,
				}, true)
				require.NoError(t, err)
			}
			require.NoError(t, e.storage.SnapshotCreated(e.ctx, "snap", uint64(count)*4096, 0, count, nil))
			task := newBackupSnapshotDataTask(e.storage, e.follower, "snap")
			require.NoError(t, task.Run(e.ctx, newBackupExecutionContext(e.ctx)))
			data, err := readBackupData(e.ctx, e.follower, "snap", 4096)
			require.NoError(t, err)
			require.Len(t, data, int(count)*4096)
			require.True(t, bytes.Equal(make([]byte, int(count)*4096), data))
			length, err := e.storage.GetBackupChunkQueueLength(e.ctx)
			require.NoError(t, err)
			require.Zero(t, length)
		})
	}
}

func TestBackupFaultSharedChunksSurviveSourceDeletion(t *testing.T) {
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
	// Back up both owners. This does not assume that a child can complete
	// independently of a parent whose chunks have not been backed up yet.
	for _, id := range []string{"parent", "child"} {
		follower := e.follower
		follower.encryptedDEK, err = follower.backupS3.NewEncryptedDEK()
		require.NoError(t, err)
		task := newBackupSnapshotDataTask(e.storage, follower, id)
		requireBackupWaiting(t, task.Run(e.ctx, execCtx))
		e.copyChunks(t)
		require.NoError(t, task.Run(e.ctx, execCtx))
	}
	parentObject, err := e.follower.getRawObject(e.ctx, backup.ChunkKey(parentChunk))
	require.NoError(t, err)
	childObject, err := e.follower.getRawObject(e.ctx, backup.ChunkKey(childChunk))
	require.NoError(t, err)
	require.NotEqual(t, *parentObject.Metadata["Encrypted-Dek"], *childObject.Metadata["Encrypted-Dek"],
		"a child backup contains objects encrypted by different per-snapshot DEKs")
	for _, id := range []string{"parent", "child"} {
		_, err := e.storage.DeletingSnapshot(e.ctx, id, "delete-"+id)
		require.NoError(t, err)
		require.NoError(t, e.storage.DeleteSnapshotData(e.ctx, id))
	}
	_, err = e.storage.ReadChunkBlob(e.ctx, parentChunk, true)
	require.Error(t, err, "the source chunk must actually be gone")
	data, err := readBackupData(e.ctx, e.follower, "child", 3)
	require.NoError(t, err)
	require.Equal(t, []byte("abcdef"), data)
}
