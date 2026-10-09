package dataplane

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

type backupFaultHTTP struct {
	mu      sync.Mutex
	failKey string
	objects map[string][]byte
}

func (s *backupFaultHTTP) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if r.Method != http.MethodPut {
		http.Error(w, "unexpected method", http.StatusMethodNotAllowed)
		return
	}
	if s.failKey != "" && strings.HasSuffix(r.URL.Path, s.failKey) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = io.WriteString(w, "<Error><Code>ServiceUnavailable</Code></Error>")
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	s.objects[r.URL.Path] = data
}

func newBackupFaultFixture(t *testing.T) (context.Context, *backup.S3, *backupFaultHTTP) {
	t.Helper()
	ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.InfoLevel))
	store := &backupFaultHTTP{objects: make(map[string][]byte)}
	server := httptest.NewServer(store)
	t.Cleanup(server.Close)
	client, err := persistence.NewS3Client(server.URL, "test",
		persistence.NewS3Credentials("test", "test"), 5*time.Second,
		metrics.NewEmptyRegistry(), 0, nil, nil)
	require.NoError(t, err)
	follower, err := backup.NewS3(client, "backup", "", "", nil)
	require.NoError(t, err)
	return ctx, follower, store
}

func backupMapChannels(entries []snapshot_storage.ChunkMapEntry, err error) (<-chan snapshot_storage.ChunkMapEntry, <-chan error) {
	items := make(chan snapshot_storage.ChunkMapEntry, len(entries))
	for _, entry := range entries {
		items <- entry
	}
	close(items)
	errors := make(chan error, 1)
	errors <- err
	close(errors)
	return items, errors
}

func expectBackupMap(storage *storage_mocks.StorageMock, entries []snapshot_storage.ChunkMapEntry, err error) {
	items, errors := backupMapChannels(entries, err)
	storage.On("ReadChunkMap", mock.Anything, "snap", uint32(0)).Return(items, errors).Once()
}

func TestBackupSnapshotDataResumesAfterChunkMapWriteFailure(t *testing.T) {
	ctx, follower, httpStore := newBackupFaultFixture(t)
	storage := &storage_mocks.StorageMock{}
	execCtx := tasks_mocks.NewExecutionContextMock()
	entries := []snapshot_storage.ChunkMapEntry{
		{ChunkIndex: 0, ChunkID: "task.snap.0", StoredInS3: true},
		{ChunkIndex: 1, ChunkID: ""},
		{ChunkIndex: 2, ChunkID: "task.base.0", StoredInS3: false},
	}
	storage.On("CheckSnapshotReady", ctx, "snap").Return(snapshot_storage.SnapshotMeta{ID: "snap", ChunkCount: 3}, nil)
	expectBackupMap(storage, entries, nil)
	// Only the owned chunk is enqueued. The inherited and zero entries must
	// nevertheless survive in the final map.
	storage.On("EnqueueBackupChunks", ctx, "snap", []snapshot_storage.BackupChunkQueueEntry{
		{SnapshotID: "snap", ChunkID: "task.snap.0", StoredInS3: true},
	}).Return(nil).Once()
	storage.On("GetBackedUpChunkCount", ctx, "snap").Return(uint64(0), nil).Once()
	task := &backupSnapshotDataTask{storage: storage, backupS3: follower, batchSize: 2,
		request: &protos.BackupSnapshotDataRequest{SnapshotId: "snap"},
		state:   &protos.BackupSnapshotDataTaskState{}}
	var saved []byte
	execCtx.On("SaveState", ctx).Run(func(mock.Arguments) {
		var err error
		saved, err = task.Save()
		require.NoError(t, err)
	}).Return(nil)
	err := task.Run(ctx, execCtx)
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	require.Equal(t, uint32(3), task.state.MilestoneChunkIndex)
	require.Equal(t, uint32(1), task.state.EnqueuedChunkCount)
	require.False(t, task.state.ChunkMapBackedUp)
	request, err := proto.Marshal(task.request)
	require.NoError(t, err)

	// Recreate the task from durable bytes. Completion is still forbidden
	// while the follower rejects the map.
	task = &backupSnapshotDataTask{storage: storage, backupS3: follower, batchSize: 2}
	require.NoError(t, task.Load(request, saved))
	storage.On("GetBackedUpChunkCount", ctx, "snap").Return(uint64(1), nil).Twice()
	expectBackupMap(storage, entries, nil)
	httpStore.mu.Lock()
	httpStore.failKey = "chunk_maps/snap"
	httpStore.mu.Unlock()
	require.Error(t, task.Run(ctx, execCtx))
	require.False(t, task.state.ChunkMapBackedUp)

	task = &backupSnapshotDataTask{storage: storage, backupS3: follower, batchSize: 2}
	require.NoError(t, task.Load(request, saved))
	expectBackupMap(storage, entries, nil)
	httpStore.mu.Lock()
	httpStore.failKey = ""
	httpStore.mu.Unlock()
	storage.On("ClearCompletedBackupChunks", ctx, "snap", 2).Return(2, nil).Once()
	storage.On("ClearCompletedBackupChunks", ctx, "snap", 2).Return(0, nil).Once()
	require.NoError(t, task.Run(ctx, execCtx))
	require.True(t, task.state.ChunkMapBackedUp)
	httpStore.mu.Lock()
	data := append([]byte(nil), httpStore.objects["/backup/chunk_maps/snap"]...)
	httpStore.mu.Unlock()
	chunkMap := &protos.BackupChunkMap{}
	require.NoError(t, proto.Unmarshal(data, chunkMap))
	require.Equal(t, []string{"task.snap.0", "", "task.base.0"}, chunkMap.ChunkIds)
	mock.AssertExpectationsForObjects(t, storage, execCtx)
}

func TestBackupSnapshotDataEnqueueFailuresAreReturned(t *testing.T) {
	for _, where := range []string{"read-map", "out-of-range", "enqueue", "save-state"} {
		t.Run(where, func(t *testing.T) {
			ctx, follower, _ := newBackupFaultFixture(t)
			storage := &storage_mocks.StorageMock{}
			execCtx := tasks_mocks.NewExecutionContextMock()
			failure := fmt.Errorf("injected %s", where)
			entries := []snapshot_storage.ChunkMapEntry{{ChunkIndex: 0, ChunkID: "task.snap.0"}}
			var readErr error
			if where == "read-map" {
				entries = nil
				readErr = failure
			}
			if where == "out-of-range" {
				entries[0].ChunkIndex = 1
			}
			expectBackupMap(storage, entries, readErr)
			if where == "enqueue" || where == "save-state" {
				var enqueueErr error
				if where == "enqueue" {
					enqueueErr = failure
				}
				storage.On("EnqueueBackupChunks", ctx, "snap",
					[]snapshot_storage.BackupChunkQueueEntry{{SnapshotID: "snap", ChunkID: "task.snap.0"}},
				).Return(enqueueErr).Once()
			}
			if where == "save-state" {
				execCtx.On("SaveState", ctx).Return(failure).Once()
			}
			task := &backupSnapshotDataTask{storage: storage, backupS3: follower, batchSize: 2,
				request: &protos.BackupSnapshotDataRequest{SnapshotId: "snap"},
				state:   &protos.BackupSnapshotDataTaskState{ChunkCount: 1}}
			err := task.enqueueChunks(ctx, execCtx)
			require.Error(t, err)
			require.False(t, task.state.ChunkMapBackedUp)
			if where != "out-of-range" {
				require.ErrorIs(t, err, failure)
			}
			mock.AssertExpectationsForObjects(t, storage, execCtx)
		})
	}
}

func TestBackupSnapshotDataReadinessAndCompletionErrors(t *testing.T) {
	ctx, follower, _ := newBackupFaultFixture(t)
	failure := fmt.Errorf("storage unavailable")
	storage := &storage_mocks.StorageMock{}
	task := &backupSnapshotDataTask{storage: storage, backupS3: follower, batchSize: 2,
		request: &protos.BackupSnapshotDataRequest{SnapshotId: "snap"},
		state:   &protos.BackupSnapshotDataTaskState{}}
	storage.On("CheckSnapshotReady", ctx, "snap").Return(snapshot_storage.SnapshotMeta{}, failure).Once()
	require.ErrorIs(t, task.Run(ctx, tasks_mocks.NewExecutionContextMock()), failure)
	storage.On("GetBackedUpChunkCount", ctx, "snap").Return(uint64(0), failure).Once()
	require.ErrorIs(t, task.waitForChunksBackupCompleted(ctx, "snap"), failure)
	storage.On("ClearCompletedBackupChunks", ctx, "snap", 2).Return(0, failure).Once()
	require.ErrorIs(t, task.clearCompletedBackupChunks(ctx, "snap"), failure)
	storage.AssertExpectations(t)
}

func TestBackupSnapshotDataRejectsInvalidDEKBeforeStorage(t *testing.T) {
	ctx, follower, _ := newBackupFaultFixture(t)
	storage := &storage_mocks.StorageMock{}
	task := &backupSnapshotDataTask{storage: storage, backupS3: follower,
		request: &protos.BackupSnapshotDataRequest{SnapshotId: "snap", EncryptedDek: []byte("invalid")},
		state:   &protos.BackupSnapshotDataTaskState{}}
	require.Error(t, task.Run(ctx, tasks_mocks.NewExecutionContextMock()))
	require.Empty(t, storage.Calls)
}

func TestBackupChunksRetainsFailedCopyAndMakesIndependentProgress(t *testing.T) {
	ctx, follower, httpStore := newBackupFaultFixture(t)
	storage := &storage_mocks.StorageMock{}
	good := snapshot_storage.BackupChunkQueueEntry{SnapshotID: "good", ChunkID: "task.good.0", StoredInS3: true}
	bad := snapshot_storage.BackupChunkQueueEntry{SnapshotID: "bad", ChunkID: "task.bad.0", StoredInS3: false}
	goodBlob := chunks.ChunkBlob{Data: []byte("good"), Checksum: 123}
	badBlob := chunks.ChunkBlob{Data: []byte("bad"), Checksum: 456}
	storage.On("GetQueuedChunksToBackup", ctx, backupChunkQueueWindowSize).Return([]snapshot_storage.BackupChunkQueueEntry{bad, good}, nil).Once()
	storage.On("GetQueuedChunksToBackup", ctx, backupChunkQueueWindowSize).Return([]snapshot_storage.BackupChunkQueueEntry{bad}, nil).Twice()
	storage.On("GetQueuedChunksToBackup", ctx, backupChunkQueueWindowSize).Return([]snapshot_storage.BackupChunkQueueEntry{}, nil).Once()
	storage.On("ReadChunkBlob", mock.Anything, good.ChunkID, true).Return(goodBlob, nil).Once()
	storage.On("ReadChunkBlob", mock.Anything, bad.ChunkID, false).Return(badBlob, nil).Times(3)
	storage.On("ChunksBackupCompleted", ctx, []snapshot_storage.BackupChunkQueueEntry{good}).Return(nil).Once()
	storage.On("ChunksBackupCompleted", ctx, []snapshot_storage.BackupChunkQueueEntry{bad}).Return(nil).Once()
	task := &backupChunksTask{storage: storage, backupS3: follower, batchSize: 10, inflightLimit: 2,
		registry: metrics.NewEmptyRegistry(), state: &protos.BackupChunksTaskState{}}
	httpStore.mu.Lock()
	httpStore.failKey = bad.ChunkID
	httpStore.mu.Unlock()
	require.Error(t, task.Run(ctx, tasks_mocks.NewExecutionContextMock()))
	httpStore.mu.Lock()
	require.Equal(t, goodBlob.Data, httpStore.objects["/backup/chunks/"+good.ChunkID])
	_, exists := httpStore.objects["/backup/chunks/"+bad.ChunkID]
	httpStore.failKey = ""
	httpStore.mu.Unlock()
	require.False(t, exists)
	err := task.Run(ctx, tasks_mocks.NewExecutionContextMock())
	require.True(t, errors.Is(err, errors.NewInterruptExecutionError()))
	httpStore.mu.Lock()
	require.Equal(t, goodBlob.Data, httpStore.objects["/backup/chunks/"+good.ChunkID])
	require.Equal(t, badBlob.Data, httpStore.objects["/backup/chunks/"+bad.ChunkID])
	httpStore.mu.Unlock()
	storage.AssertExpectations(t)
}

func TestBackupChunksReturnsStorageFailures(t *testing.T) {
	ctx, follower, _ := newBackupFaultFixture(t)
	failure := fmt.Errorf("queue unavailable")
	storage := &storage_mocks.StorageMock{}
	storage.On("GetQueuedChunksToBackup", ctx, backupChunkQueueWindowSize).Return([]snapshot_storage.BackupChunkQueueEntry{}, failure).Once()
	task := &backupChunksTask{storage: storage, backupS3: follower, batchSize: 1, inflightLimit: 1,
		registry: metrics.NewEmptyRegistry(), state: &protos.BackupChunksTaskState{}}
	require.ErrorIs(t, task.Run(ctx, tasks_mocks.NewExecutionContextMock()), failure)
	entry := snapshot_storage.BackupChunkQueueEntry{ChunkID: "missing", StoredInS3: true}
	storage.On("ReadChunkBlob", ctx, "missing", true).Return(chunks.ChunkBlob{}, failure).Once()
	require.ErrorIs(t, task.copyChunk(ctx, entry), failure)
	storage.AssertExpectations(t)
}

func TestBackupTaskStateSerializationRejectsCorruption(t *testing.T) {
	task := &backupSnapshotDataTask{}
	require.Error(t, task.Load([]byte{255}, nil))
	request, err := proto.Marshal(&protos.BackupSnapshotDataRequest{SnapshotId: "snap"})
	require.NoError(t, err)
	require.Error(t, task.Load(request, []byte{255}))
	require.NoError(t, task.Load(request, nil))
	task.state.MilestoneChunkIndex = 17
	task.state.EnqueuedChunkCount = 12
	task.state.ChunkMapBackedUp = true
	state, err := task.Save()
	require.NoError(t, err)
	restored := &backupSnapshotDataTask{}
	require.NoError(t, restored.Load(request, state))
	require.True(t, proto.Equal(task.state, restored.state), "persisted protobuf fields must survive reload")
	chunksTask := &backupChunksTask{}
	require.Error(t, chunksTask.Load(nil, []byte{255}))
	require.NoError(t, chunksTask.Load(nil, nil))
	state, err = chunksTask.Save()
	require.NoError(t, err)
	require.NoError(t, chunksTask.Load(nil, state))
}
