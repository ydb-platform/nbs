package dataplane

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	sm "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tm "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

type backupGauge struct{ values []float64 }

func (g *backupGauge) Set(v float64) { g.values = append(g.values, v) }
func (g *backupGauge) Add(v float64) { panic("unexpected Add") }

type backupRegistry struct {
	metrics.Registry
	queue backupGauge
}

func (r *backupRegistry) Gauge(name string) metrics.Gauge {
	if name == "backup/chunkQueueLength" {
		return &r.queue
	}
	return r.Registry.Gauge(name)
}

type backupQuota struct{ cleared bool }

func (q *backupQuota) Report(context.Context) error { return nil }
func (q *backupQuota) Clear()                       { q.cleared = true }

func TestBackupFailureMetricsClearStaleQueue(t *testing.T) {
	for _, failQueue := range []bool{false, true} {
		t.Run(fmt.Sprint(failQueue), func(t *testing.T) {
			ctx, _, _ := newBackupFaultFixture(t)
			failure := fmt.Errorf("storage failed")
			s := &sm.StorageMock{}
			q := &backupQuota{}
			r := &backupRegistry{Registry: metrics.NewEmptyRegistry()}
			for _, method := range []string{"GetDeletingSnapshotCount", "GetSnapshotCount", "GetTotalSnapshotSize", "GetTotalSnapshotStorageSize"} {
				s.On(method, ctx).Return(uint64(7), nil).Once()
			}
			if failQueue {
				s.On("GetBackupChunkQueueLength", ctx).Return(uint64(0), failure).Once()
			} else {
				s.On("GetBackupChunkQueueLength", ctx).Return(uint64(13), nil).Once()
				s.On("GetDeletingSnapshotCount", ctx).Return(uint64(0), failure).Once()
			}
			task := collectSnapshotMetricsTask{registry: r, storage: s, storageQuotaReporter: q, metricsCollectionInterval: time.Millisecond, backupEnabled: true}
			require.ErrorIs(t, task.Run(ctx, tm.NewExecutionContextMock()), failure)
			want := []float64{13, 0}
			if failQueue {
				want = []float64{0}
			}
			require.Equal(t, want, r.queue.values)
			require.True(t, q.cleared)
			s.AssertExpectations(t)
		})
	}
}
func TestBackupFailureCopiedObjectWithoutCommittedProgressRetries(t *testing.T) {
	ctx, follower, httpStore := newBackupFaultFixture(t)
	s := &sm.StorageMock{}
	failure := fmt.Errorf("save acknowledgement lost")
	entry := storage.BackupChunkQueueEntry{SnapshotID: "snap", ChunkID: "task.snap.0", StoredInS3: true}
	blob := chunks.ChunkBlob{Data: []byte("must remain unchanged"), Checksum: 55}
	s.On("GetQueuedChunksToBackup", ctx, backupChunkQueueWindowSize).Return([]storage.BackupChunkQueueEntry{entry}, nil).Twice()
	s.On("GetQueuedChunksToBackup", ctx, backupChunkQueueWindowSize).Return([]storage.BackupChunkQueueEntry{}, nil).Once()
	s.On("ReadChunkBlob", mock.Anything, entry.ChunkID, true).Return(blob, nil).Twice()
	s.On("ChunksBackupCompleted", ctx, []storage.BackupChunkQueueEntry{entry}).Return(failure).Once()
	s.On("ChunksBackupCompleted", ctx, []storage.BackupChunkQueueEntry{entry}).Return(nil).Once()
	task := &backupChunksTask{storage: s, backupS3: follower, batchSize: 1, inflightLimit: 1, registry: metrics.NewEmptyRegistry()}
	require.ErrorIs(t, task.Run(ctx, tm.NewExecutionContextMock()), failure)
	httpStore.mu.Lock()
	require.Equal(t, blob.Data, httpStore.objects["/backup/chunks/"+entry.ChunkID])
	httpStore.mu.Unlock()
	require.Error(t, task.Run(ctx, tm.NewExecutionContextMock())) // an empty queue interrupts this regular task
	httpStore.mu.Lock()
	require.Equal(t, blob.Data, httpStore.objects["/backup/chunks/"+entry.ChunkID])
	httpStore.mu.Unlock()
	s.AssertExpectations(t)
}
func TestBackupFailureMapPublicationRejectsMalformedEntries(t *testing.T) {
	for _, where := range []string{"out-of-range", "read-error", "invalid-utf8"} {
		t.Run(where, func(t *testing.T) {
			ctx, follower, httpStore := newBackupFaultFixture(t)
			s := &sm.StorageMock{}
			s.On("GetBackedUpChunkCount", ctx, "snap").Return(uint64(1), nil).Once()
			entry := storage.ChunkMapEntry{ChunkIndex: 0, ChunkID: "task.snap.0"}
			var readErr error
			if where == "out-of-range" {
				entry.ChunkIndex = 1
			}
			if where == "read-error" {
				readErr = fmt.Errorf("stream failure")
			}
			if where == "invalid-utf8" {
				entry.ChunkID = string([]byte{255})
			}
			expectBackupMap(s, []storage.ChunkMapEntry{entry}, readErr)
			task := &backupSnapshotDataTask{storage: s, backupS3: follower, request: &protos.BackupSnapshotDataRequest{SnapshotId: "snap"}, state: &protos.BackupSnapshotDataTaskState{EnqueuedChunkCount: 1}}
			require.Error(t, task.backupChunkMap(ctx, tm.NewExecutionContextMock(), storage.SnapshotMeta{ID: "snap", ChunkCount: 1}))
			require.False(t, task.state.ChunkMapBackedUp)
			require.Empty(t, httpStore.objects)
			s.AssertExpectations(t)
		})
	}
}
func TestBackupFailureLifecycleAndStoppedWorkers(t *testing.T) {
	ctx, _, _ := newBackupFaultFixture(t)
	tasks := []tasks.Task{&backupChunksTask{}, &backupSnapshotDataTask{request: &protos.BackupSnapshotDataRequest{SnapshotId: "snap"}}}
	for _, task := range tasks {
		require.NoError(t, task.Cancel(ctx, tm.NewExecutionContextMock()))
		meta, err := task.GetMetadata(ctx)
		require.NoError(t, err)
		require.NotNil(t, meta)
		require.NotNil(t, task.GetResponse())
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	worker := &backupChunksTask{inflightLimit: 1}
	copied, err := worker.copyChunks(cancelled, []storage.BackupChunkQueueEntry{{ChunkID: "one"}, {ChunkID: "two"}})
	require.ErrorIs(t, err, context.Canceled)
	require.Empty(t, copied)
}
