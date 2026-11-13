package snapshots

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dpproto "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	rm "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	tm "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func TestBackupControlFailuresPreservePendingWork(t *testing.T) {
	for _, where := range []string{"get", "absent", "not-ready", "no-disk", "metadata", "marshal", "put", "schedule", "save", "wait", "mark", "success"} {
		t.Run(where, func(t *testing.T) {
			ctx := logging.SetLogger(context.Background(), logging.NewStderrLogger(logging.InfoLevel))
			failure := fmt.Errorf("injected %s", where)
			storage, scheduler, exec := rm.NewStorageMock(), tm.NewSchedulerMock(), tm.NewExecutionContextMock()
			var writes atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, http.MethodPut, r.Method)
				writes.Add(1)
				if where == "put" {
					w.Header().Set("Content-Type", "application/xml")
					w.WriteHeader(503)
					_, _ = io.WriteString(w, "<Error><Code>ServiceUnavailable</Code></Error>")
					return
				}
				b, err := io.ReadAll(r.Body)
				require.NoError(t, err)
				require.Contains(t, string(b), `"id":"resource"`)
			}))
			defer server.Close()
			s3, err := persistence.NewS3Client(server.URL, "test", persistence.NewS3Credentials("test", "test"), time.Second, metrics.NewEmptyRegistry(), 0, nil, nil)
			require.NoError(t, err)
			follower, err := backup.NewS3(s3, "backup", "", "", nil)
			require.NoError(t, err)
			meta := &resources.SnapshotMeta{ID: "resource", Ready: true, Size: 4 << 20, Disk: &types.Disk{DiskId: "disk", ZoneId: "zone"}}
			if where == "absent" {
				meta = nil
			}
			if where == "not-ready" {
				meta.Ready = false
			}
			if where == "no-disk" {
				meta.Disk = nil
			}
			if where == "metadata" {
				meta.Encryption = &types.EncryptionDesc{Mode: types.EncryptionMode(999), Key: &types.EncryptionDesc_KmsKey{KmsKey: &types.KmsKey{}}}
			}
			if where == "marshal" {
				meta.CreatingAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
			}
			var getErr error
			if where == "get" {
				getErr = failure
			}
			storage.On("GetSnapshotMeta", ctx, "resource").Return(meta, getErr).Once()
			early := where == "get" || where == "absent" || where == "not-ready" || where == "no-disk" || where == "metadata" || where == "marshal"
			if where == "absent" || where == "not-ready" {
				storage.On("SnapshotBackupCancelled", ctx, "resource").Return(nil).Once()
			}
			if !early && where != "put" {
				exec.On("GetTaskID").Return("parent").Once()
				var scheduleErr error
				if where == "schedule" {
					scheduleErr = failure
				}
				scheduler.On("ScheduleTask", mock.MatchedBy(func(c context.Context) bool { return headers.GetIdempotencyKey(c) == "parent_resource_backup" }), "dataplane.BackupSnapshotData", "",
					mock.MatchedBy(func(r *dpproto.BackupSnapshotDataRequest) bool { return r.SnapshotId == "resource" })).Return("child", scheduleErr).Once()
				if where != "schedule" {
					var saveErr error
					if where == "save" {
						saveErr = failure
					}
					exec.On("SaveState", ctx).Return(saveErr).Once()
					if where != "save" {
						var waitErr error
						if where == "wait" {
							waitErr = failure
						}
						scheduler.On("WaitTask", ctx, exec, "child").Return(nil, waitErr).Once()
						if where != "wait" {
							var markErr error
							if where == "mark" {
								markErr = failure
							}
							storage.On("SnapshotBackupScheduled", ctx, "resource").Return(markErr).Once()
						}
					}
				}
			}
			task := &backupSnapshotTask{storage: storage, scheduler: scheduler, backupS3: follower, request: &protos.BackupSnapshotRequest{SnapshotId: "resource"}, state: &protos.BackupSnapshotTaskState{}}
			err = task.Run(ctx, exec)
			if where == "success" || where == "absent" || where == "not-ready" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			if early {
				require.Zero(t, writes.Load())
			} else {
				require.Positive(t, writes.Load())
			}
			if where != "mark" && where != "success" {
				storage.AssertNotCalled(t, "SnapshotBackupScheduled", mock.Anything, mock.Anything)
			}
			mock.AssertExpectationsForObjects(t, storage, scheduler, exec)
		})
	}
}

func TestBackupControlStateAndCancellation(t *testing.T) {
	task := &backupSnapshotTask{}
	require.Error(t, task.Load([]byte{255}, nil))
	request, err := proto.Marshal(&protos.BackupSnapshotRequest{SnapshotId: "resource"})
	require.NoError(t, err)
	require.Error(t, task.Load(request, []byte{255}))
	require.NoError(t, task.Load(request, nil))
	task.state.DataplaneTaskID = "durable-child"
	task.state.EncryptedDek = []byte("opaque-persisted-key")
	saved, err := task.Save()
	require.NoError(t, err)
	restored := &backupSnapshotTask{}
	require.NoError(t, restored.Load(request, saved))
	require.True(t, proto.Equal(task.state, restored.state))
	ctx := context.Background()
	storage := rm.NewStorageMock()
	restored.storage = storage
	failure := fmt.Errorf("cancel storage unavailable")
	storage.On("SnapshotBackupCancelled", ctx, "resource").Return(failure).Once()
	require.ErrorIs(t, restored.Cancel(ctx, tm.NewExecutionContextMock()), failure)
	storage.On("SnapshotBackupCancelled", ctx, "resource").Return(nil).Once()
	require.NoError(t, restored.Cancel(ctx, tm.NewExecutionContextMock()))
	metadata, err := restored.GetMetadata(ctx)
	require.NoError(t, err)
	require.NotNil(t, metadata)
	require.NotNil(t, restored.GetResponse())
	storage.AssertExpectations(t)
}
func TestBackupControlSchedulingRetryAndLifecycle(t *testing.T) {
	ctx := context.Background()
	failure := fmt.Errorf("unavailable")
	for _, where := range []string{"list", "schedule"} {
		t.Run(where, func(t *testing.T) {
			storage, scheduler := rm.NewStorageMock(), tm.NewSchedulerMock()
			var listErr error
			if where == "list" {
				listErr = failure
			}
			storage.On("ListSnapshotsToBackup", ctx, 2).Return([]string{"one", "two"}, listErr).Once()
			if where == "schedule" {
				scheduler.On("ScheduleTask", mock.Anything, "snapshots.BackupSnapshot", "", mock.Anything).Return("", failure).Once()
			}
			task := &scheduleBackupSnapshotTasks{storage: storage, scheduler: scheduler, limit: 2}
			require.ErrorIs(t, task.Run(ctx, tm.NewExecutionContextMock()), failure)
			require.NoError(t, task.Load(nil, nil))
			state, err := task.Save()
			require.NoError(t, err)
			require.Empty(t, state)
			require.NoError(t, task.Cancel(ctx, tm.NewExecutionContextMock()))
			meta, err := task.GetMetadata(ctx)
			require.NoError(t, err)
			require.NotNil(t, meta)
			require.NotNil(t, task.GetResponse())
			mock.AssertExpectationsForObjects(t, storage, scheduler)
		})
	}
}
