package snapshots

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	dataplane_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	resources_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/snapshots/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	s3_fault_proxy "github.com/ydb-platform/nbs/cloud/disk_manager/test/mocks/s3_fault_proxy"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func TestBackupSnapshotFaultMissingOrNotReady(t *testing.T) {
	for _, meta := range []*resources.SnapshotMeta{nil, {ID: "snap", Ready: false}} {
		t.Run(fmt.Sprintf("missing_%v", meta == nil), func(t *testing.T) {
			ctx := test.NewContext()
			storage := resources_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			storage.On("GetSnapshotMeta", mock.Anything, "snap").Return(meta, nil).Once()
			storage.On("SnapshotBackupCancelled", mock.Anything, "snap").Return(nil).Once()
			task := &backupSnapshotTask{
				storage: storage, scheduler: scheduler,
				request: &protos.BackupSnapshotRequest{SnapshotId: "snap"},
				state:   &protos.BackupSnapshotTaskState{},
			}
			require.NoError(t, task.Run(ctx, execCtx))
			mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
			storage.AssertNotCalled(t, "SnapshotBackupScheduled", mock.Anything, mock.Anything)
		})
	}
}

type snapshotBackupCheckpoint struct {
	tasks.ExecutionContext
	task    tasks.Task
	durable []byte
	saves   int
	failAt  int
}

func (c *snapshotBackupCheckpoint) SaveState(ctx context.Context) error {
	c.saves++
	if c.saves == c.failAt {
		return errors.NewRetriableErrorf("injected durable checkpoint failure")
	}
	state, err := c.task.Save()
	if err == nil {
		c.durable = state
	}
	return err
}

func TestBackupSnapshotFaultDoesNotReportCompletion(t *testing.T) {
	for _, fault := range []string{"dek_checkpoint", "metadata_rejected", "metadata_response_lost", "schedule_checkpoint", "dataplane"} {
		t.Run(fault, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(test.NewContext(), time.Minute)
			defer cancel()
			s3, proxy := test.NewFaultyS3Client(t)
			require.NoError(t, s3.CreateBucket(ctx, backupTestBucket))
			backupS3, err := backup.NewS3(s3, backupTestBucket, t.Name(), "test-kek", make([]byte, 32))
			require.NoError(t, err)
			storage := resources_mocks.NewStorageMock()
			scheduler := tasks_mocks.NewSchedulerMock()
			execCtx := tasks_mocks.NewExecutionContextMock()
			expectedSchedules := 1
			if fault == "schedule_checkpoint" || fault == "dataplane" {
				expectedSchedules = 2
			}
			execCtx.On("GetTaskID").Return("backup-task").Times(expectedSchedules)
			storage.On("GetSnapshotMeta", mock.Anything, "snap").Return(&resources.SnapshotMeta{
				ID: "snap", Ready: true, Disk: &types.Disk{DiskId: "disk", ZoneId: "zone"},
			}, nil)
			task := &backupSnapshotTask{
				storage: storage, scheduler: scheduler, backupS3: backupS3,
				request: &protos.BackupSnapshotRequest{SnapshotId: "snap"},
				state:   &protos.BackupSnapshotTaskState{},
			}
			checkpoint := &snapshotBackupCheckpoint{ExecutionContext: execCtx, task: task}
			failure := errors.NewRetriableErrorf("injected task boundary failure")
			var scheduledRequests []*dataplane_protos.BackupSnapshotDataRequest
			var idempotencyKeys []string
			schedule := func() {
				scheduler.On("ScheduleTask", mock.Anything, "dataplane.BackupSnapshotData", "", mock.Anything).
					Return("data-task", nil).Times(expectedSchedules).Run(func(args mock.Arguments) {
					idempotencyKeys = append(idempotencyKeys, headers.GetIdempotencyKey(args.Get(0).(context.Context)))
					request := args.Get(3).(*dataplane_protos.BackupSnapshotDataRequest)
					scheduledRequests = append(scheduledRequests, proto.Clone(request).(*dataplane_protos.BackupSnapshotDataRequest))
				})
			}
			switch fault {
			case "dek_checkpoint":
				checkpoint.failAt = 1
			case "metadata_rejected":
				proxy.Set(s3_fault_proxy.Fault{Method: http.MethodPut, StatusCode: http.StatusServiceUnavailable})
			case "metadata_response_lost":
				proxy.Set(s3_fault_proxy.Fault{Method: http.MethodPut, DropResponse: true})
			case "schedule_checkpoint":
				schedule()
				checkpoint.failAt = 2
			case "dataplane":
				schedule()
				scheduler.On("WaitTask", mock.Anything, mock.Anything, "data-task").Return(nil, failure).Once()
			}
			require.Error(t, task.Run(ctx, checkpoint))
			scheduler.AssertNumberOfCalls(t, "ScheduleTask", expectedSchedules-1)
			storage.AssertNotCalled(t, "SnapshotBackupScheduled", mock.Anything, mock.Anything)
			if fault == "dek_checkpoint" {
				require.Empty(t, checkpoint.durable)
				_, err := backupS3.GetObject(ctx, backup.SnapshotMetaKey("disk", "snap"))
				require.Error(t, err, "no metadata may be written before the DEK checkpoint")
			}
			if fault == "metadata_rejected" || fault == "metadata_response_lost" {
				require.Positive(t, proxy.Hits())
			}
			if fault == "dek_checkpoint" || fault == "metadata_rejected" || fault == "metadata_response_lost" {
				scheduler.AssertNotCalled(t, "ScheduleTask", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
				schedule()
			}
			proxy.Clear()
			if fault == "metadata_response_lost" {
				_, err := s3.GetObject(ctx, backupTestBucket, backupS3.Key(backup.SnapshotMetaKey("disk", "snap")))
				require.NoError(t, err, "metadata exists, but backup is not complete")
			}
			// Restart from the durable checkpoint, not mutated task memory.
			request, err := proto.Marshal(task.request)
			require.NoError(t, err)
			restarted := &backupSnapshotTask{storage: storage, scheduler: scheduler, backupS3: backupS3}
			require.NoError(t, restarted.Load(request, checkpoint.durable))
			persistedDEK := append([]byte(nil), restarted.state.EncryptedDek...)
			checkpoint = &snapshotBackupCheckpoint{ExecutionContext: execCtx, task: restarted}
			scheduler.On("WaitTask", mock.Anything, mock.Anything, "data-task").Return(&empty.Empty{}, nil).Once()
			storage.On("SnapshotBackupScheduled", mock.Anything, "snap").Return(nil).Once()
			require.NoError(t, restarted.Run(ctx, checkpoint))
			require.NotEmpty(t, restarted.state.EncryptedDek)
			if fault != "dek_checkpoint" {
				require.Equal(t, persistedDEK, restarted.state.EncryptedDek, "restart must reuse the durable DEK")
			}
			scheduler.AssertNumberOfCalls(t, "ScheduleTask", expectedSchedules)
			require.Len(t, scheduledRequests, expectedSchedules)
			require.Len(t, idempotencyKeys, expectedSchedules)
			expectedRequest := &dataplane_protos.BackupSnapshotDataRequest{
				SnapshotId: "snap", EncryptedDek: restarted.state.EncryptedDek,
			}
			for i, request := range scheduledRequests {
				require.True(t, proto.Equal(expectedRequest, request), "scheduled request differs on attempt %d", i+1)
				require.NotEmpty(t, idempotencyKeys[i])
				require.Equal(t, "backup-task_snap_backup", idempotencyKeys[i])
				require.Equal(t, idempotencyKeys[0], idempotencyKeys[i], "replay must deduplicate the scheduled child task")
			}
			mock.AssertExpectationsForObjects(t, storage, scheduler, execCtx)
		})
	}
}
