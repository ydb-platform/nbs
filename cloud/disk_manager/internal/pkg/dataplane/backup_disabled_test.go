package dataplane

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

func TestBackupDisabledDoesNotRegisterTaskTypes(t *testing.T) {
	registry := tasks.NewRegistry()
	require.NoError(t, Register(newContext(), registry, false))
	for _, taskType := range []string{
		"dataplane.BackupSnapshotData",
		"dataplane.BackupChunks",
		"dataplane.ScheduleBackupChunksTasks",
		"dataplane.DeleteBackupSnapshotData",
		"dataplane.DeleteBackupChunks",
		"dataplane.TransferFromBackupToDisk",
	} {
		require.NotContains(t, registry.TaskTypes(), taskType)
	}
	require.Contains(t, registry.TaskTypes(), "dataplane.CreateSnapshotFromDisk")
	_, err := registry.NewTask("dataplane.DeleteBackupChunks")
	require.Error(t, err)

	enabled := tasks.NewRegistry()
	require.NoError(t, Register(newContext(), enabled, true))
	require.Contains(t, enabled.TaskTypes(), "dataplane.DeleteBackupChunks")
}

func TestBackupDisabledIgnoresUnusedIntervals(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		var backupS3 *backup.S3
		if enabled {
			name = "enabled"
			backupS3 = &backup.S3{}
		}
		t.Run(name, func(t *testing.T) {
			ctx := newContext()
			scheduler := tasks_mocks.NewSchedulerMock()
			for _, taskType := range []string{"dataplane.CollectSnapshots", "dataplane.CollectSnapshotMetrics"} {
				scheduler.On("ScheduleRegularTasks", ctx, taskType, mock.Anything).Once()
			}
			cfg := &config.DataplaneConfig{
				BackupChunksTaskScheduleInterval:       proto.String("invalid"),
				DeleteBackupChunksTaskScheduleInterval: proto.String("invalid"),
			}
			registry := tasks.NewRegistry()
			err := RegisterForExecution(ctx, cfg, nil, nil, "", registry, scheduler,
				nil, nil, metrics.NewEmptyRegistry(), nil, nil, nil, false, backupS3)
			if enabled {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotContains(t, registry.TaskTypesForExecution(), "dataplane.DeleteBackupChunks")
			}
			scheduler.AssertExpectations(t)
		})
	}
}

func TestBackupDisabledDoesNotResolveMissingBlockSize(t *testing.T) {
	f := newCreateSnapshotDiskZoneTest(t, "zone", false,
		&protos.CreateSnapshotFromDiskTaskState{}, nil)
	f.execCtx.On("GetTaskID").Return("create").Twice()
	f.storage.On("CreateSnapshot", f.ctx, storage.SnapshotMeta{
		ID:           "snapshot",
		Disk:         f.task.request.SrcDisk,
		CheckpointID: "checkpoint",
		CreateTaskID: "create",
	}).Return(&storage.SnapshotMeta{ID: "snapshot", Ready: true}, nil).Once()

	// A retried ready snapshot does not need an extra metadata read or a
	// description of a disk that might no longer exist.
	require.NoError(t, f.task.run(f.ctx, f.execCtx))
}
