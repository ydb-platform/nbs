package dataplane

import (
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func TestRegisterForExecutionWithInvalidBackupIntervals(t *testing.T) {
	testCases := []struct {
		name     string
		backupS3 *backup.S3
	}{
		{name: "backups disabled"},
		{name: "backups enabled", backupS3: &backup.S3{}},
	}

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := newContext()
			scheduler := tasks_mocks.NewSchedulerMock()
			registry := tasks.NewRegistry()

			for _, taskType := range []string{
				"dataplane.CollectSnapshots",
				"dataplane.CollectSnapshotMetrics",
			} {
				scheduler.On(
					"ScheduleRegularTasks",
					ctx,
					taskType,
					mock.Anything,
				).Once()
			}

			config := &config.DataplaneConfig{
				BackupChunksTaskScheduleInterval:       proto.String("invalid"),
				DeleteBackupChunksTaskScheduleInterval: proto.String("invalid"),
			}

			err := RegisterForExecution(
				ctx,
				config,
				nil, // performanceConfig
				nil, // disabledZones
				"",  // proxyOverlayDiskIDPrefix
				registry,
				scheduler,
				nil, // nbsFactory
				nil, // storage
				metrics.NewEmptyRegistry(),
				nil,   // snapshotStorageQuotaReporter
				nil,   // urlMetricsRegistry
				nil,   // migrationDstStorage
				false, // useS3InMigration
				testCase.backupS3,
			)
			if testCase.backupS3 != nil {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.NotContains(
					t,
					registry.TaskTypesForExecution(),
					"dataplane.DeleteBackupChunks",
				)
			}

			scheduler.AssertExpectations(t)
		})
	}
}
