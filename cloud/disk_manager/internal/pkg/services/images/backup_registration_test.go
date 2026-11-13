package images

import (
	"context"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/backup"
	cfg "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/images/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"testing"
	"time"
)

func TestBackupRegistrationImages(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		name := "disabled"
		if enabled {
			name = "enabled"
		}
		t.Run(name, func(t *testing.T) {
			registry := tasks.NewRegistry()
			scheduler := tasks_mocks.NewSchedulerMock()
			scheduler.On("ScheduleRegularTasks", mock.Anything, mock.Anything, mock.Anything).Return()
			config := &cfg.ImagesConfig{ScheduleBackupImageTasksScheduleInterval: proto.String("7s")}
			var source *backup.S3
			if enabled {
				var err error
				source, err = backup.NewS3(nil, "bucket", "", "", nil)
				require.NoError(t, err)
			}
			err := RegisterForExecution(context.Background(), config, registry, scheduler, nil, nil, nil, nil, source)
			require.NoError(t, err)
			for _, taskType := range []string{"images.BackupImage", "images.ScheduleBackupImageTasks"} {
				if enabled {
					task, err := registry.NewTask(taskType)
					require.NoError(t, err)
					require.NotNil(t, task)
					require.Contains(t, registry.TaskTypesForExecution(), taskType)
				} else {
					require.NotContains(t, registry.TaskTypesForExecution(), taskType)
				}
			}
			if enabled {
				scheduler.AssertCalled(t, "ScheduleRegularTasks", mock.Anything, "images.ScheduleBackupImageTasks", tasks.TaskSchedule{ScheduleInterval: 7 * time.Second, MaxTasksInflight: 1})
			} else {
				scheduler.AssertNotCalled(t, "ScheduleRegularTasks", mock.Anything, "images.ScheduleBackupImageTasks", mock.Anything)
			}
		})
	}
}
func TestBackupRegistrationImagesRejectsInvalidPeriodAndDuplicates(t *testing.T) {
	for _, bad := range []string{"period", "images.BackupImage", "images.ScheduleBackupImageTasks"} {
		t.Run(bad, func(t *testing.T) {
			registry := tasks.NewRegistry()
			scheduler := tasks_mocks.NewSchedulerMock()
			scheduler.On("ScheduleRegularTasks", mock.Anything, mock.Anything, mock.Anything).Return()
			config := &cfg.ImagesConfig{}
			source, err := backup.NewS3(nil, "bucket", "", "", nil)
			require.NoError(t, err)
			if bad == "period" {
				config.ScheduleBackupImageTasksScheduleInterval = proto.String("invalid")
			} else {
				require.NoError(t, registry.RegisterForExecution(bad, func() tasks.Task { return nil }))
			}
			err = RegisterForExecution(context.Background(), config, registry, scheduler, nil, nil, nil, nil, source)
			require.Error(t, err)
		})
	}
}
