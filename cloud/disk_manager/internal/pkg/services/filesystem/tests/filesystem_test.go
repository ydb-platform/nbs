package tests

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/disks/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem"
	filesystem_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/services/filesystem/config"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

func newYDB(ctx context.Context) (*persistence.YDBClient, error) {
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("KIKIMR_SERVER_PORT"),
	)
	database := "/Root"
	rootPath := "disk_manager"

	return persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint: &endpoint,
			Database: &database,
			RootPath: &rootPath,
		},
		metrics.NewEmptyRegistry(),
	)
}

func newTaskStorage(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	config *tasks_config.TasksConfig,
	metricsRegistry metrics.Registry,
) (tasks_storage.Storage, error) {

	folder := fmt.Sprintf("%v/tasks", t.Name())
	config.StorageFolder = &folder

	err := tasks_storage.CreateYDBTables(
		ctx,
		config,
		db,
		false, // dropUnusedColumns
	)
	if err != nil {
		return nil, err
	}

	return tasks_storage.NewStorage(config, metricsRegistry, db)
}

func newResourceStorage(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
) (resources.Storage, error) {

	disksFolder := fmt.Sprintf("%v/disks", t.Name())
	imagesFolder := fmt.Sprintf("%v/images", t.Name())
	snapshotsFolder := fmt.Sprintf("%v/snapshots", t.Name())
	filesystemsFolder := fmt.Sprintf("%v/filesystems", t.Name())
	placementGroupsFolder := fmt.Sprintf("%v/placement_groups", t.Name())

	err := resources.CreateYDBTables(
		ctx,
		disksFolder,
		imagesFolder,
		snapshotsFolder,
		filesystemsFolder,
		placementGroupsFolder,
		db,
		false, // dropUnusedColumns
	)
	if err != nil {
		return nil, err
	}

	disksConfig := config.DisksConfig{}
	endedMigrationExpirationTimeout, err := time.ParseDuration(
		disksConfig.GetEndedMigrationExpirationTimeout(),
	)
	if err != nil {
		return nil, err
	}

	return resources.NewStorage(
		disksFolder,
		imagesFolder,
		snapshotsFolder,
		filesystemsFolder,
		placementGroupsFolder,
		db,
		endedMigrationExpirationTimeout,
	)
}

func createServices(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
) (tasks.Scheduler, filesystem.Service) {

	pollForTaskUpdatesPeriod := "100ms"
	pollForTasksPeriodMin := "50ms"
	pollForTasksPeriodMax := "100ms"
	pollForStallingTasksPeriodMin := "100ms"
	pollForStallingTasksPeriodMax := "400ms"
	taskPingPeriod := "1s"
	taskStallingTimeout := "1s"
	taskWaitingTimeout := "1s"
	scheduleRegularTasksPeriodMin := "100ms"
	scheduleRegularTasksPeriodMax := "400ms"
	runnersCount := uint64(10)
	stalkingRunnersCount := uint64(5)
	endedTaskExpirationTimeout := "500s"
	clearEndedTasksTaskScheduleInterval := "6s"
	clearEndedTasksLimit := uint64(10)
	maxRetriableErrorCount := uint64(1000)
	inflightDurationHangTimeout := "100s"

	tasksConfig := &tasks_config.TasksConfig{
		PollForTaskUpdatesPeriod:            &pollForTaskUpdatesPeriod,
		PollForTasksPeriodMin:               &pollForTasksPeriodMin,
		PollForTasksPeriodMax:               &pollForTasksPeriodMax,
		PollForStallingTasksPeriodMin:       &pollForStallingTasksPeriodMin,
		PollForStallingTasksPeriodMax:       &pollForStallingTasksPeriodMax,
		TaskPingPeriod:                      &taskPingPeriod,
		TaskStallingTimeout:                 &taskStallingTimeout,
		TaskWaitingTimeout:                  &taskWaitingTimeout,
		ScheduleRegularTasksPeriodMin:       &scheduleRegularTasksPeriodMin,
		ScheduleRegularTasksPeriodMax:       &scheduleRegularTasksPeriodMax,
		RunnersCount:                        &runnersCount,
		StalkingRunnersCount:                &stalkingRunnersCount,
		EndedTaskExpirationTimeout:          &endedTaskExpirationTimeout,
		ClearEndedTasksTaskScheduleInterval: &clearEndedTasksTaskScheduleInterval,
		ClearEndedTasksLimit:                &clearEndedTasksLimit,
		MaxRetriableErrorCount:              &maxRetriableErrorCount,
		InflightDurationHangTimeout:         &inflightDurationHangTimeout,
	}

	taskStorage, err := newTaskStorage(
		t,
		ctx,
		db,
		tasksConfig,
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	taskRegistry := tasks.NewRegistry()
	taskScheduler, err := tasks.NewScheduler(
		ctx,
		taskRegistry,
		taskStorage,
		tasksConfig,
		metrics.NewEmptyRegistry(),
	)
	require.NoError(t, err)

	resourceStorage, err := newResourceStorage(t, ctx, db)
	require.NoError(t, err)

	insecure := true
	nfsFactory := nfs.NewFactory(
		ctx,
		&nfs_config.ClientConfig{
			Zones: map[string]*nfs_config.Zone{
				"zone": {
					Endpoints: []string{
						fmt.Sprintf(
							"localhost:%v",
							os.Getenv("NFS_SERVER_PORT"),
						),
						fmt.Sprintf(
							"localhost:%v",
							os.Getenv("NFS_SERVER_PORT"),
						),
					},
				},
			},
			Insecure: &insecure,
		},
		metrics.NewEmptyRegistry(),
	)

	deletedFilesystemExpirationTimeout := "1s"
	clearDeletedFilesystemsTaskScheduleInterval := "100ms"
	clearDeletedFilesystemsLimit := uint32(10)
	filesystemConfig := &filesystem_config.FilesystemConfig{
		DeletedFilesystemExpirationTimeout:          &deletedFilesystemExpirationTimeout,
		ClearDeletedFilesystemsTaskScheduleInterval: &clearDeletedFilesystemsTaskScheduleInterval,
		ClearDeletedFilesystemsLimit:                &clearDeletedFilesystemsLimit,
	}

	err = filesystem.RegisterForExecution(
		ctx,
		filesystemConfig,
		taskScheduler,
		taskRegistry,
		resourceStorage,
		nfsFactory,
	)
	require.NoError(t, err)

	err = tasks.StartRunners(
		ctx,
		taskStorage,
		taskRegistry,
		metrics.NewEmptyRegistry(),
		tasksConfig,
		"localhost",
	)
	require.NoError(t, err)

	return taskScheduler,
		filesystem.NewService(taskScheduler, filesystemConfig, nfsFactory)
}

////////////////////////////////////////////////////////////////////////////////

var lastReqNumber int

func getRequestContext(t *testing.T, ctx context.Context) context.Context {
	lastReqNumber++

	cookie := fmt.Sprintf("%v_%v", t.Name(), lastReqNumber)
	ctx = headers.SetIncomingIdempotencyKey(ctx, cookie)
	ctx = headers.SetIncomingRequestID(ctx, cookie)
	return ctx
}

////////////////////////////////////////////////////////////////////////////////

var defaultTimeout = 20 * time.Minute

func waitTask(
	ctx context.Context,
	scheduler tasks.Scheduler,
	taskID string,
) error {

	_, err := scheduler.WaitTaskSync(ctx, taskID, defaultTimeout)
	return err
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateDeleteFilesystem(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	scheduler, service := createServices(t, ctx, db)

	reqCtx := getRequestContext(t, ctx)
	taskID, err := service.CreateFilesystem(reqCtx, &disk_manager.CreateFilesystemRequest{
		Size:      100500 * 4096,
		BlockSize: 4096,
		FilesystemId: &disk_manager.FilesystemId{
			ZoneId:       "zone",
			FilesystemId: "filesystem",
		},
		CloudId:  "cloud",
		FolderId: "folder",
		Kind:     disk_manager.FilesystemKind_FILESYSTEM_KIND_HDD,
	})
	require.NoError(t, err)
	require.NotEmpty(t, taskID)
	err = waitTask(ctx, scheduler, taskID)
	require.NoError(t, err)

	reqCtx = getRequestContext(t, ctx)
	taskID, err = service.DeleteFilesystem(reqCtx, &disk_manager.DeleteFilesystemRequest{
		FilesystemId: &disk_manager.FilesystemId{
			ZoneId:       "zone",
			FilesystemId: "filesystem",
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, taskID)
	err = waitTask(ctx, scheduler, taskID)
	require.NoError(t, err)
}

func TestCreateResizeFilesystem(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	scheduler, service := createServices(t, ctx, db)

	reqCtx := getRequestContext(t, ctx)
	taskID, err := service.CreateFilesystem(reqCtx, &disk_manager.CreateFilesystemRequest{
		Size:      100500 * 4096,
		BlockSize: 4096,
		FilesystemId: &disk_manager.FilesystemId{
			ZoneId:       "zone",
			FilesystemId: "filesystem",
		},
		CloudId:  "cloud",
		FolderId: "folder",
		Kind:     disk_manager.FilesystemKind_FILESYSTEM_KIND_HDD,
	})
	require.NoError(t, err)
	require.NotEmpty(t, taskID)
	err = waitTask(ctx, scheduler, taskID)
	require.NoError(t, err)

	reqCtx = getRequestContext(t, ctx)
	taskID, err = service.ResizeFilesystem(reqCtx, &disk_manager.ResizeFilesystemRequest{
		FilesystemId: &disk_manager.FilesystemId{
			ZoneId:       "zone",
			FilesystemId: "filesystem",
		},
		Size: 200500 * 4096,
	})

	require.NoError(t, err)
	require.NotEmpty(t, taskID)
	err = waitTask(ctx, scheduler, taskID)
	require.NoError(t, err)

	reqCtx = getRequestContext(t, ctx)
	taskID, err = service.DeleteFilesystem(reqCtx, &disk_manager.DeleteFilesystemRequest{
		FilesystemId: &disk_manager.FilesystemId{
			ZoneId:       "zone",
			FilesystemId: "filesystem",
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, taskID)
	err = waitTask(ctx, scheduler, taskID)
	require.NoError(t, err)
}

func TestDescribeModel(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	_, service := createServices(t, ctx, db)

	reqCtx := getRequestContext(t, ctx)
	model, err := service.DescribeFilesystemModel(reqCtx, &disk_manager.DescribeFilesystemModelRequest{
		ZoneId:    "zone",
		BlockSize: 4096,
		Size:      100500 * 4096,
		Kind:      disk_manager.FilesystemKind_FILESYSTEM_KIND_SSD,
	})

	require.NoError(t, err)
	require.NotNil(t, model)

	require.Equal(t, model.Size, int64(100500*4096))
	require.Equal(t, model.BlockSize, int64(4096))
	require.NotEqual(t, model.ChannelsCount, int64(0))
	require.Equal(t, model.Kind, disk_manager.FilesystemKind_FILESYSTEM_KIND_SSD)

	require.NotEqual(t, model.PerformanceProfile.MaxReadBandwidth, int64(0))
	require.NotEqual(t, model.PerformanceProfile.MaxReadIops, int64(0))
	require.NotEqual(t, model.PerformanceProfile.MaxWriteBandwidth, int64(0))
	require.NotEqual(t, model.PerformanceProfile.MaxWriteIops, int64(0))
}
