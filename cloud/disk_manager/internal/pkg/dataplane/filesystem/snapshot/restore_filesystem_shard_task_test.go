package snapshot

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	snapshot_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_metrics_empty "github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

func (f *fixture) startRestoreFilesystemShardTaskRunner(
	t *testing.T,
	ctx context.Context,
) tasks.Scheduler {

	config := f.newTasksConfig(t)
	runnersCount := uint64(5)
	config.RunnersCount = &runnersCount

	err := tasks_storage.CreateYDBTables(f.ctx, config, f.db, false)
	require.NoError(t, err)

	taskStorage, err := tasks_storage.NewStorage(
		config,
		tasks_metrics_empty.NewRegistry(),
		f.db,
	)
	require.NoError(t, err)

	registry := tasks.NewRegistry()
	scheduler, err := tasks.NewScheduler(
		ctx,
		registry,
		taskStorage,
		config,
		tasks_metrics_empty.NewRegistry(),
	)
	require.NoError(t, err)

	taskConfig := f.newConfig()
	fetchNodesFromStorageLimit := uint32(2)
	taskConfig.FetchNodesFromStorageLimit = &fetchNodesFromStorageLimit

	err = registry.RegisterForExecution(
		"dataplane.RestoreFilesystemShard",
		func() tasks.Task {
			return &restoreFilesystemShardTask{
				config:       taskConfig,
				factory:      f.factory,
				storage:      f.snapshotStorage,
				nodesStorage: f.nodesStorage,
			}
		},
	)
	require.NoError(t, err)

	err = tasks.StartRunners(
		ctx,
		taskStorage,
		registry,
		tasks_metrics_empty.NewRegistry(),
		config,
		"localhost",
	)
	require.NoError(t, err)

	return scheduler
}

func shardIDs(filesystemID string, shardCount int) []string {
	ids := make([]string, 0, shardCount)
	for i := 1; i <= shardCount; i++ {
		ids = append(ids, fmt.Sprintf("%s_s%d", filesystemID, i))
	}
	return ids
}

func (f *fixture) deleteShards(
	t *testing.T,
	filesystemID string,
	shardIDs []string,
	deletedShardIDs []string,
) {

	t.Helper()

	for _, shardID := range deletedShardIDs {
		shardIndex := slices.Index(shardIDs, shardID)
		require.NotEqual(t, -1, shardIndex, "unknown shard %v", shardID)

		err := f.client.ConfigureShards(
			f.ctx,
			shardID,
			nfs.ConfigureShardsParams{
				ShardFileSystemIDs: []string{},
				Force:              true,
			},
		)
		require.NoError(t, err)

		err = f.client.Delete(f.ctx, shardID, true /* force */)
		require.NoError(t, err)

		err = f.client.Create(f.ctx, shardID, nfs.CreateFilesystemParams{
			FolderID:    "folder",
			CloudID:     "cloud",
			BlocksCount: 1024,
			BlockSize:   4096,
			Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
		})
		require.NoError(t, err)

		err = f.client.ConfigureAsShard(
			f.ctx,
			shardID,
			nfs.ConfigureAsShardParams{
				ShardNo:                          uint32(shardIndex + 1),
				ShardFileSystemIDs:               shardIDs,
				MainFileSystemID:                 filesystemID,
				DirectoryCreationInShardsEnabled: true,
			},
		)
		require.NoError(t, err)
	}
}

func scheduleRestoreFilesystemShard(
	t *testing.T,
	ctx context.Context,
	scheduler tasks.Scheduler,
	idempotencyKey string,
	snapshotID string,
	shardID string,
) string {

	t.Helper()

	taskID, err := scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(ctx, idempotencyKey),
		"dataplane.RestoreFilesystemShard",
		"",
		&snapshot_protos.RestoreFilesystemShardRequest{
			Shard: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: shardID,
			},
			SnapshotId: snapshotID,
		},
	)
	require.NoError(t, err)
	return taskID
}

////////////////////////////////////////////////////////////////////////////////

func testRestoreFilesystemShards(
	t *testing.T,
	root nfs_testing.Node,
	shardCount int,
	deletedShardCount int,
) {

	t.Helper()

	f := newFixture(t)
	defer f.close(t)

	filesystemID := t.Name()
	f.prepareMultishardFilesystem(t, filesystemID, uint32(shardCount))
	defer f.cleanupFilesystem(t, filesystemID)

	model := f.fillFilesystem(t, filesystemID, root)
	sourceNodes := model.ListAllNodesRecursively(true)
	require.NotEmpty(t, sourceNodes)

	allShardIDs := shardIDs(filesystemID, shardCount)

	snapshotID := "snapshot"
	createExecCtx := tasks_mocks.NewExecutionContextMock()
	createExecCtx.On("GetTaskID").Return("create-snapshot")
	createExecCtx.On("SaveState", mock.Anything).Return(nil)

	createTask := f.newCreateSnapshotFromFilesystemTask(
		f.newConfig(),
		filesystemID,
		snapshotID,
	)
	err := createTask.Run(f.ctx, createExecCtx)
	require.NoError(t, err)

	model.Close()

	deletedShardIDs := allShardIDs[:deletedShardCount]
	f.deleteShards(t, filesystemID, allShardIDs, deletedShardIDs)

	session, err := f.client.CreateSession(f.ctx, filesystemID, "", false)
	require.NoError(t, err)
	model.SetSession(session)
	defer model.Close()

	runnerCtx, cancelRunner := context.WithCancel(f.ctx)
	defer cancelRunner()
	scheduler := f.startRestoreFilesystemShardTaskRunner(t, runnerCtx)

	restoreTaskIDs := make([]string, 0, len(deletedShardIDs))
	for _, shardID := range deletedShardIDs {
		restoreTaskIDs = append(
			restoreTaskIDs,
			scheduleRestoreFilesystemShard(
				t,
				f.ctx,
				scheduler,
				"restore-"+shardID,
				snapshotID,
				shardID,
			),
		)
	}

	for _, taskID := range restoreTaskIDs {
		_, err = scheduler.WaitTaskSync(f.ctx, taskID, 60*time.Second)
		require.NoError(t, err)
	}

	restoredNodes := model.ListAllNodesRecursively(true)
	require.Equal(t, sourceNodes, restoredNodes)
}

func TestRestoreFilesystemShards(t *testing.T) {
	testRestoreFilesystemShards(
		t,
		nfs_testing.StandardFilesystem,
		3, // shardCount
		2, // deletedShardCount
	)
}

func TestRestoreFilesystemShardsLargeHomogeneousTree(t *testing.T) {
	layers := []nfs_testing.FilesystemLayerConfig{
		{DirsCount: 10, FilesCount: 10},
		{DirsCount: 3, FilesCount: 3},
		{DirsCount: 1, FilesCount: 2},
	}

	testRestoreFilesystemShards(
		t,
		nfs_testing.HomogeneousDirectoryTree(layers),
		20, // shardCount
		10, // deletedShardCount
	)
}

func TestRestoreFilesystemShardErrors(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	const shardID = "shard"

	notReadySnapshotID := "not-ready-snapshot"
	_, err := f.snapshotStorage.CreateFilesystemSnapshot(
		f.ctx,
		snapshot_storage.FilesystemSnapshotMeta{
			ID:           notReadySnapshotID,
			CreateTaskID: "create-not-ready-snapshot",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: shardID,
			},
		},
	)
	require.NoError(t, err)

	deletingSnapshotID := "deleting-snapshot"
	_, err = f.snapshotStorage.CreateFilesystemSnapshot(
		f.ctx,
		snapshot_storage.FilesystemSnapshotMeta{
			ID:           deletingSnapshotID,
			CreateTaskID: "create-deleting-snapshot",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: shardID,
			},
		},
	)
	require.NoError(t, err)

	err = f.snapshotStorage.FilesystemSnapshotCreated(
		f.ctx,
		deletingSnapshotID,
		0, // nodesCount
		0, // storageSize
		0, // chunkCount
	)
	require.NoError(t, err)

	_, err = f.snapshotStorage.DeletingFilesystemSnapshot(
		f.ctx,
		deletingSnapshotID,
		"delete-snapshot",
	)
	require.NoError(t, err)

	for _, testCase := range []struct {
		name       string
		snapshotID string
	}{
		{
			name:       "missing",
			snapshotID: "missing-snapshot",
		},
		{
			name:       "not-ready",
			snapshotID: notReadySnapshotID,
		},
		{
			name:       "deleting",
			snapshotID: deletingSnapshotID,
		},
	} {
		task := &restoreFilesystemShardTask{
			config:       f.newConfig(),
			factory:      f.factory,
			storage:      f.snapshotStorage,
			nodesStorage: f.nodesStorage,
			request: &snapshot_protos.RestoreFilesystemShardRequest{
				Shard: &types.Filesystem{
					ZoneId:       "zone",
					FilesystemId: shardID,
				},
				SnapshotId: testCase.snapshotID,
			},
			state: &snapshot_protos.RestoreFilesystemShardTaskState{},
		}

		err = task.Run(f.ctx, tasks_mocks.NewExecutionContextMock())
		require.Error(
			t,
			err,
			"%v restore unexpectedly succeeded",
			testCase.name,
		)
		require.ErrorIs(t, err, task_errors.NewEmptyNonRetriableError())
	}
}
