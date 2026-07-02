package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/mocks"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/config"
	snapshot_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage"
	nodes_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/nodes"
	snapshot_storage_schema "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/schema"
	traversal_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/config"
	traversal_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage"
	traversal_storage_schema "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
	tasks_metrics_empty "github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
	tasks_storage "github.com/ydb-platform/nbs/cloud/tasks/storage"
)

////////////////////////////////////////////////////////////////////////////////

func newYDB(ctx context.Context) (*persistence.YDBClient, error) {
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_YDB_PORT"),
	)
	database := "/Root"
	rootPath := "disk_manager"
	connectionTimeout := "10s"

	return persistence.NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint:          &endpoint,
			Database:          &database,
			RootPath:          &rootPath,
			ConnectionTimeout: &connectionTimeout,
		},
		metrics.NewEmptyRegistry(),
	)
}

////////////////////////////////////////////////////////////////////////////////

type fixture struct {
	ctx              context.Context
	db               *persistence.YDBClient
	snapshotStorage  snapshot_storage.Storage
	traversalStorage traversal_storage.Storage
	nodesStorage     nodes_storage.Storage
	client           nfs.Client
	factory          nfs.Factory
}

func newFixture(t *testing.T) *fixture {
	ctx := nfs_testing.NewContext()
	client := nfs_testing.NewClient(t, ctx)
	factory := nfs_testing.NewFactory(ctx)

	return newFixtureWithFactory(t, ctx, client, factory)
}

func newFixtureWithFactory(
	t *testing.T,
	ctx context.Context,
	client nfs.Client,
	factory nfs.Factory,
) *fixture {
	db, err := newYDB(ctx)
	require.NoError(t, err)

	traversalStorageFolder := fmt.Sprintf(
		"snapshot_transfer_tests/%v/traversal",
		t.Name(),
	)
	err = traversal_storage_schema.Create(ctx, traversalStorageFolder, db, false)
	require.NoError(t, err)

	nodesStorageFolder := fmt.Sprintf(
		"snapshot_transfer_tests/%v/nodes",
		t.Name(),
	)
	err = snapshot_storage_schema.Create(ctx, nodesStorageFolder, db, false)
	require.NoError(t, err)

	return &fixture{
		ctx:              ctx,
		db:               db,
		snapshotStorage:  snapshot_storage.NewStorage(db, nodesStorageFolder),
		traversalStorage: traversal_storage.NewStorage(db, traversalStorageFolder, 100),
		nodesStorage:     nodes_storage.NewStorage(db, nodesStorageFolder, 100),
		client:           client,
		factory:          factory,
	}
}

func (f *fixture) close(t *testing.T) {
	require.NoError(t, f.client.Close())
	require.NoError(t, f.db.Close(f.ctx))
}

func (f *fixture) prepareFilesystem(t *testing.T, filesystemID string) {
	err := f.client.Create(f.ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
	})
	require.NoError(t, err)
}

func (f *fixture) prepareMultishardFilesystem(
	t *testing.T,
	filesystemID string,
	shardCount uint32,
) {

	err := f.client.Create(f.ctx, filesystemID, nfs.CreateFilesystemParams{
		FolderID:    "folder",
		CloudID:     "cloud",
		BlocksCount: 1024,
		BlockSize:   4096,
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
		ShardCount:  shardCount,
	})
	require.NoError(t, err)

	err = f.client.EnableDirectoryCreationInShards(
		f.ctx,
		filesystemID,
		shardCount,
	)
	require.NoError(t, err)
}

func (f *fixture) cleanupFilesystem(t *testing.T, filesystemID string) {
	err := f.client.Delete(f.ctx, filesystemID, true /*force*/)
	require.NoError(t, err)
}

func (f *fixture) fillFilesystem(
	t *testing.T,
	filesystemID string,
	rootDir nfs_testing.Node,
) *nfs_testing.FileSystemModel {

	session, err := f.client.CreateSession(f.ctx, filesystemID, "", false)
	require.NoError(t, err)
	model := nfs_testing.NewFileSystemModel(t, f.ctx, session, rootDir)
	model.CreateAllNodesRecursively()
	return model
}

func (f *fixture) fillFilesystemParallel(
	t *testing.T,
	filesystemID string,
	rootDir nfs_testing.Node,
) nfs_testing.FilesystemModelInterface {

	session, err := f.client.CreateSession(f.ctx, filesystemID, "", false)
	require.NoError(t, err)
	model := nfs_testing.NewParallelFilesystemModel(t, f.ctx, session, rootDir)
	model.CreateAllNodesRecursively()
	return model
}

func (f *fixture) newConfig() *snapshot_config.FilesystemSnapshotConfig {
	listNodesMaxBytes := uint32(1000)
	selectNodesToListLimit := uint64(100)
	restoreHardlinksBatchSize := uint32(100)
	traversalQueueDeletionLimit := uint64(100)
	return &snapshot_config.FilesystemSnapshotConfig{
		TraversalConfig: &traversal_config.FilesystemTraversalConfig{
			SelectNodesToListLimit:      &selectNodesToListLimit,
			TraversalQueueDeletionLimit: &traversalQueueDeletionLimit,
		},
		ListNodesMaxBytes:         &listNodesMaxBytes,
		RestoreHardlinksBatchSize: &restoreHardlinksBatchSize,
	}
}

func (f *fixture) newSlowConfig() *snapshot_config.FilesystemSnapshotConfig {
	listNodesMaxBytes := uint32(1)
	selectNodesToListLimit := uint64(1)
	traversalWorkersCount := uint32(1)
	restoreHardlinksBatchSize := uint32(1)
	traversalQueueDeletionLimit := uint64(100)
	return &snapshot_config.FilesystemSnapshotConfig{
		TraversalConfig: &traversal_config.FilesystemTraversalConfig{
			SelectNodesToListLimit:      &selectNodesToListLimit,
			TraversalWorkersCount:       &traversalWorkersCount,
			TraversalQueueDeletionLimit: &traversalQueueDeletionLimit,
		},
		ListNodesMaxBytes:         &listNodesMaxBytes,
		RestoreHardlinksBatchSize: &restoreHardlinksBatchSize,
	}
}

func (f *fixture) newTasksConfig(t *testing.T) *tasks_config.TasksConfig {
	pollForTaskUpdatesPeriod := "50ms"
	pollForTasksPeriodMin := "50ms"
	pollForTasksPeriodMax := "100ms"
	pollForStallingTasksPeriodMin := "100ms"
	pollForStallingTasksPeriodMax := "200ms"
	taskPingPeriod := "100ms"
	taskStallingTimeout := "1s"
	taskWaitingTimeout := "10s"
	scheduleRegularTasksPeriodMin := "100ms"
	scheduleRegularTasksPeriodMax := "200ms"
	storageFolder := fmt.Sprintf(
		"snapshot_transfer_tests/%v/tasks",
		t.Name(),
	)
	runnersCount := uint64(4)
	stalkingRunnersCount := uint64(1)
	maxRetriableErrorCount := uint64(100)
	maxPanicCount := uint64(0)
	regularSystemTasksEnabled := false
	tasksToListLimit := uint64(100)

	return &tasks_config.TasksConfig{
		PollForTaskUpdatesPeriod:      &pollForTaskUpdatesPeriod,
		PollForTasksPeriodMin:         &pollForTasksPeriodMin,
		PollForTasksPeriodMax:         &pollForTasksPeriodMax,
		PollForStallingTasksPeriodMin: &pollForStallingTasksPeriodMin,
		PollForStallingTasksPeriodMax: &pollForStallingTasksPeriodMax,
		TaskPingPeriod:                &taskPingPeriod,
		TaskStallingTimeout:           &taskStallingTimeout,
		TaskWaitingTimeout:            &taskWaitingTimeout,
		ScheduleRegularTasksPeriodMin: &scheduleRegularTasksPeriodMin,
		ScheduleRegularTasksPeriodMax: &scheduleRegularTasksPeriodMax,
		RunnersCount:                  &runnersCount,
		StalkingRunnersCount:          &stalkingRunnersCount,
		StorageFolder:                 &storageFolder,
		MaxRetriableErrorCount:        &maxRetriableErrorCount,
		MaxPanicCount:                 &maxPanicCount,
		RegularSystemTasksEnabled:     &regularSystemTasksEnabled,
		TasksToListLimit:              &tasksToListLimit,
	}
}

func (f *fixture) startSnapshotCollectionTaskRunner(
	t *testing.T,
	ctx context.Context,
	maxInflight int,
	collectionTimeout time.Duration,
) tasks.Scheduler {

	config := f.newTasksConfig(t)
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
	err = registry.RegisterForExecution(
		"dataplane.CreateSnapshotFromFilesystem",
		func() tasks.Task {
			return &createSnapshotFromFilesystemTask{
				config:           taskConfig,
				factory:          f.factory,
				storage:          f.snapshotStorage,
				traversalStorage: f.traversalStorage,
				nodesStorage:     f.nodesStorage,
			}
		},
	)
	require.NoError(t, err)

	err = registry.RegisterForExecution(
		"dataplane.DeleteFilesystemSnapshot",
		func() tasks.Task {
			return &deleteFilesystemSnapshotTask{
				storage: f.snapshotStorage,
			}
		},
	)
	require.NoError(t, err)

	err = registry.RegisterForExecution(
		"dataplane.DeleteFilesystemSnapshotData",
		func() tasks.Task {
			return &deleteFilesystemSnapshotDataTask{
				nodesStorage:     f.nodesStorage,
				traversalStorage: f.traversalStorage,
			}
		},
	)
	require.NoError(t, err)

	err = registry.RegisterForExecution(
		"dataplane.CollectFilesystemSnapshots",
		func() tasks.Task {
			return &collectFilesystemSnapshotsTask{
				scheduler:                       scheduler,
				storage:                         f.snapshotStorage,
				snapshotCollectionTimeout:       collectionTimeout,
				snapshotCollectionInflightLimit: maxInflight,
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

func (f *fixture) requireSnapshotStorageTablesEmpty(t *testing.T) {
	t.Helper()

	require.True(t, f.snapshotStorageTablesEmpty(t))
}

func (f *fixture) snapshotStorageTablesEmpty(t *testing.T) bool {
	t.Helper()

	empty, err := f.snapshotStorage.TablesEmpty(f.ctx)
	require.NoError(t, err)
	return empty
}

func (f *fixture) requireTraversalQueuesEmpty(
	t *testing.T,
	snapshotIDs []string,
) {
	t.Helper()

	require.True(t, f.traversalQueuesEmpty(t, snapshotIDs))
}

func (f *fixture) traversalQueuesEmpty(
	t *testing.T,
	snapshotIDs []string,
) bool {
	t.Helper()

	for _, snapshotID := range snapshotIDs {
		queueEntries, err := f.traversalStorage.SelectNodesToList(
			f.ctx,
			snapshotID,
			map[uint64]struct{}{},
			100,
		)
		require.NoError(t, err)

		if len(queueEntries) != 0 {
			return false
		}
	}

	return true
}

func (f *fixture) snapshotDataDeleted(
	t *testing.T,
	snapshotIDs []string,
) bool {
	t.Helper()

	return f.snapshotStorageTablesEmpty(t) &&
		f.traversalQueuesEmpty(t, snapshotIDs)
}

func TestValidateConfigRejectsZeroLimits(t *testing.T) {
	restoreHardlinksBatchSize := uint32(0)
	fetchNodesFromStorageLimit := uint32(0)
	snapshotDataDeletionLimit := uint64(0)

	err := validateConfig(&snapshot_config.FilesystemSnapshotConfig{
		RestoreHardlinksBatchSize: &restoreHardlinksBatchSize,
	})
	require.Error(t, err)

	err = validateConfig(&snapshot_config.FilesystemSnapshotConfig{
		FetchNodesFromStorageLimit: &fetchNodesFromStorageLimit,
	})
	require.Error(t, err)

	err = validateConfig(&snapshot_config.FilesystemSnapshotConfig{
		SnapshotDataDeletionLimit: &snapshotDataDeletionLimit,
	})
	require.Error(t, err)
}

func (f *fixture) newCreateSnapshotFromFilesystemTask(
	config *snapshot_config.FilesystemSnapshotConfig,
	filesystemID string,
	snapshotID string,
) *createSnapshotFromFilesystemTask {

	return &createSnapshotFromFilesystemTask{
		config:           config,
		factory:          f.factory,
		storage:          f.snapshotStorage,
		traversalStorage: f.traversalStorage,
		nodesStorage:     f.nodesStorage,
		request: &snapshot_protos.CreateFilesystemSnapshotRequest{
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: filesystemID,
			},
			SnapshotId: snapshotID,
		},
		state: &snapshot_protos.CreateSnapshotFromFilesystemTaskState{},
	}
}

func (f *fixture) newTransferFromSnapshotToFilesystemTask(
	config *snapshot_config.FilesystemSnapshotConfig,
	filesystemID string,
	snapshotID string,
) *transferFromSnapshotToFilesystemTask {

	return &transferFromSnapshotToFilesystemTask{
		config:           config,
		factory:          f.factory,
		storage:          f.snapshotStorage,
		traversalStorage: f.traversalStorage,
		nodesStorage:     f.nodesStorage,
		request: &snapshot_protos.TransferFromSnapshotToFilesystemRequest{
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: filesystemID,
			},
			SnapshotId: snapshotID,
		},
		state: &snapshot_protos.TransferFromSnapshotToFilesystemTaskState{},
	}
}

func (f *fixture) newDeleteFilesystemSnapshotTask(
	snapshotID string,
) *deleteFilesystemSnapshotTask {

	return &deleteFilesystemSnapshotTask{
		storage: f.snapshotStorage,
		request: &snapshot_protos.DeleteFilesystemSnapshotRequest{
			SnapshotId: snapshotID,
		},
		state: &snapshot_protos.DeleteFilesystemSnapshotTaskState{},
	}
}

type cancelOnListNodesStorage struct {
	nodes_storage.Storage
	cancelAt int
	cancel   context.CancelFunc
	calls    int
}

////////////////////////////////////////////////////////////////////////////////
// TODO (jkuradobery): regroup classes and methods.

type withDeletionNodeStorage struct {
	nodes_storage.Storage
	deleteSnapshotFunc func(context.Context, string) error
	deleteOnce         sync.Once
	deleteResult       chan error
}

func newWithDeletionNodeStorage(
	storage nodes_storage.Storage,
	deleteSnapshot func(context.Context, string) error,
) *withDeletionNodeStorage {

	return &withDeletionNodeStorage{
		Storage:            storage,
		deleteSnapshotFunc: deleteSnapshot,
		deleteResult:       make(chan error, 1),
	}
}

func (s *withDeletionNodeStorage) deleteSnapshotOnce(
	ctx context.Context,
	snapshotID string,
) {

	s.deleteOnce.Do(func() {
		s.deleteResult <- s.deleteSnapshotFunc(ctx, snapshotID)
	})
}

func (s *withDeletionNodeStorage) requireDeletion(t *testing.T) {
	t.Helper()

	select {
	case err := <-s.deleteResult:
		require.NoError(t, err)
	case <-time.After(time.Second):
		require.Fail(t, "delete was not attempted")
	}
}

func (s *withDeletionNodeStorage) SaveNodes(
	ctx context.Context,
	snapshotID string,
	nodes []nfs.Node,
) error {

	err := s.Storage.SaveNodes(ctx, snapshotID, nodes)
	if err != nil {
		return err
	}

	s.deleteSnapshotOnce(ctx, snapshotID)
	return nil
}

func (s *withDeletionNodeStorage) UpdateRestorationNodeIDMapping(
	ctx context.Context,
	snapshotID string,
	filesystemID string,
	mappings map[uint64]uint64,
) error {

	err := s.Storage.UpdateRestorationNodeIDMapping(
		ctx,
		snapshotID,
		filesystemID,
		mappings,
	)
	if err != nil {
		return err
	}

	s.deleteSnapshotOnce(ctx, snapshotID)
	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (s *cancelOnListNodesStorage) ListNodes(
	ctx context.Context,
	snapshotID string,
	parentNodeID uint64,
	cookie string,
	limit int,
) ([]nfs.Node, string, error) {

	s.calls++
	if s.calls == s.cancelAt {
		s.cancel()
		return nil, "", context.Canceled
	}

	return s.Storage.ListNodes(
		ctx,
		snapshotID,
		parentNodeID,
		cookie,
		limit,
	)
}

type cancelOnListNodesSession struct {
	nfs.Session
	factory *cancelOnListNodesFactory
}

func (s *cancelOnListNodesSession) ListNodes(
	ctx context.Context,
	parentNodeID uint64,
	cookie string,
	maxBytes uint32,
	unsafe bool,
) ([]nfs.Node, string, error) {

	s.factory.calls++
	if s.factory.calls == s.factory.cancelAt {
		s.factory.cancel()
		return nil, "", context.Canceled
	}

	return s.Session.ListNodes(ctx, parentNodeID, cookie, maxBytes, unsafe)
}

type cancelOnListNodesClient struct {
	nfs.Client
	factory *cancelOnListNodesFactory
}

func (c *cancelOnListNodesClient) CreateSession(
	ctx context.Context,
	fileSystemID string,
	checkpointID string,
	readonly bool,
) (nfs.Session, error) {

	session, err := c.Client.CreateSession(
		ctx,
		fileSystemID,
		checkpointID,
		readonly,
	)
	if err != nil {
		return nil, err
	}

	return &cancelOnListNodesSession{
		Session: session,
		factory: c.factory,
	}, nil
}

type cancelOnListNodesFactory struct {
	nfs.Factory
	cancelAt int
	cancel   context.CancelFunc
	calls    int
}

func (f *cancelOnListNodesFactory) NewClient(
	ctx context.Context,
	zoneID string,
) (nfs.Client, error) {

	client, err := f.Factory.NewClient(ctx, zoneID)
	if err != nil {
		return nil, err
	}

	return &cancelOnListNodesClient{
		Client:  client,
		factory: f,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateSnapshotFromFilesystemSkipsInvalidNodeIDs(t *testing.T) {
	filesystemID := "filesystem"
	snapshotID := "snapshot"
	zoneID := "zone"
	checkpointID := "checkpoint"

	listNodesMaxBytes := uint32(1000)
	selectNodesToListLimit := uint64(100)
	traversalQueueDeletionLimit := uint64(100)
	traversalWorkersCount := uint32(1)
	config := &snapshot_config.FilesystemSnapshotConfig{
		TraversalConfig: &traversal_config.FilesystemTraversalConfig{
			SelectNodesToListLimit:      &selectNodesToListLimit,
			TraversalQueueDeletionLimit: &traversalQueueDeletionLimit,
			TraversalWorkersCount:       &traversalWorkersCount,
		},
		ListNodesMaxBytes: &listNodesMaxBytes,
	}

	validNode := nfs.Node{
		NodeID:       123,
		ParentNodeID: nfs.RootNodeID,
		Name:         "valid",
		Type:         nfs.NODE_KIND_FILE,
		Links:        1,
	}
	anotherValidNode := nfs.Node{
		NodeID:       456,
		ParentNodeID: nfs.RootNodeID,
		Name:         "another-valid",
		Type:         nfs.NODE_KIND_FILE,
		Links:        1,
	}
	invalidNode := nfs.Node{
		NodeID:       nfs.InvalidNodeID,
		ParentNodeID: nfs.RootNodeID,
		Name:         "invalid",
		Type:         nfs.NODE_KIND_FILE,
		Links:        1,
	}

	factoryMock := nfs_mocks.NewFactoryMock()
	clientMock := nfs_mocks.NewClientMock()
	sessionMock := nfs_mocks.NewSessionMock()
	clientMock.On("Close").Return(nil)

	f := newFixtureWithFactory(
		t,
		nfs_testing.NewContext(),
		clientMock,
		factoryMock,
	)
	defer f.close(t)

	factoryMock.On("NewClient", mock.Anything, zoneID).Return(clientMock, nil).Once()
	clientMock.On(
		"CreateSession",
		mock.Anything,
		filesystemID,
		checkpointID,
		true,
	).Return(sessionMock, nil).Once()
	sessionMock.On(
		"ListNodes",
		mock.Anything,
		nfs.RootNodeID,
		"",
		listNodesMaxBytes,
		true,
	).Return([]nfs.Node{validNode, invalidNode, anotherValidNode}, "", nil).Once()
	sessionMock.On("Close", mock.Anything).Return(nil).Once()

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("create-snapshot-from-filesystem")
	execCtx.On("SaveState", mock.Anything).Return(nil).Once()

	task := f.newCreateSnapshotFromFilesystemTask(
		config,
		filesystemID,
		snapshotID,
	)
	task.request.CheckpointId = checkpointID

	err := task.Run(f.ctx, execCtx)
	require.NoError(t, err)

	nodes, nextCookie, err := f.nodesStorage.ListNodes(
		f.ctx,
		snapshotID,
		nfs.RootNodeID,
		"",
		100,
	)
	require.NoError(t, err)
	require.Empty(t, nextCookie)
	require.Equal(t, []nfs.Node{anotherValidNode, validNode}, nodes)

	factoryMock.AssertExpectations(t)
	clientMock.AssertExpectations(t)
	sessionMock.AssertExpectations(t)
	execCtx.AssertExpectations(t)
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateSnapshotFromFilesystemAndBack(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	filesystemID := t.Name()
	f.prepareFilesystem(t, filesystemID)
	defer f.cleanupFilesystem(t, filesystemID)

	root := nfs_testing.Root(
		nfs_testing.Dir("etc",
			nfs_testing.File("passwd"),
			nfs_testing.File("hosts"),
		),
		nfs_testing.Dir("var",
			nfs_testing.Dir("log"),
		),
		nfs_testing.Symlink("bin", "/usr/bin"),
		nfs_testing.Dir("usr",
			nfs_testing.Dir("bin",
				nfs_testing.File("bash"),
				nfs_testing.File("ls"),
			),
			nfs_testing.Dir("lib",
				nfs_testing.File("libc.so"),
			),
		),
	)
	fsModel := f.fillFilesystem(t, filesystemID, root)
	defer fsModel.Close()

	expectedNames := tasks_common.NewStringSet(
		nfs_testing.NodeNames(fsModel.ListAllNodesRecursively(false))...,
	)

	config := f.newConfig()
	snapshotID := "snapshot-1"

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("transfer-to-snapshot-task")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	toSnapshotTask := f.newCreateSnapshotFromFilesystemTask(
		config,
		filesystemID,
		snapshotID,
	)

	err := toSnapshotTask.Run(f.ctx, execCtx)
	require.NoError(t, err)

	dstFilesystemID := filesystemID + "_restored"
	f.prepareFilesystem(t, dstFilesystemID)
	defer f.cleanupFilesystem(t, dstFilesystemID)

	restoreExecCtx := tasks_mocks.NewExecutionContextMock()
	restoreExecCtx.On("GetTaskID").Return("transfer-from-snapshot-task")
	restoreExecCtx.On("SaveState", mock.Anything).Return(nil)

	fromSnapshotTask := f.newTransferFromSnapshotToFilesystemTask(
		config,
		dstFilesystemID,
		snapshotID,
	)

	err = fromSnapshotTask.Run(f.ctx, restoreExecCtx)
	require.NoError(t, err)

	session, err := f.client.CreateSession(f.ctx, dstFilesystemID, "", true)
	require.NoError(t, err)
	defer session.Close(f.ctx)

	restoredModel := nfs_testing.NewFileSystemModel(
		t,
		f.ctx,
		session,
		nfs_testing.Root(),
	)
	restoredNames := tasks_common.NewStringSet(
		nfs_testing.NodeNames(restoredModel.ListAllNodesRecursively(false))...,
	)
	require.True(t, expectedNames.Equals(restoredNames))
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateSnapshotFromFilesystemAndBackWithDeviceHardlink(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	filesystemID := t.Name()
	f.prepareFilesystem(t, filesystemID)
	defer f.cleanupFilesystem(t, filesystemID)

	const (
		deviceName     = "device"
		deviceLinkName = "device-link"
		deviceID       = uint64(0x12345)
	)

	root := nfs_testing.Root(
		nfs_testing.Dir("dir",
			nfs_testing.CharDev(deviceName, deviceID),
		),
		nfs_testing.Dir("dir2"),
	)
	fsModel := f.fillFilesystem(t, filesystemID, root)
	defer fsModel.Close()

	sourceSession, err := f.client.CreateSession(f.ctx, filesystemID, "", false)
	require.NoError(t, err)
	defer sourceSession.Close(f.ctx)

	dirNode, err := sourceSession.GetNodeAttr(f.ctx, nfs.RootNodeID, "dir")
	require.NoError(t, err)
	deviceNode, err := sourceSession.GetNodeAttr(f.ctx, dirNode.NodeID, deviceName)
	require.NoError(t, err)
	require.Equal(t, deviceID, deviceNode.DevID)

	dir2Node, err := sourceSession.GetNodeAttr(f.ctx, nfs.RootNodeID, "dir2")
	require.NoError(t, err)
	_, err = sourceSession.CreateNode(f.ctx, nfs.Node{
		ParentNodeID: dir2Node.NodeID,
		NodeID:       deviceNode.NodeID,
		Name:         deviceLinkName,
		Type:         nfs.NODE_KIND_LINK,
	})
	require.NoError(t, err)

	deviceLinkNode, err := sourceSession.GetNodeAttr(
		f.ctx,
		dir2Node.NodeID,
		deviceLinkName,
	)
	require.NoError(t, err)
	require.Equal(t, deviceNode.NodeID, deviceLinkNode.NodeID)
	require.Equal(t, deviceID, deviceLinkNode.DevID)

	config := f.newConfig()
	snapshotID := "snapshot-device-hardlink"

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("device-hardlink-to-snapshot")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	toSnapshotTask := f.newCreateSnapshotFromFilesystemTask(
		config,
		filesystemID,
		snapshotID,
	)

	err = toSnapshotTask.Run(f.ctx, execCtx)
	require.NoError(t, err)

	dstFilesystemID := filesystemID + "_restored"
	f.prepareFilesystem(t, dstFilesystemID)
	defer f.cleanupFilesystem(t, dstFilesystemID)

	restoreExecCtx := tasks_mocks.NewExecutionContextMock()
	restoreExecCtx.On("GetTaskID").Return("device-hardlink-from-snapshot")
	restoreExecCtx.On("SaveState", mock.Anything).Return(nil)

	fromSnapshotTask := f.newTransferFromSnapshotToFilesystemTask(
		config,
		dstFilesystemID,
		snapshotID,
	)

	err = fromSnapshotTask.Run(f.ctx, restoreExecCtx)
	require.NoError(t, err)

	restoredSession, err := f.client.CreateSession(f.ctx, dstFilesystemID, "", true)
	require.NoError(t, err)
	defer restoredSession.Close(f.ctx)

	restoredDirNode, err := restoredSession.GetNodeAttr(
		f.ctx,
		nfs.RootNodeID,
		"dir",
	)
	require.NoError(t, err)
	restoredDeviceNode, err := restoredSession.GetNodeAttr(
		f.ctx,
		restoredDirNode.NodeID,
		deviceName,
	)
	require.NoError(t, err)

	restoredDir2Node, err := restoredSession.GetNodeAttr(
		f.ctx,
		nfs.RootNodeID,
		"dir2",
	)
	require.NoError(t, err)
	restoredDeviceLinkNode, err := restoredSession.GetNodeAttr(
		f.ctx,
		restoredDir2Node.NodeID,
		deviceLinkName,
	)
	require.NoError(t, err)

	require.Equal(t, restoredDeviceNode.NodeID, restoredDeviceLinkNode.NodeID)
	require.Equal(t, deviceID, restoredDeviceNode.DevID)
	require.Equal(t, deviceID, restoredDeviceLinkNode.DevID)
	require.Equal(t, nfs.NODE_KIND_CHARDEV, restoredDeviceLinkNode.Type)
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateSnapshotFromFilesystemAndBackWithNemesis(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	filesystemID := t.Name()
	f.prepareFilesystem(t, filesystemID)
	defer f.cleanupFilesystem(t, filesystemID)

	layers := []nfs_testing.FilesystemLayerConfig{
		{DirsCount: 3, FilesCount: 0},
		{DirsCount: 5, FilesCount: 100},
		{DirsCount: 10, FilesCount: 100},
		{DirsCount: 0, FilesCount: 5},
	}
	root := nfs_testing.HomogeneousDirectoryTree(layers)
	fsModel := f.fillFilesystemParallel(t, filesystemID, root)
	defer fsModel.Close()

	config := f.newConfig()
	snapshotID := "snapshot-nemesis"

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("nemesis-to-snapshot")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	toSnapshotTask := f.newCreateSnapshotFromFilesystemTask(
		config,
		filesystemID,
		snapshotID,
	)

	err := util.WithNemesis(
		f.ctx,
		func(ctx context.Context) error {
			return toSnapshotTask.Run(ctx, execCtx)
		},
		0,
		500*time.Millisecond,
	)
	require.NoError(t, err)

	dstFilesystemID := filesystemID + "_restored"
	f.prepareFilesystem(t, dstFilesystemID)
	defer f.cleanupFilesystem(t, dstFilesystemID)

	restoreExecCtx := tasks_mocks.NewExecutionContextMock()
	restoreExecCtx.On("GetTaskID").Return("nemesis-from-snapshot")
	restoreExecCtx.On("SaveState", mock.Anything).Return(nil)

	fromSnapshotTask := f.newTransferFromSnapshotToFilesystemTask(
		config,
		dstFilesystemID,
		snapshotID,
	)

	err = util.WithNemesis(
		f.ctx,
		func(ctx context.Context) error {
			return fromSnapshotTask.Run(ctx, restoreExecCtx)
		},
		0,
		500*time.Millisecond,
	)
	require.NoError(t, err)

	session, err := f.client.CreateSession(f.ctx, dstFilesystemID, "", true)
	require.NoError(t, err)
	defer session.Close(f.ctx)

	restoredModel := nfs_testing.NewFileSystemModel(
		t,
		f.ctx,
		session,
		nfs_testing.Root(),
	)
	expectedNames := fsModel.ExpectedNodeNames()
	restoredNames := tasks_common.NewStringSet(
		nfs_testing.NodeNames(restoredModel.ListAllNodesRecursively(false))...,
	)
	require.True(t, expectedNames.Equals(restoredNames))
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateSnapshotFromFilesystemCancelMarksDeleted(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	filesystemID := t.Name()
	f.prepareFilesystem(t, filesystemID)
	defer f.cleanupFilesystem(t, filesystemID)

	layers := []nfs_testing.FilesystemLayerConfig{
		{DirsCount: 1000, FilesCount: 10},
		{DirsCount: 5, FilesCount: 1},
	}
	root := nfs_testing.HomogeneousDirectoryTree(layers)
	fsModel := f.fillFilesystemParallel(t, filesystemID, root)
	defer fsModel.Close()

	config := f.newSlowConfig()
	snapshotID := "snapshot-cancel-to"
	runCtx, cancel := context.WithCancel(f.ctx)

	cancelFactory := &cancelOnListNodesFactory{
		Factory:  f.factory,
		cancelAt: 5,
		cancel:   cancel,
	}

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("cancel-to-snapshot")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	task := f.newCreateSnapshotFromFilesystemTask(
		config,
		filesystemID,
		snapshotID,
	)
	task.factory = cancelFactory

	err := task.Run(runCtx, execCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Equal(t, 5, cancelFactory.calls)

	queueEntries, err := f.traversalStorage.SelectNodesToList(
		f.ctx,
		snapshotID,
		map[uint64]struct{}{},
		100,
	)
	require.NoError(t, err)
	require.NotEmpty(t, queueEntries)

	savedNodes, _, err := f.nodesStorage.ListNodes(
		f.ctx,
		snapshotID,
		nfs.RootNodeID,
		"",
		100,
	)
	require.NoError(t, err)
	require.NotEmpty(t, savedNodes)

	err = task.Cancel(f.ctx, execCtx)
	require.NoError(t, err)

	savedNodes, _, err = f.nodesStorage.ListNodes(
		f.ctx,
		snapshotID,
		nfs.RootNodeID,
		"",
		100,
	)
	require.NoError(t, err)
	require.NotEmpty(t, savedNodes)

	err = f.snapshotStorage.CheckFilesystemSnapshotAlive(f.ctx, snapshotID)
	require.Error(t, err)

	keys, err := f.snapshotStorage.GetFilesystemSnapshotsToDelete(
		f.ctx,
		time.Now().Add(time.Second),
		100,
	)
	require.NoError(t, err)
	require.Len(t, keys, 1)
	require.Equal(t, snapshotID, keys[0].SnapshotId)
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateSnapshotFromFilesystemDoesNotRunWhenDeletedBeforeTransfer(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	filesystemID := t.Name()
	snapshotID := "snapshot-deleted-before-transfer"

	_, err := f.snapshotStorage.DeletingFilesystemSnapshot(
		f.ctx,
		snapshotID,
		"delete-task",
	)
	require.NoError(t, err)

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("create-deleted-snapshot")

	task := f.newCreateSnapshotFromFilesystemTask(
		f.newConfig(),
		filesystemID,
		snapshotID,
	)

	err = task.Run(f.ctx, execCtx)
	require.Error(t, err)

	savedNodes, _, err := f.nodesStorage.ListNodes(
		f.ctx,
		snapshotID,
		nfs.RootNodeID,
		"",
		100,
	)
	require.NoError(t, err)
	require.Empty(t, savedNodes)
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateSnapshotFromFilesystemStopsWhenDeletedDuringTransfer(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	filesystemID := t.Name()
	f.prepareFilesystem(t, filesystemID)
	defer f.cleanupFilesystem(t, filesystemID)

	root := nfs_testing.Root(
		nfs_testing.Dir("dir",
			nfs_testing.File("file"),
		),
	)
	fsModel := f.fillFilesystem(t, filesystemID, root)
	defer fsModel.Close()

	config := f.newConfig()
	snapshotID := "snapshot-delete-during-transfer"

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("create-snapshot-task")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	task := f.newCreateSnapshotFromFilesystemTask(
		config,
		filesystemID,
		snapshotID,
	)
	storage := newWithDeletionNodeStorage(
		f.nodesStorage,
		func(ctx context.Context, snapshotID string) error {
			_, err := f.snapshotStorage.DeletingFilesystemSnapshot(
				ctx,
				snapshotID,
				"delete-task",
			)
			return err
		},
	)
	task.nodesStorage = storage

	err := task.Run(f.ctx, execCtx)
	require.Error(t, err)
	storage.requireDeletion(t)

	meta, err := f.snapshotStorage.GetFilesystemSnapshotMeta(f.ctx, snapshotID)
	require.NoError(t, err)
	require.NotNil(t, meta)
	require.False(t, meta.Ready)

	err = f.snapshotStorage.CheckFilesystemSnapshotAlive(f.ctx, snapshotID)
	require.Error(t, err)
}

////////////////////////////////////////////////////////////////////////////////

func TestRestoreFromSnapshotNotReady(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	filesystemID := t.Name()
	snapshotID := "snapshot-not-ready"

	_, err := f.snapshotStorage.CreateFilesystemSnapshot(
		f.ctx,
		snapshot_storage.FilesystemSnapshotMeta{
			ID:           snapshotID,
			CreateTaskID: "create-snapshot",
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: "source-filesystem",
			},
		},
	)
	require.NoError(t, err)

	f.prepareFilesystem(t, filesystemID)
	defer f.cleanupFilesystem(t, filesystemID)

	execCtx := tasks_mocks.NewExecutionContextMock()
	task := f.newTransferFromSnapshotToFilesystemTask(
		f.newConfig(),
		filesystemID,
		snapshotID,
	)

	err = task.Run(f.ctx, execCtx)
	require.Error(t, err)
	require.ErrorIs(t, err, task_errors.NewEmptyNonRetriableError())
}

////////////////////////////////////////////////////////////////////////////////

func TestRestoreFromSnapshotDeleted(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	filesystemID := t.Name()
	f.prepareFilesystem(t, filesystemID)
	defer f.cleanupFilesystem(t, filesystemID)

	root := nfs_testing.Root(
		nfs_testing.Dir("dir",
			nfs_testing.File("file"),
		),
	)
	fsModel := f.fillFilesystem(t, filesystemID, root)
	defer fsModel.Close()

	config := f.newConfig()
	snapshotID := "snapshot-delete-during-restore"

	toSnapshotExecCtx := tasks_mocks.NewExecutionContextMock()
	toSnapshotExecCtx.On("GetTaskID").Return("setup-to-snapshot")
	toSnapshotExecCtx.On("SaveState", mock.Anything).Return(nil)

	toSnapshotTask := f.newCreateSnapshotFromFilesystemTask(
		config,
		filesystemID,
		snapshotID,
	)

	err := toSnapshotTask.Run(f.ctx, toSnapshotExecCtx)
	require.NoError(t, err)

	dstFilesystemID := filesystemID + "_restored"
	f.prepareFilesystem(t, dstFilesystemID)
	defer f.cleanupFilesystem(t, dstFilesystemID)

	restoreExecCtx := tasks_mocks.NewExecutionContextMock()
	restoreExecCtx.On("GetTaskID").Return("delete-during-restore")
	restoreExecCtx.On("SaveState", mock.Anything).Return(nil)

	fromSnapshotTask := f.newTransferFromSnapshotToFilesystemTask(
		config,
		dstFilesystemID,
		snapshotID,
	)
	storage := newWithDeletionNodeStorage(
		f.nodesStorage,
		func(ctx context.Context, snapshotID string) error {
			deleteExecCtx := tasks_mocks.NewExecutionContextMock()
			deleteExecCtx.On("GetTaskID").Return("delete-snapshot")

			deleteTask := f.newDeleteFilesystemSnapshotTask(snapshotID)
			return deleteTask.Run(ctx, deleteExecCtx)
		},
	)
	fromSnapshotTask.nodesStorage = storage

	err = fromSnapshotTask.Run(f.ctx, restoreExecCtx)
	require.Error(t, err)
	require.ErrorIs(t, err, task_errors.NewEmptyNonRetriableError())
	storage.requireDeletion(t)

	err = f.snapshotStorage.CheckFilesystemSnapshotAlive(f.ctx, snapshotID)
	require.Error(t, err)
	require.ErrorIs(t, err, task_errors.NewEmptyNonRetriableError())
}

////////////////////////////////////////////////////////////////////////////////

func TestCreateFsFromSnapshotCancelCleansRestoreState(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	filesystemID := t.Name()
	f.prepareFilesystem(t, filesystemID)
	defer f.cleanupFilesystem(t, filesystemID)

	layers := []nfs_testing.FilesystemLayerConfig{
		{DirsCount: 5000, FilesCount: 0},
		{DirsCount: 0, FilesCount: 5},
	}
	root := nfs_testing.HomogeneousDirectoryTree(layers)
	fsModel := f.fillFilesystemParallel(t, filesystemID, root)
	defer fsModel.Close()

	config := f.newSlowConfig()
	snapshotID := "snapshot-cancel-from"

	toSnapshotExecCtx := tasks_mocks.NewExecutionContextMock()
	toSnapshotExecCtx.On("GetTaskID").Return("setup-to-snapshot")
	toSnapshotExecCtx.On("SaveState", mock.Anything).Return(nil)

	toSnapshotTask := f.newCreateSnapshotFromFilesystemTask(
		f.newConfig(),
		filesystemID,
		snapshotID,
	)

	err := toSnapshotTask.Run(f.ctx, toSnapshotExecCtx)
	require.NoError(t, err)

	srcRootNodes, _, err := f.nodesStorage.ListNodes(
		f.ctx,
		snapshotID,
		nfs.RootNodeID,
		"",
		100,
	)
	require.NoError(t, err)
	require.NotEmpty(t, srcRootNodes)

	var srcDirectoryNodeIDs []uint64
	for _, node := range srcRootNodes {
		if node.Type.IsDirectory() {
			srcDirectoryNodeIDs = append(srcDirectoryNodeIDs, node.NodeID)
		}
	}
	require.NotEmpty(t, srcDirectoryNodeIDs)

	dstFilesystemID := filesystemID + "_restored"
	f.prepareFilesystem(t, dstFilesystemID)
	defer f.cleanupFilesystem(t, dstFilesystemID)

	restoreExecCtx := tasks_mocks.NewExecutionContextMock()
	restoreExecCtx.On("GetTaskID").Return("cancel-from-snapshot")
	restoreExecCtx.On("SaveState", mock.Anything).Return(nil)

	runCtx, cancel := context.WithCancel(f.ctx)
	cancelStorage := &cancelOnListNodesStorage{
		Storage:  f.nodesStorage,
		cancelAt: 5,
		cancel:   cancel,
	}

	fromSnapshotTask := f.newTransferFromSnapshotToFilesystemTask(
		config,
		dstFilesystemID,
		snapshotID,
	)
	fromSnapshotTask.nodesStorage = cancelStorage

	err = fromSnapshotTask.Run(runCtx, restoreExecCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	require.Equal(t, 5, cancelStorage.calls)

	queueEntries, err := f.traversalStorage.SelectNodesToList(
		f.ctx,
		fromSnapshotTask.traversalID(restoreExecCtx),
		map[uint64]struct{}{},
		100,
	)
	require.NoError(t, err)

	mappings, err := f.nodesStorage.GetDestinationNodeIDs(
		f.ctx,
		snapshotID,
		dstFilesystemID,
		srcDirectoryNodeIDs,
	)
	require.NoError(t, err)
	require.NotEmpty(t, queueEntries)
	require.NotEmpty(t, mappings)

	err = fromSnapshotTask.Cancel(f.ctx, restoreExecCtx)
	require.NoError(t, err)

	queueEntries, err = f.traversalStorage.SelectNodesToList(
		f.ctx,
		fromSnapshotTask.traversalID(restoreExecCtx),
		map[uint64]struct{}{},
		100,
	)
	require.NoError(t, err)
	require.Empty(t, queueEntries)

	mappings, err = f.nodesStorage.GetDestinationNodeIDs(
		f.ctx,
		snapshotID,
		dstFilesystemID,
		srcDirectoryNodeIDs,
	)
	require.NoError(t, err)
	require.Empty(t, mappings)

	snapshotNodes, _, err := f.nodesStorage.ListNodes(
		f.ctx,
		snapshotID,
		nfs.RootNodeID,
		"",
		100,
	)
	require.NoError(t, err)
	require.NotEmpty(t, snapshotNodes)
}

////////////////////////////////////////////////////////////////////////////////

func TestCollectFilesystemSnapshotsTaskDeletesSnapshotData(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	runnerCtx, cancelRunner := context.WithCancel(f.ctx)
	defer cancelRunner()

	maxInflight := 2
	collectionTimeout := 50 * time.Millisecond
	scheduler := f.startSnapshotCollectionTaskRunner(
		t,
		runnerCtx,
		maxInflight,
		collectionTimeout,
	)

	filesystemID := t.Name()
	shardCount := uint32(3)
	f.prepareMultishardFilesystem(t, filesystemID, shardCount)
	defer f.cleanupFilesystem(t, filesystemID)

	fsModel := f.fillFilesystem(t, filesystemID, nfs_testing.StandardFilesystem)
	defer fsModel.Close()

	sourceSession, err := f.client.CreateSession(f.ctx, filesystemID, "", false)
	require.NoError(t, err)
	defer sourceSession.Close(f.ctx)

	etcNode, err := sourceSession.GetNodeAttr(f.ctx, nfs.RootNodeID, "etc")
	require.NoError(t, err)
	passwdNode, err := sourceSession.GetNodeAttr(f.ctx, etcNode.NodeID, "passwd")
	require.NoError(t, err)

	_, err = sourceSession.CreateNode(f.ctx, nfs.Node{
		ParentNodeID: etcNode.NodeID,
		NodeID:       passwdNode.NodeID,
		Name:         "passwd-hardlink",
		Type:         nfs.NODE_KIND_LINK,
	})
	require.NoError(t, err)

	hardlinkNode, err := sourceSession.GetNodeAttr(
		f.ctx,
		etcNode.NodeID,
		"passwd-hardlink",
	)
	require.NoError(t, err)
	require.Equal(t, passwdNode.NodeID, hardlinkNode.NodeID)
	require.GreaterOrEqual(t, hardlinkNode.Links, uint32(2))

	sourceNodes := fsModel.ListAllNodesRecursively(true)
	require.NotEmpty(t, sourceNodes)

	shardIDs := tasks_common.NewStringSet()
	for _, node := range sourceNodes {
		require.NotEmpty(t, node.ShardFileSystemID)
		require.NotEmpty(t, node.ShardNodeName)
		shardIDs.Add(node.ShardFileSystemID)
	}
	require.NotZero(t, shardIDs.Size())

	snapshotIDs := make([]string, maxInflight+2)
	for i := range snapshotIDs {
		snapshotIDs[i] = fmt.Sprintf("snapshot-%d", i)
		taskID, err := scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(
				f.ctx,
				fmt.Sprintf("create-%s", snapshotIDs[i]),
			),
			"dataplane.CreateSnapshotFromFilesystem",
			"",
			&snapshot_protos.CreateFilesystemSnapshotRequest{
				Filesystem: &types.Filesystem{
					ZoneId:       "zone",
					FilesystemId: filesystemID,
				},
				SnapshotId: snapshotIDs[i],
			},
		)
		require.NoError(t, err)

		_, err = scheduler.WaitTaskSync(f.ctx, taskID, 30*time.Second)
		require.NoError(t, err)

		for _, shardID := range shardIDs.List() {
			nodes, nextCookie, err := f.nodesStorage.ListNodesByShard(
				f.ctx,
				snapshotIDs[i],
				shardID,
				100,
				nil,
			)
			require.NoError(t, err)
			require.NotEmpty(t, nodes)
			require.Nil(t, nextCookie)
		}
	}

	hardlinks, err := f.nodesStorage.ListHardLinks(
		f.ctx,
		snapshotIDs[0],
		100,
		0,
	)
	require.NoError(t, err)
	require.NotEmpty(t, hardlinks)

	sourceNodeIDs := make([]uint64, 0, 2)
	restorationMapping := make(map[uint64]uint64)
	for _, node := range sourceNodes {
		if len(sourceNodeIDs) == 2 {
			break
		}

		sourceNodeIDs = append(sourceNodeIDs, node.NodeID)
		restorationMapping[node.NodeID] = node.NodeID + 1000000
	}
	require.Len(t, sourceNodeIDs, 2)

	restoredFilesystemID := filesystemID + "_restored"
	err = f.nodesStorage.UpdateRestorationNodeIDMapping(
		f.ctx,
		snapshotIDs[0],
		restoredFilesystemID,
		restorationMapping,
	)
	require.NoError(t, err)

	mappings, err := f.nodesStorage.GetDestinationNodeIDs(
		f.ctx,
		snapshotIDs[0],
		restoredFilesystemID,
		sourceNodeIDs,
	)
	require.NoError(t, err)
	require.NotEmpty(t, mappings)

	err = f.traversalStorage.SchedulerDirectoryForTraversal(
		f.ctx,
		snapshotIDs[0],
		nfs.RootNodeID,
	)
	require.NoError(t, err)
	queueEntries, err := f.traversalStorage.SelectNodesToList(
		f.ctx,
		snapshotIDs[0],
		map[uint64]struct{}{},
		100,
	)
	require.NoError(t, err)
	require.NotEmpty(t, queueEntries)

	for _, snapshotID := range snapshotIDs {
		taskID, err := scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(
				f.ctx,
				fmt.Sprintf("delete-%s", snapshotID),
			),
			"dataplane.DeleteFilesystemSnapshot",
			"",
			&snapshot_protos.DeleteFilesystemSnapshotRequest{
				SnapshotId: snapshotID,
			},
		)
		require.NoError(t, err)

		_, err = scheduler.WaitTaskSync(f.ctx, taskID, 30*time.Second)
		require.NoError(t, err)
	}

	keys, err := f.snapshotStorage.GetFilesystemSnapshotsToDelete(
		f.ctx,
		time.Now().Add(time.Second),
		len(snapshotIDs)+1,
	)
	require.NoError(t, err)
	require.Len(t, keys, len(snapshotIDs))

	time.Sleep(2 * collectionTimeout)

	_, err = scheduler.ScheduleTask(
		headers.SetIncomingIdempotencyKey(f.ctx, "collect-filesystem-snapshots"),
		"dataplane.CollectFilesystemSnapshots",
		"",
		&empty.Empty{},
	)
	require.NoError(t, err)

	require.Eventually(
		t,
		func() bool {
			return f.snapshotDataDeleted(t, snapshotIDs)
		},
		30*time.Second,
		100*time.Millisecond,
	)
	f.requireSnapshotStorageTablesEmpty(t)
	f.requireTraversalQueuesEmpty(t, snapshotIDs)

	keys, err = f.snapshotStorage.GetFilesystemSnapshotsToDelete(
		f.ctx,
		time.Now().Add(time.Second),
		len(snapshotIDs)+1,
	)
	require.NoError(t, err)
	require.Empty(t, keys)

	snapshots, err := f.snapshotStorage.ListFilesystemSnapshots(f.ctx)
	require.NoError(t, err)
	require.Zero(t, snapshots.Size())

	snapshotCount, err := f.snapshotStorage.GetFilesystemSnapshotCount(f.ctx)
	require.NoError(t, err)
	require.Zero(t, snapshotCount)

	for _, snapshotID := range snapshotIDs {
		err = f.snapshotStorage.CheckFilesystemSnapshotReady(
			f.ctx,
			snapshotID,
		)
		require.Error(t, err)

		meta, err := f.snapshotStorage.GetFilesystemSnapshotMeta(
			f.ctx,
			snapshotID,
		)
		require.NoError(t, err)
		require.Nil(t, meta)
	}
}
