package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/config"
	snapshot_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/protos"
	nodes_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/nodes"
	snapshot_storage_schema "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/schema"
	traversal_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/config"
	traversal_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage"
	traversal_storage_schema "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
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
	traversalStorage traversal_storage.Storage
	nodesStorage     nodes_storage.Storage
	client           nfs.Client
	factory          nfs.Factory
}

func newFixture(t *testing.T) *fixture {
	ctx := nfs_testing.NewContext()

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

	client := nfs_testing.NewClient(t, ctx)
	factory := nfs_testing.NewFactory(ctx)

	return &fixture{
		ctx:              ctx,
		db:               db,
		traversalStorage: traversal_storage.NewStorage(db, traversalStorageFolder),
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
			SelectNodesToListLimit: &selectNodesToListLimit,
		},
		ListNodesMaxBytes:           &listNodesMaxBytes,
		RestoreHardlinksBatchSize:   &restoreHardlinksBatchSize,
		TraversalQueueDeletionLimit: &traversalQueueDeletionLimit,
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
			SelectNodesToListLimit: &selectNodesToListLimit,
			TraversalWorkersCount:  &traversalWorkersCount,
		},
		ListNodesMaxBytes:           &listNodesMaxBytes,
		RestoreHardlinksBatchSize:   &restoreHardlinksBatchSize,
		TraversalQueueDeletionLimit: &traversalQueueDeletionLimit,
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestTransferFromFilesystemToSnapshotAndBack(t *testing.T) {
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
		nfs_testing.NodeNames(fsModel.ListAllNodesRecursively())...,
	)

	config := f.newConfig()
	snapshotID := "snapshot-1"

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("transfer-to-snapshot-task")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	toSnapshotTask := &transferFromFilesystemToSnapshotTask{
		config:           config,
		factory:          f.factory,
		traversalStorage: f.traversalStorage,
		nodesStorage:     f.nodesStorage,
		request: &snapshot_protos.CreateFilesystemSnapshotRequest{
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: filesystemID,
			},
			SnapshotId: snapshotID,
		},
		state: &snapshot_protos.TransferFromFilesystemToSnapshotTaskState{},
	}

	err := toSnapshotTask.Run(f.ctx, execCtx)
	require.NoError(t, err)

	dstFilesystemID := filesystemID + "_restored"
	f.prepareFilesystem(t, dstFilesystemID)
	defer f.cleanupFilesystem(t, dstFilesystemID)

	restoreExecCtx := tasks_mocks.NewExecutionContextMock()
	restoreExecCtx.On("GetTaskID").Return("transfer-from-snapshot-task")
	restoreExecCtx.On("SaveState", mock.Anything).Return(nil)

	fromSnapshotTask := &transferFromSnapshotToFilesystemTask{
		config:           config,
		factory:          f.factory,
		traversalStorage: f.traversalStorage,
		nodesStorage:     f.nodesStorage,
		request: &snapshot_protos.TransferFromSnapshotToFilesystemRequest{
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: dstFilesystemID,
			},
			SnapshotId: snapshotID,
		},
		state: &snapshot_protos.TransferFromSnapshotToFilesystemTaskState{},
	}

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
		nfs_testing.NodeNames(restoredModel.ListAllNodesRecursively())...,
	)
	require.True(t, expectedNames.Equals(restoredNames))
}

////////////////////////////////////////////////////////////////////////////////

func TestTransferFromFilesystemToSnapshotAndBackWithNemesis(t *testing.T) {
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

	toSnapshotTask := &transferFromFilesystemToSnapshotTask{
		config:           config,
		factory:          f.factory,
		traversalStorage: f.traversalStorage,
		nodesStorage:     f.nodesStorage,
		request: &snapshot_protos.CreateFilesystemSnapshotRequest{
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: filesystemID,
			},
			SnapshotId: snapshotID,
		},
		state: &snapshot_protos.TransferFromFilesystemToSnapshotTaskState{},
	}

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

	fromSnapshotTask := &transferFromSnapshotToFilesystemTask{
		config:           config,
		factory:          f.factory,
		traversalStorage: f.traversalStorage,
		nodesStorage:     f.nodesStorage,
		request: &snapshot_protos.TransferFromSnapshotToFilesystemRequest{
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: dstFilesystemID,
			},
			SnapshotId: snapshotID,
		},
		state: &snapshot_protos.TransferFromSnapshotToFilesystemTaskState{},
	}

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
		nfs_testing.NodeNames(restoredModel.ListAllNodesRecursively())...,
	)
	require.True(t, expectedNames.Equals(restoredNames))
}

////////////////////////////////////////////////////////////////////////////////

func TestTransferFromFilesystemToSnapshotCancelDeletesData(t *testing.T) {
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

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("cancel-to-snapshot")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	task := &transferFromFilesystemToSnapshotTask{
		config:           config,
		factory:          f.factory,
		traversalStorage: f.traversalStorage,
		nodesStorage:     f.nodesStorage,
		request: &snapshot_protos.CreateFilesystemSnapshotRequest{
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: filesystemID,
			},
			SnapshotId: snapshotID,
		},
		state: &snapshot_protos.TransferFromFilesystemToSnapshotTaskState{},
	}

	runCtx, cancel := context.WithCancel(f.ctx)
	time.AfterFunc(100*time.Millisecond, cancel)

	err := task.Run(runCtx, execCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))

	queueEntries, err := f.traversalStorage.SelectNodesToList(
		f.ctx,
		snapshotID,
		map[uint64]struct{}{},
		100,
	)
	require.NoError(t, err)
	require.NotEmpty(t, queueEntries)

	err = task.Cancel(f.ctx, execCtx)
	require.NoError(t, err)

	queueEntries, err = f.traversalStorage.SelectNodesToList(
		f.ctx,
		snapshotID,
		map[uint64]struct{}{},
		100,
	)
	require.NoError(t, err)
	require.Empty(t, queueEntries)
}

////////////////////////////////////////////////////////////////////////////////

func TestTransferFromSnapshotToFilesystemCancelDeletesData(t *testing.T) {
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

	toSnapshotTask := &transferFromFilesystemToSnapshotTask{
		config:           f.newConfig(),
		factory:          f.factory,
		traversalStorage: f.traversalStorage,
		nodesStorage:     f.nodesStorage,
		request: &snapshot_protos.CreateFilesystemSnapshotRequest{
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: filesystemID,
			},
			SnapshotId: snapshotID,
		},
		state: &snapshot_protos.TransferFromFilesystemToSnapshotTaskState{},
	}

	err := toSnapshotTask.Run(f.ctx, toSnapshotExecCtx)
	require.NoError(t, err)

	dstFilesystemID := filesystemID + "_restored"
	f.prepareFilesystem(t, dstFilesystemID)
	defer f.cleanupFilesystem(t, dstFilesystemID)

	restoreExecCtx := tasks_mocks.NewExecutionContextMock()
	restoreExecCtx.On("GetTaskID").Return("cancel-from-snapshot")
	restoreExecCtx.On("SaveState", mock.Anything).Return(nil)

	fromSnapshotTask := &transferFromSnapshotToFilesystemTask{
		config:           config,
		factory:          f.factory,
		traversalStorage: f.traversalStorage,
		nodesStorage:     f.nodesStorage,
		request: &snapshot_protos.TransferFromSnapshotToFilesystemRequest{
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: dstFilesystemID,
			},
			SnapshotId: snapshotID,
		},
		state: &snapshot_protos.TransferFromSnapshotToFilesystemTaskState{},
	}

	runCtx, cancel := context.WithCancel(f.ctx)
	time.AfterFunc(100*time.Millisecond, cancel)

	err = fromSnapshotTask.Run(runCtx, restoreExecCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))

	queueEntries, err := f.traversalStorage.SelectNodesToList(
		f.ctx,
		snapshotID,
		map[uint64]struct{}{},
		100,
	)
	require.NoError(t, err)

	mappings, err := f.nodesStorage.GetDestinationNodeIDs(
		f.ctx,
		snapshotID,
		dstFilesystemID,
		[]uint64{nfs.RootNodeID},
	)
	require.NoError(t, err)
	require.True(
		t,
		len(queueEntries) > 0 || len(mappings) > 0,
		"expected some data to be written before cancellation",
	)

	err = fromSnapshotTask.Cancel(f.ctx, restoreExecCtx)
	require.NoError(t, err)

	queueEntries, err = f.traversalStorage.SelectNodesToList(
		f.ctx,
		snapshotID,
		map[uint64]struct{}{},
		100,
	)
	require.NoError(t, err)
	require.Empty(t, queueEntries)

	err = f.nodesStorage.DeleteSnapshotData(f.ctx, snapshotID)
	require.NoError(t, err)
}
