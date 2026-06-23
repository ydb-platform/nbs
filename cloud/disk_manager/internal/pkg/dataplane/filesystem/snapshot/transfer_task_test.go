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
	nfs_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/mocks"
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

func (f *fixture) newTransferFromFilesystemToSnapshotTask(
	config *snapshot_config.FilesystemSnapshotConfig,
	filesystemID string,
	snapshotID string,
) *transferFromFilesystemToSnapshotTask {

	return &transferFromFilesystemToSnapshotTask{
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
}

func (f *fixture) newTransferFromSnapshotToFilesystemTask(
	config *snapshot_config.FilesystemSnapshotConfig,
	filesystemID string,
	snapshotID string,
) *transferFromSnapshotToFilesystemTask {

	return &transferFromSnapshotToFilesystemTask{
		config:           config,
		factory:          f.factory,
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

type cancelOnListNodesStorage struct {
	nodes_storage.Storage
	cancelAt int
	cancel   context.CancelFunc
	calls    int
}

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

func TestTransferFromFilesystemToSnapshotSkipsInvalidNodeIDs(t *testing.T) {
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
	execCtx.On("SaveState", mock.Anything).Return(nil).Once()

	task := f.newTransferFromFilesystemToSnapshotTask(
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
		nfs_testing.NodeNames(fsModel.ListAllNodesRecursively(false))...,
	)

	config := f.newConfig()
	snapshotID := "snapshot-1"

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("transfer-to-snapshot-task")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	toSnapshotTask := f.newTransferFromFilesystemToSnapshotTask(
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

func TestTransferFromFilesystemToSnapshotAndBackWithDeviceHardlink(t *testing.T) {
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

	toSnapshotTask := f.newTransferFromFilesystemToSnapshotTask(
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

	toSnapshotTask := f.newTransferFromFilesystemToSnapshotTask(
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
	runCtx, cancel := context.WithCancel(f.ctx)

	cancelFactory := &cancelOnListNodesFactory{
		Factory:  f.factory,
		cancelAt: 5,
		cancel:   cancel,
	}

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("cancel-to-snapshot")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	task := f.newTransferFromFilesystemToSnapshotTask(
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

	queueEntries, err = f.traversalStorage.SelectNodesToList(
		f.ctx,
		snapshotID,
		map[uint64]struct{}{},
		100,
	)
	require.NoError(t, err)
	require.Empty(t, queueEntries)

	savedNodes, _, err = f.nodesStorage.ListNodes(
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

	toSnapshotTask := f.newTransferFromFilesystemToSnapshotTask(
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
