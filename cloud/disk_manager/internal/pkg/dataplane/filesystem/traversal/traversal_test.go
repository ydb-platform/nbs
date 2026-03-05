package traversal

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/listers"
	listers_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/listers/mock"
	traversal_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage"
	storage_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
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

func newStorage(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	storageFolder string,
) storage.Storage {

	err := schema.Create(ctx, storageFolder, db, false)
	require.NoError(t, err)

	storage := storage.NewStorage(db, storageFolder)
	require.NotNil(t, storage)

	return storage
}

////////////////////////////////////////////////////////////////////////////////

type fixture struct {
	ctx     context.Context
	db      *persistence.YDBClient
	storage storage.Storage
	client  nfs.Client
}

func newFixture(t *testing.T) *fixture {
	ctx := nfs_testing.NewContext()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	storageFolder := fmt.Sprintf(
		"filesystem_traversal_tests/%v", t.Name(),
	)
	storage := newStorage(t, ctx, db, storageFolder)

	client := nfs_testing.NewClient(t, ctx)

	return &fixture{
		ctx:     ctx,
		storage: storage,
		client:  client,
		db:      db,
	}
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
	err := f.client.Delete(f.ctx, filesystemID, false)
	require.NoError(t, err)
}

func (f *fixture) close(t *testing.T) {
	require.NoError(t, f.client.Close())
	require.NoError(t, f.db.Close(f.ctx))
}

func (f *fixture) fillFilesystem(
	t *testing.T,
	filesystemID string,
	rootDir nfs_testing.Node,
) *nfs_testing.FileSystemModel {

	session, err := f.client.CreateSession(f.ctx, filesystemID, "", false)
	require.NoError(t, err)
	model := nfs_testing.NewFileSystemModel(
		t,
		f.ctx,
		session,
		rootDir,
	)
	model.CreateAllNodesRecursively()
	return model
}

func (f *fixture) getFilesAfterTraversal(
	t *testing.T,
	filesystemID string,
	fsModel *nfs_testing.FileSystemModel,
) []string {

	workersCount := uint32(10)
	selectNodesToListLimit := uint64(10)
	traverser := NewFilesystemTraverser(
		fmt.Sprintf("snapshot_%v", filesystemID),
		filesystemID,
		"",
		listers.NewFilestoreOpener(
			f.client,
			0,     // listNodesMaxBytes
			true,  // readOnly
			false, // unsafe
		),
		f.storage,
		func(ctx context.Context) error {
			return nil
		},
		&traversal_config.FilesystemTraversalConfig{
			TraversalWorkersCount:  &workersCount,
			SelectNodesToListLimit: &selectNodesToListLimit,
		},
		false,
		nfs.RootNodeID,
	)

	actualNodeNames := []string{}
	nodesMutex := &sync.Mutex{}
	err := traverser.Traverse(
		f.ctx,
		func(
			ctx context.Context,
			nodes []nfs.Node,
			_ listers.FilesystemLister,
		) error {

			nodesMutex.Lock()
			defer nodesMutex.Unlock()
			for _, node := range nodes {
				actualNodeNames = append(actualNodeNames, node.Name)
			}

			return nil
		},
	)
	require.NoError(t, err)

	return actualNodeNames
}

////////////////////////////////////////////////////////////////////////////////

func TestTraversal(t *testing.T) {
	fixture := newFixture(t)
	defer fixture.close(t)

	filesystemID := t.Name()
	fixture.prepareFilesystem(t, filesystemID)
	defer fixture.cleanupFilesystem(t, filesystemID)

	fsModel := fixture.fillFilesystem(
		t,
		filesystemID,
		nfs_testing.Root(
			nfs_testing.Dir("dir1",
				nfs_testing.File("file1"),
				nfs_testing.Symlink("symlink1", "file1"),
			),
			nfs_testing.File("file2"),
		),
	)
	defer fsModel.Close()

	expectedNodeNames := nfs_testing.NodeNames(fsModel.ExpectedNodes)
	actualNodeNames := fixture.getFilesAfterTraversal(
		t,
		filesystemID,
		fsModel,
	)
	require.ElementsMatch(t, expectedNodeNames, actualNodeNames)
}

func TestRandomFilesystemTraversal(t *testing.T) {
	fixture := newFixture(t)
	defer fixture.close(t)

	filesystemID := t.Name()
	fixture.prepareFilesystem(t, filesystemID)
	defer fixture.cleanupFilesystem(t, filesystemID)
	session, err := fixture.client.CreateSession(fixture.ctx, filesystemID, "", false)
	require.NoError(t, err)
	defer session.Close(fixture.ctx)

	rootDir := nfs_testing.RandomDirectoryTree(
		3,   // maxDepth
		10,  // maxDirsPerDir
		100, // maxFilesPerDir
	)
	fsModel := nfs_testing.NewFileSystemModel(
		t,
		fixture.ctx,
		session,
		rootDir,
	)
	fsModel.CreateAllNodesRecursively()

	expectedNodeNames := nfs_testing.NodeNames(fsModel.ExpectedNodes)
	actualNodeNames := fixture.getFilesAfterTraversal(
		t,
		filesystemID,
		fsModel,
	)
	require.ElementsMatch(t, expectedNodeNames, actualNodeNames)
}

func TestTraversalShouldCloseSessionOnError(t *testing.T) {
	fixture := newFixture(t)
	defer fixture.close(t)

	filesystemID := t.Name()
	fixture.prepareFilesystem(t, filesystemID)
	// If sessions are not closed properly, filesystem deletion will fail.
	defer fixture.cleanupFilesystem(t, filesystemID)

	session, err := fixture.client.CreateSession(
		fixture.ctx,
		filesystemID,
		"",
		false,
	)
	require.NoError(t, err)
	defer session.Close(fixture.ctx)
	rootDir := nfs_testing.Root(nfs_testing.Dir("Some"))
	fsModel := nfs_testing.NewFileSystemModel(
		t,
		fixture.ctx,
		session,
		rootDir,
	)
	fsModel.CreateAllNodesRecursively()

	workersCount := uint32(10)
	selectNodesToListLimit := uint64(10)
	traverser := NewFilesystemTraverser(
		fmt.Sprintf("snapshot_%v", filesystemID),
		filesystemID,
		"",
		listers.NewFilestoreOpener(
			fixture.client,
			0,     // listNodesMaxBytes
			true,  // readOnly
			false, // unsafe
		),
		fixture.storage,
		func(ctx context.Context) error {
			return nil
		},
		&traversal_config.FilesystemTraversalConfig{
			TraversalWorkersCount:  &workersCount,
			SelectNodesToListLimit: &selectNodesToListLimit,
		},
		false,
		nfs.RootNodeID,
	)

	expectedError := fmt.Errorf("some error")
	err = traverser.Traverse(
		fixture.ctx,
		func(
			ctx context.Context,
			nodes []nfs.Node,
			_ listers.FilesystemLister,
		) error {
			return expectedError
		},
	)
	require.Error(t, err)
	require.ErrorIs(t, err, expectedError)
}

func TestTraversalFiltersInvalidNodeIDs(t *testing.T) {
	ctx := nfs_testing.NewContext()

	storageMock := storage_mocks.NewStorageMock()
	openerMock := listers_mocks.NewFilesystemOpenerMock()
	listerMock := listers_mocks.NewFilesystemListerMock()

	snapshotID := "test-snapshot"
	filesystemID := "test-filesystem"
	checkpointID := "test-checkpoint"
	workersCount := uint32(1)
	selectLimit := uint64(1000)

	config := &traversal_config.FilesystemTraversalConfig{
		TraversalWorkersCount:  &workersCount,
		SelectNodesToListLimit: &selectLimit,
	}

	validDirNodeID := uint64(100)
	validFileNodeID := uint64(200)

	children := []nfs.Node{
		nfs.Node(nfs_client.Node{
			NodeID: validFileNodeID,
			Name:   "file.txt",
			Type:   nfs_client.NODE_KIND_FILE,
		}),
		nfs.Node(nfs_client.Node{
			NodeID: validDirNodeID,
			Name:   "subdir",
			Type:   nfs_client.NODE_KIND_DIR,
		}),
		nfs.Node(nfs_client.Node{
			NodeID: nfs.InvalidNodeID,
			Name:   "corrupted_dir_1",
			Type:   nfs_client.NODE_KIND_DIR,
		}),
		nfs.Node(nfs_client.Node{
			NodeID: nfs.InvalidNodeID,
			Name:   "corrupted_dir_2",
			Type:   nfs_client.NODE_KIND_DIR,
		}),
	}

	// Schedule root node for traversal.
	storageMock.On(
		"SchedulerDirectoryForTraversal",
		mock.Anything,
		snapshotID,
		nfs.RootNodeID,
	).Return(nil)

	// First SelectNodesToList returns root node.
	storageMock.On(
		"SelectNodesToList",
		mock.Anything,
		snapshotID,
		mock.Anything, // nodesToExclude
		selectLimit,
	).Return(
		[]storage.NodeQueueEntry{{NodeID: nfs.RootNodeID, Cookie: ""}},
		nil,
	).Once()

	// Second SelectNodesToList returns subdir.
	storageMock.On(
		"SelectNodesToList",
		mock.Anything,
		snapshotID,
		mock.Anything, // nodesToExclude
		selectLimit,
	).Return(
		[]storage.NodeQueueEntry{{NodeID: validDirNodeID, Cookie: ""}},
		nil,
	).Once()

	// Third SelectNodesToList returns empty, traversal done.
	storageMock.On(
		"SelectNodesToList",
		mock.Anything,
		snapshotID,
		mock.Anything, // nodesToExclude
		selectLimit,
	).Return([]storage.NodeQueueEntry{}, nil)

	// OpenFilesystem returns lister mock.
	openerMock.On(
		"OpenFilesystem",
		mock.Anything,
		filesystemID,
		checkpointID,
	).Return(listerMock, nil)

	// ListNodes for root returns children with some invalid node IDs.
	listerMock.On(
		"ListNodes",
		mock.Anything,
		nfs.RootNodeID,
		"", // cookie
	).Return(children, "", nil)

	// ListNodes for subdir returns empty.
	listerMock.On(
		"ListNodes",
		mock.Anything,
		validDirNodeID,
		"", // cookie
	).Return([]nfs.Node{}, "", nil)

	// ScheduleChildNodesForListing for root — only valid dir expected.
	storageMock.On(
		"ScheduleChildNodesForListing",
		mock.Anything,
		snapshotID,
		nfs.RootNodeID,
		"", // nextCookie
		[]nfs.Node{nfs.Node(nfs_client.Node{
			NodeID: validDirNodeID,
			Name:   "subdir",
			Type:   nfs_client.NODE_KIND_DIR,
		})},
	).Return(nil)

	// ScheduleChildNodesForListing for subdir — no children.
	storageMock.On(
		"ScheduleChildNodesForListing",
		mock.Anything,
		snapshotID,
		validDirNodeID,
		"", // nextCookie
		[]nfs.Node(nil),
	).Return(nil)

	listerMock.On("Close", mock.Anything).Return(nil)

	traverser := NewFilesystemTraverser(
		snapshotID,
		filesystemID,
		checkpointID,
		openerMock,
		storageMock,
		func(ctx context.Context) error {
			return nil
		},
		config,
		false, // rootNodeAlreadyScheduled
		nfs.RootNodeID,
	)

	err := traverser.Traverse(
		ctx,
		func(
			ctx context.Context,
			nodes []nfs.Node,
			_ listers.FilesystemLister,
		) error {
			return nil
		},
	)
	require.NoError(t, err)

	storageMock.AssertExpectations(t)
	openerMock.AssertExpectations(t)
	listerMock.AssertExpectations(t)
}
