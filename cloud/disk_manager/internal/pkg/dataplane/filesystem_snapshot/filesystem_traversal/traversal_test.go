package filesystemtraversal

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
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
	err := f.client.Delete(f.ctx, filesystemID)
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
		f.client,
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

	traverser := NewFilesystemTraverser(
		fmt.Sprintf("snapshot_%v", filesystemID),
		filesystemID,
		"",
		f.client,
		f.storage,
		func(ctx context.Context) error {
			return nil
		},
		10,
		10,
		false,
	)

	actualNodeNames := []string{}
	nodesMutex := &sync.Mutex{}
	err := traverser.Traverse(
		f.ctx,
		func(
			ctx context.Context,
			nodes []nfs.Node,
			_ nfs.Session,
			_ nfs.Client,
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
	defer fixture.client.DestroySession(fixture.ctx, session)

	rootDir := nfs_testing.RandomDirectoryTree(
		3,   // maxDepth
		10,  // maxDirsPerDir
		100, // maxFilesPerDir
	)
	fsModel := nfs_testing.NewFileSystemModel(
		t,
		fixture.ctx,
		fixture.client,
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
