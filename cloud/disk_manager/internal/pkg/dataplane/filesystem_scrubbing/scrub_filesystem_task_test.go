package filesystem_scrubbing

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	scrubbing_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_scrubbing/config"
	scrubbing_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_scrubbing/protos"
	filesystem_snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/storage"
	filesystem_snapshot_schema "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
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

func newStorage(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	storageFolder string,
) filesystem_snapshot_storage.Storage {

	err := filesystem_snapshot_schema.Create(ctx, storageFolder, db, false)
	require.NoError(t, err)

	storage := filesystem_snapshot_storage.NewStorage(db, storageFolder)
	require.NotNil(t, storage)

	return storage
}

////////////////////////////////////////////////////////////////////////////////

type fixture struct {
	ctx     context.Context
	db      *persistence.YDBClient
	storage filesystem_snapshot_storage.Storage
	client  nfs.Client
	factory nfs.Factory
}

func newFixture(t *testing.T) *fixture {
	ctx := nfs_testing.NewContext()

	db, err := newYDB(ctx)
	require.NoError(t, err)

	storageFolder := fmt.Sprintf(
		"filesystem_scrubbing_tests/%v",
		t.Name(),
	)
	storage := newStorage(t, ctx, db, storageFolder)

	client := nfs_testing.NewClient(t, ctx)
	factory := nfs_testing.NewFactory(ctx)

	return &fixture{
		ctx:     ctx,
		db:      db,
		storage: storage,
		client:  client,
		factory: factory,
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
	err := f.client.Delete(f.ctx, filesystemID)
	require.NoError(t, err)
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

////////////////////////////////////////////////////////////////////////////////

func withNemesis(
	ctx context.Context,
	runFunc func(ctx context.Context) error,
	minDuration time.Duration,
	maxDuration time.Duration,
) error {

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	durationRange := maxDuration - minDuration

	for {
		runCtx, cancel := context.WithCancel(ctx)
		delay := minDuration + time.Duration(rng.Int63n(int64(durationRange)))
		timer := time.AfterFunc(delay, cancel)

		err := runFunc(runCtx)

		timer.Stop()
		cancel()

		if err == nil {
			return nil
		}

		if errors.Is(err, context.Canceled) {
			continue
		}

		return err
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestScrubFilesystemTaskWithNemesis(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	filesystemID := t.Name()
	f.prepareFilesystem(t, filesystemID)
	defer f.cleanupFilesystem(t, filesystemID)

	layers := []nfs_testing.FilesystemLayerConfig{
		{
			DirsCount:  3,
			FilesCount: 0,
		},
		{
			DirsCount:  10000,
			FilesCount: 100000,
		},
		{
			DirsCount:  3,
			FilesCount: 3,
		},
	}

	root := nfs_testing.HomogeneousDirectoryTree(layers)
	fsModel := f.fillFilesystem(t, filesystemID, root)
	defer fsModel.Close()

	expectedNodeNames := tasks_common.NewStringSet(
		nfs_testing.NodeNames(fsModel.ExpectedNodes)...,
	)

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("scrub-task")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	actualNodeNames := tasks_common.NewStringSet()
	var namesMu sync.Mutex

	task := &scrubFilesystemTask{
		config:  &scrubbing_config.FilesystemScrubbingConfig{},
		factory: f.factory,
		storage: f.storage,
		request: &scrubbing_protos.ScrubFilesystemRequest{
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: filesystemID,
			},
			FilesystemCheckpointId: "",
		},
		state: &scrubbing_protos.ScrubFilesystemTaskState{},
	}

	task.callback = func(nodes []nfs.Node) {
		time.Sleep(100 * time.Nanosecond)
		namesMu.Lock()
		defer namesMu.Unlock()

		for _, node := range nodes {
			actualNodeNames.Add(node.Name)
		}
	}

	err := withNemesis(
		f.ctx,
		func(ctx context.Context) error {
			return task.Run(ctx, execCtx)
		},
		0,
		10*time.Second,
	)
	require.NoError(t, err)

	require.ElementsMatch(t, expectedNodeNames.List(), actualNodeNames.List())
}
