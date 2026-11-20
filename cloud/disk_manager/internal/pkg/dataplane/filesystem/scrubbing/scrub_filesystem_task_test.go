package scrubbing

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	scrubbing_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/config"
	scrubbing_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/protos"
	traversal_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/config"
	traversal_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage"
	filesystem_snapshot_schema "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/util"
	"github.com/ydb-platform/nbs/cloud/tasks"
	tasks_common "github.com/ydb-platform/nbs/cloud/tasks/common"
	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	tasks_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
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

func newStorage(
	t *testing.T,
	ctx context.Context,
	db *persistence.YDBClient,
	storageFolder string,
) traversal_storage.Storage {

	err := filesystem_snapshot_schema.Create(ctx, storageFolder, db, false)
	require.NoError(t, err)

	storage := traversal_storage.NewStorage(db, storageFolder, 1000)
	require.NotNil(t, storage)

	return storage
}

////////////////////////////////////////////////////////////////////////////////

type fixture struct {
	ctx     context.Context
	db      *persistence.YDBClient
	storage traversal_storage.Storage
	client  nfs.Client
	factory nfs.Factory
}

func newFixture(t *testing.T) *fixture {
	ctx := nfs_testing.NewContext()

	db, err := newYDB(ctx)
	require.NoError(t, err)

	storageFolder := fmt.Sprintf(
		"scrubbing_tests/%v",
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
	// We interrupt the scrubbing which creates a session at random time.
	// The session be created but the session creation can be cancelled before
	// the session id is received, so it won't be destroyed.
	// We need to force-delete the filesystem to ignore dangling sessions.
	err := f.client.Delete(f.ctx, filesystemID, true /*force*/)
	require.NoError(t, err)
}

func (f *fixture) fillFilesystem(
	t *testing.T,
	filesystemID string,
	rootDir nfs_testing.Node,
	parallel bool,
) nfs_testing.FilesystemModelInterface {

	session, err := f.client.CreateSession(f.ctx, filesystemID, "", false)
	require.NoError(t, err)
	if parallel {
		model := nfs_testing.NewParallelFilesystemModel(
			t,
			f.ctx,
			session,
			rootDir,
		)
		model.CreateAllNodesRecursively()
		return model
	}
	model := nfs_testing.NewFileSystemModel(
		t,
		f.ctx,
		session,
		rootDir,
	)
	model.CreateAllNodesRecursively()
	return model
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
			DirsCount:  5,
			FilesCount: 1000,
		},
		{
			DirsCount:  1000,
			FilesCount: 1000,
		},
		{
			DirsCount:  0,
			FilesCount: 2,
		},
	}

	root := nfs_testing.HomogeneousDirectoryTree(layers)
	fsModel := f.fillFilesystem(t, filesystemID, root, true /* parallel */)
	defer fsModel.Close()

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("scrub-task")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	actualNodeNames := tasks_common.NewStringSet()
	var namesMu sync.Mutex

	listNodesMaxBytes := uint32(1000)
	// scrubFilesystemTask configured with a small ListNodesMaxBytes limit
	// to test traversal behavior when listing is interrupted mid-way,
	// without requiring large filesystem fixtures.
	task := &scrubFilesystemTask{
		config: &scrubbing_config.FilesystemScrubbingConfig{
			ListNodesMaxBytes: &listNodesMaxBytes,
		},
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

	err := util.WithNemesis(
		f.ctx,
		func(ctx context.Context) error {
			// State is updated inside the run method, the task is not recreated
			return task.Run(ctx, execCtx)
		},
		0,
		500*time.Millisecond,
	)
	require.NoError(t, err)

	require.True(t, fsModel.ExpectedNodeNames().Equals(actualNodeNames))
}

func TestScrubFilesystemTaskCancel(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	filesystemID := t.Name()
	f.prepareFilesystem(t, filesystemID)
	defer f.cleanupFilesystem(t, filesystemID)

	layers := []nfs_testing.FilesystemLayerConfig{
		{
			DirsCount:  100,
			FilesCount: 10,
		},
		{
			DirsCount:  5,
			FilesCount: 10,
		},
	}

	root := nfs_testing.HomogeneousDirectoryTree(layers)
	fsModel := f.fillFilesystem(t, filesystemID, root, true /* parallel */)
	defer fsModel.Close()

	taskID := "cancel-task"
	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return(taskID)
	execCtx.On("SaveState", mock.Anything).Return(nil)

	listNodesMaxBytes := uint32(1000)
	task := &scrubFilesystemTask{
		config: &scrubbing_config.FilesystemScrubbingConfig{
			ListNodesMaxBytes: &listNodesMaxBytes,
		},
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

	var callCount int
	var mu sync.Mutex
	runCtx, cancel := context.WithCancel(f.ctx)

	task.callback = func(nodes []nfs.Node) {
		mu.Lock()
		defer mu.Unlock()

		callCount++
		if callCount >= 10 {
			cancel()
		}
	}

	err := task.Run(runCtx, execCtx)
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
	snapshotID := task.getSnapshotID(execCtx)
	entries, err := f.storage.SelectNodesToList(
		f.ctx,
		snapshotID,
		map[uint64]struct{}{}, // nodesToExclude
		100,                   // limit
	)
	require.NoError(t, err)
	require.NotEmpty(t, entries)

	err = task.Cancel(f.ctx, execCtx)
	require.NoError(t, err)

	entries, err = f.storage.SelectNodesToList(
		f.ctx,
		snapshotID,
		map[uint64]struct{}{}, // nodesToExclude
		100,                   // limit
	)
	require.NoError(t, err)
	require.Empty(t, entries)
}

////////////////////////////////////////////////////////////////////////////////

func rmTree(
	ctx context.Context,
	session nfs.Session,
	parentNodeID uint64,
	name string,
) error {

	node, err := session.GetNodeAttr(ctx, parentNodeID, name)
	if err != nil {
		return err
	}

	for cookie := ""; ; {
		var nodes []nfs.Node
		nodes, cookie, err = session.ListNodes(
			ctx,
			node.NodeID,
			cookie,
			4096,
			false, // unsafe
		)
		if err != nil {
			return err
		}

		for _, child := range nodes {
			err = session.UnlinkNode(
				ctx,
				node.NodeID,
				child.Name,
				false, // unlinkDirectory
			)
			if err != nil {
				return err
			}
		}

		if cookie == "" {
			break
		}
	}

	return session.UnlinkNode(ctx, parentNodeID, name, true /* unlinkDirectory */)
}

////////////////////////////////////////////////////////////////////////////////

func TestScrubFilesystemTaskNodeDeletedDuringScrubbing(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	filesystemID := t.Name()
	f.prepareFilesystem(t, filesystemID)
	defer f.cleanupFilesystem(t, filesystemID)

	targetDirName := "dir_to_delete"
	root := nfs_testing.Root(
		nfs_testing.Dir("a",
			nfs_testing.File("a1"),
			nfs_testing.File("a2"),
		),
		nfs_testing.Dir(targetDirName,
			nfs_testing.File("f1"),
			nfs_testing.File("f2"),
			nfs_testing.File("f3"),
			nfs_testing.File("f4"),
			nfs_testing.File("f5"),
			nfs_testing.File("f6"),
			nfs_testing.File("f7"),
			nfs_testing.File("f8"),
			nfs_testing.File("f9"),
			nfs_testing.File("f10"),
		),
		nfs_testing.Dir("b",
			nfs_testing.File("b1"),
			nfs_testing.File("b2"),
		),
	)
	fsModel := f.fillFilesystem(t, filesystemID, root, false /* parallel */)
	defer fsModel.Close()

	// Create a separate writable session for deleting nodes during scrubbing.
	deleteSession, err := f.client.CreateSession(
		f.ctx,
		filesystemID,
		"",
		false, // readonly
	)
	require.NoError(t, err)
	defer deleteSession.Close(f.ctx)

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("scrub-delete-task")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	listNodesMaxBytes := uint32(1)
	task := &scrubFilesystemTask{
		config: &scrubbing_config.FilesystemScrubbingConfig{
			ListNodesMaxBytes: &listNodesMaxBytes,
		},
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

	var deleted bool
	var mu sync.Mutex

	task.callback = func(nodes []nfs.Node) {
		mu.Lock()
		defer mu.Unlock()

		if deleted {
			return
		}

		for _, node := range nodes {
			if node.Name == targetDirName {
				err := rmTree(
					f.ctx,
					deleteSession,
					node.ParentNodeID,
					node.Name,
				)
				require.NoError(t, err)
				deleted = true
			}
		}
	}

	err = task.Run(f.ctx, execCtx)
	require.NoError(t, err)
	require.True(t, deleted)
}

func TestAddToFileSizeHistogram(t *testing.T) {
	state := &scrubbing_protos.ScrubFilesystemTaskState{}
	addToFileSizeHistogram(state, []nfs.Node{
		{NodeID: 2, Type: nfs.NODE_KIND_FILE, Size: 0, Links: 1},
		{NodeID: 3, Type: nfs.NODE_KIND_FILE, Size: 3, Links: 1},
		// Hard link listed twice.
		{NodeID: 4, Type: nfs.NODE_KIND_FILE, Size: 4, Links: 2},
		{NodeID: 4, Type: nfs.NODE_KIND_FILE, Size: 4, Links: 2},
		{NodeID: 5, Type: nfs.NODE_KIND_SYMLINK, Size: 100, Links: 1},
		{NodeID: 6, Type: nfs.NODE_KIND_DIR, Size: 100, Links: 1},
		// Node present in the index tablet but absent in the shard.
		{NodeID: nfs.InvalidNodeID},
	})

	require.Equal(t, []float64{1, 0, 1, 1}, state.FileCountBySizeLog2)
	require.Equal(t, []float64{0, 0, 3, 4}, state.FileBytesBySizeLog2)
}

type interruptionMode int

const (
	noInterruption interruptionMode = iota
	retriableErrorBeforePageCommit
	interruptionAfterPageCommit
	interruptionAfterFinalPageCommit
)

type interruptingTraversalStorage struct {
	traversal_storage.Storage
	mode        interruptionMode
	interrupted atomic.Bool
}

func (s *interruptingTraversalStorage) ScheduleChildNodesForListing(
	ctx context.Context,
	snapshotID string,
	parentNodeID uint64,
	nextCookie string,
	children []nfs.Node,
) error {

	interruptOnPage := parentNodeID == nfs.RootNodeID
	if s.mode == interruptionAfterFinalPageCommit {
		// ponytail: the fixture has one child directory, which is listed last.
		interruptOnPage = parentNodeID != nfs.RootNodeID
	}
	interrupt := s.mode != noInterruption && interruptOnPage &&
		!s.interrupted.Swap(true)
	if interrupt && s.mode == retriableErrorBeforePageCommit {
		// The runner saves task state on a retriable error, and the page
		// remains queued for the next attempt.
		return tasks_errors.NewRetriableErrorf(
			"interrupted before saving traversal progress",
		)
	}

	err := s.Storage.ScheduleChildNodesForListing(
		ctx,
		snapshotID,
		parentNodeID,
		nextCookie,
		children,
	)
	if err == nil && interrupt {
		// Leave the task running with only its last saved state, as on a crash.
		return tasks_errors.NewInterruptExecutionError()
	}
	return err
}

// Session API cannot set the file size, so create the node directly. Each name
// is a hard link to the same node under the root.
func (f *fixture) createFileWithSize(
	t *testing.T,
	filesystemID string,
	nodeID uint64,
	size uint64,
	names ...string,
) {

	err := f.client.UnsafeCreateNode(f.ctx, filesystemID, nfs.Node{
		NodeID: nodeID,
		Type:   nfs.NODE_KIND_FILE,
		Mode:   0644,
		Size:   size,
		Links:  uint32(len(names)),
	})
	require.NoError(t, err)

	for _, name := range names {
		err = f.client.UnsafeCreateNodeRef(
			f.ctx,
			filesystemID,
			nfs.RootNodeID,
			name,
			nodeID,
			"", // shardID
			"", // shardNodeName
		)
		require.NoError(t, err)
	}
}

func TestScrubFilesystemTaskMarksHistogramUnreliableAfterInterruption(t *testing.T) {
	f := newFixture(t)
	defer f.close(t)

	filesystemID := t.Name()
	f.prepareFilesystem(t, filesystemID)
	defer f.cleanupFilesystem(t, filesystemID)

	// All files below are empty; the symlink and the directories must not be
	// counted.
	fsModel := f.fillFilesystem(t, filesystemID, nfs_testing.Root(
		nfs_testing.File("a"),
		nfs_testing.Dir("dir", nfs_testing.File("nested")),
		nfs_testing.Symlink("link", "a"),
	), false /* parallel */)
	defer fsModel.Close()

	f.createFileWithSize(t, filesystemID, 1000, 3, "hardlink1", "hardlink2")

	testCases := []struct {
		name                    string
		mode                    interruptionMode
		expectedCounts          []float64
		expectedBytes           []float64
		expectedRetriableErrors uint64
		expectedUnreliable      bool
	}{
		{
			name:           "no_interruption",
			expectedCounts: []float64{2, 0, 1},
			expectedBytes:  []float64{0, 0, 3},
		},
		{
			// The root page is counted by both attempts, "dir" only once.
			name:                    "retriable_error_before_page_commit",
			mode:                    retriableErrorBeforePageCommit,
			expectedCounts:          []float64{3, 0, 2},
			expectedBytes:           []float64{0, 0, 6},
			expectedRetriableErrors: 1,
			expectedUnreliable:      true,
		},
		{
			// Counts of the root page are lost, only "dir" is listed after
			// the restart.
			name:               "interruption_after_page_commit",
			mode:               interruptionAfterPageCommit,
			expectedCounts:     []float64{1},
			expectedBytes:      []float64{0},
			expectedUnreliable: true,
		},
		{
			name:               "interruption_after_final_page_commit",
			mode:               interruptionAfterFinalPageCommit,
			expectedUnreliable: true,
		},
	}

	pollPeriod := proto.String("100ms")
	config := &tasks_config.TasksConfig{
		PollForTaskUpdatesPeriod:      pollPeriod,
		PollForTasksPeriodMin:         pollPeriod,
		PollForTasksPeriodMax:         pollPeriod,
		PollForStallingTasksPeriodMin: pollPeriod,
		PollForStallingTasksPeriodMax: pollPeriod,
		TaskPingPeriod:                proto.String("250ms"),
		TaskStallingTimeout:           proto.String("3s"),
		RunnersCount:                  proto.Uint64(1),
		StalkingRunnersCount:          proto.Uint64(1),
		StorageFolder:                 proto.String("scrubbing_tests/" + t.Name() + "/tasks"),
		RegularSystemTasksEnabled:     proto.Bool(false),
	}
	require.NoError(t, tasks_storage.CreateYDBTables(f.ctx, config, f.db, false))
	metricsRegistry := metrics.NewEmptyRegistry()
	taskStorage, err := tasks_storage.NewStorage(config, metricsRegistry, f.db)
	require.NoError(t, err)

	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(f.ctx)
			defer cancel()

			taskRegistry := tasks.NewRegistry()
			scheduler, err := tasks.NewScheduler(
				ctx,
				taskRegistry,
				taskStorage,
				config,
				metricsRegistry,
			)
			require.NoError(t, err)

			// Keep child directories pending until the interrupted worker exits.
			err = RegisterForExecution(
				taskRegistry,
				&scrubbing_config.FilesystemScrubbingConfig{
					TraversalConfig: &traversal_config.FilesystemTraversalConfig{
						TraversalWorkersCount: proto.Uint32(1),
					},
				},
				f.factory,
				&interruptingTraversalStorage{
					Storage: f.storage,
					mode:    testCase.mode,
				},
				scheduler,
			)
			require.NoError(t, err)
			err = tasks.StartRunners(
				ctx,
				taskStorage,
				taskRegistry,
				metricsRegistry,
				config,
				"localhost",
			)
			require.NoError(t, err)

			taskID, err := ScheduleScrubFilesystem(
				headers.SetIncomingIdempotencyKey(ctx, t.Name()),
				scheduler,
				"zone",
				filesystemID,
			)
			require.NoError(t, err)
			_, err = scheduler.WaitTaskSync(ctx, taskID, 30*time.Second)
			require.NoError(t, err)

			taskState, err := taskStorage.GetTask(ctx, taskID)
			require.NoError(t, err)
			require.Equal(t, tasks_storage.TaskStatusFinished, taskState.Status)
			require.Equal(t, testCase.expectedRetriableErrors, taskState.RetriableErrorCount)

			var state scrubbing_protos.ScrubFilesystemTaskState
			require.NoError(t, proto.Unmarshal(taskState.State, &state))
			require.True(t, state.RootNodeScheduled)
			require.Equal(
				t,
				testCase.expectedUnreliable,
				state.FileSizeHistogramUnreliable,
			)
			require.Equal(t, testCase.expectedCounts, state.FileCountBySizeLog2)
			require.Equal(t, testCase.expectedBytes, state.FileBytesBySizeLog2)
		})
	}
}
