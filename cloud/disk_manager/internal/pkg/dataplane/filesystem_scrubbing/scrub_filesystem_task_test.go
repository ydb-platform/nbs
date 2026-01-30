package filesystem_scrubbing

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	nfs_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/config"
	nfs_testing "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs/testing"
	scrubbing_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_scrubbing/config"
	scrubbing_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_scrubbing/protos"
	filesystem_snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/storage"
	filesystem_snapshot_schema "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	nfs_protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	tasks_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	tasks_mocks "github.com/ydb-platform/nbs/cloud/tasks/mocks"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
)

////////////////////////////////////////////////////////////////////////////////

const (
	baseDirCount        = 10000
	baseFileCount       = 100000
	lowLevelFilesPerDir = 20
	baseFileDepth       = 2
	lowLevelDirDepth    = 2
	lowLevelFileDepth   = 3
)

var baseDirNames = []string{"one", "two", "three"}

////////////////////////////////////////////////////////////////////////////////

type fixture struct {
	ctx     context.Context
	db      *persistence.YDBClient
	storage filesystem_snapshot_storage.Storage
	client  nfs.Client
	factory nfs.Factory
}

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

func newFactory(ctx context.Context) nfs.Factory {
	clientTimeout := "60s"
	rootCertsFile := os.Getenv("DISK_MANAGER_RECIPE_ROOT_CERTS_FILE")

	return nfs.NewFactory(
		ctx,
		&nfs_config.ClientConfig{
			Zones: map[string]*nfs_config.Zone{
				"zone": {
					Endpoints: []string{
						nfs_testing.GetEndpoint(),
						nfs_testing.GetEndpoint(),
					},
				},
			},
			RootCertsFile:        &rootCertsFile,
			DurableClientTimeout: &clientTimeout,
		},
		metrics.NewEmptyRegistry(),
	)
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
	factory := newFactory(ctx)

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

////////////////////////////////////////////////////////////////////////////////

func createLargeFilesystem(
	t *testing.T,
	ctx context.Context,
	client nfs.Client,
	filesystemID string,
) {

	session, err := client.CreateSession(ctx, filesystemID, "", false)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, client.DestroySession(ctx, session))
	}()

	rng := rand.New(rand.NewSource(1))
	maxNameLen := int(nfs_protos.EFilestoreLimits_E_FS_LIMITS_NAME)

	for _, base := range baseDirNames {
		baseID, err := createDir(ctx, client, session, nfs.RootNodeID, base)
		require.NoError(t, err)

		createBaseFiles(t, ctx, client, session, base, baseID, maxNameLen, rng)
		createLowLevelDirs(t, ctx, client, session, base, baseID, maxNameLen, rng)
	}
}

func createBaseFiles(
	t *testing.T,
	ctx context.Context,
	client nfs.Client,
	session nfs.Session,
	base string,
	parentID uint64,
	maxNameLen int,
	rng *rand.Rand,
) {

	for i := 0; i < baseFileCount; i++ {
		name := makeLongName("file", base, baseFileDepth, i, maxNameLen, rng)
		_, err := createFile(ctx, client, session, parentID, name)
		require.NoError(t, err)
	}
}

func createLowLevelDirs(
	t *testing.T,
	ctx context.Context,
	client nfs.Client,
	session nfs.Session,
	base string,
	parentID uint64,
	maxNameLen int,
	rng *rand.Rand,
) {

	for i := 0; i < baseDirCount; i++ {
		dirName := makeLongName("dir", base, lowLevelDirDepth, i, maxNameLen, rng)
		dirID, err := createDir(ctx, client, session, parentID, dirName)
		require.NoError(t, err)

		for j := 0; j < lowLevelFilesPerDir; j++ {
			fileName := makeShortName("file", base, lowLevelFileDepth, j)
			_, err := createFile(ctx, client, session, dirID, fileName)
			require.NoError(t, err)
		}
	}
}

func createDir(
	ctx context.Context,
	client nfs.Client,
	session nfs.Session,
	parentID uint64,
	name string,
) (uint64, error) {

	node := nfs.Node{
		ParentID: parentID,
		Name:     name,
		Type:     nfs_client.NODE_KIND_DIR,
		Mode:     0o755,
		UID:      1,
		GID:      1,
	}

	return createNode(ctx, client, session, node)
}

func createFile(
	ctx context.Context,
	client nfs.Client,
	session nfs.Session,
	parentID uint64,
	name string,
) (uint64, error) {

	node := nfs.Node{
		ParentID: parentID,
		Name:     name,
		Type:     nfs_client.NODE_KIND_FILE,
		Mode:     0o644,
		UID:      1,
		GID:      1,
	}

	return createNode(ctx, client, session, node)
}

func createNode(
	ctx context.Context,
	client nfs.Client,
	session nfs.Session,
	node nfs.Node,
) (uint64, error) {

	var nodeID uint64
	err := retry(ctx, func() error {
		var err error
		nodeID, err = client.CreateNode(ctx, session, node)
		return err
	})
	return nodeID, err
}

func retry(ctx context.Context, fn func() error) error {
	const maxRetries = 5
	const retryDelay = 100 * time.Millisecond

	var err error
	for i := 0; i < maxRetries; i++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		err = fn()
		if err == nil {
			return nil
		}

		if !tasks_errors.CanRetry(err) {
			return err
		}

		time.Sleep(retryDelay)
	}

	return err
}

////////////////////////////////////////////////////////////////////////////////

type scrubStats struct {
	mu                sync.Mutex
	baseDirs          map[uint64]string
	lowLevelDirs      map[uint64]string
	rootDirCount      int
	rootFileCount     int
	baseDirFileCount  map[uint64]int
	baseDirChildCount map[uint64]int
	lowLevelFileCount map[uint64]int
	firstErr          error
	maxNameLen        int
}

func newScrubStats(maxNameLen int) *scrubStats {
	return &scrubStats{
		baseDirs:          make(map[uint64]string),
		lowLevelDirs:      make(map[uint64]string),
		baseDirFileCount:  make(map[uint64]int),
		baseDirChildCount: make(map[uint64]int),
		lowLevelFileCount: make(map[uint64]int),
		maxNameLen:        maxNameLen,
	}
}

func (s *scrubStats) consume(nodes []nfs.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.firstErr != nil {
		return s.firstErr
	}

	for _, node := range nodes {
		err := s.consumeNode(node)
		if err != nil {
			s.firstErr = err
			return err
		}
	}

	return nil
}

func (s *scrubStats) consumeNode(node nfs.Node) error {
	switch {
	case node.ParentID == nfs.RootNodeID:
		if !node.Type.IsDirectory() {
			return fmt.Errorf("root child %q is not a directory", node.Name)
		}

		if !isBaseDirName(node.Name) {
			return fmt.Errorf("unexpected root directory name %q", node.Name)
		}

		s.rootDirCount++
		s.baseDirs[node.NodeID] = node.Name
		return nil

	case s.baseDirs[node.ParentID] != "":
		baseName := s.baseDirs[node.ParentID]
		if node.Type.IsDirectory() {
			if err := validateName(node.Name, "dir", baseName, lowLevelDirDepth, true, s.maxNameLen); err != nil {
				return err
			}
			s.baseDirChildCount[node.ParentID]++
			s.lowLevelDirs[node.NodeID] = baseName
			return nil
		}

		if err := validateName(node.Name, "file", baseName, baseFileDepth, true, s.maxNameLen); err != nil {
			return err
		}
		s.baseDirFileCount[node.ParentID]++
		return nil

	case s.lowLevelDirs[node.ParentID] != "":
		baseName := s.lowLevelDirs[node.ParentID]
		if node.Type.IsDirectory() {
			return fmt.Errorf("unexpected directory %q inside low-level dir", node.Name)
		}

		if err := validateName(node.Name, "file", baseName, lowLevelFileDepth, false, s.maxNameLen); err != nil {
			return err
		}

		s.lowLevelFileCount[node.ParentID]++
		return nil

	default:
		return fmt.Errorf("unexpected parent %v for node %q", node.ParentID, node.Name)
	}
}

////////////////////////////////////////////////////////////////////////////////

func makeShortName(kind, parent string, depth int, index int) string {
	return fmt.Sprintf("%s_%s_%d_%d", kind, parent, depth, index)
}

func makeLongName(
	kind string,
	parent string,
	depth int,
	index int,
	maxLen int,
	rng *rand.Rand,
) string {

	prefix := makeShortName(kind, parent, depth, index)
	if len(prefix) >= maxLen {
		return prefix[:maxLen]
	}

	suffixLen := maxLen - len(prefix) - 1
	if suffixLen <= 0 {
		return prefix
	}

	return prefix + "_" + randomSuffix(rng, suffixLen)
}

func randomSuffix(rng *rand.Rand, length int) string {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	if length <= 0 {
		return ""
	}

	buf := make([]byte, length)
	for i := range buf {
		buf[i] = alphabet[rng.Intn(len(alphabet))]
	}

	return string(buf)
}

func isBaseDirName(name string) bool {
	for _, base := range baseDirNames {
		if name == base {
			return true
		}
	}

	return false
}

func validateName(
	name string,
	kind string,
	parent string,
	depth int,
	wantSuffix bool,
	maxNameLen int,
) error {

	parsed, err := parseName(name)
	if err != nil {
		return err
	}

	if parsed.kind != kind {
		return fmt.Errorf("unexpected kind %q for name %q", parsed.kind, name)
	}

	if parsed.parent != parent {
		return fmt.Errorf("unexpected parent %q for name %q", parsed.parent, name)
	}

	if parsed.depth != depth {
		return fmt.Errorf("unexpected depth %d for name %q", parsed.depth, name)
	}

	if parsed.hasSuffix != wantSuffix {
		return fmt.Errorf("unexpected suffix flag for name %q", name)
	}

	if wantSuffix {
		if parsed.suffix == "" {
			return fmt.Errorf("empty suffix for name %q", name)
		}
		if len(name) != maxNameLen {
			return fmt.Errorf("unexpected length %d for name %q", len(name), name)
		}
	} else if len(name) >= maxNameLen {
		return fmt.Errorf("name %q should not be max length", name)
	}

	return nil
}

type parsedName struct {
	kind      string
	parent    string
	depth     int
	index     int
	hasSuffix bool
	suffix    string
}

func parseName(name string) (parsedName, error) {
	parts := strings.SplitN(name, "_", 5)
	if len(parts) < 4 {
		return parsedName{}, fmt.Errorf("invalid name %q", name)
	}

	depth, err := strconv.Atoi(parts[2])
	if err != nil {
		return parsedName{}, fmt.Errorf("invalid depth in name %q", name)
	}

	index, err := strconv.Atoi(parts[3])
	if err != nil {
		return parsedName{}, fmt.Errorf("invalid index in name %q", name)
	}

	parsed := parsedName{
		kind:   parts[0],
		parent: parts[1],
		depth:  depth,
		index:  index,
	}

	if len(parts) == 5 {
		parsed.hasSuffix = true
		parsed.suffix = parts[4]
	}

	return parsed, nil
}

////////////////////////////////////////////////////////////////////////////////

func TestScrubFilesystemTaskWithNemesis(t *testing.T) {
	fixture := newFixture(t)
	defer fixture.close(t)

	filesystemID := t.Name()
	fixture.prepareFilesystem(t, filesystemID)
	defer fixture.cleanupFilesystem(t, filesystemID)

	createLargeFilesystem(t, fixture.ctx, fixture.client, filesystemID)

	execCtx := tasks_mocks.NewExecutionContextMock()
	execCtx.On("GetTaskID").Return("scrub-task")
	execCtx.On("SaveState", mock.Anything).Return(nil)

	maxNameLen := int(nfs_protos.EFilestoreLimits_E_FS_LIMITS_NAME)
	stats := newScrubStats(maxNameLen)

	task := &scrubFilesystemTask{
		config:  &scrubbing_config.FilesystemScrubbingConfig{},
		factory: fixture.factory,
		storage: fixture.storage,
		request: &scrubbing_protos.ScrubFilesystemRequest{
			Filesystem: &types.Filesystem{
				ZoneId:       "zone",
				FilesystemId: filesystemID,
			},
			FilesystemCheckpointId: "",
		},
		state:    &scrubbing_protos.ScrubFilesystemTaskState{},
		callback: func(nodes []nfs.Node) { _ = stats.consume(nodes) },
	}

	err := task.Run(fixture.ctx, execCtx)
	require.NoError(t, err)

	require.NoError(t, stats.firstErr)

	stats.mu.Lock()
	defer stats.mu.Unlock()

	require.Equal(t, 0, stats.rootFileCount)
	require.Len(t, stats.baseDirs, len(baseDirNames))

	baseNameSet := make(map[string]struct{}, len(baseDirNames))
	for _, name := range baseDirNames {
		baseNameSet[name] = struct{}{}
	}

	for _, name := range stats.baseDirs {
		if _, ok := baseNameSet[name]; !ok {
			require.Fail(t, "unexpected base directory", "name=%s", name)
		}
	}

	for baseID := range stats.baseDirs {
		require.Equal(t, baseDirCount, stats.baseDirChildCount[baseID])
		require.Equal(t, baseFileCount, stats.baseDirFileCount[baseID])
	}

	require.Len(t, stats.lowLevelDirs, baseDirCount*len(baseDirNames))
	for dirID := range stats.lowLevelDirs {
		require.Equal(t, lowLevelFilesPerDir, stats.lowLevelFileCount[dirID])
	}
}
