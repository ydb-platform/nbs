package storage

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage/schema"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/test"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
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
) Storage {

	err := schema.Create(ctx, storageFolder, db, false)
	require.NoError(t, err)

	storage := NewStorage(db, storageFolder)
	require.NotNil(t, storage)

	return storage
}

////////////////////////////////////////////////////////////////////////////////

func getNodeIDs(entries []NodeQueueEntry) []uint64 {
	ids := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, entry.NodeID)
	}

	return ids
}

////////////////////////////////////////////////////////////////////////////////

type fixture struct {
	t             *testing.T
	ctx           context.Context
	cancel        context.CancelFunc
	db            *persistence.YDBClient
	storageFolder string
	storage       Storage
}

func (f *fixture) teardown() {
	err := f.db.Close(f.ctx)
	require.NoError(f.t, err)
	f.cancel()
}

func createFixture(t *testing.T) *fixture {
	ctx, cancel := context.WithCancel(test.NewContext())

	db, err := newYDB(ctx)
	require.NoError(t, err)

	storageFolder := fmt.Sprintf(
		"filesystem_snapshot_storage_ydb_test/%v",
		t.Name(),
	)
	storage := newStorage(t, ctx, db, storageFolder)

	return &fixture{
		t:             t,
		ctx:           ctx,
		cancel:        cancel,
		db:            db,
		storageFolder: storageFolder,
		storage:       storage,
	}
}

////////////////////////////////////////////////////////////////////////////////

func TestNodesScheduling(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	filesystemSnapshotID := "snapshot"
	err := f.storage.ScheduleRootNodeForListing(
		f.ctx,
		filesystemSnapshotID,
	)
	require.NoError(t, err)

	require.NoError(
		t,
		f.storage.ScheduleChildNodesForListing(
			f.ctx,
			filesystemSnapshotID,
			1,
			"cookie1",
			0,
			[]nfs.Node{
				{NodeID: 2, ParentID: 1},
				{NodeID: 3, ParentID: 1},
				{NodeID: 5, ParentID: 1},
				{NodeID: 6, ParentID: 1},
			},
		),
	)
	require.NoError(
		t,
		f.storage.ScheduleChildNodesForListing(
			f.ctx,
			filesystemSnapshotID,
			2,
			"cookie2",
			1,
			[]nfs.Node{
				{NodeID: 4, ParentID: 2},
			},
		),
	)

	otherSnapshot := "other"
	err = f.storage.ScheduleRootNodeForListing(f.ctx, otherSnapshot)
	require.NoError(t, err)
	require.NoError(
		t,
		f.storage.ScheduleChildNodesForListing(
			f.ctx,
			otherSnapshot,
			1, // nodeID
			"cookie99",
			0, // depth
			[]nfs.Node{
				{NodeID: 99, ParentID: 1},
				{NodeID: 100, ParentID: 1},
			}, // children
		),
	)

	entries, err := f.storage.SelectNodesToList(
		f.ctx,
		filesystemSnapshotID,
		map[uint64]struct{}{}, // nodesToExclude
		100,                   // limit
	)
	require.NoError(t, err)
	require.ElementsMatch(t, getNodeIDs(entries), []uint64{1, 2, 3, 4, 5, 6})

	entries, err = f.storage.SelectNodesToList(
		f.ctx,
		filesystemSnapshotID,
		map[uint64]struct{}{2: {}, 4: {}}, // nodesToExclude
		100,                               // limit
	)
	require.NoError(t, err)
	require.ElementsMatch(t, getNodeIDs(entries), []uint64{1, 3, 5, 6})

	// 3) limit 1, fetched 1 element
	entries, err = f.storage.SelectNodesToList(
		f.ctx,
		filesystemSnapshotID,
		map[uint64]struct{}{}, // nodesToExclude
		1,                     // limit
	)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Contains(t, []uint64{1, 2, 3, 4, 5, 6}, entries[0].NodeID)
	nodeIDsToExclude := map[uint64]struct{}{
		1: {}, 2: {}, 3: {}, 4: {}, 5: {}, 6: {},
	}
	entries, err = f.storage.SelectNodesToList(
		f.ctx,
		filesystemSnapshotID,
		nodeIDsToExclude,
		100, // limit
	)
	require.NoError(t, err)
	require.Len(t, entries, 0)

	err = f.storage.ScheduleChildNodesForListing(
		f.ctx,
		otherSnapshot,
		99,                                      // nodeID
		"",                                      // nextCookie
		1,                                       // depth
		[]nfs.Node{{NodeID: 101, ParentID: 99}}, // children
	)
	require.NoError(t, err)
	entries, err = f.storage.SelectNodesToList(
		f.ctx,
		otherSnapshot,
		map[uint64]struct{}{}, // nodesToExclude
		100,                   // limit
	)
	require.NoError(t, err)
	require.ElementsMatch(t, getNodeIDs(entries), []uint64{1, 100, 101})
}
