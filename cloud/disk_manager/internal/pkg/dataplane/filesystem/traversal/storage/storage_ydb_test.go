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

var emptyNodesToExclude = map[uint64]struct{}{}

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
	err := f.storage.SchedulerDirectoryForTraversal(
		f.ctx,
		filesystemSnapshotID,
		nfs.RootNodeID,
	)
	require.NoError(t, err)

	require.NoError(
		t,
		f.storage.ScheduleChildNodesForListing(
			f.ctx,
			filesystemSnapshotID,
			1,
			"cookie1",
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
			[]nfs.Node{
				{NodeID: 4, ParentID: 2},
			},
		),
	)

	otherSnapshot := "other"
	err = f.storage.SchedulerDirectoryForTraversal(
		f.ctx,
		otherSnapshot,
		nfs.RootNodeID,
	)
	require.NoError(t, err)
	require.NoError(
		t,
		f.storage.ScheduleChildNodesForListing(
			f.ctx,
			otherSnapshot,
			1, // nodeID
			"cookie99",
			[]nfs.Node{
				{NodeID: 99, ParentID: 1},
				{NodeID: 100, ParentID: 1},
			}, // children
		),
	)

	entries, err := f.storage.SelectNodesToList(
		f.ctx,
		filesystemSnapshotID,
		emptyNodesToExclude, // nodesToExclude
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
		emptyNodesToExclude, // nodesToExclude
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
		[]nfs.Node{{NodeID: 101, ParentID: 99}}, // children
	)
	require.NoError(t, err)
	entries, err = f.storage.SelectNodesToList(
		f.ctx,
		otherSnapshot,
		emptyNodesToExclude, // nodesToExclude
		100,                   // limit
	)
	require.NoError(t, err)
	require.ElementsMatch(t, getNodeIDs(entries), []uint64{1, 100, 101})
}

func TestClearDirectoryListingQueue(t *testing.T) {
	f := createFixture(t)
	defer f.teardown()

	s := f.storage.(*storageYDB)

	snapshot1 := "snapshot1"
	snapshot2 := "snapshot2"
	snapshot3 := "snapshot3"

	// 1. Schedule root nodes for snapshot1 and snapshot2.
	err := f.storage.SchedulerDirectoryForTraversal(f.ctx, snapshot1, nfs.RootNodeID)
	require.NoError(t, err)
	err = f.storage.SchedulerDirectoryForTraversal(f.ctx, snapshot2, nfs.RootNodeID)
	require.NoError(t, err)

	// 2. Schedule 10 nodes for snapshot1 and 5 nodes for snapshot2
	//    with intersecting IDs.
	require.NoError(t, f.storage.ScheduleChildNodesForListing(
		f.ctx,
		snapshot1,
		nfs.RootNodeID,
		"cookie1",
		[]nfs.Node{
			{NodeID: 2, ParentID: nfs.RootNodeID},
			{NodeID: 3, ParentID: nfs.RootNodeID},
			{NodeID: 4, ParentID: nfs.RootNodeID},
			{NodeID: 5, ParentID: nfs.RootNodeID},
			{NodeID: 1001, ParentID: nfs.RootNodeID},
			{NodeID: 1002, ParentID: nfs.RootNodeID},
			{NodeID: 1003, ParentID: nfs.RootNodeID},
			{NodeID: 1004, ParentID: nfs.RootNodeID},
			{NodeID: 1005, ParentID: nfs.RootNodeID},
			{NodeID: 1006, ParentID: nfs.RootNodeID},
		},
	))

	require.NoError(t, f.storage.ScheduleChildNodesForListing(
		f.ctx,
		snapshot2,
		nfs.RootNodeID,
		"cookie2",
		[]nfs.Node{
			{NodeID: 2, ParentID: nfs.RootNodeID},
			{NodeID: 3, ParentID: nfs.RootNodeID},
			{NodeID: 4, ParentID: nfs.RootNodeID},
			{NodeID: 1002, ParentID: nfs.RootNodeID},
			{NodeID: 1005, ParentID: nfs.RootNodeID},
		},
	))

	snapshot1ExpectedIDs := []uint64{
		nfs.RootNodeID, 2, 3, 4, 5, 1001, 1002, 1003, 1004, 1005, 1006,
	}
	snapshot2ExpectedIDs := []uint64{
		nfs.RootNodeID, 2, 3, 4, 1002, 1005,
	}

	// 3. List nodes for snapshot1 and snapshot2: 11 and 6 entries.
	entries1, err := f.storage.SelectNodesToList(
		f.ctx, snapshot1, emptyNodesToExclude, 100,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, getNodeIDs(entries1), snapshot1ExpectedIDs)

	entries2, err := f.storage.SelectNodesToList(
		f.ctx, snapshot2, emptyNodesToExclude, 100,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, getNodeIDs(entries2), snapshot2ExpectedIDs)

	// 4. Delete 1 element from snapshot1, check 10 remain,
	//    check snapshot2 still has 6 elements matching expected IDs.
	var remains bool
	err = s.db.Execute(
		f.ctx,
		func(ctx context.Context, session *persistence.Session) error {
			remains, err = s.clearDirectoryListingQueue(
				ctx, session, snapshot1, 1,
			)
			return err
		},
	)
	require.NoError(t, err)
	require.True(t, remains)

	entries1, err = f.storage.SelectNodesToList(
		f.ctx, snapshot1, emptyNodesToExclude, 100,
	)
	require.NoError(t, err)
	require.Len(t, entries1, 10)
	savedSnapshot1IDs := getNodeIDs(entries1)

	entries2, err = f.storage.SelectNodesToList(
		f.ctx, snapshot2, emptyNodesToExclude, 100,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, getNodeIDs(entries2), snapshot2ExpectedIDs)

	// 5. Delete 2 elements from snapshot2, check snapshot1 unchanged,
	//    save snapshot2 entries. Require remains = false (6 - 2 = 4, limit 2 == deleted 2).
	err = s.db.Execute(
		f.ctx,
		func(ctx context.Context, session *persistence.Session) error {
			remains, err = s.clearDirectoryListingQueue(
				ctx, session, snapshot2, 2,
			)
			return err
		},
	)
	require.NoError(t, err)

	entries1, err = f.storage.SelectNodesToList(
		f.ctx, snapshot1, emptyNodesToExclude, 100,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, getNodeIDs(entries1), savedSnapshot1IDs)

	entries2, err = f.storage.SelectNodesToList(
		f.ctx, snapshot2, emptyNodesToExclude, 100,
	)
	require.NoError(t, err)
	require.Len(t, entries2, 4)
	savedSnapshot2IDs := getNodeIDs(entries2)

	// 6. Delete from snapshot3 (nonexistent), require no error,
	//    require remains = false.
	err = s.db.Execute(
		f.ctx,
		func(ctx context.Context, session *persistence.Session) error {
			remains, err = s.clearDirectoryListingQueue(
				ctx, session, snapshot3, 100,
			)
			return err
		},
	)
	require.NoError(t, err)
	require.False(t, remains)

	// 7. Delete 100 elements from snapshot1, require remains = false.
	err = s.db.Execute(
		f.ctx,
		func(ctx context.Context, session *persistence.Session) error {
			remains, err = s.clearDirectoryListingQueue(
				ctx, session, snapshot1, 100,
			)
			return err
		},
	)
	require.NoError(t, err)
	require.False(t, remains)

	entries1, err = f.storage.SelectNodesToList(
		f.ctx, snapshot1, emptyNodesToExclude, 100,
	)
	require.NoError(t, err)
	require.Len(t, entries1, 0)

	entries2, err = f.storage.SelectNodesToList(
		f.ctx, snapshot2, emptyNodesToExclude, 100,
	)
	require.NoError(t, err)
	require.ElementsMatch(t, getNodeIDs(entries2), savedSnapshot2IDs)

	// 8. Delete from snapshot2 using public method, require no elements remain.
	err = f.storage.ClearDirectoryListingQueue(f.ctx, snapshot2, 2)
	require.NoError(t, err)

	entries2, err = f.storage.SelectNodesToList(
		f.ctx, snapshot2, emptyNodesToExclude, 100,
	)
	require.NoError(t, err)
	require.Len(t, entries2, 0)
}
