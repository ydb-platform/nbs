package filesystemtraversal

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

const RootNodeID = uint64(1)

////////////////////////////////////////////////////////////////////////////////

type StateSaver func(ctx context.Context) error

type OnListedNodesFunc func(ctx context.Context, nodes []nfs.Node, session nfs.Session, client nfs.Client) error

////////////////////////////////////////////////////////////////////////////////

type FilesystemTraverser struct {
	scheduledNodes           chan *storage.NodeQueueEntry
	processedNodes           chan uint64
	workersCount             int
	filesystemSnapshotID     string
	filesystemID             string
	checkpointID             string
	client                   nfs.Client
	storage                  storage.Storage
	stateSaver               StateSaver
	selectLimit              uint64
	rootNodeAlreadyScheduled bool
}

// FilesystemTravers follows the following protocol:
// 1. Checks whether the root node was already scheduled
// 2. If not, schedules the root node
// 3. Saves the state via StateSaver
// 4. Starts directoryScheduler method which selects nodes to list
// (storage.SelectNodesToList(filesystemID, processingNodes map[uint64]struct{},limit uint64)([]nodeQueueEntry, error))
// in a loop and sends them to scheduledNodes channel. Before scheduling a node is is added to processingNodes map
// when the node is fully processed, it is send to processedNodes channel
// directoryScheduler select both channels using select statement, so it can receive processed nodes while scheduling nodes
// SelectNodesToList excludes nodes already in processingNodes map

// workers start in parallel and read from scheduledNodes channel,
// they list node based on cookie and node id using ListNodes api call

// 5. Start directoryLister's using waitgroup up to workersCount
// 6. Each worker get callback passed, lists nodes, calls callback with the list of nodes and a client,
// and schedules child directories back to scheduledNodes channel, updating the initial queue entry via the following call:
// storage.EnqueueChildren(filesystemSnapshotID string, parentNodeID string, parentCookie string, children []nfs.Node)(error)
// worker keeps listing the node until no more cookie (see the dfs traversal in nfs client)
// 7. When all workers are done, the method returns
// All the mentioned things happen in Traverse() method which takes a onListedNodes callback
// func OnListedNodes(ctx context.Context, nodes []nfs.Node, client nfs.Client) error

func NewFilesystemTraverser(
	filesystemSnapshotID string,
	filesystemID string,
	checkpointID string,
	client nfs.Client,
	snapshotStorage storage.Storage,
	stateSaver StateSaver,
	workersCount int,
	selectLimit uint64,
	rootNodeAlreadyScheduled bool,
) *FilesystemTraverser {
	return &FilesystemTraverser{
		scheduledNodes:           make(chan *storage.NodeQueueEntry),
		processedNodes:           make(chan uint64),
		workersCount:             workersCount,
		filesystemSnapshotID:     filesystemSnapshotID,
		filesystemID:             filesystemID,
		checkpointID:             checkpointID,
		client:                   client,
		storage:                  snapshotStorage,
		stateSaver:               stateSaver,
		selectLimit:              selectLimit,
		rootNodeAlreadyScheduled: rootNodeAlreadyScheduled,
	}
}

func (t *FilesystemTraverser) Traverse(
	ctx context.Context,
	onListedNodes OnListedNodesFunc,
) error {

	if !t.rootNodeAlreadyScheduled {
		scheduled, err := t.storage.ScheduleRootNodeForListing(ctx, t.filesystemSnapshotID)
		if err != nil {
			return errors.NewRetriableErrorf("failed to schedule root node: %w", err)
		}
		if scheduled {
			logging.Info(ctx, "Root node scheduled for filesystem snapshot %s", t.filesystemSnapshotID)
		}

		if t.stateSaver != nil {
			err = t.stateSaver(ctx)
			if err != nil {
				return errors.NewRetriableErrorf("failed to save state: %w", err)
			}
		}
	}

	eg, ctx := errgroup.WithContext(ctx)

	eg.Go(func() error {
		return t.directoryScheduler(ctx)
	})

	for i := 0; i < t.workersCount; i++ {
		eg.Go(func() error {
			return t.directoryLister(ctx, onListedNodes)
		})
	}

	return eg.Wait()
}

func (t *FilesystemTraverser) directoryScheduler(ctx context.Context) error {
	defer close(t.scheduledNodes)

	processingNodes := make(map[uint64]struct{})
	pendingNodes := make([]*storage.NodeQueueEntry, 0)

	for {
		if len(pendingNodes) > 0 {
			node := pendingNodes[0]
			select {
			case <-ctx.Done():
				return ctx.Err()
			case nodeID := <-t.processedNodes:
				delete(processingNodes, nodeID)
			case t.scheduledNodes <- node:
				pendingNodes = pendingNodes[1:]
				processingNodes[node.NodeID] = struct{}{}
			}
			continue
		}

		entries, err := t.storage.SelectNodesToList(
			ctx,
			t.filesystemSnapshotID,
			processingNodes,
			t.selectLimit,
		)
		if err != nil {
			return errors.NewRetriableErrorf("failed to select nodes: %w", err)
		}

		if len(entries) == 0 {
			if len(processingNodes) == 0 {
				logging.Info(ctx, "Traversal complete for %s", t.filesystemSnapshotID)
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case nodeID := <-t.processedNodes:
				delete(processingNodes, nodeID)
			}
			continue
		}

		for _, entry := range entries {
			pendingNodes = append(pendingNodes, &storage.NodeQueueEntry{
				NodeID: entry.NodeID,
				Cookie: entry.Cookie,
				Depth:  entry.Depth,
			})
		}
	}
}

func (t *FilesystemTraverser) directoryLister(
	ctx context.Context,
	onListedNodes OnListedNodesFunc,
) error {

	session, err := t.client.CreateSession(ctx, t.filesystemID, t.checkpointID, true)
	if err != nil {
		return errors.NewRetriableErrorf("failed to create session: %w", err)
	}
	defer t.client.DestroySession(ctx, session)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case node, ok := <-t.scheduledNodes:
			if !ok {
				return nil
			}

			err := t.listNode(ctx, session, node, onListedNodes)
			if err != nil {
				return err
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case t.processedNodes <- node.NodeID:
			}
		}
	}
}

func (t *FilesystemTraverser) listNode(
	ctx context.Context,
	session nfs.Session,
	node *storage.NodeQueueEntry,
	onListedNodes OnListedNodesFunc,
) error {

	cookie := node.Cookie
	for {
		nodes, nextCookie, err := t.client.ListNodes(ctx, session, node.NodeID, cookie)
		if err != nil {
			return errors.NewRetriableErrorf("failed to list node %d: %w", node.NodeID, err)
		}

		if len(nodes) > 0 {
			err = onListedNodes(ctx, nodes, session, t.client)
			if err != nil {
				return errors.NewRetriableErrorf("callback failed for node %d: %w", node.NodeID, err)
			}
		}

		var childDirs []nfs.Node
		for _, n := range nodes {
			if n.Type.IsDirectory() {
				childDirs = append(childDirs, n)
			}
		}

		err = t.storage.ScheduleNodesForListing(
			ctx,
			t.filesystemSnapshotID,
			node.NodeID,
			nextCookie,
			node.Depth,
			childDirs,
		)
		if err != nil {
			return errors.NewRetriableErrorf("failed to schedule children for node %d: %w", node.NodeID, err)
		}

		if nextCookie == "" {
			return nil
		}

		cookie = nextCookie
	}
}

