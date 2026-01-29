package filesystemtraversal

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type StateSaver func(ctx context.Context) error

type OnListedNodesFunc func(
	ctx context.Context,
	nodes []nfs.Node,
	session nfs.Session,
	client nfs.Client,
) error

////////////////////////////////////////////////////////////////////////////////

type FilesystemTraverser struct {
	scheduledNodes           chan *storage.NodeQueueEntry
	processedNodeIDs         chan uint64
	workersCount             int
	filesystemSnapshotID     string
	filesystemID             string
	filesystemCheckpointID   string
	client                   nfs.Client
	storage                  storage.Storage
	stateSaver               StateSaver
	selectNodesToListLimit   uint64
	rootNodeAlreadyScheduled bool
}

func NewFilesystemTraverser(
	filesystemSnapshotID string,
	filesystemID string,
	filesystemCheckpointID string,
	client nfs.Client,
	snapshotStorage storage.Storage,
	stateSaver StateSaver,
	workersCount int,
	selectNodesToListLimit uint64,
	rootNodeAlreadyScheduled bool,
) *FilesystemTraverser {

	return &FilesystemTraverser{
		scheduledNodes:           make(chan *storage.NodeQueueEntry),
		processedNodeIDs:         make(chan uint64),
		workersCount:             workersCount,
		filesystemSnapshotID:     filesystemSnapshotID,
		filesystemID:             filesystemID,
		filesystemCheckpointID:   filesystemCheckpointID,
		client:                   client,
		storage:                  snapshotStorage,
		stateSaver:               stateSaver,
		selectNodesToListLimit:   selectNodesToListLimit,
		rootNodeAlreadyScheduled: rootNodeAlreadyScheduled,
	}
}

func (t *FilesystemTraverser) Traverse(
	ctx context.Context,
	onListedNodes OnListedNodesFunc,
) error {

	if !t.rootNodeAlreadyScheduled {
		err := t.storage.ScheduleRootNodeForListing(ctx, t.filesystemSnapshotID)
		if err != nil {
			return err
		}

		if t.stateSaver != nil {
			err = t.stateSaver(ctx)
			if err != nil {
				return err
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
			node := pendingNodes[len(pendingNodes)-1]
			select {
			case <-ctx.Done():
				return ctx.Err()
			case nodeID := <-t.processedNodeIDs:
				delete(processingNodes, nodeID)
			case t.scheduledNodes <- node:
				pendingNodes = pendingNodes[:len(pendingNodes)-1]
				processingNodes[node.NodeID] = struct{}{}
			}
			continue
		}

		entries, err := t.storage.SelectNodesToList(
			ctx,
			t.filesystemSnapshotID,
			processingNodes,
			t.selectNodesToListLimit,
		)
		if err != nil {
			return err
		}

		if len(entries) == 0 {
			if len(processingNodes) == 0 {
				logging.Info(ctx, "Traversal complete for %s", t.filesystemSnapshotID)
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case nodeID := <-t.processedNodeIDs:
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

	session, err := t.client.CreateSession(ctx, t.filesystemID, t.filesystemCheckpointID, true)
	if err != nil {
		return err
	}

	cleanupCtx := context.WithoutCancel(ctx)
	defer func() {
		err = t.client.DestroySession(cleanupCtx, session)
		if err != nil {
			logging.Error(
				cleanupCtx,
				"failed to destroy session for traversal: %v",
				err,
			)
		}
	}()

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
			case t.processedNodeIDs <- node.NodeID:
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
		nodes, nextCookie, err := t.client.ListNodes(
			ctx,
			session,
			node.NodeID,
			cookie,
		)
		if err != nil {
			return err
		}

		if len(nodes) > 0 {
			err = onListedNodes(ctx, nodes, session, t.client)
			if err != nil {
				return err
			}
		}

		var childDirs []nfs.Node
		for _, n := range nodes {
			// In case of filesystem scrubbing, ListNodes may return InvalidNodeID
			// for nodes present in index tablet and absent in shard.
			// See: https://github.com/ydb-platform/nbs/issues/5094
			if node.NodeID == nfs.InvalidNodeID {
				continue
			}

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
			return err
		}

		if nextCookie == "" {
			return nil
		}

		cookie = nextCookie
	}
}
