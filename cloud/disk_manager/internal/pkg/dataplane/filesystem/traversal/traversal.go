package traversal

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/listers"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type StateSaver func(ctx context.Context) error

type OnListedNodesFunc func(
	ctx context.Context,
	nodes []nfs.Node,
	filesystemLister listers.FilesystemLister,
) error

////////////////////////////////////////////////////////////////////////////////

type FilesystemTraverser struct {
	scheduledNodes           chan *storage.NodeQueueEntry
	processedNodeIDs         chan uint64
	filesystemSnapshotID     string
	filesystemID             string
	filesystemCheckpointID   string
	filesystemListerFactory  listers.FilesystemListerFactory
	storage                  storage.Storage
	stateSaver               StateSaver
	config                   *config.FilesystemTraversalConfig
	rootNodeAlreadyScheduled bool
	rootNodeID               uint64
}

// FilesystemTravers performs parallel traversal of a filesystem.
// For traversal durability, the traversal queue is stored within a database table.
// 1. Checks whether the root node was already scheduled.
// 2. If not, schedules the root node.
// 3. Saves the state via StateSaver.
// Saving the information about root node scheduling is required,
// because nodes are removed from the traversal queue when they are fully processed.
// 4. Starts directoryScheduler method which fetches nodes from database, saves them to inflight hashmap, and sends them to scheduledNodes channel.
// directoryScheduler also removes inflight nodes when it receives their IDs from processedNodeIDs channel.
// 5. Start directoryLister workers.
// A directoryLister worker fetches the node from scheduledNodes channel, lists its children via the Filestore client,
// calls onListedNodes callback with the listed nodes, and schedules child directories for listing.
// Scheduling means saving child nodes to the database and they will be later fetched by directoryScheduler.
// Node listing operation has pagination support using pagination cookie.
// The cookie must be saved to the database in the same transaction as scheduling child nodes for listing.
// Each node is repeatedly listed until the cookie becomes empty.
// Traversal is finished when there are no processing nodes and no nodes to be scheduled.

func NewFilesystemTraverser(
	filesystemSnapshotID string,
	filesystemID string,
	filesystemCheckpointID string,
	filesystemListerFactory listers.FilesystemListerFactory,
	snapshotStorage storage.Storage,
	stateSaver StateSaver,
	config *config.FilesystemTraversalConfig,
	rootNodeAlreadyScheduled bool,
	rootNodeID uint64,
) *FilesystemTraverser {

	return &FilesystemTraverser{
		scheduledNodes:           make(chan *storage.NodeQueueEntry),
		processedNodeIDs:         make(chan uint64),
		filesystemSnapshotID:     filesystemSnapshotID,
		filesystemID:             filesystemID,
		filesystemCheckpointID:   filesystemCheckpointID,
		filesystemListerFactory:  filesystemListerFactory,
		storage:                  snapshotStorage,
		stateSaver:               stateSaver,
		config:                   config,
		rootNodeAlreadyScheduled: rootNodeAlreadyScheduled,
		rootNodeID:               rootNodeID,
	}
}

func (t *FilesystemTraverser) Traverse(
	ctx context.Context,
	onListedNodes OnListedNodesFunc,
) error {

	ctx = logging.WithFields(
		ctx,
		logging.Bool("FILESYSTEM_TRAVERSAL_IN_PROGRESS", true),
		logging.String("FILESYSTEM_SNAPSHOT_ID", t.filesystemSnapshotID),
		logging.String("FILESYSTEM_ID", t.filesystemID),
		logging.String("FILESYSTEM_CHECKPOINT_ID", t.filesystemCheckpointID),
	)
	defer close(t.processedNodeIDs)

	if !t.rootNodeAlreadyScheduled {
		logging.Info(ctx, "Scheduling root node for listing.")
		err := t.storage.SchedulerDirectoryForTraversal(
			ctx,
			t.filesystemSnapshotID,
			t.rootNodeID,
		)
		if err != nil {
			return err
		}

		logging.Info(ctx, "Root node scheduled, saving state.")
		err = t.stateSaver(ctx)
		if err != nil {
			return err
		}
	}

	eg, ctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		return t.directoryScheduler(ctx)
	})

	for i := uint32(0); i < t.config.GetTraversalWorkersCount(); i++ {
		eg.Go(func() error {
			return t.directoryLister(ctx, onListedNodes)
		})
	}

	return eg.Wait()
}

func (t *FilesystemTraverser) directoryScheduler(ctx context.Context) error {
	defer close(t.scheduledNodes)

	processingNodeIDs := make(map[uint64]struct{})
	pendingNodes := make([]*storage.NodeQueueEntry, 0)

	for {
		if len(pendingNodes) > 0 {
			node := pendingNodes[len(pendingNodes)-1]
			select {
			case <-ctx.Done():
				return ctx.Err()
			case nodeID := <-t.processedNodeIDs:
				delete(processingNodeIDs, nodeID)
			case t.scheduledNodes <- node:
				pendingNodes = pendingNodes[:len(pendingNodes)-1]
				processingNodeIDs[node.NodeID] = struct{}{}
			}
			continue
		}

		entries, err := t.storage.SelectNodesToList(
			ctx,
			t.filesystemSnapshotID,
			processingNodeIDs,
			t.config.GetSelectNodesToListLimit(),
		)
		if err != nil {
			return err
		}

		if len(entries) == 0 {
			if len(processingNodeIDs) == 0 {
				logging.Info(ctx, "Traversal complete for %s", t.filesystemSnapshotID)
				return nil
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case nodeID := <-t.processedNodeIDs:
				delete(processingNodeIDs, nodeID)
			}
			continue
		}

		for _, entry := range entries {
			pendingNodes = append(pendingNodes, &storage.NodeQueueEntry{
				NodeID: entry.NodeID,
				Cookie: entry.Cookie,
			})
		}
	}
}

func (t *FilesystemTraverser) directoryLister(
	ctx context.Context,
	onListedNodes OnListedNodesFunc,
) error {

	filesystemLister, err := t.filesystemListerFactory.CreateLister(
		ctx,
		t.filesystemID,
		t.filesystemCheckpointID,
	)
	if err != nil {
		return err
	}

	cleanupCtx := context.WithoutCancel(ctx)
	defer func() {
		err = filesystemLister.Close(cleanupCtx)
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

			err := t.listNode(ctx, filesystemLister, node, onListedNodes)
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
	filesystemLister listers.FilesystemLister,
	node *storage.NodeQueueEntry,
	onListedNodes OnListedNodesFunc,
) error {

	cookie := node.Cookie
	for {
		children, nextCookie, err := filesystemLister.ListNodes(
			ctx,
			node.NodeID,
			cookie,
		)
		if err != nil {
			return err
		}

		if len(children) > 0 {
			err = onListedNodes(ctx, children, filesystemLister)
			if err != nil {
				return err
			}
		}

		var childDirs []nfs.Node
		for _, n := range children {
			// In case of filesystem scrubbing, ListNodes may return InvalidNodeID
			// for nodes present in index tablet and absent in shard.
			// See: https://github.com/ydb-platform/nbs/issues/5094
			if n.NodeID == nfs.InvalidNodeID {
				continue
			}

			if n.Type.IsDirectory() {
				childDirs = append(childDirs, n)
			}
		}

		err = t.storage.ScheduleChildNodesForListing(
			ctx,
			t.filesystemSnapshotID,
			node.NodeID,
			nextCookie,
			childDirs,
		)
		if err != nil {
			return err
		}

		if nextCookie == "" { // Listing of this node is finished
			return nil
		}

		cookie = nextCookie
	}
}

func (t *FilesystemTraverser) Cleanup(ctx context.Context) error {
	return t.storage.ClearDirectoryListingQueue(ctx, t.filesystemSnapshotID, 0)
}
