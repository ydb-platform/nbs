package storage

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
)

////////////////////////////////////////////////////////////////////////////////

type NodeQueueEntry struct {
	NodeID uint64
	Cookie string
}

////////////////////////////////////////////////////////////////////////////////

type Storage interface {
	// Saves the subtree root directory to the directory listing queue.
	SchedulerDirectoryForTraversal(
		ctx context.Context,
		snapshotID string,
		nodeID uint64,
	) error

	// Selects nodes to be listed from the directory listing queue.
	SelectNodesToList(
		ctx context.Context,
		snapshotID string,
		nodesToExclude map[uint64]struct{},
		limit uint64,
	) ([]NodeQueueEntry, error)

	// Updates the node cookie after listing and saves child nodes to the listing queue.
	ScheduleChildNodesForListing(
		ctx context.Context,
		snapshotID string,
		parentNodeID uint64,
		nextCookie string,
		children []nfs.Node,
	) error

	// Clears directory listing queue for the given snapshot.
	ClearDirectoryListingQueue(
		ctx context.Context,
		snapshotID string,
		deletionLimit int,
	) error
}
