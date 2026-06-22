package nodes

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
)

////////////////////////////////////////////////////////////////////////////////

type Storage interface {
	SaveNodes(
		ctx context.Context,
		snapshotID string,
		nodes []nfs.Node,
	) error

	ListNodes(
		ctx context.Context,
		snapshotID string,
		parentNodeID uint64,
		cookie string,
		limit int,
	) (nodes []nfs.Node, nextCookie string, err error)

	ListNodesByShard(
		ctx context.Context,
		snapshotID string,
		shardFilesystemID string,
		limit uint64,
		cookie *NodeRefsByShardCookie,
	) ([]nfs.Node, *NodeRefsByShardCookie, error)

	DeleteSnapshotData(
		ctx context.Context,
		snapshotID string,
	) error

	UpdateRestorationNodeIDMapping(
		ctx context.Context,
		srcID string,
		dstID string,
		nodeIDMapping map[uint64]uint64,
	) error

	GetDestinationNodeIDs(
		ctx context.Context,
		srcSnapshotID string,
		dstFilesystemID string,
		srcNodeIDs []uint64,
	) (map[uint64]uint64, error)

	ListHardLinks(
		ctx context.Context,
		snapshotID string,
		limit int,
		offset int,
	) ([]nfs.Node, error)

	CleanupRestorationNodeIDsMapping(
		ctx context.Context,
		snapshotID string,
		destinationFilesystemID string,
	) error
}

// NodeRefsByShardCookie identifies a position in the node_refs_by_shard order.
type NodeRefsByShardCookie struct {
	ParentNodeID uint64
	Name         string
	StoreAsChild bool
}
