package nodes

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
)

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

	DeleteSnapshotData(
		ctx context.Context,
		snapshotID string,
	) (bool, error)

	UpdateRestorationNodeIDMapping(
		ctx context.Context,
		srcID string,
		dstID string,
		srcNodeIds []uint64,
		dstNodeIds []uint64,
	) error

	GetDestinationNodeID(
		ctx context.Context,
		srcSnapshotID string,
		dstFilesystemID string,
		srcNodeID uint64,
	) (dstNodeID uint64, ok bool, err error)
}
