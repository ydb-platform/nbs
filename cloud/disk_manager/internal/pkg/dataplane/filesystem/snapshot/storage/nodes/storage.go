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

	SaveRestoredDirectories(
		ctx context.Context,
		srcSnapshotID string,
		dstSnapshotID string,
		srcNodeIds []uint64,
		dstNodeIds []uint64,
	) error
}
