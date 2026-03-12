package listers

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
)

////////////////////////////////////////////////////////////////////////////////

type FilesystemLister interface {
	ListNodes(
		ctx context.Context,
		nodeID uint64,
		cookie string,
	) (children []nfs.Node, nextCookie string, err error)

	Close(ctx context.Context) error
}

////////////////////////////////////////////////////////////////////////////////

type FilesystemListerFactory interface {
	CreateLister(
		ctx context.Context,
		filesystemID string,
		checkpointID string,
	) (FilesystemLister, error)
}
