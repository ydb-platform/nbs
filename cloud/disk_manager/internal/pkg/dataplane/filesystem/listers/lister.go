package listers

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

// ErrNodeNotFound is returned by FilesystemLister.ListNodes when the node
// does not exist (E_FS_NOENT). Callers may use errors.Is to detect this.
var ErrNodeNotFound = errors.NewNonRetriableErrorf("node not found")

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
