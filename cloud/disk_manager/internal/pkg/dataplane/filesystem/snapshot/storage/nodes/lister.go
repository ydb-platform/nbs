package nodes

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/listers"
)

////////////////////////////////////////////////////////////////////////////////

type nodeStorageLister struct {
	storage    Storage
	snapshotID string
	limit      int
}

func (l *nodeStorageLister) ListNodes(
	ctx context.Context,
	nodeID uint64,
	cookie string,
) ([]nfs.Node, string, error) {

	return l.storage.ListNodes(
		ctx,
		l.snapshotID,
		nodeID,
		cookie,
		l.limit,
	)
}

func (l *nodeStorageLister) Close(ctx context.Context) error {
	return nil
}

////////////////////////////////////////////////////////////////////////////////

type nodeStorageListerFactory struct {
	storage Storage
	limit   int
}

func NewNodeStorageListerFactory(
	storage Storage,
	limit int,
) listers.FilesystemListerFactory {

	return &nodeStorageListerFactory{
		storage: storage,
		limit:   limit,
	}
}

func (f *nodeStorageListerFactory) CreateLister(
	ctx context.Context,
	_ string,
	checkpointID string,
) (listers.FilesystemLister, error) {

	return &nodeStorageLister{
		storage:    f.storage,
		snapshotID: checkpointID,
		limit:      f.limit,
	}, nil
}
