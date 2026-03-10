package listers

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
)

////////////////////////////////////////////////////////////////////////////////

type FilestoreLister struct {
	Session           nfs.Session
	listNodesMaxBytes uint32
	unsafe            bool
}

func (l *FilestoreLister) ListNodes(
	ctx context.Context,
	nodeID uint64,
	cookie string,
) ([]nfs.Node, string, error) {

	return l.Session.ListNodes(
		ctx,
		nodeID,
		cookie,
		l.listNodesMaxBytes,
		l.unsafe,
	)
}

func (l *FilestoreLister) Close(ctx context.Context) error {
	return l.Session.Close(ctx)
}

////////////////////////////////////////////////////////////////////////////////

type filestoreListerFactory struct {
	nfsClient         nfs.Client
	listNodesMaxBytes uint32
	readOnly          bool
	unsafe            bool
}

func NewFilestoreListerFactory(
	nfsClient nfs.Client,
	listNodesMaxBytes uint32,
	readOnly bool,
	unsafe bool,
) FilesystemListerFactory {

	return &filestoreListerFactory{
		nfsClient:         nfsClient,
		listNodesMaxBytes: listNodesMaxBytes,
		readOnly:          readOnly,
		unsafe:            unsafe,
	}
}

func (o *filestoreListerFactory) CreateLister(
	ctx context.Context,
	filesystemID string,
	checkpointID string,
) (FilesystemLister, error) {

	session, err := o.nfsClient.CreateSession(
		ctx,
		filesystemID,
		checkpointID,
		o.readOnly,
	)
	if err != nil {
		return nil, err
	}

	return &FilestoreLister{
		Session:           session,
		listNodesMaxBytes: o.listNodesMaxBytes,
		unsafe:            o.unsafe,
	}, nil
}
