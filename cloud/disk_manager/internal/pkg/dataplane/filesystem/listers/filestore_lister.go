package listers

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
)

////////////////////////////////////////////////////////////////////////////////

type filestoreLister struct {
	session           nfs.Session
	listNodesMaxBytes uint32
	unsafe            bool
}

func (l *filestoreLister) ListNodes(
	ctx context.Context,
	nodeID uint64,
	cookie string,
) ([]nfs.Node, string, error) {
	return l.session.ListNodes(
		ctx,
		nodeID,
		cookie,
		l.listNodesMaxBytes,
		l.unsafe,
	)
}

func (l *filestoreLister) Close(ctx context.Context) error {
	return l.session.Close(ctx)
}

////////////////////////////////////////////////////////////////////////////////

type filestoreOpener struct {
	nfsClient         nfs.Client
	listNodesMaxBytes uint32
	readOnly          bool
	unsafe            bool
}

func NewFilestoreOpener(
	nfsClient nfs.Client,
	listNodesMaxBytes uint32,
	readOnly bool,
	unsafe bool,
) FilesystemOpener {
	return &filestoreOpener{
		nfsClient:         nfsClient,
		listNodesMaxBytes: listNodesMaxBytes,
		readOnly:          readOnly,
		unsafe:            unsafe,
	}
}

func (o *filestoreOpener) OpenFilesystem(
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

	return &filestoreLister{
		session:           session,
		listNodesMaxBytes: o.listNodesMaxBytes,
		unsafe:            o.unsafe,
	}, nil
}
