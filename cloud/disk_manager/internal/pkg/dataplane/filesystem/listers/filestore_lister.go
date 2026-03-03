package listers

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
)

////////////////////////////////////////////////////////////////////////////////

type filestoreLister struct {
	nfsClient         nfs.Client
	session           nfs.Session
	listNodesMaxBytes uint32
	readOnly          bool
	unsafe            bool
}

type FilestoreOpener struct {
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
) *FilestoreOpener {
	return &FilestoreOpener{
		nfsClient:         nfsClient,
		listNodesMaxBytes: listNodesMaxBytes,
		readOnly:          readOnly,
		unsafe:            unsafe,
	}
}

func (o *FilestoreOpener) OpenFilesystem(
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
		nfsClient:         o.nfsClient,
		session:           session,
		listNodesMaxBytes: o.listNodesMaxBytes,
		readOnly:          o.readOnly,
		unsafe:            o.unsafe,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func (l *filestoreLister) ListNodes(
	ctx context.Context,
	nodeID uint64,
	cookie string,
) ([]nfs.Node, string, error) {
	return l.nfsClient.ListNodes(
		ctx,
		l.session,
		nodeID,
		cookie,
		l.listNodesMaxBytes,
		l.unsafe,
	)
}

func (l *filestoreLister) Close(ctx context.Context) error {
	return l.nfsClient.DestroySession(ctx, l.session)
}
