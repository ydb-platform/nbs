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
	ignoreNotFound    bool
}

func (l *FilestoreLister) ListNodes(
	ctx context.Context,
	nodeID uint64,
	cookie string,
) ([]nfs.Node, string, error) {

	nodes, cookcookie, err := l.Session.ListNodes(
		ctx,
		nodeID,
		cookie,
		l.listNodesMaxBytes,
		l.unsafe,
	)
	if err == nil {
		return nodes, cookcookie, nil
	}

	if !l.ignoreNotFound {
		return nil, "", err
	}

	if nfs.IsEnoEntError(err) {
		return []nfs.Node{}, "", nil
	}

	return nil, "", err
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
	ignoreNotFound    bool
}

func NewFilestoreListerFactory(
	nfsClient nfs.Client,
	listNodesMaxBytes uint32,
	readOnly bool,
	unsafe bool,
	ignoreNotFound bool,
) FilesystemListerFactory {

	return &filestoreListerFactory{
		nfsClient:         nfsClient,
		listNodesMaxBytes: listNodesMaxBytes,
		readOnly:          readOnly,
		unsafe:            unsafe,
		ignoreNotFound:    ignoreNotFound,
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
		ignoreNotFound:    o.ignoreNotFound,
	}, nil
}
