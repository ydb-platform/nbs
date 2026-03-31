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
	ignoreNotFound    bool
}

func (l *filestoreLister) ListNodes(
	ctx context.Context,
	nodeID uint64,
	cookie string,
) ([]nfs.Node, string, error) {

	nodes, cookcookie, err := l.session.ListNodes(
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

	if nfs.IsNotFoundError(err) {
		return []nfs.Node{}, "", nil
	}

	return nil, "", err
}

func (l *filestoreLister) Close(ctx context.Context) error {
	return l.session.Close(ctx)
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

	return &filestoreLister{
		session:           session,
		listNodesMaxBytes: o.listNodesMaxBytes,
		unsafe:            o.unsafe,
		ignoreNotFound:    o.ignoreNotFound,
	}, nil
}
