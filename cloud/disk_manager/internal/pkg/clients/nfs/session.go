package nfs

import (
	"context"

	client_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/metrics"
	nfs_protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
)

////////////////////////////////////////////////////////////////////////////////

type Node nfs_client.Node

////////////////////////////////////////////////////////////////////////////////

const (
	InvalidNodeID = uint64(nfs_protos.ENodeConstants_E_INVALID_NODE_ID)
	RootNodeID    = uint64(nfs_protos.ENodeConstants_E_ROOT_NODE_ID)
)

const (
	NODE_KIND_INVALID = nfs_client.NodeType(nfs_client.NODE_KIND_INVALID)
	NODE_KIND_FILE    = nfs_client.NodeType(nfs_client.NODE_KIND_FILE)
	NODE_KIND_DIR     = nfs_client.NodeType(nfs_client.NODE_KIND_DIR)
	NODE_KIND_SYMLINK = nfs_client.NodeType(nfs_client.NODE_KIND_SYMLINK)
	NODE_KIND_LINK    = nfs_client.NodeType(nfs_client.NODE_KIND_LINK)
	NODE_KIND_SOCK    = nfs_client.NodeType(nfs_client.NODE_KIND_SOCK)
)

////////////////////////////////////////////////////////////////////////////////

type session struct {
	nfs     nfs_client.ClientInterface
	session nfs_client.Session
	metrics client_metrics.Metrics
}

func NewSession(
	nfs nfs_client.ClientInterface,
	nfsSession nfs_client.Session,
	metrics client_metrics.Metrics,
) Session {

	return &session{
		nfs:     nfs,
		session: nfsSession,
		metrics: metrics,
	}
}

func (s *session) CreateCheckpoint(
	ctx context.Context,
	filesystemID string,
	checkpointID string,
	nodeID uint64,
) (err error) {

	defer s.metrics.StatRequest("CreateCheckpoint")(&err)

	return wrapError(
		s.nfs.CreateCheckpoint(
			ctx,
			s.session,
			filesystemID,
			&nfs_client.CreateCheckpointOpts{
				CheckpointID: checkpointID,
				NodeID:       nodeID,
			},
		),
	)
}

func (s *session) Close(ctx context.Context) (err error) {
	defer s.metrics.StatRequest("DestroySession")(&err)

	return wrapError(s.nfs.DestroySession(ctx, s.session))
}

func (s *session) ListNodes(
	ctx context.Context,
	parentNodeID uint64,
	cookie string,
	maxBytes uint32,
	unsafe bool,
) (_ []Node, _ string, err error) {

	defer s.metrics.StatRequest("ListNodes")(&err)

	nodes, cookie, err := s.nfs.ListNodes(
		ctx,
		s.session,
		parentNodeID,
		cookie,
		maxBytes,
		unsafe,
	)
	resultNodes := make([]Node, len(nodes))
	for i := range nodes {
		resultNodes[i] = Node(nodes[i])
	}

	return resultNodes, cookie, wrapError(err)
}

func (s *session) CreateNode(
	ctx context.Context,
	node Node,
) (_ uint64, err error) {

	defer s.metrics.StatRequest("CreateNode")(&err)

	nodeID, err := s.nfs.CreateNode(
		ctx,
		s.session,
		nfs_client.Node(node),
	)
	return nodeID, wrapError(err)
}

func (s *session) CreateNodeIdempotent(
	ctx context.Context,
	node Node,
) (_ uint64, err error) {

	defer s.metrics.StatRequest("CreateNodeIdempotent")(&err)

	nodeID, err := s.nfs.CreateNode(
		ctx,
		s.session,
		nfs_client.Node(node),
	)
	if err != nil && isAlreadyExistsError(err) {
		existing, err := s.nfs.GetNodeAttr(
			ctx,
			s.session,
			node.ParentID,
			node.Name,
		)
		if err != nil {
			return 0, wrapError(err)
		}

		return existing.NodeID, nil
	}

	return nodeID, wrapError(err)
}

func (s *session) ReadLink(
	ctx context.Context,
	nodeID uint64,
) (_ []byte, err error) {

	defer s.metrics.StatRequest("ReadLink")(&err)

	data, err := s.nfs.ReadLink(ctx, s.session, nodeID)
	return data, wrapError(err)
}

func (s *session) GetNodeAttr(
	ctx context.Context,
	parentNodeID uint64,
	name string,
) (_ Node, err error) {

	defer s.metrics.StatRequest("GetNodeAttr")(&err)

	node, err := s.nfs.GetNodeAttr(ctx, s.session, parentNodeID, name)
	return Node(node), wrapError(err)
}

func (s *session) UnlinkNode(
	ctx context.Context,
	parentNodeID uint64,
	name string,
	unlinkDirectory bool,
) (err error) {

	defer s.metrics.StatRequest("UnlinkNode")(&err)

	return wrapError(
		s.nfs.UnlinkNode(ctx, s.session, parentNodeID, name, unlinkDirectory),
	)
}
