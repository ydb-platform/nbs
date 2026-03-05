package nfs

import (
	"context"

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

////////////////////////////////////////////////////////////////////////////////

type session struct {
	nfs     *nfs_client.Client
	session nfs_client.Session
}

func (s *session) CreateCheckpoint(
	ctx context.Context,
	filesystemID string,
	checkpointID string,
	nodeID uint64,
) error {

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

func (s *session) Close(ctx context.Context) error {
	return wrapError(s.nfs.DestroySession(ctx, s.session))
}

func (s *session) ListNodes(
	ctx context.Context,
	parentNodeID uint64,
	cookie string,
	maxBytes uint32,
	unsafe bool,
) ([]Node, string, error) {

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
) (uint64, error) {

	nodeID, err := s.nfs.CreateNode(
		ctx,
		s.session,
		nfs_client.Node(node),
	)
	return nodeID, wrapError(err)
}

func (s *session) ReadLink(
	ctx context.Context,
	nodeID uint64,
) ([]byte, error) {

	data, err := s.nfs.ReadLink(ctx, s.session, nodeID)
	return data, wrapError(err)
}
