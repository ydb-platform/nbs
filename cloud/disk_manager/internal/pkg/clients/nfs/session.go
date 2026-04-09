package nfs

import (
	"context"

	client_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/metrics"
	nfs_protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
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

type sessionWithMetrics struct {
	nfs     nfs_client.ClientInterface
	session nfs_client.Session
	metrics client_metrics.Metrics
}

func (s *sessionWithMetrics) SetSession(nfsSession nfs_client.Session) {
	s.session = nfsSession
}

func (s *sessionWithMetrics) GetID() string {
	return s.session.SessionID
}

func NewSession(
	nfs nfs_client.ClientInterface,
	nfsSession nfs_client.Session,
	metrics client_metrics.Metrics,
) Session {

	return &sessionWithMetrics{
		nfs:     nfs,
		session: nfsSession,
		metrics: metrics,
	}
}

func (s *sessionWithMetrics) CreateCheckpoint(
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

func (s *sessionWithMetrics) Close(ctx context.Context) (err error) {
	defer s.metrics.StatRequest("DestroySession")(&err)

	return wrapError(s.nfs.DestroySession(ctx, s.session))
}

func (s *sessionWithMetrics) ListNodes(
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

func (s *sessionWithMetrics) CreateNode(
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

func (s *sessionWithMetrics) CreateNodeIdempotent(
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

func (s *sessionWithMetrics) ReadLink(
	ctx context.Context,
	nodeID uint64,
) (_ []byte, err error) {

	defer s.metrics.StatRequest("ReadLink")(&err)

	data, err := s.nfs.ReadLink(ctx, s.session, nodeID)
	return data, wrapError(err)
}

func (s *sessionWithMetrics) GetNodeAttr(
	ctx context.Context,
	parentNodeID uint64,
	name string,
) (_ Node, err error) {

	defer s.metrics.StatRequest("GetNodeAttr")(&err)

	node, err := s.nfs.GetNodeAttr(ctx, s.session, parentNodeID, name)
	return Node(node), wrapError(err)
}

func (s *sessionWithMetrics) UnlinkNode(
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

////////////////////////////////////////////////////////////////////////////////

type sessionWithReEstablish struct {
	impl         Session
	nfs          nfs_client.ClientInterface
	filesystemID string
	clientID     string
	checkpointID string
	readonly     bool
}

func requestWithReEstablishSession[T any](
	ctx context.Context,
	s *sessionWithReEstablish,
	callback func() (T, error),
) (T, error) {

	for {
		result, err := callback()
		if !isSessionInvalidError(err) {
			return result, err
		}

		logging.Warn(
			ctx,
			"re-establishing invalid session for filesystem %v, checkpoint %v",
			s.filesystemID,
			s.checkpointID,
		)

		newSession, err := s.nfs.CreateSession(
			ctx,
			s.filesystemID,
			s.clientID,
			s.checkpointID,
			s.readonly,
		)
		if err != nil {
			var zero T
			return zero, err
		}

		s.impl.SetSession(newSession)
	}
}

func (s *sessionWithReEstablish) SetSession(
	nfsSession nfs_client.Session,
) {

	s.impl.SetSession(nfsSession)
}

func (s *sessionWithReEstablish) GetID() string {
	return s.impl.GetID()
}

func NewSessionWithReEstablish(
	impl Session,
	nfs nfs_client.ClientInterface,
	filesystemID string,
	clientID string,
	checkpointID string,
	readonly bool,
) Session {

	return &sessionWithReEstablish{
		impl:         impl,
		nfs:          nfs,
		filesystemID: filesystemID,
		clientID:     clientID,
		checkpointID: checkpointID,
		readonly:     readonly,
	}
}

func (s *sessionWithReEstablish) Close(ctx context.Context) error {
	return s.impl.Close(ctx)
}

func (s *sessionWithReEstablish) CreateCheckpoint(
	ctx context.Context,
	filesystemID string,
	checkpointID string,
	nodeID uint64,
) error {

	_, err := requestWithReEstablishSession(ctx, s, func() (struct{}, error) {
		return struct{}{}, s.impl.CreateCheckpoint(
			ctx,
			filesystemID,
			checkpointID,
			nodeID,
		)
	})
	return err
}

func (s *sessionWithReEstablish) ListNodes(
	ctx context.Context,
	parentNodeID uint64,
	cookie string,
	maxBytes uint32,
	unsafe bool,
) ([]Node, string, error) {

	type result struct {
		nodes  []Node
		cookie string
	}

	r, err := requestWithReEstablishSession(ctx, s, func() (result, error) {
		nodes, cookie, err := s.impl.ListNodes(
			ctx,
			parentNodeID,
			cookie,
			maxBytes,
			unsafe,
		)
		return result{nodes, cookie}, err
	})
	return r.nodes, r.cookie, err
}

func (s *sessionWithReEstablish) CreateNode(
	ctx context.Context,
	node Node,
) (uint64, error) {

	return requestWithReEstablishSession(ctx, s, func() (uint64, error) {
		return s.impl.CreateNode(ctx, node)
	})
}

func (s *sessionWithReEstablish) CreateNodeIdempotent(
	ctx context.Context,
	node Node,
) (uint64, error) {

	return requestWithReEstablishSession(ctx, s, func() (uint64, error) {
		return s.impl.CreateNodeIdempotent(ctx, node)
	})
}

func (s *sessionWithReEstablish) ReadLink(
	ctx context.Context,
	nodeID uint64,
) ([]byte, error) {

	return requestWithReEstablishSession(ctx, s, func() ([]byte, error) {
		return s.impl.ReadLink(ctx, nodeID)
	})
}

func (s *sessionWithReEstablish) GetNodeAttr(
	ctx context.Context,
	parentNodeID uint64,
	name string,
) (Node, error) {

	return requestWithReEstablishSession(ctx, s, func() (Node, error) {
		return s.impl.GetNodeAttr(ctx, parentNodeID, name)
	})
}

func (s *sessionWithReEstablish) UnlinkNode(
	ctx context.Context,
	parentNodeID uint64,
	name string,
	unlinkDirectory bool,
) error {

	_, err := requestWithReEstablishSession(ctx, s, func() (struct{}, error) {
		return struct{}{}, s.impl.UnlinkNode(
			ctx,
			parentNodeID,
			name,
			unlinkDirectory,
		)
	})
	return err
}

////////////////////////////////////////////////////////////////////////////////

func assertSessionWithMetricsIsSession(arg *sessionWithMetrics) Session {
	return arg
}

func assertSessionWithReEstablishIsSession(arg *sessionWithReEstablish) Session {
	return arg
}
