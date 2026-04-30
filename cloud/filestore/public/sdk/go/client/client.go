package client

import (
	protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	coreprotos "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"

	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

type CreateFileStoreOpts struct {
	ProjectID string
	FolderID  string
	CloudID   string

	BlockSize        uint32
	BlocksCount      uint64
	StorageMediaKind coreprotos.EStorageMediaKind
	ShardCount       uint32
}

type AlterFileStoreOpts struct {
	ProjectID string
	FolderID  string
	CloudID   string
}

type CreateCheckpointOpts struct {
	CheckpointID string
	NodeID       uint64
}

type Session struct {
	SessionID    string
	SessionSeqNo uint64
	FileSystemID string
	CheckpointId string
	ClientID     string
}

////////////////////////////////////////////////////////////////////////////////

type NodeType uint32

func (t NodeType) IsDirectory() bool {
	return t == NODE_KIND_DIR
}

func (t NodeType) IsSymlink() bool {
	return t == NODE_KIND_SYMLINK
}

////////////////////////////////////////////////////////////////////////////////

const (
	NODE_KIND_INVALID  NodeType = NodeType(protos.ENodeType_E_INVALID_NODE)
	NODE_KIND_FILE     NodeType = NodeType(protos.ENodeType_E_REGULAR_NODE)
	NODE_KIND_DIR      NodeType = NodeType(protos.ENodeType_E_DIRECTORY_NODE)
	NODE_KIND_LINK     NodeType = NodeType(protos.ENodeType_E_LINK_NODE)
	NODE_KIND_SOCK     NodeType = NodeType(protos.ENodeType_E_SOCK_NODE)
	NODE_KIND_SYMLINK  NodeType = NodeType(protos.ENodeType_E_SYMLINK_NODE)
	NODE_KIND_FIFO     NodeType = NodeType(protos.ENodeType_E_FIFO_NODE)
	NODE_KIND_CHARDEV  NodeType = NodeType(protos.ENodeType_E_CHARDEV_NODE)
	NODE_KIND_BLOCKDEV NodeType = NodeType(protos.ENodeType_E_BLOCKDEV_NODE)
)

////////////////////////////////////////////////////////////////////////////////

type Node struct {
	ParentID          uint64
	NodeID            uint64
	Name              string
	Atime             uint64
	Mtime             uint64
	Ctime             uint64
	Size              uint64
	Mode              uint32
	UID               uint64
	GID               uint64
	Links             uint32
	Type              NodeType
	LinkTarget        string
	ShardFileSystemID string
	ShardNodeName     string
	DevID             uint64
}

////////////////////////////////////////////////////////////////////////////////

func headersForSession(session Session) *protos.THeaders {
	headers := &protos.THeaders{
		SessionSeqNo: session.SessionSeqNo,
		SessionId:    []byte(session.SessionID),
	}

	if session.ClientID != "" {
		headers.ClientId = []byte(session.ClientID)
	}

	return headers
}

////////////////////////////////////////////////////////////////////////////////

type Client struct {
	Impl ClientIface
}

func (client *Client) Close() error {
	return client.Impl.Close()
}

func (client *Client) Ping(ctx context.Context) error {
	req := &protos.TPingRequest{}
	_, err := client.Impl.Ping(ctx, req)
	return err
}

func (client *Client) CreateFileStore(
	ctx context.Context,
	fileSystemID string,
	opts *CreateFileStoreOpts,
) (*protos.TFileStore, error) {

	req := &protos.TCreateFileStoreRequest{
		FileSystemId: fileSystemID,
	}

	if opts != nil {
		req.ProjectId = opts.ProjectID
		req.FolderId = opts.FolderID
		req.CloudId = opts.CloudID

		req.BlockSize = opts.BlockSize
		req.BlocksCount = opts.BlocksCount
		req.StorageMediaKind = opts.StorageMediaKind
		if opts.ShardCount > 0 {
			req.ShardCount = opts.ShardCount
		}
	}

	resp, err := client.Impl.CreateFileStore(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.FileStore, nil
}

func (client *Client) AlterFileStore(
	ctx context.Context,
	fileSystemID string,
	opts *AlterFileStoreOpts,
	configVersion uint32,

) error {

	req := &protos.TAlterFileStoreRequest{
		FileSystemId:  fileSystemID,
		ConfigVersion: configVersion,
	}

	if opts != nil {
		req.ProjectId = opts.ProjectID
		req.FolderId = opts.FolderID
		req.CloudId = opts.CloudID
	}

	_, err := client.Impl.AlterFileStore(ctx, req)
	return err
}

func (client *Client) ResizeFileStore(
	ctx context.Context,
	fileSystemID string,
	blocksCount uint64,
	configVersion uint32,
) error {

	req := &protos.TResizeFileStoreRequest{
		FileSystemId:  fileSystemID,
		BlocksCount:   blocksCount,
		ConfigVersion: configVersion,
	}

	_, err := client.Impl.ResizeFileStore(ctx, req)
	return err
}

func (client *Client) EnableDirectoryCreationInShards(
	ctx context.Context,
	filesystemID string,
	blocksCount uint64,
	configVersion uint32,
	shardCount uint32,
) error {

	req := &protos.TResizeFileStoreRequest{
		FileSystemId:                    filesystemID,
		BlocksCount:                     blocksCount,
		ConfigVersion:                   configVersion,
		EnableDirectoryCreationInShards: true,
		ShardCount:                      shardCount,
	}

	_, err := client.Impl.ResizeFileStore(ctx, req)
	return err
}

func (client *Client) DestroyFileStore(
	ctx context.Context,
	fileSystemID string,
	force bool,
) error {
	req := &protos.TDestroyFileStoreRequest{
		FileSystemId: fileSystemID,
		ForceDestroy: force,
	}

	_, err := client.Impl.DestroyFileStore(ctx, req)
	return err
}

func (client *Client) GetFileStoreInfo(
	ctx context.Context,
	fileSystemID string,
) (*protos.TFileStore, error) {
	req := &protos.TGetFileStoreInfoRequest{
		FileSystemId: fileSystemID,
	}

	resp, err := client.Impl.GetFileStoreInfo(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.FileStore, nil
}

func (client *Client) CreateCheckpoint(
	ctx context.Context,
	session Session,
	fileSystemID string,
	opts *CreateCheckpointOpts,
) error {
	req := &protos.TCreateCheckpointRequest{
		FileSystemId: fileSystemID,
		Headers:      headersForSession(session),
	}

	if opts != nil {
		req.CheckpointId = opts.CheckpointID
		req.NodeId = opts.NodeID
	}

	_, err := client.Impl.CreateCheckpoint(ctx, req)
	return err
}

func (client *Client) DestroyCheckpoint(
	ctx context.Context,
	fileSystemID string,
	checkpointID string,
) error {
	req := &protos.TDestroyCheckpointRequest{
		FileSystemId: fileSystemID,
		CheckpointId: checkpointID,
	}

	_, err := client.Impl.DestroyCheckpoint(ctx, req)
	return err
}

func (client *Client) DescribeFileStoreModel(
	ctx context.Context,
	blocksCount uint64,
	blockSize uint32,
	kind coreprotos.EStorageMediaKind,
) (*protos.TFileStoreModel, error) {

	req := &protos.TDescribeFileStoreModelRequest{
		BlocksCount:      blocksCount,
		BlockSize:        blockSize,
		StorageMediaKind: kind,
	}

	model, err := client.Impl.DescribeFileStoreModel(ctx, req)
	if err != nil {
		return nil, err
	}

	return model.FileStoreModel, nil
}

func (client *Client) CreateSession(
	ctx context.Context,
	fileSystemID string,
	clientID string,
	checkpointId string,
	readonly bool,
) (Session, error) {

	req := &protos.TCreateSessionRequest{
		FileSystemId:         fileSystemID,
		ReadOnly:             readonly,
		CheckpointId:         checkpointId,
		RestoreClientSession: true,
	}
	if clientID != "" {
		req.Headers = &protos.THeaders{
			ClientId: []byte(clientID),
		}
	}
	resp, err := client.Impl.CreateSession(ctx, req)
	if err != nil {
		return Session{}, err
	}

	session := resp.GetSession()
	return Session{
		SessionID:    session.GetSessionId(),
		SessionSeqNo: session.GetSessionSeqNo(),
		FileSystemID: fileSystemID,
		CheckpointId: checkpointId,
		ClientID:     clientID,
	}, nil
}

func (client *Client) DestroySession(
	ctx context.Context,
	session Session,
) error {

	req := &protos.TDestroySessionRequest{
		FileSystemId: session.FileSystemID,
		Headers:      headersForSession(session),
	}
	_, err := client.Impl.DestroySession(ctx, req)
	return err
}

func (client *Client) ListNodes(
	ctx context.Context,
	session Session,
	nodeID uint64,
	cookie string,
	maxBytes uint32,
	unsafe bool,
) ([]Node, string, error) {

	req := &protos.TListNodesRequest{
		FileSystemId: session.FileSystemID,
		NodeId:       nodeID,
		Cookie:       []byte(cookie),
		Headers:      headersForSession(session),
		MaxBytes:     maxBytes,
		Unsafe:       unsafe,
	}
	resp, err := client.Impl.ListNodes(ctx, req)
	if err != nil {
		return nil, "", err
	}

	if len(resp.GetNames()) != len(resp.GetNodes()) {
		return nil, "", errors.NewNonRetriableErrorf(
			"ListNodes: got %d names, but only %d nodes",
			len(resp.GetNames()),
			len(resp.GetNodes()),
		)
	}

	nodes := resp.GetNodes()
	result := make([]Node, len(nodes))
	for idx, name := range resp.GetNames() {
		// On the filestore side, symlinks are represented as links
		// and links are regular files with Links >= 2.
		nodeType := NodeType(nodes[idx].GetType())
		if nodeType == NODE_KIND_LINK {
			nodeType = NODE_KIND_SYMLINK
		}

		result[idx] = Node{
			ParentID:          nodeID,
			NodeID:            nodes[idx].GetId(),
			Name:              string(name),
			Atime:             nodes[idx].GetATime(),
			Mtime:             nodes[idx].GetMTime(),
			Ctime:             nodes[idx].GetCTime(),
			Size:              nodes[idx].GetSize(),
			Mode:              nodes[idx].GetMode(),
			UID:               uint64(nodes[idx].GetUid()),
			GID:               uint64(nodes[idx].GetGid()),
			Links:             nodes[idx].GetLinks(),
			Type:              nodeType,
			ShardFileSystemID: string(nodes[idx].GetShardFileSystemId()),
			ShardNodeName:     string(nodes[idx].GetShardNodeName()),
			DevID:             nodes[idx].GetDevId(),
		}
	}

	return result, string(resp.GetCookie()), nil
}

func (client *Client) CreateNode(
	ctx context.Context,
	session Session,
	node Node,
) (uint64, error) {

	req := &protos.TCreateNodeRequest{
		NodeId:       node.ParentID,
		Name:         []byte(node.Name),
		FileSystemId: session.FileSystemID,
		Uid:          node.UID,
		Gid:          node.GID,
		Headers:      headersForSession(session),
	}

	switch node.Type {
	case NODE_KIND_FILE:
		req.Params = &protos.TCreateNodeRequest_File{
			File: &protos.TCreateNodeRequest_TFile{
				Mode: node.Mode,
			},
		}
	case NODE_KIND_DIR:
		req.Params = &protos.TCreateNodeRequest_Directory{
			Directory: &protos.TCreateNodeRequest_TDirectory{
				Mode: node.Mode,
			},
		}
	case NODE_KIND_SOCK:
		req.Params = &protos.TCreateNodeRequest_Socket{
			Socket: &protos.TCreateNodeRequest_TSocket{
				Mode: node.Mode,
			},
		}
	case NODE_KIND_SYMLINK:
		req.Params = &protos.TCreateNodeRequest_SymLink{
			SymLink: &protos.TCreateNodeRequest_TSymLink{
				TargetPath: []byte(node.LinkTarget),
			},
		}
	case NODE_KIND_LINK:
		req.Params = &protos.TCreateNodeRequest_Link{
			Link: &protos.TCreateNodeRequest_TLink{
				TargetNode: node.NodeID,
			},
		}
	case NODE_KIND_FIFO:
		req.Params = &protos.TCreateNodeRequest_Fifo{
			Fifo: &protos.TCreateNodeRequest_TFifo{
				Mode: node.Mode,
			},
		}
	case NODE_KIND_CHARDEV:
		req.Params = &protos.TCreateNodeRequest_CharDevice{
			CharDevice: &protos.TCreateNodeRequest_TCharDevice{
				Mode:   node.Mode,
				Device: node.DevID,
			},
		}
	case NODE_KIND_BLOCKDEV:
		req.Params = &protos.TCreateNodeRequest_BlockDevice{
			BlockDevice: &protos.TCreateNodeRequest_TBlockDevice{
				Mode:   node.Mode,
				Device: node.DevID,
			},
		}
	case NODE_KIND_INVALID:
		return 0, errors.NewNonRetriableErrorf("CreateNode: invalid node type")
	default:
		return 0, errors.NewNonRetriableErrorf(
			"CreateNode: unknown node type %v",
			node.Type,
		)
	}

	resp, err := client.Impl.CreateNode(ctx, req)
	if err != nil {
		return 0, err
	}

	return resp.GetNode().GetId(), nil
}

func (client *Client) ReadLink(
	ctx context.Context,
	session Session,
	nodeID uint64,
) ([]byte, error) {

	req := &protos.TReadLinkRequest{
		NodeId:       nodeID,
		FileSystemId: session.FileSystemID,
		Headers:      headersForSession(session),
	}

	resp, err := client.Impl.ReadLink(ctx, req)
	if err != nil {
		return []byte{}, err
	}

	return resp.GetSymLink(), nil
}

func (client *Client) GetNodeAttr(
	ctx context.Context,
	session Session,
	parentNodeID uint64,
	name string,
) (Node, error) {

	req := &protos.TGetNodeAttrRequest{
		FileSystemId: session.FileSystemID,
		NodeId:       parentNodeID,
		Name:         []byte(name),
		Headers:      headersForSession(session),
	}

	resp, err := client.Impl.GetNodeAttr(ctx, req)
	if err != nil {
		return Node{}, err
	}

	attr := resp.GetNode()
	return Node{
		ParentID: parentNodeID,
		NodeID:   attr.GetId(),
		Name:     name,
		Atime:    attr.GetATime(),
		Mtime:    attr.GetMTime(),
		Ctime:    attr.GetCTime(),
		Size:     attr.GetSize(),
		Mode:     attr.GetMode(),
		UID:      uint64(attr.GetUid()),
		GID:      uint64(attr.GetGid()),
		Links:    attr.GetLinks(),
		Type:     NodeType(attr.GetType()),
		DevID:    attr.GetDevId(),
	}, nil
}

func (client *Client) UnlinkNode(
	ctx context.Context,
	session Session,
	parentNodeID uint64,
	name string,
	unlinkDirectory bool,
) error {

	req := &protos.TUnlinkNodeRequest{
		FileSystemId:    session.FileSystemID,
		NodeId:          parentNodeID,
		Name:            []byte(name),
		UnlinkDirectory: unlinkDirectory,
		Headers:         headersForSession(session),
	}

	_, err := client.Impl.UnlinkNode(ctx, req)
	return err
}

////////////////////////////////////////////////////////////////////////////////

type StartEndpointOpts struct {
	ClientID   string
	SocketPath string
	ReadOnly   bool

	SessionPingTimeout uint32

	ServiceEndpoint string
}

type EndpointClient struct {
	Impl EndpointClientIface
}

func (client *EndpointClient) Close() error {
	return client.Impl.Close()
}

func (client *EndpointClient) StartEndpoint(
	fileSystemID string,
	opts StartEndpointOpts,
	ctx context.Context,
) error {

	endpoint := &protos.TEndpointConfig{
		FileSystemId: fileSystemID,

		ClientId:   opts.ClientID,
		SocketPath: opts.SocketPath,
		ReadOnly:   opts.ReadOnly,

		SessionPingTimeout: opts.SessionPingTimeout,

		ServiceEndpoint: opts.ServiceEndpoint,
	}

	req := &protos.TStartEndpointRequest{
		Endpoint: endpoint,
	}

	_, err := client.Impl.StartEndpoint(ctx, req)
	return err
}

func (client *EndpointClient) StopEndpoint(
	socketPath string,
	ctx context.Context,
) error {

	req := &protos.TStopEndpointRequest{
		SocketPath: socketPath,
	}

	_, err := client.Impl.StopEndpoint(ctx, req)
	return err
}

func (client *EndpointClient) ListEndpoints(
	ctx context.Context,
) ([]*protos.TEndpointConfig, error) {

	req := &protos.TListEndpointsRequest{}

	resp, err := client.Impl.ListEndpoints(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp.GetEndpoints(), nil
}

func (client *EndpointClient) KickEndpoint(
	keyringID uint32,
	ctx context.Context,
) error {

	req := &protos.TKickEndpointRequest{
		KeyringId: keyringID,
	}

	_, err := client.Impl.KickEndpoint(ctx, req)
	return err
}

func (client *EndpointClient) Ping(
	ctx context.Context,
) error {

	req := &protos.TPingRequest{}

	_, err := client.Impl.Ping(ctx, req)
	return err
}

////////////////////////////////////////////////////////////////////////////////

func NewClient(
	grpcOpts *GrpcClientOpts,
	durableOpts *DurableClientOpts,
	logger Logger,
) (*Client, error) {

	grpcClient, err := NewGrpcClient(grpcOpts, logger)
	if err != nil {
		return nil, err
	}

	durableClient := NewDurableClient(grpcClient, durableOpts, logger)

	return &Client{
		durableClient,
	}, nil
}

func NewEndpointClient(
	grpcOpts *GrpcClientOpts,
	durableOpts *DurableClientOpts,
	logger Logger,
) (*EndpointClient, error) {

	grpcClient, err := NewGrpcEndpointClient(grpcOpts, logger)
	if err != nil {
		return nil, err
	}

	durableClient := NewDurableEndpointClient(grpcClient, durableOpts, logger)

	return &EndpointClient{
		durableClient,
	}, nil
}
