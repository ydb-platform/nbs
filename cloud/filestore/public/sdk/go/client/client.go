package client

import (
	"fmt"

	protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	coreprotos "github.com/ydb-platform/nbs/cloud/storage/core/protos"
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
}

////////////////////////////////////////////////////////////////////////////////

type NodeType uint32

func (t NodeType) IsDirectory() bool {
	return t == NODE_KIND_DIR
}

const (
	NODE_KIND_INVALID NodeType = iota
	NODE_KIND_FILE
	NODE_KIND_DIR
	NODE_KIND_SYMLINK
	NODE_KIND_LINK
	NODE_KIND_SOCK
)

////////////////////////////////////////////////////////////////////////////////

type Node struct {
	ParentID   uint64
	NodeID     uint64
	Name       string
	Atime      uint64
	Mtime      uint64
	Ctime      uint64
	Size       uint64
	Mode       uint32
	UID        uint64
	GID        uint64
	Type       NodeType
	LinkTarget string
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

func (client *Client) DestroyFileStore(
	ctx context.Context,
	fileSystemID string,
) error {
	req := &protos.TDestroyFileStoreRequest{
		FileSystemId: fileSystemID,
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
	fileSystemID string,
	opts *CreateCheckpointOpts,
) error {
	req := &protos.TCreateCheckpointRequest{
		FileSystemId: fileSystemID,
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
	readonly bool,
) (Session, error) {

	req := &protos.TCreateSessionRequest{
		FileSystemId:         fileSystemID,
		ReadOnly:             readonly,
		RestoreClientSession: false,
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
	}, nil
}

func (client *Client) DestroySession(
	ctx context.Context,
	session Session,
) error {

	req := &protos.TDestroySessionRequest{
		FileSystemId: session.FileSystemID,
		Headers: &protos.THeaders{
			SessionSeqNo: session.SessionSeqNo,
			SessionId:    []byte(session.SessionID),
		},
	}
	_, err := client.Impl.DestroySession(ctx, req)
	return err
}

func (client *Client) ListNodes(
	ctx context.Context,
	session Session,
	nodeID uint64,
	cookie string,
) ([]Node, string, error) {

	req := &protos.TListNodesRequest{
		FileSystemId: session.FileSystemID,
		NodeId:       nodeID,
		Cookie:       []byte(cookie),
		Headers: &protos.THeaders{
			SessionSeqNo: session.SessionSeqNo,
			SessionId:    []byte(session.SessionID),
		},
	}

	resp, err := client.Impl.ListNodes(ctx, req)
	if err != nil {
		return nil, "", err
	}

	if len(resp.GetNames()) != len(resp.GetNodes()) {
		return nil, "", fmt.Errorf(
			"ListNodes: got %d names, but only %d nodes",
			len(resp.GetNames()),
			len(resp.GetNodes()),
		)
	}

	nodes := resp.GetNodes()
	result := make([]Node, len(nodes))
	for idx, name := range resp.GetNames() {
		result[idx] = Node{
			ParentID: nodeID,
			NodeID:   nodes[idx].GetId(),
			Name:     string(name),
			Atime:    nodes[idx].GetATime(),
			Mtime:    nodes[idx].GetMTime(),
			Ctime:    nodes[idx].GetCTime(),
			Size:     nodes[idx].GetSize(),
			Mode:     nodes[idx].GetMode(),
			UID:      uint64(nodes[idx].GetUid()),
			GID:      uint64(nodes[idx].GetGid()),
			Type:     NodeType(nodes[idx].GetType()),
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
		Headers: &protos.THeaders{
			SessionSeqNo: session.SessionSeqNo,
			SessionId:    []byte(session.SessionID),
		},
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
		Headers: &protos.THeaders{
			SessionSeqNo: session.SessionSeqNo,
			SessionId:    []byte(session.SessionID),
		},
	}

	resp, err := client.Impl.ReadLink(ctx, req)
	if err != nil {
		return []byte{}, err
	}

	return resp.GetSymLink(), nil
}

////////////////////////////////////////////////////////////////////////////////

type StartEndpointOpts struct {
	ClientID   string
	SocketPath string
	ReadOnly   bool

	SessionRetryTimeout uint32
	SessionPingTimeout  uint32

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

		SessionRetryTimeout: opts.SessionRetryTimeout,
		SessionPingTimeout:  opts.SessionPingTimeout,

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
	log Log,
) (*Client, error) {

	grpcClient, err := NewGrpcClient(grpcOpts, log)
	if err != nil {
		return nil, err
	}

	durableClient := NewDurableClient(grpcClient, durableOpts, log)

	return &Client{
		durableClient,
	}, nil
}

func NewEndpointClient(
	grpcOpts *GrpcClientOpts,
	durableOpts *DurableClientOpts,
	log Log,
) (*EndpointClient, error) {

	grpcClient, err := NewGrpcEndpointClient(grpcOpts, log)
	if err != nil {
		return nil, err
	}

	durableClient := NewDurableEndpointClient(grpcClient, durableOpts, log)

	return &EndpointClient{
		durableClient,
	}, nil
}
