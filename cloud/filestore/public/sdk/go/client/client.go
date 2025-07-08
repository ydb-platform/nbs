package client

import (
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

func (client *Client) ReadNodeRefs(
	ctx context.Context,
	fileSystemID string,
	nodeID uint64,
	cookie string,
	limit uint32,
) (*protos.TReadNodeRefsResponse, error) {

	req := &protos.TReadNodeRefsRequest{
		FileSystemId: fileSystemID,
		NodeId:       nodeID,
		Cookie:       cookie,
		Limit:        limit,
	}
	resp, err := client.Impl.ReadNodeRefs(ctx, req)
	if err != nil {
		return nil, err
	}

	return resp, nil
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
