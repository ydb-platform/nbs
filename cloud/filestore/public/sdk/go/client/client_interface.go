package client

import (
	protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	coreprotos "github.com/ydb-platform/nbs/cloud/storage/core/protos"

	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

type ClientInterface interface {
	Close() error
	Ping(ctx context.Context) error

	CreateFileStore(
		ctx context.Context,
		fileSystemID string,
		opts *CreateFileStoreOpts,
	) (*protos.TFileStore, error)

	AlterFileStore(
		ctx context.Context,
		fileSystemID string,
		opts *AlterFileStoreOpts,
		configVersion uint32,
	) error

	ResizeFileStore(
		ctx context.Context,
		fileSystemID string,
		blocksCount uint64,
		configVersion uint32,
	) error

	EnableDirectoryCreationInShards(
		ctx context.Context,
		filesystemID string,
		blocksCount uint64,
		configVersion uint32,
		shardCount uint32,
	) error

	DestroyFileStore(
		ctx context.Context,
		fileSystemID string,
		force bool,
	) error

	GetFileStoreInfo(
		ctx context.Context,
		fileSystemID string,
	) (*protos.TFileStore, error)

	CreateCheckpoint(
		ctx context.Context,
		session Session,
		fileSystemID string,
		opts *CreateCheckpointOpts,
	) error

	DestroyCheckpoint(
		ctx context.Context,
		fileSystemID string,
		checkpointID string,
	) error

	DescribeFileStoreModel(
		ctx context.Context,
		blocksCount uint64,
		blockSize uint32,
		kind coreprotos.EStorageMediaKind,
	) (*protos.TFileStoreModel, error)

	CreateSession(
		ctx context.Context,
		fileSystemID string,
		checkpointId string,
		readonly bool,
	) (Session, error)

	DestroySession(
		ctx context.Context,
		session Session,
	) error

	ListNodes(
		ctx context.Context,
		session Session,
		nodeID uint64,
		cookie string,
		maxBytes uint32,
		unsafe bool,
	) ([]Node, string, error)

	CreateNode(
		ctx context.Context,
		session Session,
		node Node,
	) (uint64, error)

	ReadLink(
		ctx context.Context,
		session Session,
		nodeID uint64,
	) ([]byte, error)

	GetNodeAttr(
		ctx context.Context,
		session Session,
		parentNodeID uint64,
		name string,
	) (Node, error)

	UnlinkNode(
		ctx context.Context,
		session Session,
		parentNodeID uint64,
		name string,
		unlinkDirectory bool,
	) error
}

////////////////////////////////////////////////////////////////////////////////

type EndpointClientInterface interface {
	Close() error

	StartEndpoint(
		fileSystemID string,
		opts StartEndpointOpts,
		ctx context.Context,
	) error

	StopEndpoint(
		socketPath string,
		ctx context.Context,
	) error

	ListEndpoints(
		ctx context.Context,
	) ([]*protos.TEndpointConfig, error)

	KickEndpoint(
		keyringID uint32,
		ctx context.Context,
	) error

	Ping(ctx context.Context) error
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that *Client implements ClientInterface.
func assertClientIsClientInterface(c *Client) ClientInterface {
	return c
}

// Ensure that *EndpointClient implements EndpointClientInterface.
func assertEndpointClientIsEndpointClientInterface(
	c *EndpointClient,
) EndpointClientInterface {

	return c
}
