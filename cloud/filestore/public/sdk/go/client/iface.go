package client

import (
	protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

type ClientIface interface {
	//
	// Destroy client to free any resources allocated.
	//

	Close() error

	//
	// Service requests.
	//

	Ping(
		ctx context.Context,
		req *protos.TPingRequest,
	) (*protos.TPingResponse, error)

	CreateFileStore(
		ctx context.Context,
		req *protos.TCreateFileStoreRequest,
	) (*protos.TCreateFileStoreResponse, error)

	AlterFileStore(
		ctx context.Context,
		req *protos.TAlterFileStoreRequest,
	) (*protos.TAlterFileStoreResponse, error)

	ResizeFileStore(
		ctx context.Context,
		req *protos.TResizeFileStoreRequest,
	) (*protos.TResizeFileStoreResponse, error)

	DestroyFileStore(
		ctx context.Context,
		req *protos.TDestroyFileStoreRequest,
	) (*protos.TDestroyFileStoreResponse, error)

	GetFileStoreInfo(
		ctx context.Context,
		req *protos.TGetFileStoreInfoRequest,
	) (*protos.TGetFileStoreInfoResponse, error)

	CreateCheckpoint(
		ctx context.Context,
		req *protos.TCreateCheckpointRequest,
	) (*protos.TCreateCheckpointResponse, error)

	DestroyCheckpoint(
		ctx context.Context,
		req *protos.TDestroyCheckpointRequest,
	) (*protos.TDestroyCheckpointResponse, error)

	DescribeFileStoreModel(
		ctx context.Context,
		req *protos.TDescribeFileStoreModelRequest,
	) (*protos.TDescribeFileStoreModelResponse, error)
	CreateSession(
		ctx context.Context,
		req *protos.TCreateSessionRequest,
	) (*protos.TCreateSessionResponse, error)

	DestroySession(
		ctx context.Context,
		req *protos.TDestroySessionRequest,
	) (*protos.TDestroySessionResponse, error)

	ListNodes(
		ctx context.Context,
		req *protos.TListNodesRequest,
	) (*protos.TListNodesResponse, error)

	CreateNode(
		ctx context.Context,
		req *protos.TCreateNodeRequest,
	) (*protos.TCreateNodeResponse, error)

	ReadLink(
		ctx context.Context,
		req *protos.TReadLinkRequest,
	) (*protos.TReadLinkResponse, error)
}

type EndpointClientIface interface {
	//
	// Destroy client to free any resources allocated.
	//

	Close() error

	//
	// Service requests.
	//

	StartEndpoint(
		ctx context.Context,
		req *protos.TStartEndpointRequest,
	) (*protos.TStartEndpointResponse, error)

	StopEndpoint(
		ctx context.Context,
		req *protos.TStopEndpointRequest,
	) (*protos.TStopEndpointResponse, error)

	ListEndpoints(
		ctx context.Context,
		req *protos.TListEndpointsRequest,
	) (*protos.TListEndpointsResponse, error)

	KickEndpoint(
		ctx context.Context,
		req *protos.TKickEndpointRequest,
	) (*protos.TKickEndpointResponse, error)

	Ping(
		ctx context.Context,
		req *protos.TPingRequest,
	) (*protos.TPingResponse, error)
}
