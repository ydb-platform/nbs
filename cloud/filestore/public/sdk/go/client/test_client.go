package client

import (
	protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

type closeHandlerFunc func() error
type pingHandlerFunc func(ctx context.Context, req *protos.TPingRequest) (*protos.TPingResponse, error)
type createFileStoreHandlerFunc func(ctx context.Context, req *protos.TCreateFileStoreRequest) (*protos.TCreateFileStoreResponse, error)
type alterFileStoreHandlerFunc func(ctx context.Context, req *protos.TAlterFileStoreRequest) (*protos.TAlterFileStoreResponse, error)
type resizeFileStoreHandlerFunc func(ctx context.Context, req *protos.TResizeFileStoreRequest) (*protos.TResizeFileStoreResponse, error)
type destroyFileStoreHandlerFunc func(ctx context.Context, req *protos.TDestroyFileStoreRequest) (*protos.TDestroyFileStoreResponse, error)
type getFileStoreInfoHandlerFunc func(ctx context.Context, req *protos.TGetFileStoreInfoRequest) (*protos.TGetFileStoreInfoResponse, error)
type createCheckpointHandlerFunc func(ctx context.Context, req *protos.TCreateCheckpointRequest) (*protos.TCreateCheckpointResponse, error)
type destroyCheckpointHandlerFunc func(ctx context.Context, req *protos.TDestroyCheckpointRequest) (*protos.TDestroyCheckpointResponse, error)
type describeFileStoreModelHandlerFunc func(ctx context.Context, req *protos.TDescribeFileStoreModelRequest) (*protos.TDescribeFileStoreModelResponse, error)

////////////////////////////////////////////////////////////////////////////////

type testClient struct {
	CloseHandlerFunc              closeHandlerFunc
	PingHandler                   pingHandlerFunc
	CreateFileStoreHandler        createFileStoreHandlerFunc
	AlterFileStoreHandler         alterFileStoreHandlerFunc
	ResizeFileStoreHandler        resizeFileStoreHandlerFunc
	DestroyFileStoreHandler       destroyFileStoreHandlerFunc
	GetFileStoreInfoHandler       getFileStoreInfoHandlerFunc
	CreateCheckpointHandler       createCheckpointHandlerFunc
	DestroyCheckpointHandler      destroyCheckpointHandlerFunc
	DescribeFileStoreModelHandler describeFileStoreModelHandlerFunc
}

func (client *testClient) Close() error {
	if client.CloseHandlerFunc != nil {
		return client.CloseHandlerFunc()
	}

	return nil
}

func (client *testClient) Ping(
	ctx context.Context,
	req *protos.TPingRequest,
) (*protos.TPingResponse, error) {

	if client.PingHandler != nil {
		return client.PingHandler(ctx, req)
	}

	return &protos.TPingResponse{}, nil
}

func (client *testClient) CreateFileStore(
	ctx context.Context,
	req *protos.TCreateFileStoreRequest,
) (*protos.TCreateFileStoreResponse, error) {

	if client.CreateFileStoreHandler != nil {
		return client.CreateFileStoreHandler(ctx, req)
	}

	return &protos.TCreateFileStoreResponse{}, nil
}

func (client *testClient) AlterFileStore(
	ctx context.Context,
	req *protos.TAlterFileStoreRequest,
) (*protos.TAlterFileStoreResponse, error) {

	if client.AlterFileStoreHandler != nil {
		return client.AlterFileStoreHandler(ctx, req)
	}

	return &protos.TAlterFileStoreResponse{}, nil
}

func (client *testClient) ResizeFileStore(
	ctx context.Context,
	req *protos.TResizeFileStoreRequest,
) (*protos.TResizeFileStoreResponse, error) {

	if client.ResizeFileStoreHandler != nil {
		return client.ResizeFileStoreHandler(ctx, req)
	}

	return &protos.TResizeFileStoreResponse{}, nil
}

func (client *testClient) DestroyFileStore(
	ctx context.Context,
	req *protos.TDestroyFileStoreRequest,
) (*protos.TDestroyFileStoreResponse, error) {

	if client.DestroyFileStoreHandler != nil {
		return client.DestroyFileStoreHandler(ctx, req)
	}

	return &protos.TDestroyFileStoreResponse{}, nil
}

func (client *testClient) GetFileStoreInfo(
	ctx context.Context,
	req *protos.TGetFileStoreInfoRequest,
) (*protos.TGetFileStoreInfoResponse, error) {

	if client.GetFileStoreInfoHandler != nil {
		return client.GetFileStoreInfoHandler(ctx, req)
	}

	return &protos.TGetFileStoreInfoResponse{}, nil
}

func (client *testClient) CreateCheckpoint(
	ctx context.Context,
	req *protos.TCreateCheckpointRequest,
) (*protos.TCreateCheckpointResponse, error) {

	if client.CreateCheckpointHandler != nil {
		return client.CreateCheckpointHandler(ctx, req)
	}

	return &protos.TCreateCheckpointResponse{}, nil
}

func (client *testClient) DestroyCheckpoint(
	ctx context.Context,
	req *protos.TDestroyCheckpointRequest,
) (*protos.TDestroyCheckpointResponse, error) {

	if client.DestroyCheckpointHandler != nil {
		return client.DestroyCheckpointHandler(ctx, req)
	}

	return &protos.TDestroyCheckpointResponse{}, nil
}

func (client *testClient) DescribeFileStoreModel(
	ctx context.Context,
	req *protos.TDescribeFileStoreModelRequest,
) (*protos.TDescribeFileStoreModelResponse, error) {

	if client.DescribeFileStoreModelHandler != nil {
		return client.DescribeFileStoreModelHandler(ctx, req)
	}

	return &protos.TDescribeFileStoreModelResponse{}, nil
}

func (client *testClient) CreateSession(
	ctx context.Context,
	req *protos.TCreateSessionRequest,
) (*protos.TCreateSessionResponse, error) {

	return &protos.TCreateSessionResponse{}, nil
}

func (client *testClient) DestroySession(
	ctx context.Context,
	req *protos.TDestroySessionRequest,
) (*protos.TDestroySessionResponse, error) {

	return &protos.TDestroySessionResponse{}, nil
}

func (client *testClient) ListNodes(
	ctx context.Context,
	req *protos.TListNodesRequest,
) (*protos.TListNodesResponse, error) {

	return &protos.TListNodesResponse{}, nil
}

func (client *testClient) CreateNode(
	ctx context.Context,
	req *protos.TCreateNodeRequest,
) (*protos.TCreateNodeResponse, error) {

	return &protos.TCreateNodeResponse{}, nil
}

func (client *testClient) ReadLink(
	ctx context.Context,
	req *protos.TReadLinkRequest,
) (*protos.TReadLinkResponse, error) {

	return &protos.TReadLinkResponse{}, nil
}
