package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	nfs "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
)

////////////////////////////////////////////////////////////////////////////////

type NfsClientMock struct {
	mock.Mock
}

func (c *NfsClientMock) Close() error {
	args := c.Called()
	return args.Error(0)
}

func (c *NfsClientMock) Ping(
	ctx context.Context,
	req *protos.TPingRequest,
) (*protos.TPingResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TPingResponse), args.Error(1)
}

func (c *NfsClientMock) CreateFileStore(
	ctx context.Context,
	req *protos.TCreateFileStoreRequest,
) (*protos.TCreateFileStoreResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TCreateFileStoreResponse), args.Error(1)
}

func (c *NfsClientMock) AlterFileStore(
	ctx context.Context,
	req *protos.TAlterFileStoreRequest,
) (*protos.TAlterFileStoreResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TAlterFileStoreResponse), args.Error(1)
}

func (c *NfsClientMock) ResizeFileStore(
	ctx context.Context,
	req *protos.TResizeFileStoreRequest,
) (*protos.TResizeFileStoreResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TResizeFileStoreResponse), args.Error(1)
}

func (c *NfsClientMock) DestroyFileStore(
	ctx context.Context,
	req *protos.TDestroyFileStoreRequest,
) (*protos.TDestroyFileStoreResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TDestroyFileStoreResponse), args.Error(1)
}

func (c *NfsClientMock) GetFileStoreInfo(
	ctx context.Context,
	req *protos.TGetFileStoreInfoRequest,
) (*protos.TGetFileStoreInfoResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TGetFileStoreInfoResponse), args.Error(1)
}

func (c *NfsClientMock) CreateCheckpoint(
	ctx context.Context,
	req *protos.TCreateCheckpointRequest,
) (*protos.TCreateCheckpointResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TCreateCheckpointResponse), args.Error(1)
}

func (c *NfsClientMock) DestroyCheckpoint(
	ctx context.Context,
	req *protos.TDestroyCheckpointRequest,
) (*protos.TDestroyCheckpointResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TDestroyCheckpointResponse), args.Error(1)
}

func (c *NfsClientMock) DescribeFileStoreModel(
	ctx context.Context,
	req *protos.TDescribeFileStoreModelRequest,
) (*protos.TDescribeFileStoreModelResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TDescribeFileStoreModelResponse), args.Error(1)
}

func (c *NfsClientMock) CreateSession(
	ctx context.Context,
	req *protos.TCreateSessionRequest,
) (*protos.TCreateSessionResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TCreateSessionResponse), args.Error(1)
}

func (c *NfsClientMock) DestroySession(
	ctx context.Context,
	req *protos.TDestroySessionRequest,
) (*protos.TDestroySessionResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TDestroySessionResponse), args.Error(1)
}

func (c *NfsClientMock) ListNodes(
	ctx context.Context,
	req *protos.TListNodesRequest,
) (*protos.TListNodesResponse, error) {

	args := c.Called(ctx, req)
	res, _ := args.Get(0).(*protos.TListNodesResponse)
	return res, args.Error(1)
}

func (c *NfsClientMock) CreateNode(
	ctx context.Context,
	req *protos.TCreateNodeRequest,
) (*protos.TCreateNodeResponse, error) {

	args := c.Called(ctx, req)
	res, _ := args.Get(0).(*protos.TCreateNodeResponse)
	return res, args.Error(1)
}

func (c *NfsClientMock) ReadLink(
	ctx context.Context,
	req *protos.TReadLinkRequest,
) (*protos.TReadLinkResponse, error) {

	args := c.Called(ctx, req)
	res, _ := args.Get(0).(*protos.TReadLinkResponse)
	return res, args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewNfsClientMock() *NfsClientMock {
	return &NfsClientMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that NfsClientMock implements Client.
func assertNfsClientMockIsEndpointClient(arg *NfsClientMock) nfs.ClientIface {
	return arg
}
