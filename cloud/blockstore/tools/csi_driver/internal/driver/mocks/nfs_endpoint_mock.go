package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	nfs "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
)

////////////////////////////////////////////////////////////////////////////////

type NfsEndpointClientMock struct {
	mock.Mock
}

func (c *NfsEndpointClientMock) Close() error {
	args := c.Called()
	return args.Error(0)
}

func (c *NfsEndpointClientMock) StartEndpoint(
	ctx context.Context,
	req *protos.TStartEndpointRequest,
) (*protos.TStartEndpointResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TStartEndpointResponse), args.Error(1)
}

func (c *NfsEndpointClientMock) StopEndpoint(
	ctx context.Context,
	req *protos.TStopEndpointRequest,
) (*protos.TStopEndpointResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TStopEndpointResponse), args.Error(1)
}

func (c *NfsEndpointClientMock) ListEndpoints(
	ctx context.Context,
	req *protos.TListEndpointsRequest,
) (*protos.TListEndpointsResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TListEndpointsResponse), args.Error(1)
}

func (c *NfsEndpointClientMock) KickEndpoint(
	ctx context.Context,
	req *protos.TKickEndpointRequest,
) (*protos.TKickEndpointResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TKickEndpointResponse), args.Error(1)
}

func (c *NfsEndpointClientMock) Ping(
	ctx context.Context,
	req *protos.TPingRequest,
) (*protos.TPingResponse, error) {

	args := c.Called(ctx, req)
	return args.Get(0).(*protos.TPingResponse), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewNfsEndpointClientMock() *NfsEndpointClientMock {
	return &NfsEndpointClientMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that NfsEndpointClientMock implements EndpointClient.
func assertNfsEndpointClientMockIsEndpointClient(
	arg *NfsEndpointClientMock) nfs.EndpointClientIface {

	return arg
}
