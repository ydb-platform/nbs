package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs2"
)

////////////////////////////////////////////////////////////////////////////////

type ClientMock struct {
	mock.Mock
}

func (c *ClientMock) CreatePartition(
	ctx context.Context,
	params nbs2.CreatePartitionParams,
) (string, error) {

	args := c.Called(ctx, params)
	return args.String(0), args.Error(1)
}

func (c *ClientMock) DeletePartition(ctx context.Context, tabletID string) error {
	args := c.Called(ctx, tabletID)
	return args.Error(0)
}

func (c *ClientMock) ZoneID() string {
	args := c.Called()
	return args.String(0)
}

func NewClientMock() *ClientMock {
	return &ClientMock{}
}

////////////////////////////////////////////////////////////////////////////////

type FactoryMock struct {
	mock.Mock
}

func (f *FactoryMock) GetClient(
	ctx context.Context,
	zoneID string,
) (nbs2.Client, error) {

	args := f.Called(ctx, zoneID)
	res, _ := args.Get(0).(nbs2.Client)
	return res, args.Error(1)
}

func NewFactoryMock() *FactoryMock {
	return &FactoryMock{}
}

func assertClientMockIsClient(arg *ClientMock) nbs2.Client {
	return arg
}

func assertFactoryMockIsFactory(arg *FactoryMock) nbs2.Factory {
	return arg
}
