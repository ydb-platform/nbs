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

func (c *ClientMock) Create(
	ctx context.Context,
	params nbs2.CreateDiskParams,
) error {

	args := c.Called(ctx, params)
	return args.Error(0)
}

func (c *ClientMock) Delete(ctx context.Context, diskID string) error {
	args := c.Called(ctx, diskID)
	return args.Error(0)
}

////////////////////////////////////////////////////////////////////////////////

func NewClientMock() *ClientMock {
	return &ClientMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that ClientMock implements Client.
func assertClientMockIsClient(arg *ClientMock) nbs2.Client {
	return arg
}
