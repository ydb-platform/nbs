package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs2"
)

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

////////////////////////////////////////////////////////////////////////////////

func NewFactoryMock() *FactoryMock {
	return &FactoryMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that FactoryMock implements Factory.
func assertFactoryMockIsFactory(arg *FactoryMock) nbs2.Factory {
	return arg
}
