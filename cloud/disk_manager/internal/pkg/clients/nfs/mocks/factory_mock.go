package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
)

////////////////////////////////////////////////////////////////////////////////

type FactoryMock struct {
	mock.Mock
}

func (f *FactoryMock) NewClient(
	ctx context.Context,
	zoneID string,
) (nfs.Client, error) {

	args := f.Called(ctx, zoneID)
	res, _ := args.Get(0).(nfs.Client)

	return res, args.Error(1)
}

func (f *FactoryMock) NewClientFromDefaultZone(
	ctx context.Context,
) (nfs.Client, error) {

	args := f.Called(ctx)
	res, _ := args.Get(0).(nfs.Client)

	return res, args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewFactoryMock() *FactoryMock {
	return &FactoryMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that FactoryMock implements Factory.
func assertFactoryMockIsFactory(arg *FactoryMock) nfs.Factory {
	return arg
}
