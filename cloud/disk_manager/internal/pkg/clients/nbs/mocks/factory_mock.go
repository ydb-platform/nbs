package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
)

////////////////////////////////////////////////////////////////////////////////

type FactoryMock struct {
	mock.Mock
}

func (f *FactoryMock) GetZones() []string {
	args := f.Called()
	return args.Get(0).([]string)
}

func (f *FactoryMock) GetShards(zoneID string) []string {
	args := f.Called(zoneID)
	return args.Get(0).([]string)
}

func (f *FactoryMock) HasClient(zoneID string) bool {
	args := f.Called(zoneID)
	return args.Bool(0)
}

func (f *FactoryMock) GetClient(
	ctx context.Context,
	zoneID string,
) (nbs.Client, error) {

	args := f.Called(ctx, zoneID)
	res, _ := args.Get(0).(nbs.Client)

	return res, args.Error(1)
}

func (f *FactoryMock) GetClientFromDefaultZone(
	ctx context.Context,
) (nbs.Client, error) {

	args := f.Called(ctx)
	res, _ := args.Get(0).(nbs.Client)

	return res, args.Error(1)
}

func (f *FactoryMock) GetMultiZoneClient(
	srcZoneID string,
	dstZoneID string,
) (nbs.MultiZoneClient, error) {

	args := f.Called(srcZoneID, dstZoneID)
	res, _ := args.Get(0).(nbs.MultiZoneClient)

	return res, args.Error(1)
}

func (f *FactoryMock) ShouldUseShardsForFolder(folderID string) bool {

	args := f.Called(folderID)

	return args.Get(0).(bool)
}

////////////////////////////////////////////////////////////////////////////////

func NewFactoryMock() *FactoryMock {
	return &FactoryMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that FactoryMock implements Factory.
func assertFactoryMockIsFactory(arg *FactoryMock) nbs.Factory {
	return arg
}
