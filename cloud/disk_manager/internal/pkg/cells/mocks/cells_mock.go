package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type CellSelectorMock struct {
	mock.Mock
}

func NewCellSelectorMock() *CellSelectorMock {
	return &CellSelectorMock{}
}

////////////////////////////////////////////////////////////////////////////////

func (s *CellSelectorMock) PrepareZoneID(
	ctx context.Context,
	req *disk_manager.CreateDiskRequest,
) (string, error) {

	args := s.Called(ctx, req)
	return args.String(0), args.Error(1)
}

func (s *CellSelectorMock) GetZoneIDForExistingDisk(
	ctx context.Context,
	diskID *disk_manager.DiskId,
) (string, error) {

	args := s.Called(ctx, diskID)
	return args.String(0), args.Error(1)
}
