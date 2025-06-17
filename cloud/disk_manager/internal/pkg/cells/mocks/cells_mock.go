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

func (s *CellSelectorMock) SelectCell(
	ctx context.Context,
	req *disk_manager.CreateDiskRequest,
) string {

	args := s.Called(ctx, req)
	return args.String(0)
}

func (s *CellSelectorMock) IsCellOfZone(cellID string, zoneID string) bool {
	args := s.Called(ctx, req)
	return args.Bool(0)
}
