package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
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
	diskID *types.Disk,
	folderID string,
) string {

	args := s.Called(diskID, folderID)
	return args.String(0)
}

func (s *CellSelectorMock) SelectCellForLocalDisk(
	ctx context.Context,
	diskID *types.Disk,
	folderID string,
	agentID string,
) (string, error) {

	args := s.Called(ctx, diskID, folderID, agentID)
	return args.String(0), args.Error(1)
}

func (s *CellSelectorMock) IsCellOfZone(cellID string, zoneID string) bool {
	args := s.Called(cellID, zoneID)
	return args.Bool(0)
}
