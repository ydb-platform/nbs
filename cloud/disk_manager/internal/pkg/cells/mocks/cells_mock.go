package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
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
	zoneID string,
	folderID string,
) (nbs.Client, error) {

	args := s.Called(ctx, zoneID, folderID)
	return args.Get(0).(nbs.Client), args.Error(1)
}

func (s *CellSelectorMock) IsCellOfZone(cellID string, zoneID string) bool {
	args := s.Called(cellID, zoneID)
	return args.Bool(0)
}
