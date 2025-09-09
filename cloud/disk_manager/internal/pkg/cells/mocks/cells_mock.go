package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
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
	ctx context.Context,
	disk *types.Disk,
	folderID string,
) (nbs.Client, error) {

	args := s.Called(ctx, disk, folderID)
	res, _ := args.Get(0).(nbs.Client)

	return res, args.Error(1)
}

func (s *CellSelectorMock) IsCellOfZone(cellID string, zoneID string) bool {
	args := s.Called(cellID, zoneID)
	return args.Bool(0)
}
