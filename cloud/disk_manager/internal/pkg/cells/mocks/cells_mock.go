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
	zoneID string,
	folderID string,
	kind types.DiskKind,
	requireExactCellIdMatch bool,
) (nbs.Client, error) {

	args := s.Called(ctx, zoneID, folderID, kind, requireExactCellIdMatch)
	return args.Get(0).(nbs.Client), args.Error(1)
}

func (s *CellSelectorMock) SelectCellForLocalDisk(
	ctx context.Context,
	zoneID string,
	agentIDs []string,
) (nbs.Client, error) {

	args := s.Called(ctx, zoneID, agentIDs)
	return args.Get(0).(nbs.Client), args.Error(1)
}

func (s *CellSelectorMock) ZoneContainsCell(zoneID string, cellID string) bool {
	args := s.Called(zoneID, cellID)
	return args.Bool(0)
}
