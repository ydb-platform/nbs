package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nbs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/resources"
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

func (s *CellSelectorMock) ReplaceZoneIdWithCellIdInDiskMeta(
	ctx context.Context,
	storage resources.Storage,
	disk *types.Disk,
) (*types.Disk, error) {

	args := s.Called(ctx, storage, disk)
	return args.Get(0).(*types.Disk), args.Error(1)
}

func (s *CellSelectorMock) SelectCellForDisk(
	ctx context.Context,
	zoneID string,
	folderID string,
	kind types.DiskKind,
	requireExactCellIDMatch bool,
) (nbs.Client, error) {

	args := s.Called(ctx, zoneID, folderID, kind, requireExactCellIDMatch)
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

func (s *CellSelectorMock) SelectCellForFilesystem(
	ctx context.Context,
	zoneID string,
	folderID string,
) (nfs.Client, error) {

	args := s.Called(ctx, zoneID, folderID)
	return args.Get(0).(nfs.Client), args.Error(1)
}

func (s *CellSelectorMock) ZoneContainsCell(zoneID string, cellID string) bool {
	args := s.Called(zoneID, cellID)
	return args.Bool(0)
}
