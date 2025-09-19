package mocks

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/cells/storage"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

type StorageMock struct {
	mock.Mock
}

func (s *StorageMock) UpdateClusterCapacities(
	ctx context.Context,
	capacities []storage.ClusterCapacity,
	deleteBefore time.Time,
) error {

	args := s.Called(ctx, capacities, deleteBefore)
	return args.Error(0)
}

func (s *StorageMock) GetRecentClusterCapacities(
	ctx context.Context,
	zoneID string,
	kind types.DiskKind,
) ([]storage.ClusterCapacity, error) {

	args := s.Called(ctx, zoneID, kind)
	return args.Get(0).([]storage.ClusterCapacity), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewStorageMock() *StorageMock {
	return &StorageMock{}
}
