package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

////////////////////////////////////////////////////////////////////////////////

type ServiceMock struct {
	mock.Mock
}

func NewServiceMock() *ServiceMock {
	return &ServiceMock{}
}

////////////////////////////////////////////////////////////////////////////////

func (s *ServiceMock) PickShard(
	ctx context.Context,
	disk *disk_manager.DiskId,
	folderID string,
) (string, error) {

	args := s.Called(ctx, disk, folderID)
	return args.String(0), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewServiceMock() *ServiceMock {
	return &ServiceMock{}
}
