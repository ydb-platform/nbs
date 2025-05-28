package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	disk_manager "github.com/ydb-platform/nbs/cloud/disk_manager/api"
)

type ServiceMock struct {
	mock.Mock
}

func (s *ServiceMock) SelectShard(
	ctx context.Context,
	disk *disk_manager.DiskId,
) string {

	args := s.Called(ctx, disk, folderID)
	return args.String(0), args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewServiceMock() *ServiceMock {
	return &ServiceMock{}
}
