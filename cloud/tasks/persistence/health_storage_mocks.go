package persistence

import (
	"context"
	"time"

	"github.com/stretchr/testify/mock"
)

////////////////////////////////////////////////////////////////////////////////

type StorageMock struct {
	mock.Mock
}

func (s *StorageMock) HeartbeatNode(
	ctx context.Context,
	// host string,
	ts time.Time,
	// inflightTaskCount uint32,
) error {

	args := s.Called(ctx, ts)
	return args.Error(0)
}

////////////////////////////////////////////////////////////////////////////////

func NewStorageMock() *StorageMock {
	return &StorageMock{}
}

////////////////////////////////////////////////////////////////////////////////

// // Ensure that StorageMock implements tasks_storage.Storage.
// func assertStorageMockIsStorage(arg *StorageMock) tasks_storage.Storage {
// 	return arg
// }
