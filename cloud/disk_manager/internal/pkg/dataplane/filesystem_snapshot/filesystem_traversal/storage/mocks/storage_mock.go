package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/filesystem_traversal/storage"
)

////////////////////////////////////////////////////////////////////////////////

type StorageMock struct {
	mock.Mock
}

func (s *StorageMock) ScheduleRootNodeForListing(
	ctx context.Context,
	snapshotID string,
) error {

	args := s.Called(ctx, snapshotID)
	return args.Error(0)
}

func (s *StorageMock) SelectNodesToList(
	ctx context.Context,
	snapshotID string,
	nodesToExclude map[uint64]struct{},
	limit uint64,
) ([]storage.NodeQueueEntry, error) {

	args := s.Called(ctx, snapshotID, nodesToExclude, limit)
	return args.Get(0).([]storage.NodeQueueEntry), args.Error(1)
}

func (s *StorageMock) ScheduleChildNodesForListing(
	ctx context.Context,
	snapshotID string,
	parentNodeID uint64,
	nextCookie string,
	depth uint64,
	children []nfs.Node,
) error {

	args := s.Called(ctx, snapshotID, parentNodeID, nextCookie, depth, children)
	return args.Error(0)
}

////////////////////////////////////////////////////////////////////////////////

func NewStorageMock() *StorageMock {
	return &StorageMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that StorageMock implements storage.Storage.
func assertStorageMockIsStorage(arg *StorageMock) storage.Storage {
	return arg
}
