package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/nodes"
)

////////////////////////////////////////////////////////////////////////////////

type StorageMock struct {
	mock.Mock
}

func (s *StorageMock) SaveNodes(
	ctx context.Context,
	snapshotID string,
	nodeList []nfs.Node,
) error {

	args := s.Called(ctx, snapshotID, nodeList)
	return args.Error(0)
}

func (s *StorageMock) ListNodes(
	ctx context.Context,
	snapshotID string,
	parentNodeID uint64,
	cookie string,
	limit int,
) ([]nfs.Node, string, error) {

	args := s.Called(ctx, snapshotID, parentNodeID, cookie, limit)
	res, _ := args.Get(0).([]nfs.Node)
	return res, args.String(1), args.Error(2)
}

func (s *StorageMock) DeleteSnapshotData(
	ctx context.Context,
	snapshotID string,
) (bool, error) {

	args := s.Called(ctx, snapshotID)
	return args.Bool(0), args.Error(1)
}

func (s *StorageMock) UpdateRestorationNodeIDMapping(
	ctx context.Context,
	srcSnapshotID string,
	dstSnapshotID string,
	srcNodeIds []uint64,
	dstNodeIds []uint64,
) error {

	args := s.Called(ctx, srcSnapshotID, dstSnapshotID, srcNodeIds, dstNodeIds)
	return args.Error(0)
}

func (s *StorageMock) GetDestinationNodeIDs(
	ctx context.Context,
	srcSnapshotID string,
	dstFilesystemID string,
	srcNodeIDs []uint64,
) (map[uint64]uint64, error) {

	args := s.Called(ctx, srcSnapshotID, dstFilesystemID, srcNodeIDs)
	res, _ := args.Get(0).(map[uint64]uint64)
	return res, args.Error(1)
}

func (s *StorageMock) ListHardLinks(
	ctx context.Context,
	snapshotID string,
	limit int,
	offset int,
) ([]nfs.Node, error) {

	args := s.Called(ctx, snapshotID, limit, offset)
	res, _ := args.Get(0).([]nfs.Node)
	return res, args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewStorageMock() *StorageMock {
	return &StorageMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that StorageMock implements nodes.Storage.
func assertStorageMockIsStorage(arg *StorageMock) nodes.Storage {
	return arg
}
