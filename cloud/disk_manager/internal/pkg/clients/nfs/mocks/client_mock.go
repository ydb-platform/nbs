package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
)

////////////////////////////////////////////////////////////////////////////////

type ClientMock struct {
	mock.Mock
}

func (c *ClientMock) Close() error {
	args := c.Called()
	return args.Error(0)
}

func (c *ClientMock) ZoneID() string {
	args := c.Called()
	return args.String(0)
}

func (c *ClientMock) Create(
	ctx context.Context,
	filesystemID string,
	params nfs.CreateFilesystemParams,
) error {

	args := c.Called(ctx, filesystemID, params)
	return args.Error(0)
}

func (c *ClientMock) Delete(
	ctx context.Context,
	filesystemID string,
	force bool,
) error {

	args := c.Called(ctx, filesystemID, force)
	return args.Error(0)
}

func (c *ClientMock) Resize(
	ctx context.Context,
	filesystemID string,
	size uint64,
) error {

	args := c.Called(ctx, filesystemID, size)
	return args.Error(0)
}

func (c *ClientMock) EnableDirectoryCreationInShards(
	ctx context.Context,
	filesystemID string,
	shardCount uint32,
) error {
	args := c.Called(ctx, filesystemID, shardCount)
	return args.Error(0)
}

func (c *ClientMock) DescribeModel(
	ctx context.Context,
	blocksCount uint64,
	blockSize uint32,
	kind types.FilesystemKind,
) (nfs.FilesystemModel, error) {

	args := c.Called(ctx, blocksCount, blockSize, kind)
	res, _ := args.Get(0).(nfs.FilesystemModel)

	return res, args.Error(1)
}

func (c *ClientMock) DestroyCheckpoint(
	ctx context.Context,
	filesystemID string,
	checkpointID string,
) error {

	args := c.Called(ctx, filesystemID, checkpointID)
	return args.Error(0)
}

func (c *ClientMock) CreateSession(
	ctx context.Context,
	fileSystemID string,
	checkpointID string,
	readonly bool,
) (nfs.Session, error) {

	args := c.Called(ctx, fileSystemID, checkpointID, readonly)
	res, _ := args.Get(0).(nfs.Session)
	return res, args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewClientMock() *ClientMock {
	return &ClientMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that ClientMock implements Client.
func assertClientMockIsClient(arg *ClientMock) nfs.Client {
	return arg
}
