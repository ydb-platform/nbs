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
) error {

	args := c.Called(ctx, filesystemID)
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

func (c *ClientMock) CreateSession(
	ctx context.Context,
	fileSystemID string,
	readonly bool,
) (nfs.Session, error) {

	args := c.Called(ctx, fileSystemID, readonly)
	res, _ := args.Get(0).(nfs.Session)
	return res, args.Error(1)
}

func (c *ClientMock) DestroySession(
	ctx context.Context,
	session nfs.Session,
) error {

	args := c.Called(ctx, session)
	return args.Error(0)
}

func (c *ClientMock) ListNodes(
	ctx context.Context,
	session nfs.Session,
	parentId uint64,
	cookie string,
) ([]nfs.Node, string, error) {

	args := c.Called(ctx, session, parentId, cookie)
	res, _ := args.Get(0).([]nfs.Node)
	return res, args.Get(1).(string), args.Error(2)
}

func (c *ClientMock) CreateNode(
	ctx context.Context,
	session nfs.Session,
	node nfs.Node,
) (uint64, error) {

	args := c.Called(ctx, session, node)
	return args.Get(0).(uint64), args.Error(1)
}

func (c *ClientMock) ReadLink(
	ctx context.Context,
	session nfs.Session,
	nodeID uint64,
) ([]byte, error) {

	args := c.Called(ctx, session, nodeID)
	return args.Get(0).([]byte), args.Error(1)
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
