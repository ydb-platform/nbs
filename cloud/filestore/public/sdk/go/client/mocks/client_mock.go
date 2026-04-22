package mocks

import (
	"github.com/stretchr/testify/mock"
	protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	coreprotos "github.com/ydb-platform/nbs/cloud/storage/core/protos"

	"golang.org/x/net/context"
)

////////////////////////////////////////////////////////////////////////////////

type ClientInterfaceMock struct {
	mock.Mock
}

func (m *ClientInterfaceMock) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *ClientInterfaceMock) Ping(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *ClientInterfaceMock) CreateFileStore(
	ctx context.Context,
	fileSystemID string,
	opts *nfs_client.CreateFileStoreOpts,
) (*protos.TFileStore, error) {

	args := m.Called(ctx, fileSystemID, opts)
	res, _ := args.Get(0).(*protos.TFileStore)
	return res, args.Error(1)
}

func (m *ClientInterfaceMock) AlterFileStore(
	ctx context.Context,
	fileSystemID string,
	opts *nfs_client.AlterFileStoreOpts,
	configVersion uint32,
) error {

	args := m.Called(ctx, fileSystemID, opts, configVersion)
	return args.Error(0)
}

func (m *ClientInterfaceMock) ResizeFileStore(
	ctx context.Context,
	fileSystemID string,
	blocksCount uint64,
	configVersion uint32,
) error {

	args := m.Called(ctx, fileSystemID, blocksCount, configVersion)
	return args.Error(0)
}

func (m *ClientInterfaceMock) EnableDirectoryCreationInShards(
	ctx context.Context,
	filesystemID string,
	blocksCount uint64,
	configVersion uint32,
	shardCount uint32,
) error {

	args := m.Called(ctx, filesystemID, blocksCount, configVersion, shardCount)
	return args.Error(0)
}

func (m *ClientInterfaceMock) DestroyFileStore(
	ctx context.Context,
	fileSystemID string,
	force bool,
) error {

	args := m.Called(ctx, fileSystemID, force)
	return args.Error(0)
}

func (m *ClientInterfaceMock) GetFileStoreInfo(
	ctx context.Context,
	fileSystemID string,
) (*protos.TFileStore, error) {

	args := m.Called(ctx, fileSystemID)
	res, _ := args.Get(0).(*protos.TFileStore)
	return res, args.Error(1)
}

func (m *ClientInterfaceMock) CreateCheckpoint(
	ctx context.Context,
	session nfs_client.Session,
	fileSystemID string,
	opts *nfs_client.CreateCheckpointOpts,
) error {

	args := m.Called(ctx, session, fileSystemID, opts)
	return args.Error(0)
}

func (m *ClientInterfaceMock) DestroyCheckpoint(
	ctx context.Context,
	fileSystemID string,
	checkpointID string,
) error {

	args := m.Called(ctx, fileSystemID, checkpointID)
	return args.Error(0)
}

func (m *ClientInterfaceMock) DescribeFileStoreModel(
	ctx context.Context,
	blocksCount uint64,
	blockSize uint32,
	kind coreprotos.EStorageMediaKind,
) (*protos.TFileStoreModel, error) {

	args := m.Called(ctx, blocksCount, blockSize, kind)
	res, _ := args.Get(0).(*protos.TFileStoreModel)
	return res, args.Error(1)
}

func (m *ClientInterfaceMock) CreateSession(
	ctx context.Context,
	fileSystemID string,
	clientID string,
	checkpointId string,
	readonly bool,
) (nfs_client.Session, error) {

	args := m.Called(ctx, fileSystemID, clientID, checkpointId, readonly)
	res, _ := args.Get(0).(nfs_client.Session)
	return res, args.Error(1)
}

func (m *ClientInterfaceMock) DestroySession(
	ctx context.Context,
	session nfs_client.Session,
) error {

	args := m.Called(ctx, session)
	return args.Error(0)
}

func (m *ClientInterfaceMock) ListNodes(
	ctx context.Context,
	session nfs_client.Session,
	nodeID uint64,
	cookie string,
	maxBytes uint32,
	unsafe bool,
) ([]nfs_client.Node, string, error) {

	args := m.Called(ctx, session, nodeID, cookie, maxBytes, unsafe)
	res, _ := args.Get(0).([]nfs_client.Node)
	return res, args.String(1), args.Error(2)
}

func (m *ClientInterfaceMock) CreateNode(
	ctx context.Context,
	session nfs_client.Session,
	node nfs_client.Node,
) (uint64, error) {

	args := m.Called(ctx, session, node)
	return args.Get(0).(uint64), args.Error(1)
}

func (m *ClientInterfaceMock) ReadLink(
	ctx context.Context,
	session nfs_client.Session,
	nodeID uint64,
) ([]byte, error) {

	args := m.Called(ctx, session, nodeID)
	res, _ := args.Get(0).([]byte)
	return res, args.Error(1)
}

func (m *ClientInterfaceMock) GetNodeAttr(
	ctx context.Context,
	session nfs_client.Session,
	parentNodeID uint64,
	name string,
) (nfs_client.Node, error) {

	args := m.Called(ctx, session, parentNodeID, name)
	res, _ := args.Get(0).(nfs_client.Node)
	return res, args.Error(1)
}

func (m *ClientInterfaceMock) UnlinkNode(
	ctx context.Context,
	session nfs_client.Session,
	parentNodeID uint64,
	name string,
	unlinkDirectory bool,
) error {

	args := m.Called(ctx, session, parentNodeID, name, unlinkDirectory)
	return args.Error(0)
}

////////////////////////////////////////////////////////////////////////////////

func NewClientInterfaceMock() *ClientInterfaceMock {
	return &ClientInterfaceMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that ClientInterfaceMock implements ClientInterface.
func assertClientInterfaceMockIsClientInterface(
	m *ClientInterfaceMock,
) nfs_client.ClientInterface {

	return m
}
