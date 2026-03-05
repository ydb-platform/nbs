package mocks

import (
	"context"

	"github.com/stretchr/testify/mock"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/listers"
)

////////////////////////////////////////////////////////////////////////////////

type FilesystemListerMock struct {
	mock.Mock
}

func (m *FilesystemListerMock) ListNodes(
	ctx context.Context,
	nodeID uint64,
	cookie string,
) ([]nfs.Node, string, error) {

	args := m.Called(ctx, nodeID, cookie)
	res, _ := args.Get(0).([]nfs.Node)
	return res, args.String(1), args.Error(2)
}

func (m *FilesystemListerMock) Close(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

////////////////////////////////////////////////////////////////////////////////

func NewFilesystemListerMock() *FilesystemListerMock {
	return &FilesystemListerMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that FilesystemListerMock implements listers.FilesystemLister.
func assertFilesystemListerMockIsFilesystemLister(
	arg *FilesystemListerMock,
) listers.FilesystemLister {

	return arg
}

////////////////////////////////////////////////////////////////////////////////

type FilesystemOpenerMock struct {
	mock.Mock
}

func (m *FilesystemOpenerMock) OpenFilesystem(
	ctx context.Context,
	filesystemID string,
	checkpointID string,
) (listers.FilesystemLister, error) {

	args := m.Called(ctx, filesystemID, checkpointID)
	res, _ := args.Get(0).(listers.FilesystemLister)
	return res, args.Error(1)
}

////////////////////////////////////////////////////////////////////////////////

func NewFilesystemOpenerMock() *FilesystemOpenerMock {
	return &FilesystemOpenerMock{}
}

////////////////////////////////////////////////////////////////////////////////

// Ensure that FilesystemOpenerMock implements listers.FilesystemOpener.
func assertFilesystemOpenerMockIsFilesystemOpener(
	arg *FilesystemOpenerMock,
) listers.FilesystemOpener {

	return arg
}
