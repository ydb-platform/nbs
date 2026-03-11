package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	client_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/metrics"
	client_metrics_mocks "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/metrics/mocks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	protos "github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	nfs_client_mocks "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client/mocks"
	coreprotos "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
	metrics_mocks "github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
)

////////////////////////////////////////////////////////////////////////////////

func setupRequestMocks(
	registryMock *metrics_mocks.RegistryMock,
	requestName string,
	expectSuccess bool,
) {

	tags := map[string]string{"request": requestName}

	errorsCounter := registryMock.GetCounter(
		"errors",
		tags,
	)
	countCounter := registryMock.GetCounter(
		"count",
		tags,
	)
	timeTimer := registryMock.GetDurationHistogram(
		"time",
		tags,
		nil,
	)

	if expectSuccess {
		countCounter.On("Inc").Once()
		timeTimer.On("RecordDuration", mock.Anything).Once()
	} else {
		errorsCounter.On("Inc").Once()
	}
}

func newTestClient(
	nfsMock nfs_client.ClientInterface,
	registryMock *metrics_mocks.RegistryMock,
) nfs.Client {

	return nfs.NewClient(
		nfsMock,
		client_metrics.NewClientMetrics(registryMock),
		registryMock,
		"test-zone",
	)
}

func newTestSession(
	nfsMock nfs_client.ClientInterface,
	registryMock *metrics_mocks.RegistryMock,
	nfsSession nfs_client.Session,
) nfs.Session {

	return nfs.NewSession(
		nfsMock,
		nfsSession,
		client_metrics.NewClientMetrics(registryMock),
	)
}

////////////////////////////////////////////////////////////////////////////////

func newTestClientWithMetricsMock(
	nfsMock nfs_client.ClientInterface,
	metricsMock *client_metrics_mocks.MetricsMock,
) nfs.Client {

	return nfs.NewClient(
		nfsMock,
		metricsMock,
		nil,
		"test-zone",
	)
}

func newTestSessionWithMetricsMock(
	nfsMock nfs_client.ClientInterface,
	metricsMock *client_metrics_mocks.MetricsMock,
	nfsSession nfs_client.Session,
) nfs.Session {

	return nfs.NewSession(
		nfsMock,
		nfsSession,
		metricsMock,
	)
}

////////////////////////////////////////////////////////////////////////////////

var testError = assert.AnError

var testNfsClientError = &nfs_client.ClientError{
	Code:    nfs_client.E_REJECTED,
	Message: "test nfs client error",
}

var testNfsClientNonRetriableError = &nfs_client.ClientError{
	Code:    nfs_client.E_ARGUMENT,
	Message: "test nfs client non-retriable error",
}

var retriableError = task_errors.NewRetriableError(testNfsClientError)

////////////////////////////////////////////////////////////////////////////////

func TestClientCreateFileStoreSuccess(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "CreateFileStore", true)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("CreateFileStore", mock.Anything, "fs-1", mock.Anything).
		Return(&protos.TFileStore{}, nil)

	c := newTestClient(nfsMock, registryMock)

	err := c.Create(ctx, "fs-1", nfs.CreateFilesystemParams{
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
		BlockSize:   4096,
		BlocksCount: 100,
	})
	require.NoError(t, err)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestClientCreateFileStoreError(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "CreateFileStore", false)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("CreateFileStore", mock.Anything, "fs-1", mock.Anything).
		Return(nil, testError)

	c := newTestClient(nfsMock, registryMock)

	err := c.Create(ctx, "fs-1", nfs.CreateFilesystemParams{
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
		BlockSize:   4096,
		BlocksCount: 100,
	})
	require.Error(t, err)
	require.ErrorIs(t, err, testError)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestClientDestroyFileStoreSuccess(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "DestroyFileStore", true)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("DestroyFileStore", mock.Anything, "fs-1", false).
		Return(nil)

	c := newTestClient(nfsMock, registryMock)

	err := c.Delete(ctx, "fs-1", false)
	require.NoError(t, err)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestClientDestroyFileStoreError(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "DestroyFileStore", false)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("DestroyFileStore", mock.Anything, "fs-1", false).
		Return(testError)

	c := newTestClient(nfsMock, registryMock)

	err := c.Delete(ctx, "fs-1", false)
	require.Error(t, err)
	require.ErrorIs(t, err, testError)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestClientResizeFileStoreSuccess(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "ResizeFileStore", true)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("GetFileStoreInfo", mock.Anything, "fs-1").
		Return(&protos.TFileStore{
			BlockSize:     4096,
			ConfigVersion: 1,
		}, nil)
	nfsMock.On("ResizeFileStore", mock.Anything, "fs-1", uint64(2), uint32(1)).
		Return(nil)

	c := newTestClient(nfsMock, registryMock)

	err := c.Resize(ctx, "fs-1", 8192)
	require.NoError(t, err)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestClientResizeFileStoreGetFileStoreInfoError(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "ResizeFileStore", false)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("GetFileStoreInfo", mock.Anything, "fs-1").
		Return(nil, testError)

	c := newTestClient(nfsMock, registryMock)

	err := c.Resize(ctx, "fs-1", 8192)
	require.Error(t, err)
	require.ErrorIs(t, err, testError)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestClientDescribeFileStoreModelSuccess(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "DescribeFileStoreModel", true)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On(
		"DescribeFileStoreModel",
		mock.Anything,
		uint64(100),
		uint32(4096),
		coreprotos.EStorageMediaKind_STORAGE_MEDIA_SSD,
	).Return(&protos.TFileStoreModel{
		BlockSize:   4096,
		BlocksCount: 100,
		PerformanceProfile: &protos.TFileStorePerformanceProfile{
			MaxReadBandwidth: 100,
			MaxReadIops:      200,
		},
	}, nil)

	c := newTestClient(nfsMock, registryMock)

	model, err := c.DescribeModel(
		ctx,
		100,
		4096,
		types.FilesystemKind_FILESYSTEM_KIND_SSD,
	)
	require.NoError(t, err)
	require.Equal(t, uint32(4096), model.BlockSize)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestClientDescribeFileStoreModelError(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "DescribeFileStoreModel", false)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On(
		"DescribeFileStoreModel",
		mock.Anything,
		uint64(100),
		uint32(4096),
		coreprotos.EStorageMediaKind_STORAGE_MEDIA_SSD,
	).Return(nil, testError)

	c := newTestClient(nfsMock, registryMock)

	_, err := c.DescribeModel(
		ctx,
		100,
		4096,
		types.FilesystemKind_FILESYSTEM_KIND_SSD,
	)
	require.Error(t, err)
	require.ErrorIs(t, err, testError)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestClientDestroyCheckpointSuccess(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "DestroyCheckpoint", true)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("DestroyCheckpoint", mock.Anything, "fs-1", "cp-1").
		Return(nil)

	c := newTestClient(nfsMock, registryMock)

	err := c.DestroyCheckpoint(ctx, "fs-1", "cp-1")
	require.NoError(t, err)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestClientDestroyCheckpointError(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "DestroyCheckpoint", false)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("DestroyCheckpoint", mock.Anything, "fs-1", "cp-1").
		Return(testError)

	c := newTestClient(nfsMock, registryMock)

	err := c.DestroyCheckpoint(ctx, "fs-1", "cp-1")
	require.Error(t, err)
	require.ErrorIs(t, err, testError)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestClientCreateSessionSuccess(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "CreateSession", true)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("CreateSession", mock.Anything, "fs-1", "cp-1", true).
		Return(nfs_client.Session{
			SessionID:    "session-1",
			SessionSeqNo: 1,
			FileSystemID: "fs-1",
			CheckpointId: "cp-1",
		}, nil)

	c := newTestClient(nfsMock, registryMock)

	s, err := c.CreateSession(ctx, "fs-1", "cp-1", true)
	require.NoError(t, err)
	require.NotNil(t, s)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestClientCreateSessionError(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "CreateSession", false)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("CreateSession", mock.Anything, "fs-1", "cp-1", true).
		Return(nfs_client.Session{}, testError)

	c := newTestClient(nfsMock, registryMock)

	_, err := c.CreateSession(ctx, "fs-1", "cp-1", true)
	require.Error(t, err)
	require.ErrorIs(t, err, testError)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

////////////////////////////////////////////////////////////////////////////////

var testSession = nfs_client.Session{
	SessionID:    "session-1",
	SessionSeqNo: 1,
	FileSystemID: "fs-1",
	CheckpointId: "cp-1",
}

func TestSessionCreateCheckpointSuccess(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "CreateCheckpoint", true)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On(
		"CreateCheckpoint",
		mock.Anything,
		testSession,
		"fs-1",
		mock.Anything,
	).Return(nil)

	s := newTestSession(nfsMock, registryMock, testSession)

	err := s.CreateCheckpoint(ctx, "fs-1", "cp-1", 42)
	require.NoError(t, err)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestSessionCreateCheckpointError(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "CreateCheckpoint", false)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On(
		"CreateCheckpoint",
		mock.Anything,
		testSession,
		"fs-1",
		mock.Anything,
	).Return(testError)

	s := newTestSession(nfsMock, registryMock, testSession)

	err := s.CreateCheckpoint(ctx, "fs-1", "cp-1", 42)
	require.Error(t, err)
	require.ErrorIs(t, err, testError)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestSessionDestroySessionSuccess(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "DestroySession", true)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("DestroySession", mock.Anything, testSession).
		Return(nil)

	s := newTestSession(nfsMock, registryMock, testSession)

	err := s.Close(ctx)
	require.NoError(t, err)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestSessionDestroySessionError(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "DestroySession", false)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("DestroySession", mock.Anything, testSession).
		Return(testError)

	s := newTestSession(nfsMock, registryMock, testSession)

	err := s.Close(ctx)
	require.Error(t, err)
	require.ErrorIs(t, err, testError)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestSessionListNodesSuccess(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "ListNodes", true)

	expectedNodes := []nfs_client.Node{
		{NodeID: 1, Name: "a"},
		{NodeID: 2, Name: "b"},
	}

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On(
		"ListNodes",
		mock.Anything,
		testSession,
		uint64(0),
		"",
		uint32(1024),
		false,
	).Return(expectedNodes, "next", nil)

	s := newTestSession(nfsMock, registryMock, testSession)

	nodes, cookie, err := s.ListNodes(ctx, 0, "", 1024, false)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	require.Equal(t, "next", cookie)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestSessionListNodesError(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "ListNodes", false)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On(
		"ListNodes",
		mock.Anything,
		testSession,
		uint64(0),
		"",
		uint32(1024),
		false,
	).Return(nil, "", testError)

	s := newTestSession(nfsMock, registryMock, testSession)

	_, _, err := s.ListNodes(ctx, 0, "", 1024, false)
	require.Error(t, err)
	require.ErrorIs(t, err, testError)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestSessionCreateNodeSuccess(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "CreateNode", true)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("CreateNode", mock.Anything, testSession, mock.Anything).
		Return(uint64(42), nil)

	s := newTestSession(nfsMock, registryMock, testSession)

	nodeID, err := s.CreateNode(ctx, nfs.Node{Name: "test"})
	require.NoError(t, err)
	require.Equal(t, uint64(42), nodeID)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestSessionCreateNodeError(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "CreateNode", false)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("CreateNode", mock.Anything, testSession, mock.Anything).
		Return(uint64(0), testError)

	s := newTestSession(nfsMock, registryMock, testSession)

	_, err := s.CreateNode(ctx, nfs.Node{Name: "test"})
	require.Error(t, err)
	require.ErrorIs(t, err, testError)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestSessionReadLinkSuccess(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "ReadLink", true)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("ReadLink", mock.Anything, testSession, uint64(42)).
		Return([]byte("/target"), nil)

	s := newTestSession(nfsMock, registryMock, testSession)

	data, err := s.ReadLink(ctx, 42)
	require.NoError(t, err)
	require.Equal(t, []byte("/target"), data)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

func TestSessionReadLinkError(t *testing.T) {
	ctx := context.Background()
	registryMock := metrics_mocks.NewRegistryMock()
	setupRequestMocks(registryMock, "ReadLink", false)

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("ReadLink", mock.Anything, testSession, uint64(42)).
		Return(nil, testError)

	s := newTestSession(nfsMock, registryMock, testSession)

	_, err := s.ReadLink(ctx, 42)
	require.Error(t, err)
	require.ErrorIs(t, err, testError)

	nfsMock.AssertExpectations(t)
	registryMock.AssertAllExpectations(t)
}

////////////////////////////////////////////////////////////////////////////////

func TestClientCreateFileStoreWrappedError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "CreateFileStore").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.ErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("CreateFileStore", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, testNfsClientError)

	c := newTestClientWithMetricsMock(nfsMock, metricsMock)

	err := c.Create(ctx, "fs-1", nfs.CreateFilesystemParams{
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
		BlockSize:   4096,
		BlocksCount: 100,
	})
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.ErrorIs(t, err, retriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestClientDestroyFileStoreWrappedError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "DestroyFileStore").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.ErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("DestroyFileStore", mock.Anything, mock.Anything, mock.Anything).
		Return(testNfsClientError)

	c := newTestClientWithMetricsMock(nfsMock, metricsMock)

	err := c.Delete(ctx, "fs-1", false)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.ErrorIs(t, err, retriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestClientResizeFileStoreWrappedError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "ResizeFileStore").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.ErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("GetFileStoreInfo", mock.Anything, mock.Anything).
		Return(nil, testNfsClientError)

	c := newTestClientWithMetricsMock(nfsMock, metricsMock)

	err := c.Resize(ctx, "fs-1", 8192)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.ErrorIs(t, err, retriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestClientDescribeFileStoreModelWrappedError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "DescribeFileStoreModel").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.ErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On(
		"DescribeFileStoreModel",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(nil, testNfsClientError)

	c := newTestClientWithMetricsMock(nfsMock, metricsMock)

	_, err := c.DescribeModel(
		ctx,
		100,
		4096,
		types.FilesystemKind_FILESYSTEM_KIND_SSD,
	)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.ErrorIs(t, err, retriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestClientDestroyCheckpointWrappedError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "DestroyCheckpoint").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.ErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("DestroyCheckpoint", mock.Anything, mock.Anything, mock.Anything).
		Return(testNfsClientError)

	c := newTestClientWithMetricsMock(nfsMock, metricsMock)

	err := c.DestroyCheckpoint(ctx, "fs-1", "cp-1")
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.ErrorIs(t, err, retriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestClientCreateSessionWrappedError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "CreateSession").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.ErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("CreateSession", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nfs_client.Session{}, testNfsClientError)

	c := newTestClientWithMetricsMock(nfsMock, metricsMock)

	_, err := c.CreateSession(ctx, "fs-1", "cp-1", true)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.ErrorIs(t, err, retriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

////////////////////////////////////////////////////////////////////////////////

func TestSessionCreateCheckpointWrappedError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "CreateCheckpoint").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.ErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On(
		"CreateCheckpoint",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(testNfsClientError)

	s := newTestSessionWithMetricsMock(nfsMock, metricsMock, testSession)

	err := s.CreateCheckpoint(ctx, "fs-1", "cp-1", 42)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.ErrorIs(t, err, retriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestSessionDestroySessionWrappedError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "DestroySession").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.ErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("DestroySession", mock.Anything, mock.Anything).
		Return(testNfsClientError)

	s := newTestSessionWithMetricsMock(nfsMock, metricsMock, testSession)

	err := s.Close(ctx)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.ErrorIs(t, err, retriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestSessionListNodesWrappedError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "ListNodes").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.ErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On(
		"ListNodes",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(nil, "", testNfsClientError)

	s := newTestSessionWithMetricsMock(nfsMock, metricsMock, testSession)

	_, _, err := s.ListNodes(ctx, 0, "", 1024, false)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.ErrorIs(t, err, retriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestSessionCreateNodeWrappedError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "CreateNode").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.ErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("CreateNode", mock.Anything, mock.Anything, mock.Anything).
		Return(uint64(0), testNfsClientError)

	s := newTestSessionWithMetricsMock(nfsMock, metricsMock, testSession)

	_, err := s.CreateNode(ctx, nfs.Node{Name: "test"})
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.ErrorIs(t, err, retriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestSessionReadLinkWrappedError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "ReadLink").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.ErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("ReadLink", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, testNfsClientError)

	s := newTestSessionWithMetricsMock(nfsMock, metricsMock, testSession)

	_, err := s.ReadLink(ctx, 42)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.ErrorIs(t, err, retriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

////////////////////////////////////////////////////////////////////////////////

func TestClientCreateFileStoreNonRetriableError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "CreateFileStore").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.NotErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("CreateFileStore", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, testNfsClientNonRetriableError)

	c := newTestClientWithMetricsMock(nfsMock, metricsMock)

	err := c.Create(ctx, "fs-1", nfs.CreateFilesystemParams{
		Kind:        types.FilesystemKind_FILESYSTEM_KIND_SSD,
		BlockSize:   4096,
		BlocksCount: 100,
	})
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.NotErrorIs(t, err, retriableError)
	require.ErrorIs(t, err, testNfsClientNonRetriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestClientDestroyFileStoreNonRetriableError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "DestroyFileStore").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.NotErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("DestroyFileStore", mock.Anything, mock.Anything, mock.Anything).
		Return(testNfsClientNonRetriableError)

	c := newTestClientWithMetricsMock(nfsMock, metricsMock)

	err := c.Delete(ctx, "fs-1", false)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.NotErrorIs(t, err, retriableError)
	require.ErrorIs(t, err, testNfsClientNonRetriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestClientResizeFileStoreNonRetriableError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "ResizeFileStore").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.NotErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("GetFileStoreInfo", mock.Anything, mock.Anything).
		Return(nil, testNfsClientNonRetriableError)

	c := newTestClientWithMetricsMock(nfsMock, metricsMock)

	err := c.Resize(ctx, "fs-1", 8192)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.NotErrorIs(t, err, retriableError)
	require.ErrorIs(t, err, testNfsClientNonRetriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestClientDescribeFileStoreModelNonRetriableError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "DescribeFileStoreModel").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.NotErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On(
		"DescribeFileStoreModel",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(nil, testNfsClientNonRetriableError)

	c := newTestClientWithMetricsMock(nfsMock, metricsMock)

	_, err := c.DescribeModel(
		ctx,
		100,
		4096,
		types.FilesystemKind_FILESYSTEM_KIND_SSD,
	)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.NotErrorIs(t, err, retriableError)
	require.ErrorIs(t, err, testNfsClientNonRetriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestClientDestroyCheckpointNonRetriableError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "DestroyCheckpoint").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.NotErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("DestroyCheckpoint", mock.Anything, mock.Anything, mock.Anything).
		Return(testNfsClientNonRetriableError)

	c := newTestClientWithMetricsMock(nfsMock, metricsMock)

	err := c.DestroyCheckpoint(ctx, "fs-1", "cp-1")
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.NotErrorIs(t, err, retriableError)
	require.ErrorIs(t, err, testNfsClientNonRetriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestClientCreateSessionNonRetriableError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "CreateSession").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.NotErrorIs(t, *err, retriableError)
		require.ErrorIs(t, *err, testNfsClientNonRetriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("CreateSession", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nfs_client.Session{}, testNfsClientNonRetriableError)

	c := newTestClientWithMetricsMock(nfsMock, metricsMock)

	_, err := c.CreateSession(ctx, "fs-1", "cp-1", true)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.NotErrorIs(t, err, retriableError)
	require.ErrorIs(t, err, testNfsClientNonRetriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

////////////////////////////////////////////////////////////////////////////////

func TestSessionCreateCheckpointNonRetriableError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "CreateCheckpoint").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.NotErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On(
		"CreateCheckpoint",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(testNfsClientNonRetriableError)

	s := newTestSessionWithMetricsMock(nfsMock, metricsMock, testSession)

	err := s.CreateCheckpoint(ctx, "fs-1", "cp-1", 42)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.NotErrorIs(t, err, retriableError)
	require.ErrorIs(t, err, testNfsClientNonRetriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestSessionDestroySessionNonRetriableError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "DestroySession").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.NotErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("DestroySession", mock.Anything, mock.Anything).
		Return(testNfsClientNonRetriableError)

	s := newTestSessionWithMetricsMock(nfsMock, metricsMock, testSession)

	err := s.Close(ctx)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.NotErrorIs(t, err, retriableError)
	require.ErrorIs(t, err, testNfsClientNonRetriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestSessionListNodesNonRetriableError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "ListNodes").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.NotErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On(
		"ListNodes",
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
		mock.Anything,
	).Return(nil, "", testNfsClientNonRetriableError)

	s := newTestSessionWithMetricsMock(nfsMock, metricsMock, testSession)

	_, _, err := s.ListNodes(ctx, 0, "", 1024, false)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.NotErrorIs(t, err, retriableError)
	require.ErrorIs(t, err, testNfsClientNonRetriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestSessionCreateNodeNonRetriableError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "CreateNode").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.NotErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("CreateNode", mock.Anything, mock.Anything, mock.Anything).
		Return(uint64(0), testNfsClientNonRetriableError)

	s := newTestSessionWithMetricsMock(nfsMock, metricsMock, testSession)

	_, err := s.CreateNode(ctx, nfs.Node{Name: "test"})
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.NotErrorIs(t, err, retriableError)
	require.ErrorIs(t, err, testNfsClientNonRetriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}

func TestSessionReadLinkNonRetriableError(t *testing.T) {
	ctx := context.Background()

	metricsMock := client_metrics_mocks.NewMetricsMock()
	metricsMock.On("StatRequest", "ReadLink").Return(func(err *error) {
		require.Error(t, *err)
		var clientErr *nfs_client.ClientError
		require.ErrorAs(t, *err, &clientErr)
		require.NotErrorIs(t, *err, retriableError)
	}).Once()

	nfsMock := nfs_client_mocks.NewClientInterfaceMock()
	nfsMock.On("ReadLink", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, testNfsClientNonRetriableError)

	s := newTestSessionWithMetricsMock(nfsMock, metricsMock, testSession)

	_, err := s.ReadLink(ctx, 42)
	require.Error(t, err)
	var clientErr *nfs_client.ClientError
	require.ErrorAs(t, err, &clientErr)
	require.NotErrorIs(t, err, retriableError)
	require.ErrorIs(t, err, testNfsClientNonRetriableError)

	nfsMock.AssertExpectations(t)
	metricsMock.AssertExpectations(t)
}
