package nfs

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	coreprotos "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type Node nfs_client.Node
type Session nfs_client.Session

////////////////////////////////////////////////////////////////////////////////

const (
	maxConsecutiveRetries = 3
)

func getStorageMediaKind(
	kind types.FilesystemKind,
) (coreprotos.EStorageMediaKind, error) {

	switch kind {
	case types.FilesystemKind_FILESYSTEM_KIND_SSD:
		return coreprotos.EStorageMediaKind_STORAGE_MEDIA_SSD, nil
	case types.FilesystemKind_FILESYSTEM_KIND_HDD:
		return coreprotos.EStorageMediaKind_STORAGE_MEDIA_HDD, nil
	default:
		return 0, errors.NewNonRetriableErrorf(
			"unknown fs kind %v",
			kind,
		)
	}
}

func wrapError(err error) error {
	var clientErr *nfs_client.ClientError
	if errors.As(err, &clientErr) {
		if clientErr.IsRetriable() {
			return errors.NewRetriableError(err)
		}
	}

	return err
}

func isAbortedError(err error) bool {
	var clientErr *nfs_client.ClientError
	if errors.As(err, &clientErr) {
		if clientErr.Code == nfs_client.E_ABORTED {
			return true
		}
	}

	return false
}

func isNotFoundError(err error) bool {
	var clientErr *nfs_client.ClientError
	if errors.As(err, &clientErr) {
		// TODO: remove support for PathDoesNotExist
		if clientErr.Facility() == nfs_client.FACILITY_SCHEMESHARD &&
			clientErr.Status() == 2 {
			return true
		}
		if clientErr.Code == nfs_client.E_NOT_FOUND {
			return true
		}
	}

	return false
}

func setupStderrLogger(ctx context.Context) context.Context {
	return logging.SetLogger(
		ctx,
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

////////////////////////////////////////////////////////////////////////////////

type clientMetrics struct {
	registry metrics.Registry
	errors   metrics.Counter
}

func (m *clientMetrics) OnError(err nfs_client.ClientError) {
	// TODO: split metrics into types (retriable, fatal, etc.)
	m.errors.Inc()
}

////////////////////////////////////////////////////////////////////////////////

type client struct {
	nfs     *nfs_client.Client
	metrics clientMetrics
}

func (c *client) Close() error {
	return c.nfs.Close()
}

func (c *client) Create(
	ctx context.Context,
	filesystemID string,
	params CreateFilesystemParams,
) error {

	mediaKind, err := getStorageMediaKind(params.Kind)
	if err != nil {
		return err
	}

	_, err = c.nfs.CreateFileStore(
		ctx,
		filesystemID,
		&nfs_client.CreateFileStoreOpts{
			FolderID:         params.FolderID,
			CloudID:          params.CloudID,
			BlockSize:        params.BlockSize,
			BlocksCount:      params.BlocksCount,
			StorageMediaKind: mediaKind,
		},
	)

	return wrapError(err)
}

func (c *client) Delete(
	ctx context.Context,
	filesystemID string,
) error {

	err := c.nfs.DestroyFileStore(ctx, filesystemID)
	return wrapError(err)
}

func (c *client) Resize(
	ctx context.Context,
	filesystemID string,
	size uint64,
) error {

	retries := 0
	for {
		filestore, err := c.nfs.GetFileStoreInfo(ctx, filesystemID)
		if err != nil {
			return wrapError(err)
		}

		if filestore.BlockSize == 0 {
			return errors.NewNonRetriableErrorf(
				"invalid filestore config %v",
				filestore,
			)
		}

		if size%uint64(filestore.BlockSize) != 0 {
			return errors.NewNonRetriableErrorf(
				"size %v should be divisible by filestore.BlockSize %v",
				size,
				filestore.BlockSize,
			)
		}

		newBlocksCount := size / uint64(filestore.BlockSize)

		// so far no need in checkpoint; resize is race safe as we cannot reduce space
		err = c.nfs.ResizeFileStore(
			ctx,
			filesystemID,
			newBlocksCount,
			filestore.ConfigVersion,
		)

		if err != nil {
			if !isAbortedError(err) {
				return wrapError(err)
			}

			if retries == maxConsecutiveRetries {
				return errors.NewRetriableError(err)
			}

			retries++
			continue
		}

		return nil
	}
}

func (c *client) DescribeModel(
	ctx context.Context,
	blocksCount uint64,
	blockSize uint32,
	kind types.FilesystemKind,
) (FilesystemModel, error) {

	mediaKind, err := getStorageMediaKind(kind)
	if err != nil {
		return FilesystemModel{}, err
	}

	model, err := c.nfs.DescribeFileStoreModel(
		ctx,
		blocksCount,
		blockSize,
		mediaKind,
	)
	if err != nil {
		return FilesystemModel{}, wrapError(err)
	}

	return FilesystemModel{
		BlockSize:     model.BlockSize,
		BlocksCount:   model.BlocksCount,
		ChannelsCount: model.ChannelsCount,
		Kind:          kind,
		PerformanceProfile: FilesystemPerformanceProfile{
			MaxReadBandwidth:  model.PerformanceProfile.MaxReadBandwidth,
			MaxReadIops:       model.PerformanceProfile.MaxReadIops,
			MaxWriteBandwidth: model.PerformanceProfile.MaxWriteBandwidth,
			MaxWriteIops:      model.PerformanceProfile.MaxWriteIops,
		},
	}, nil
}

func (c *client) CreateSession(
	ctx context.Context,
	fileSystemID string,
	readonly bool,
) (Session, error) {
	session, err := c.nfs.CreateSession(ctx, fileSystemID, readonly)
	return Session(session), wrapError(err)
}

func (c *client) DestroySession(ctx context.Context, session Session) error {
	return wrapError(c.nfs.DestroySession(ctx, nfs_client.Session(session)))
}

func (c *client) ListNodes(
	ctx context.Context,
	session Session,
	parentNodeID uint64,
	cookie string,
) ([]Node, string, error) {

	nodes, cookie, err := c.nfs.ListNodes(
		ctx,
		nfs_client.Session(session),
		parentNodeID,
		cookie,
	)
	resultNodes := make([]Node, len(nodes))
	for i := range nodes {
		resultNodes[i] = Node(nodes[i])
	}

	return resultNodes, cookie, wrapError(err)
}

func (c *client) CreateNode(
	ctx context.Context,
	session Session,
	node Node,
) (uint64, error) {

	nodeId, err := c.nfs.CreateNode(
		ctx,
		nfs_client.Session(session),
		nfs_client.Node(node),
	)
	return nodeId, wrapError(err)
}

func (c *client) ReadLink(
	ctx context.Context,
	session Session,
	nodeID uint64,
) ([]byte, error) {

	data, err := c.nfs.ReadLink(ctx, nfs_client.Session(session), nodeID)
	return data, wrapError(err)
}
