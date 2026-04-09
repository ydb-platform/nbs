package nfs

import (
	"context"

	client_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/filestore/public/api/protos"
	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"
	coreprotos "github.com/ydb-platform/nbs/cloud/storage/core/protos"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

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

func isSessionInvalidError(err error) bool {
	var clientErr *nfs_client.ClientError
	if errors.As(err, &clientErr) {
		if clientErr.Code == nfs_client.E_FS_INVALID_SESSION {
			return true
		}
	}

	return false
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

func IsEnoEntError(err error) bool {
	var clientErr *nfs_client.ClientError
	if errors.As(err, &clientErr) {
		if clientErr.Code == nfs_client.E_FS_NOENT {
			return true
		}
	}

	return false
}

func isAlreadyExistsError(err error) bool {
	var clientErr *nfs_client.ClientError
	if errors.As(err, &clientErr) {
		if clientErr.Code == nfs_client.E_FS_EXIST {
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

type client struct {
	nfs                    nfs_client.ClientInterface
	metrics                client_metrics.Metrics
	sessionMetricsRegistry metrics.Registry
	zoneID                 string
}

func NewClient(
	nfs nfs_client.ClientInterface,
	metrics client_metrics.Metrics,
	sessionMetricsRegistry metrics.Registry,
	zoneID string,
) Client {

	return &client{
		nfs:                    nfs,
		metrics:                metrics,
		sessionMetricsRegistry: sessionMetricsRegistry,
		zoneID:                 zoneID,
	}
}

////////////////////////////////////////////////////////////////////////////////

func (c *client) updateFilestore(
	ctx context.Context,
	filesystemID string,
	updateFunc func(
		filestore *protos.TFileStore,
	) error,
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

		err = updateFunc(filestore)

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

func (c *client) ZoneID() string {
	return c.zoneID
}

func (c *client) Close() error {
	return c.nfs.Close()
}

func (c *client) Create(
	ctx context.Context,
	filesystemID string,
	params CreateFilesystemParams,
) (err error) {

	defer c.metrics.StatRequest("CreateFileStore")(&err)

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
			ShardCount:       params.ShardCount,
		},
	)

	return wrapError(err)
}

func (c *client) Delete(
	ctx context.Context,
	filesystemID string,
	force bool,
) (err error) {

	defer c.metrics.StatRequest("DestroyFileStore")(&err)

	err = c.nfs.DestroyFileStore(ctx, filesystemID, force)
	return wrapError(err)
}

func (c *client) Resize(
	ctx context.Context,
	filesystemID string,
	size uint64,
) (err error) {

	defer c.metrics.StatRequest("ResizeFileStore")(&err)

	updateFunc := func(filestore *protos.TFileStore) error {
		if size%uint64(filestore.BlockSize) != 0 {
			return errors.NewNonRetriableErrorf(
				"size %v should be divisible by filestore.BlockSize %v",
				size,
				filestore.BlockSize,
			)
		}

		newBlocksCount := size / uint64(filestore.BlockSize)

		// so far no need in checkpoint; resize is race safe as we cannot reduce space
		return c.nfs.ResizeFileStore(
			ctx,
			filesystemID,
			newBlocksCount,
			filestore.ConfigVersion,
		)
	}

	return c.updateFilestore(ctx, filesystemID, updateFunc)
}

func (c *client) EnableDirectoryCreationInShards(
	ctx context.Context,
	filesystemID string,
	shardCount uint32,
) (err error) {

	defer c.metrics.StatRequest("EnableDirectoryCreationInShards")(&err)
	updateFunc := func(filestore *protos.TFileStore) error {
		return c.nfs.EnableDirectoryCreationInShards(
			ctx,
			filesystemID,
			filestore.BlocksCount,
			filestore.ConfigVersion,
			shardCount,
		)
	}

	return c.updateFilestore(ctx, filesystemID, updateFunc)
}

func (c *client) DescribeModel(
	ctx context.Context,
	blocksCount uint64,
	blockSize uint32,
	kind types.FilesystemKind,
) (_ FilesystemModel, err error) {

	defer c.metrics.StatRequest("DescribeFileStoreModel")(&err)

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

func (c *client) DestroyCheckpoint(
	ctx context.Context,
	filesystemID string,
	checkpointID string,
) (err error) {

	defer c.metrics.StatRequest("DestroyCheckpoint")(&err)

	return wrapError(
		c.nfs.DestroyCheckpoint(
			ctx,
			filesystemID,
			checkpointID,
		),
	)
}

func (c *client) CreateSession(
	ctx context.Context,
	fileSystemID string,
	checkpointID string,
	readonly bool,
) (_ Session, err error) {

	defer c.metrics.StatRequest("CreateSession")(&err)

	s, err := c.nfs.CreateSession(ctx, fileSystemID, checkpointID, readonly)
	if err != nil {
		return nil, wrapError(err)
	}

	return &sessionWithMetrics{
		nfs:     c.nfs,
		session: s,
		metrics: client_metrics.NewSessionMetrics(
			c.sessionMetricsRegistry,
			map[string]string{
				"filesystem": fileSystemID,
				"checkpoint": checkpointID,
			},
		),
	}, nil
}
