package storage

import (
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/metrics"
	common_metrics "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

func NewStorage(
	config *snapshot_config.SnapshotConfig,
	metricsRegistry common_metrics.Registry,
	db *persistence.YDBClient,
	s3 *persistence.S3Client,
) (Storage, error) {

	tablesPath := db.AbsolutePath(config.GetStorageFolder())
	probeCompressionPercentage := config.GetProbeCompressionPercentage()

	var chunkStorageS3 *chunks.StorageS3
	// TODO: remove when s3 will always be initialized.
	if s3 != nil {
		chunkStorageS3 = chunks.NewStorageS3(
			db,
			s3,
			config.GetS3Bucket(),
			config.GetChunkBlobsS3KeyPrefix(),
			tablesPath,
			metrics.New(metricsRegistry, "s3"),
			probeCompressionPercentage,
		)
	}

	ydbMetrics := metrics.New(metricsRegistry, "ydb")
	return &storageYDB{
		db:                       db,
		tablesPath:               tablesPath,
		metrics:                  ydbMetrics,
		deleteWorkerCount:        int(config.GetDeleteWorkerCount()),
		shallowCopyWorkerCount:   int(config.GetShallowCopyWorkerCount()),
		shallowCopyInflightLimit: int(config.GetShallowCopyInflightLimit()),
		chunkCompression:         config.GetChunkCompression(),
		chunkStorageYDB: chunks.NewStorageYDB(
			db,
			tablesPath,
			ydbMetrics,
			probeCompressionPercentage,
		),
		chunkStorageS3: chunkStorageS3,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func NewLegacyStorage(
	config *snapshot_config.SnapshotConfig,
	metricsRegistry common_metrics.Registry,
	db *persistence.YDBClient,
) (Storage, error) {

	return &legacyStorage{
		db:         db,
		tablesPath: db.AbsolutePath(config.GetLegacyStorageFolder()),
		metrics:    metrics.New(metricsRegistry, "legacy"),
	}, nil
}
