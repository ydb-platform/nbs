package schema

import (
	"context"

	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/persistence"
	ydb_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

func Create(
	ctx context.Context,
	config *snapshot_config.SnapshotConfig,
	db *persistence.YDBClient,
	s3 *persistence.S3Client,
	dropUnusedColumns bool,
) error {

	logging.Info(
		ctx,
		"Creating schema for dataplane snapshot storage in %v",
		db.AbsolutePath(config.GetStorageFolder()),
	)

	err := db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"snapshots",
		snapshotStateTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created snapshots table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"deleting",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("deleting_at", ydb_types.Optional(ydb_types.TypeTimestamp)),
			persistence.WithColumn("snapshot_id", ydb_types.Optional(ydb_types.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("deleting_at", "snapshot_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created deleting table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"chunk_blobs",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("shard_id", ydb_types.Optional(ydb_types.TypeUint64)),
			persistence.WithColumn("chunk_id", ydb_types.Optional(ydb_types.TypeUTF8)),
			persistence.WithColumn("referer", ydb_types.Optional(ydb_types.TypeUTF8)),
			persistence.WithColumn("data", ydb_types.Optional(ydb_types.TypeString)),
			persistence.WithColumn("refcnt", ydb_types.Optional(ydb_types.TypeUint32)),
			persistence.WithColumn("checksum", ydb_types.Optional(ydb_types.TypeUint32)),
			persistence.WithColumn("compression", ydb_types.Optional(ydb_types.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("shard_id", "chunk_id", "referer"),
			persistence.WithUniformPartitions(config.GetChunkBlobsTableShardCount()),
			persistence.WithExternalBlobs(config.GetExternalBlobsMediaKind()),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created chunk_blobs table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"chunk_map",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("shard_id", ydb_types.Optional(ydb_types.TypeUint64)),
			persistence.WithColumn("snapshot_id", ydb_types.Optional(ydb_types.TypeUTF8)),
			persistence.WithColumn("chunk_index", ydb_types.Optional(ydb_types.TypeUint32)),
			persistence.WithColumn("chunk_id", ydb_types.Optional(ydb_types.TypeUTF8)),
			persistence.WithColumn("stored_in_s3", ydb_types.Optional(ydb_types.TypeBool)),
			persistence.WithPrimaryKeyColumn("shard_id", "snapshot_id", "chunk_index"),
			persistence.WithUniformPartitions(config.GetChunkMapTableShardCount()),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created chunk_map table")

	if s3 != nil && len(config.GetS3Bucket()) != 0 {
		err = s3.CreateBucket(ctx, config.GetS3Bucket())
		if err != nil {
			return err
		}

		logging.Info(ctx, "Created s3 bucket")
	} else {
		logging.Info(ctx, "Skipped s3 bucket creation")
	}

	logging.Info(ctx, "Created schema for dataplane snapshot storage")

	return nil
}

func Drop(
	ctx context.Context,
	config *snapshot_config.SnapshotConfig,
	db *persistence.YDBClient,
) error {

	logging.Info(ctx, "Dropping schema for dataplane snapshot storage in %v", db.AbsolutePath(config.GetStorageFolder()))

	err := db.DropTable(ctx, config.GetStorageFolder(), "snapshots")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped snapshots table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "deleting")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped deleting table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "chunk_blobs")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped chunk_blobs table")

	err = db.DropTable(ctx, config.GetStorageFolder(), "chunk_map")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped chunk_map table")

	logging.Info(ctx, "Dropped schema for dataplane snapshot storage")

	return nil
}

func snapshotStateTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("id", ydb_types.Optional(ydb_types.TypeUTF8)),
		persistence.WithColumn("creating_at", ydb_types.Optional(ydb_types.TypeTimestamp)),
		persistence.WithColumn("created_at", ydb_types.Optional(ydb_types.TypeTimestamp)),
		persistence.WithColumn("deleting_at", ydb_types.Optional(ydb_types.TypeTimestamp)),
		persistence.WithColumn("size", ydb_types.Optional(ydb_types.TypeUint64)),
		persistence.WithColumn("storage_size", ydb_types.Optional(ydb_types.TypeUint64)),
		persistence.WithColumn("chunk_count", ydb_types.Optional(ydb_types.TypeUint32)),
		persistence.WithColumn("encryption_mode", ydb_types.Optional(ydb_types.TypeUint32)),
		persistence.WithColumn("encryption_keyhash", ydb_types.Optional(ydb_types.TypeString)),
		persistence.WithColumn("status", ydb_types.Optional(ydb_types.TypeInt64)),
		persistence.WithPrimaryKeyColumn("id"),
	)
}
