package schema

import (
	"context"

	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/config"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
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
			persistence.WithColumn("deleting_at", persistence.Optional(persistence.TypeTimestamp)),
			persistence.WithColumn("snapshot_id", persistence.Optional(persistence.TypeUTF8)),
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
		"incremental",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("disk_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("snapshot_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("checkpoint_id", persistence.Optional(persistence.TypeUTF8)),
			// We use timestamp to make it easier to find the latest snapshot
			// and it is OK to depend on time here because clock discrepancy
			// won't hurt the correctness, it can only increase amount of data
			// to be copied but the probability of such event is very low and
			// can be neglected.
			persistence.WithColumn("created_at", persistence.Optional(persistence.TypeTimestamp)),
			persistence.WithPrimaryKeyColumn("zone_id", "disk_id", "snapshot_id", "created_at"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created incremental table")

	err = db.CreateOrAlterTable(
		ctx,
		config.GetStorageFolder(),
		"chunk_blobs",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("shard_id", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("chunk_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("referer", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("data", persistence.Optional(persistence.TypeString)),
			persistence.WithColumn("refcnt", persistence.Optional(persistence.TypeUint32)),
			persistence.WithColumn("checksum", persistence.Optional(persistence.TypeUint32)),
			persistence.WithColumn("compression", persistence.Optional(persistence.TypeUTF8)),
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
			persistence.WithColumn("shard_id", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("snapshot_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("chunk_index", persistence.Optional(persistence.TypeUint32)),
			persistence.WithColumn("chunk_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("stored_in_s3", persistence.Optional(persistence.TypeBool)),
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

	err = db.DropTable(ctx, config.GetStorageFolder(), "incremental")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped incremental table")

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
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("disk_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("checkpoint_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("create_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("creating_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("created_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("deleting_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("base_snapshot_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("base_checkpoint_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("storage_size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("chunk_count", persistence.Optional(persistence.TypeUint32)),
		persistence.WithColumn("lock_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("encryption_mode", persistence.Optional(persistence.TypeUint32)),
		persistence.WithColumn("encryption_keyhash", persistence.Optional(persistence.TypeString)),
		persistence.WithColumn("status", persistence.Optional(persistence.TypeInt64)),
		persistence.WithPrimaryKeyColumn("id"),
	)
}
