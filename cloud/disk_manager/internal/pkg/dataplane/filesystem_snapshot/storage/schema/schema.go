package schema

import (
	"context"

	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

func Create(
	ctx context.Context,
	storageFolder string,
	db *persistence.YDBClient,
	dropUnusedColumns bool,
) error {

	logging.Info(
		ctx,
		"Creating schema for dataplane filesystem snapshot storage in %v",
		db.AbsolutePath(storageFolder),
	)

	err := db.CreateOrAlterTable(
		ctx,
		storageFolder,
		"filesystem_snapshots",
		filesystemSnapshotStateTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created filesystem_snapshots table")

	err = db.CreateOrAlterTable(
		ctx,
		storageFolder,
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
		storageFolder,
		"node_refs",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("filesystem_backup_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("depth", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("parent_node_id", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("name", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("child_node_id", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("node_type", persistence.Optional(persistence.TypeUint32)),
			persistence.WithPrimaryKeyColumn("filesystem_backup_id", "depth", "parent_node_id", "name"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}

	logging.Info(ctx, "Created node_refs table")
	err = db.CreateOrAlterTable(
		ctx,
		storageFolder,
		"nodes",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("filesystem_backup_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("node_id", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("mode", persistence.Optional(persistence.TypeUint32)),
			persistence.WithColumn("uid", persistence.Optional(persistence.TypeUint32)),
			persistence.WithColumn("gid", persistence.Optional(persistence.TypeUint32)),
			persistence.WithColumn("atime", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("mtime", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("ctime", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("size", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("symlink_target", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("filesystem_backup_id", "node_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}

	logging.Info(ctx, "Created nodes table")
	err = db.CreateOrAlterTable(
		ctx,
		storageFolder,
		"directory_listing_queue",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("filesystem_backup_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("node_id", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("cookie", persistence.Optional(persistence.TypeString)),
			persistence.WithColumn("depth", persistence.Optional(persistence.TypeUint64)),
			persistence.WithPrimaryKeyColumn("filesystem_backup_id", "node_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}

	logging.Info(ctx, "Created directory_listing_queue table")
	err = db.CreateOrAlterTable(
		ctx,
		storageFolder,
		"hardlinks",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("filesystem_backup_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("node_id", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("parent_node_id", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("name", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithPrimaryKeyColumn("filesystem_backup_id", "node_id", "parent_node_id", "name"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}

	logging.Info(ctx, "Created hardlinks table")
	logging.Info(ctx, "Created schema for dataplane filesystem snapshot storage")

	return nil
}

func Drop(
	ctx context.Context,
	storageFolder string,
	db *persistence.YDBClient,
) error {

	logging.Info(ctx, "Dropping schema for dataplane filesystem snapshot storage in %v", db.AbsolutePath(storageFolder))

	err := db.DropTable(ctx, storageFolder, "filesystem_snapshots")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped filesystem_snapshots table")

	err = db.DropTable(ctx, storageFolder, "deleting")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped deleting table")

	err = db.DropTable(ctx, storageFolder, "node_refs")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped node_refs table")

	err = db.DropTable(ctx, storageFolder, "nodes")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped nodes table")

	err = db.DropTable(ctx, storageFolder, "directory_listing_queue")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped directory_listing_queue table")

	err = db.DropTable(ctx, storageFolder, "hardlinks")
	if err != nil {
		return err
	}
	logging.Info(ctx, "Dropped hardlinks table")

	logging.Info(ctx, "Dropped schema for dataplane filesystem snapshot storage")

	return nil
}

func filesystemSnapshotStateTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("zone_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("filesystem_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("create_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("creating_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("created_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("deleting_at", persistence.Optional(persistence.TypeTimestamp)),
		persistence.WithColumn("size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("storage_size", persistence.Optional(persistence.TypeUint64)),
		persistence.WithColumn("chunk_count", persistence.Optional(persistence.TypeUint32)),
		persistence.WithColumn("lock_task_id", persistence.Optional(persistence.TypeUTF8)),
		persistence.WithColumn("status", persistence.Optional(persistence.TypeInt64)),
		persistence.WithPrimaryKeyColumn("id"),
	)
}
