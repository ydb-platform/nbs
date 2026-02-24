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
		"Creating schema for dataplane filesystem traversal storage in %v",
		db.AbsolutePath(storageFolder),
	)
	err := db.CreateOrAlterTable(
		ctx,
		storageFolder,
		"directory_listing_queue",
		persistence.NewCreateTableDescription(
			persistence.WithColumn("filesystem_snapshot_id", persistence.Optional(persistence.TypeUTF8)),
			persistence.WithColumn("node_id", persistence.Optional(persistence.TypeUint64)),
			persistence.WithColumn("cookie", persistence.Optional(persistence.TypeString)),
			persistence.WithColumn("depth", persistence.Optional(persistence.TypeUint64)),
			persistence.WithPrimaryKeyColumn("filesystem_snapshot_id", "node_id"),
		),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}

	logging.Info(ctx, "Created directory_listing_queue table")
	return nil
}

func Drop(
	ctx context.Context,
	storageFolder string,
	db *persistence.YDBClient,
) error {

	logging.Info(ctx, "Dropping schema for dataplane filesystem traversal storage in %v", db.AbsolutePath(storageFolder))

	err := db.DropTable(ctx, storageFolder, "directory_listing_queue")
	if err != nil {
		return err
	}

	logging.Info(ctx, "Dropped directory_listing_queue table")
	return nil
}
