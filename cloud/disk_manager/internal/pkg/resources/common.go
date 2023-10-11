package resources

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/persistence"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	ydb_table "github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydb_result "github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	ydb_named "github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydb_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

////////////////////////////////////////////////////////////////////////////////

type storageYDB struct {
	db                              *persistence.YDBClient
	disksPath                       string
	imagesPath                      string
	snapshotsPath                   string
	filesystemsPath                 string
	placementGroupsPath             string
	endedMigrationExpirationTimeout time.Duration
}

////////////////////////////////////////////////////////////////////////////////

func NewStorage(
	disksFolder string,
	imagesFolder string,
	snapshotsFolder string,
	filesystemsFolder string,
	placementGroupsPath string,
	db *persistence.YDBClient,
	endedMigrationExpirationTimeout time.Duration,
) (Storage, error) {

	return &storageYDB{
		db:                              db,
		disksPath:                       db.AbsolutePath(disksFolder),
		imagesPath:                      db.AbsolutePath(imagesFolder),
		snapshotsPath:                   db.AbsolutePath(snapshotsFolder),
		filesystemsPath:                 db.AbsolutePath(filesystemsFolder),
		placementGroupsPath:             db.AbsolutePath(placementGroupsPath),
		endedMigrationExpirationTimeout: endedMigrationExpirationTimeout,
	}, nil
}

////////////////////////////////////////////////////////////////////////////////

func CreateYDBTables(
	ctx context.Context,
	disksFolder string,
	imagesFolder string,
	snapshotsFolder string,
	filesystemsFolder string,
	placementGroupsFolder string,
	db *persistence.YDBClient,
	dropUnusedColumns bool,
) error {

	err := createDisksYDBTables(ctx, disksFolder, db, dropUnusedColumns)
	if err != nil {
		return err
	}

	err = createImagesYDBTables(ctx, imagesFolder, db, dropUnusedColumns)
	if err != nil {
		return err
	}

	err = createSnapshotsYDBTables(ctx, snapshotsFolder, db, dropUnusedColumns)
	if err != nil {
		return err
	}

	if filesystemsFolder != "" {
		err = createFilesystemsYDBTables(
			ctx,
			filesystemsFolder,
			db,
			dropUnusedColumns,
		)
		if err != nil {
			return err
		}
	}

	return createPlacementGroupsYDBTables(
		ctx,
		placementGroupsFolder,
		db,
		dropUnusedColumns,
	)
}

func DropYDBTables(
	ctx context.Context,
	disksFolder string,
	imagesFolder string,
	snapshotsFolder string,
	filesystemsFolder string,
	placementGroupsFolder string,
	db *persistence.YDBClient,
) error {

	err := dropDisksYDBTables(ctx, disksFolder, db)
	if err != nil {
		return err
	}

	err = dropImagesYDBTables(ctx, imagesFolder, db)
	if err != nil {
		return err
	}

	err = dropSnapshotsYDBTables(ctx, snapshotsFolder, db)
	if err != nil {
		return err
	}

	if filesystemsFolder != "" {
		err = dropFilesystemsYDBTables(ctx, filesystemsFolder, db)
		if err != nil {
			return err
		}
	}

	err = dropPlacementGroupsYDBTables(ctx, placementGroupsFolder, db)
	if err != nil {
		return err
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func listResources(
	ctx context.Context,
	session *persistence.Session,
	tablesPath string,
	tableName string,
	folderID string,
	creatingBefore time.Time,
) ([]string, error) {

	var (
		res ydb_result.StreamResult
		err error
	)
	if len(folderID) == 0 {
		res, err = session.StreamExecuteRO(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $creating_before as Timestamp;

			select id from %v
			where creating_at < $creating_before
		`, tablesPath, tableName), ydb_table.NewQueryParameters(
			ydb_table.ValueParam("$creating_before", persistence.TimestampValue(creatingBefore)),
		))
	} else {
		res, err = session.StreamExecuteRO(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $folder_id as Utf8;
			declare $creating_before as Timestamp;

			select id
			from %v
			where folder_id = $folder_id and creating_at < $creating_before
		`, tablesPath, tableName), ydb_table.NewQueryParameters(
			ydb_table.ValueParam("$folder_id", ydb_types.UTF8Value(folderID)),
			ydb_table.ValueParam("$creating_before", persistence.TimestampValue(creatingBefore)),
		))
	}
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var ids []string

	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var id string
			err = res.ScanNamed(
				ydb_named.OptionalWithDefault("id", &id),
			)
			if err != nil {
				return nil, errors.NewNonRetriableErrorf(
					"listResources: failed to parse row from %v: %w",
					tableName,
					err,
				)
			}

			ids = append(ids, id)
		}
	}

	// NOTE: always check stream query result after iteration.
	err = res.Err()
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	return ids, nil
}
