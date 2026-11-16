package storage

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/exporter"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

const tableName = "export_parts"

// Storage uses the task database, so confirmations and task generation checks
// share a transaction even when the snapshot itself is read only from backup.
type Storage interface {
	ReadParts(
		ctx context.Context,
		taskID string,
		after uint32,
		limit uint64,
	) ([]exporter.S3ManifestPart, error)
	UpsertParts(
		ctx context.Context,
		tx *persistence.Transaction,
		taskID string,
		parts []exporter.S3ManifestPart,
	) error
	DeleteParts(
		ctx context.Context,
		tx *persistence.Transaction,
		taskID string,
		limit uint64,
	) (uint64, error)
}

type storageYDB struct {
	db         *persistence.YDBClient
	tablesPath string
}

func NewStorage(db *persistence.YDBClient, folder string) Storage {
	return &storageYDB{db: db, tablesPath: db.AbsolutePath(folder)}
}

func CreateYDBTables(
	ctx context.Context,
	db *persistence.YDBClient,
	folder string,
	dropUnusedColumns bool,
) error {

	return db.CreateOrAlterTable(ctx, folder, tableName,
		persistence.NewCreateTableDescription(
			persistence.WithColumn(
				"task_id", persistence.Optional(persistence.TypeUTF8),
			),
			persistence.WithColumn(
				"part_number", persistence.Optional(persistence.TypeUint32),
			),
			persistence.WithColumn(
				"object_key", persistence.Optional(persistence.TypeUTF8),
			),
			persistence.WithColumn(
				"offset", persistence.Optional(persistence.TypeUint64),
			),
			persistence.WithColumn(
				"length", persistence.Optional(persistence.TypeUint64),
			),
			persistence.WithColumn(
				"stored_length", persistence.Optional(persistence.TypeUint64),
			),
			persistence.WithColumn(
				"sha256", persistence.Optional(persistence.TypeString),
			),
			persistence.WithPrimaryKeyColumn("task_id", "part_number"),
		), dropUnusedColumns,
	)
}

func (s *storageYDB) ReadParts(
	ctx context.Context,
	taskID string,
	after uint32,
	limit uint64,
) ([]exporter.S3ManifestPart, error) {

	res, err := s.db.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $task_id as Utf8;
		declare $after as Uint32;
		declare $limit as Uint64;
		select part_number, object_key, offset, length, stored_length, sha256
		from export_parts
		where task_id = $task_id and part_number > $after
		order by part_number
		limit $limit;
	`, s.tablesPath),
		persistence.ValueParam("$task_id", persistence.UTF8Value(taskID)),
		persistence.ValueParam("$after", persistence.Uint32Value(after)),
		persistence.ValueParam("$limit", persistence.Uint64Value(limit)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()
	var parts []exporter.S3ManifestPart
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var part exporter.S3ManifestPart
			var sum []byte
			err := res.ScanNamed(
				persistence.OptionalWithDefault("part_number", &part.Number),
				persistence.OptionalWithDefault("object_key", &part.Key),
				persistence.OptionalWithDefault("offset", &part.Offset),
				persistence.OptionalWithDefault("length", &part.Length),
				persistence.OptionalWithDefault("stored_length", &part.StoredLength),
				persistence.OptionalWithDefault("sha256", &sum),
			)
			if err != nil {
				return nil, err
			}
			if len(sum) != sha256.Size {
				return nil, errors.NewNonRetriableErrorf(
					"invalid hash of export part %v", part.Number,
				)
			}
			part.SHA256 = hex.EncodeToString(sum)
			parts = append(parts, part)
		}
	}
	return parts, res.Err()
}

func (s *storageYDB) UpsertParts(
	ctx context.Context,
	tx *persistence.Transaction,
	taskID string,
	parts []exporter.S3ManifestPart,
) error {

	if len(parts) == 0 {
		return nil
	}
	values := make([]persistence.Value, 0, len(parts))
	for _, part := range parts {
		sum, err := hex.DecodeString(part.SHA256)
		if err != nil || len(sum) != sha256.Size || part.Number == 0 {
			return errors.NewNonRetriableErrorf("invalid export part %v", part.Number)
		}
		values = append(values, persistence.StructValue(
			persistence.StructFieldValue("task_id", persistence.UTF8Value(taskID)),
			persistence.StructFieldValue(
				"part_number", persistence.Uint32Value(part.Number),
			),
			persistence.StructFieldValue("object_key", persistence.UTF8Value(part.Key)),
			persistence.StructFieldValue("offset", persistence.Uint64Value(part.Offset)),
			persistence.StructFieldValue("length", persistence.Uint64Value(part.Length)),
			persistence.StructFieldValue(
				"stored_length", persistence.Uint64Value(part.StoredLength),
			),
			persistence.StructFieldValue("sha256", persistence.BytesValue(sum)),
		))
	}
	_, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $parts as List<Struct<task_id:Utf8, part_number:Uint32,
			object_key:Utf8, offset:Uint64, length:Uint64,
			stored_length:Uint64, sha256:String>>;
		upsert into export_parts select * from AS_TABLE($parts);
	`, s.tablesPath),
		persistence.ValueParam("$parts", persistence.ListValue(values...)),
	)
	return err
}

// Delete one bounded primary-key range. Repeating after an uncertain commit
// removes the next rows, so cleanup needs no persisted cursor.
func (s *storageYDB) DeleteParts(
	ctx context.Context,
	tx *persistence.Transaction,
	taskID string,
	limit uint64,
) (uint64, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $task_id as Utf8;
		declare $limit as Uint64;
		$keys = (
			select task_id, part_number from export_parts
			where task_id = $task_id
			order by part_number
			limit $limit
		);
		select count(*) as count from $keys;
		delete from export_parts on select * from $keys;
	`, s.tablesPath),
		persistence.ValueParam("$task_id", persistence.UTF8Value(taskID)),
		persistence.ValueParam("$limit", persistence.Uint64Value(limit)),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()
	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, res.Err()
	}
	var count uint64
	err = res.ScanNamed(persistence.OptionalWithDefault("count", &count))
	if err != nil {
		return 0, err
	}
	return count, res.Err()
}
