package persistence

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/monitoring/metrics"
	persistence_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/persistence/config"
	ydb_table "github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydb_result "github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	ydb_named "github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydb_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

func newYDB(ctx context.Context) (*YDBClient, error) {
	endpoint := fmt.Sprintf(
		"localhost:%v",
		os.Getenv("DISK_MANAGER_RECIPE_KIKIMR_PORT"),
	)
	database := "/Root"
	rootPath := "disk_manager"

	return NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint: &endpoint,
			Database: &database,
			RootPath: &rootPath,
		},
		metrics.NewEmptyRegistry(),
	)
}

////////////////////////////////////////////////////////////////////////////////

type TableV1 struct {
	id   string
	val1 string
}

func scanTableV1s(ctx context.Context, res ydb_result.Result) (results []TableV1, err error) {
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var result TableV1
			err = res.ScanNamed(
				ydb_named.OptionalWithDefault("id", &result.id),
				ydb_named.OptionalWithDefault("val1", &result.val1),
			)
			if err != nil {
				return nil, err
			}

			results = append(results, result)
		}
	}

	return results, nil
}

func (t *TableV1) structValue() ydb_types.Value {
	return ydb_types.StructValue(
		ydb_types.StructFieldValue("id", ydb_types.UTF8Value(t.id)),
		ydb_types.StructFieldValue("val1", ydb_types.UTF8Value(t.val1)),
	)
}

func tableV1StructTypeString() string {
	return `Struct<
		id: Utf8,
		val1: Utf8>`
}

func tableV1TableDescription() CreateTableDescription {
	return NewCreateTableDescription(
		WithColumn("id", ydb_types.Optional(ydb_types.TypeUTF8)),
		WithColumn("val1", ydb_types.Optional(ydb_types.TypeUTF8)),
		WithPrimaryKeyColumn("id"),
	)
}

func insertTableV1(ctx context.Context, db *YDBClient, path string, table string, val TableV1) error {
	return db.Execute(
		ctx,
		func(ctx context.Context, session *Session) error {
			_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $values as List<%v>;

				upsert into %v
				select *
				from AS_TABLE($values)
			`, path, tableV1StructTypeString(), table), ydb_table.NewQueryParameters(
				ydb_table.ValueParam("$values", ydb_types.ListValue(val.structValue())),
			))
			return err
		},
	)
}

func selectTableV1(ctx context.Context, db *YDBClient, path string, table string) ([]TableV1, error) {
	var vals []TableV1
	err := db.Execute(
		ctx,
		func(ctx context.Context, session *Session) error {
			res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";

				select *
				from %v
			`, path, table), ydb_table.NewQueryParameters())
			if err != nil {
				return err
			}
			defer res.Close()
			vals, err = scanTableV1s(ctx, res)
			return err
		},
	)
	return vals, err
}

////////////////////////////////////////////////////////////////////////////////

type TableV2 struct {
	id   string
	val1 string
	val2 string
}

func scanTableV2s(ctx context.Context, res ydb_result.Result) (results []TableV2, err error) {
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var result TableV2
			err = res.ScanNamed(
				ydb_named.OptionalWithDefault("id", &result.id),
				ydb_named.OptionalWithDefault("val1", &result.val1),
				ydb_named.OptionalWithDefault("val2", &result.val2),
			)
			if err != nil {
				return nil, err
			}

			results = append(results, result)
		}
	}

	return results, nil
}

func (t *TableV2) structValue() ydb_types.Value {
	return ydb_types.StructValue(
		ydb_types.StructFieldValue("id", ydb_types.UTF8Value(t.id)),
		ydb_types.StructFieldValue("val1", ydb_types.UTF8Value(t.val1)),
		ydb_types.StructFieldValue("val2", ydb_types.UTF8Value(t.val2)),
	)
}

func tableV2StructTypeString() string {
	return `Struct<
		id: Utf8,
		val1: Utf8,
		val2: Utf8>`
}

func tableV2TableDescription() CreateTableDescription {
	return NewCreateTableDescription(
		WithColumn("id", ydb_types.Optional(ydb_types.TypeUTF8)),
		WithColumn("val1", ydb_types.Optional(ydb_types.TypeUTF8)),
		WithColumn("val2", ydb_types.Optional(ydb_types.TypeUTF8)),
		WithPrimaryKeyColumn("id"),
	)
}

func insertTableV2(ctx context.Context, db *YDBClient, path string, table string, val TableV2) error {
	return db.Execute(
		ctx,
		func(ctx context.Context, session *Session) error {
			_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";
				declare $values as List<%v>;

				upsert into %v
				select *
				from AS_TABLE($values)
			`, path, tableV2StructTypeString(), table), ydb_table.NewQueryParameters(
				ydb_table.ValueParam("$values", ydb_types.ListValue(val.structValue())),
			))
			return err
		},
	)
}

func selectTableV2(ctx context.Context, db *YDBClient, path string, table string) ([]TableV2, error) {
	var vals []TableV2
	err := db.Execute(
		ctx,
		func(ctx context.Context, session *Session) error {
			res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
				--!syntax_v1
				pragma TablePathPrefix = "%v";

				select *
				from %v
			`, path, table), ydb_table.NewQueryParameters())
			if err != nil {
				return err
			}
			defer res.Close()
			vals, err = scanTableV2s(ctx, res)
			return err
		},
	)
	return vals, err
}

////////////////////////////////////////////////////////////////////////////////

func TestYDBCreateTableV1(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	folder := fmt.Sprintf("ydb_test/%v", t.Name())
	table := "table"
	fullPath := db.AbsolutePath(folder)

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		table,
		tableV1TableDescription(),
		false, // dropUnusedColumns
	)
	require.NoError(t, err)

	val1 := TableV1{
		id:   "id1",
		val1: "value1",
	}

	err = insertTableV1(ctx, db, fullPath, table, val1)
	require.NoError(t, err)

	vals, err := selectTableV1(ctx, db, fullPath, table)
	require.NoError(t, err)
	assert.ElementsMatch(t, []TableV1{val1}, vals)
}

func TestYDBCreateTableV1Twice(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	folder := fmt.Sprintf("ydb_test/%v", t.Name())
	table := "table"
	fullPath := db.AbsolutePath(folder)

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		table,
		tableV1TableDescription(),
		false, // dropUnusedColumns
	)
	require.NoError(t, err)

	val1 := TableV1{
		id:   "id1",
		val1: "value1",
	}

	err = insertTableV1(ctx, db, fullPath, table, val1)
	require.NoError(t, err)

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		table,
		tableV1TableDescription(),
		false, // dropUnusedColumns
	)
	require.NoError(t, err)

	val2 := TableV1{
		id:   "id2",
		val1: "value2",
	}

	err = insertTableV1(ctx, db, fullPath, table, val2)
	require.NoError(t, err)

	vals, err := selectTableV1(ctx, db, fullPath, table)
	require.NoError(t, err)
	assert.ElementsMatch(t, []TableV1{val1, val2}, vals)
}

func TestYDBMigrateTableV1ToTableV2(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	folder := fmt.Sprintf("ydb_test/%v", t.Name())
	table := "table"
	fullPath := db.AbsolutePath(folder)

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		table,
		tableV1TableDescription(),
		false, // dropUnusedColumns
	)
	require.NoError(t, err)

	val1 := TableV1{
		id:   "id1",
		val1: "value1",
	}

	err = insertTableV1(ctx, db, fullPath, table, val1)
	require.NoError(t, err)

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		table,
		tableV2TableDescription(),
		false, // dropUnusedColumns
	)
	require.NoError(t, err)

	val2 := TableV2{
		id:   "id2",
		val1: "value2",
		val2: "other2",
	}

	err = insertTableV2(ctx, db, fullPath, table, val2)
	require.NoError(t, err)

	val1Migrated := TableV2{
		id:   val1.id,
		val1: val1.val1,
		val2: "",
	}

	vals, err := selectTableV2(ctx, db, fullPath, table)
	require.NoError(t, err)
	assert.ElementsMatch(t, []TableV2{val1Migrated, val2}, vals)
}

func TestYDBUseV1InV2(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	folder := fmt.Sprintf("ydb_test/%v", t.Name())
	table := "table"
	fullPath := db.AbsolutePath(folder)

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		table,
		tableV2TableDescription(),
		false, // dropUnusedColumns
	)
	require.NoError(t, err)

	val1 := TableV2{
		id:   "id1",
		val1: "value1",
		val2: "other1",
	}

	err = insertTableV2(ctx, db, fullPath, table, val1)
	require.NoError(t, err)

	val2 := TableV1{
		id:   "id2",
		val1: "value2",
	}

	err = insertTableV1(ctx, db, fullPath, table, val2)
	require.NoError(t, err)

	val1Transformed := TableV1{
		id:   val1.id,
		val1: val1.val1,
	}

	val2Migrated := TableV2{
		id:   val2.id,
		val1: val2.val1,
		val2: "",
	}

	valsV1, err := selectTableV1(ctx, db, fullPath, table)
	require.NoError(t, err)
	assert.ElementsMatch(t, []TableV1{val1Transformed, val2}, valsV1)

	valsV2, err := selectTableV2(ctx, db, fullPath, table)
	require.NoError(t, err)
	assert.ElementsMatch(t, []TableV2{val1, val2Migrated}, valsV2)
}

func TestYDBFailMigrationChangingType(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	folder := fmt.Sprintf("ydb_test/%v", t.Name())
	table := "table"

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		table,
		tableV1TableDescription(),
		false, // dropUnusedColumns
	)
	require.NoError(t, err)

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		table,
		NewCreateTableDescription(
			WithColumn("id", ydb_types.Optional(ydb_types.TypeUTF8)),
			WithColumn("val1", ydb_types.Optional(ydb_types.TypeUint64)),
			WithPrimaryKeyColumn("id"),
		),
		false, // dropUnusedColumns
	)
	assert.Error(t, err)
}

func TestYDBFailMigrationChangingPrimaryKey(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx)
	require.NoError(t, err)
	defer db.Close(ctx)

	folder := fmt.Sprintf("ydb_test/%v", t.Name())
	table := "table"

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		table,
		tableV1TableDescription(),
		false, // dropUnusedColumns
	)
	require.NoError(t, err)

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		table,
		NewCreateTableDescription(
			WithColumn("id", ydb_types.Optional(ydb_types.TypeUTF8)),
			WithColumn("val1", ydb_types.Optional(ydb_types.TypeUTF8)),
			WithPrimaryKeyColumn("id", "val1"),
		),
		false, // dropUnusedColumns
	)
	assert.Error(t, err)
}
