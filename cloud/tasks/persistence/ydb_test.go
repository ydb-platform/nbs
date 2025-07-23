package persistence

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/empty"
	"github.com/ydb-platform/nbs/cloud/tasks/metrics/mocks"
	persistence_config "github.com/ydb-platform/nbs/cloud/tasks/persistence/config"
	"github.com/ydb-platform/ydb-go-sdk/v3"
	"github.com/ydb-platform/ydb-go-sdk/v3/table/options"
)

////////////////////////////////////////////////////////////////////////////////

func newContext() context.Context {
	return logging.SetLogger(
		context.Background(),
		logging.NewStderrLogger(logging.DebugLevel),
	)
}

func newYDB(
	ctx context.Context,
	metricsRegistry metrics.Registry,
) (*YDBClient, error) {

	endpoint := os.Getenv("YDB_ENDPOINT")
	database := os.Getenv("YDB_DATABASE")
	rootPath := "disk_manager"

	return NewYDBClient(
		ctx,
		&persistence_config.PersistenceConfig{
			Endpoint: &endpoint,
			Database: &database,
			RootPath: &rootPath,
		},
		metricsRegistry,
	)
}

////////////////////////////////////////////////////////////////////////////////

type TableV1 struct {
	id   string
	val1 string
}

func scanTableV1s(ctx context.Context, res Result) (results []TableV1, err error) {
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var result TableV1
			err = res.ScanNamed(
				OptionalWithDefault("id", &result.id),
				OptionalWithDefault("val1", &result.val1),
			)
			if err != nil {
				return nil, err
			}

			results = append(results, result)
		}
	}

	return results, nil
}

func (t *TableV1) structValue() Value {
	return StructValue(
		StructFieldValue("id", UTF8Value(t.id)),
		StructFieldValue("val1", UTF8Value(t.val1)),
	)
}

func tableV1StructTypeString() string {
	return `Struct<
		id: Utf8,
		val1: Utf8>`
}

func tableV1TableDescription() CreateTableDescription {
	return NewCreateTableDescription(
		WithColumn("id", Optional(TypeUTF8)),
		WithColumn("val1", Optional(TypeUTF8)),
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
			`, path, tableV1StructTypeString(), table),
				ValueParam("$values", ListValue(val.structValue())),
			)
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
			`, path, table,
			))
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

func scanTableV2s(ctx context.Context, res Result) (results []TableV2, err error) {
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var result TableV2
			err = res.ScanNamed(
				OptionalWithDefault("id", &result.id),
				OptionalWithDefault("val1", &result.val1),
				OptionalWithDefault("val2", &result.val2),
			)
			if err != nil {
				return nil, err
			}

			results = append(results, result)
		}
	}

	return results, nil
}

func (t *TableV2) structValue() Value {
	return StructValue(
		StructFieldValue("id", UTF8Value(t.id)),
		StructFieldValue("val1", UTF8Value(t.val1)),
		StructFieldValue("val2", UTF8Value(t.val2)),
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
		WithColumn("id", Optional(TypeUTF8)),
		WithColumn("val1", Optional(TypeUTF8)),
		WithColumn("val2", Optional(TypeUTF8)),
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
			`, path, tableV2StructTypeString(), table),
				ValueParam("$values", ListValue(val.structValue())),
			)
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
			`, path, table,
			))
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

	db, err := newYDB(ctx, empty.NewRegistry())
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

	db, err := newYDB(ctx, empty.NewRegistry())
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

	db, err := newYDB(ctx, empty.NewRegistry())
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

	db, err := newYDB(ctx, empty.NewRegistry())
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

	db, err := newYDB(ctx, empty.NewRegistry())
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
			WithColumn("id", Optional(TypeUTF8)),
			WithColumn("val1", Optional(TypeUint64)),
			WithPrimaryKeyColumn("id"),
		),
		false, // dropUnusedColumns
	)
	assert.Error(t, err)
}

func TestYDBFailMigrationChangingPrimaryKey(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx, empty.NewRegistry())
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
			WithColumn("id", Optional(TypeUTF8)),
			WithColumn("val1", Optional(TypeUTF8)),
			WithPrimaryKeyColumn("id", "val1"),
		),
		false, // dropUnusedColumns
	)
	assert.Error(t, err)
}

func TestYDBShouldSendSchemeErrorMetric(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	metricsRegistry := mocks.NewRegistryMock()

	db, err := newYDB(ctx, metricsRegistry)
	require.NoError(t, err)
	defer db.Close(ctx)

	metricsRegistry.GetCounter(
		"errors",
		map[string]string{"call": "client/makeDirs", "type": "scheme"},
	).On("Inc").Once()

	// YDB has limited length of object name. Current limit is 255.
	extraLargeString := strings.Repeat("x", 100500)
	folder := fmt.Sprintf("ydb_test/%s/%s", extraLargeString, t.Name())
	table := "table"

	err = db.CreateOrAlterTable(
		ctx,
		folder,
		table,
		tableV1TableDescription(),
		false, // dropUnusedColumns
	)
	require.True(t, ydb.IsOperationErrorSchemeError(err))

	metricsRegistry.AssertAllExpectations(t)
}

type migrationFixture struct {
	t                 *testing.T
	ctx               context.Context
	db                *YDBClient
	folder            string
	table             string
	dropUnusedColumns bool
}

func migrateAndValidate(
	fixtures migrationFixture,
	newDescription CreateTableDescription,
	validate func(options.Description),
) {
	err := fixtures.db.CreateOrAlterTable(
		fixtures.ctx,
		fixtures.folder,
		fixtures.table,
		newDescription,
		fixtures.dropUnusedColumns,
	)
	require.NoError(fixtures.t, err)

	err = fixtures.db.Execute(fixtures.ctx, func(ctx context.Context, session *Session) error {
		path := fixtures.db.AbsolutePath(fixtures.folder, fixtures.table)
		description, err := session.session.DescribeTable(ctx, path)
		require.NoError(fixtures.t, err)

		validate(description)
		return nil
	})
	require.NoError(fixtures.t, err)
}

func TestYDBMigrateSecondaryKeys(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx, empty.NewRegistry())
	require.NoError(t, err)
	defer db.Close(ctx)

	fixtures := migrationFixture{
		t:                 t,
		ctx:               ctx,
		db:                db,
		folder:            fmt.Sprintf("ydb_test/%v", t.Name()),
		table:             "table",
		dropUnusedColumns: false,
	}

	// Create table with index on val1
	migrateAndValidate(
		fixtures,
		NewCreateTableDescription(
			WithColumn("id", Optional(TypeUTF8)),
			WithColumn("val1", Optional(TypeUTF8)),
			WithColumn("val2", Optional(TypeUTF8)),
			WithPrimaryKeyColumn("id"),
			WithSecondaryKeyColumn("val1"),
		),
		func(description options.Description) {
			require.Equal(t, 1, len(description.Indexes))
			require.Equal(t, "val1_index", description.Indexes[0].Name)
			require.Equal(t, []string{"val1"}, description.Indexes[0].IndexColumns)
		},
	)

	// Delete index on val1 and create index on val2
	migrateAndValidate(
		fixtures,
		NewCreateTableDescription(
			WithColumn("id", Optional(TypeUTF8)),
			WithColumn("val1", Optional(TypeUTF8)),
			WithColumn("val2", Optional(TypeUTF8)),
			WithPrimaryKeyColumn("id"),
			WithSecondaryKeyColumn("val2"),
		),
		func(description options.Description) {
			require.Equal(t, 1, len(description.Indexes))
			require.Equal(t, "val2_index", description.Indexes[0].Name)
			require.Equal(t, []string{"val2"}, description.Indexes[0].IndexColumns)
		},
	)

	// Create index on val1 again
	migrateAndValidate(
		fixtures,
		NewCreateTableDescription(
			WithColumn("id", Optional(TypeUTF8)),
			WithColumn("val1", Optional(TypeUTF8)),
			WithColumn("val2", Optional(TypeUTF8)),
			WithPrimaryKeyColumn("id"),
			WithSecondaryKeyColumn("val2", "val1"),
		),
		func(description options.Description) {
			require.Equal(t, 2, len(description.Indexes))
			// Indexes are sorted alphabetically
			require.Equal(t, "val1_index", description.Indexes[0].Name)
			require.Equal(t, []string{"val1"}, description.Indexes[0].IndexColumns)
			require.Equal(t, "val2_index", description.Indexes[1].Name)
			require.Equal(t, []string{"val2"}, description.Indexes[1].IndexColumns)
		},
	)

	// Remove both indexes
	migrateAndValidate(
		fixtures,
		NewCreateTableDescription(
			WithColumn("id", Optional(TypeUTF8)),
			WithColumn("val1", Optional(TypeUTF8)),
			WithColumn("val2", Optional(TypeUTF8)),
			WithPrimaryKeyColumn("id"),
		),
		func(description options.Description) {
			require.Equal(t, 0, len(description.Indexes))
		},
	)
}

func TestYDBMigrateColumnsWithSecondaryKeys(t *testing.T) {
	ctx, cancel := context.WithCancel(newContext())
	defer cancel()

	db, err := newYDB(ctx, empty.NewRegistry())
	require.NoError(t, err)
	defer db.Close(ctx)

	fixtures := migrationFixture{
		t:                 t,
		ctx:               ctx,
		db:                db,
		folder:            fmt.Sprintf("ydb_test/%v", t.Name()),
		table:             "table",
		dropUnusedColumns: true, // set to true in this test
	}

	// Create table
	migrateAndValidate(
		fixtures,
		NewCreateTableDescription(
			WithColumn("id", Optional(TypeUTF8)),
			WithColumn("val1", Optional(TypeUTF8)),
			WithPrimaryKeyColumn("id"),
		),
		func(description options.Description) {},
	)

	// Add val2 column with index
	migrateAndValidate(
		fixtures,
		NewCreateTableDescription(
			WithColumn("id", Optional(TypeUTF8)),
			WithColumn("val1", Optional(TypeUTF8)),
			WithColumn("val2", Optional(TypeUTF8)),
			WithPrimaryKeyColumn("id"),
			WithSecondaryKeyColumn("val2"),
		),
		func(description options.Description) {
			require.Equal(t, 3, len(description.Columns))
			require.Equal(t, "val2", description.Columns[2].Name)

			require.Equal(t, 1, len(description.Indexes))
			require.Equal(t, "val2_index", description.Indexes[0].Name)
			require.Equal(t, []string{"val2"}, description.Indexes[0].IndexColumns)
		},
	)

	// Drop val2 column, add val3 column with index, add index to val1
	migrateAndValidate(
		fixtures,
		NewCreateTableDescription(
			WithColumn("id", Optional(TypeUTF8)),
			WithColumn("val1", Optional(TypeUTF8)),
			WithColumn("val3", Optional(TypeUTF8)),
			WithPrimaryKeyColumn("id"),
			WithSecondaryKeyColumn("val1", "val3"),
		),
		func(description options.Description) {
			require.Equal(t, 3, len(description.Columns))
			require.Equal(t, "val3", description.Columns[2].Name)

			require.Equal(t, 2, len(description.Indexes))
			require.Equal(t, "val1_index", description.Indexes[0].Name)
			require.Equal(t, []string{"val1"}, description.Indexes[0].IndexColumns)
			require.Equal(t, "val3_index", description.Indexes[1].Name)
			require.Equal(t, []string{"val3"}, description.Indexes[1].IndexColumns)
		},
	)

	// Remove val1 column, other indexes are being unchanged
	migrateAndValidate(
		fixtures,
		NewCreateTableDescription(
			WithColumn("id", Optional(TypeUTF8)),
			WithColumn("val3", Optional(TypeUTF8)),
			WithPrimaryKeyColumn("id"),
			WithSecondaryKeyColumn("val3"),
		),
		func(description options.Description) {
			require.Equal(t, 2, len(description.Columns))
			require.Equal(t, "val3", description.Columns[1].Name)

			require.Equal(t, 1, len(description.Indexes))
			require.Equal(t, "val3_index", description.Indexes[0].Name)
			require.Equal(t, []string{"val3"}, description.Indexes[0].IndexColumns)
		},
	)

	// Remove val3 column
	migrateAndValidate(
		fixtures,
		NewCreateTableDescription(
			WithColumn("id", Optional(TypeUTF8)),
			WithPrimaryKeyColumn("id"),
		),
		func(description options.Description) {
			require.Equal(t, 1, len(description.Columns))
			require.Equal(t, "id", description.Columns[0].Name)

			require.Equal(t, 0, len(description.Indexes))
		},
	)
}
