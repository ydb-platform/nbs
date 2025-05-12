package persistence

import (
	"context"
	"fmt"
	"time"

	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
)

////////////////////////////////////////////////////////////////////////////////

const availabilityMonitoringResultsCountLimit = 100

////////////////////////////////////////////////////////////////////////////////

func availabilityMonitoringTableDescription() CreateTableDescription {
	return NewCreateTableDescription(
		WithColumn("component", Optional(TypeUTF8)),
		WithColumn("host", Optional(TypeUTF8)),
		WithColumn("created_at", Optional(TypeTimestamp)),
		WithColumn("success_rate", Optional(TypeDouble)),
		WithPrimaryKeyColumn("component", "host", "created_at"),
	)
}

func CreateAvailabilityMonitoringYDBTables(
	ctx context.Context,
	storageFolder string,
	db *YDBClient,
	dropUnusedColumns bool,
) error {

	return db.CreateOrAlterTable(
		ctx,
		storageFolder,
		"availability_monitoring",
		availabilityMonitoringTableDescription(),
		dropUnusedColumns,
	)
}

func DropAvailabilityMonitoringYDBTables(
	ctx context.Context,
	storageFolder string,
	db *YDBClient,
) error {

	return db.DropTable(ctx, storageFolder, "availability_monitoring")
}

////////////////////////////////////////////////////////////////////////////////

type AvailabilityMonitoringStorageYDB struct {
	db         *YDBClient
	tablesPath string
}

func NewAvailabilityMonitoringStorage(
	config *tasks_config.TasksConfig,
	db *YDBClient,
) *AvailabilityMonitoringStorageYDB {

	return &AvailabilityMonitoringStorageYDB{
		db:         db,
		tablesPath: db.AbsolutePath(config.GetStorageFolder()),
	}
}

////////////////////////////////////////////////////////////////////////////////

// This is mapped into a DB row. If you change this struct, make sure to update
// the mapping code.
type availabilityMonitoringResult struct {
	component   string
	host        string
	createdAt   time.Time
	successRate float64
}

func scanAvailabilityMonitoringResult(
	res Result,
) (availabilityMonitoringResult, error) {

	var result availabilityMonitoringResult
	err := res.ScanNamed(
		OptionalWithDefault("component", &result.component),
		OptionalWithDefault("host", &result.host),
		OptionalWithDefault("created_at", &result.createdAt),
		OptionalWithDefault("success_rate", &result.successRate),
	)

	return result, err
}

func scanAvailabilityMonitoringResults(
	ctx context.Context,
	res Result,
) ([]availabilityMonitoringResult, error) {

	var results []availabilityMonitoringResult
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			result, err := scanAvailabilityMonitoringResult(res)
			if err != nil {
				return nil, err
			}

			results = append(results, result)
		}
	}

	return results, nil
}

func (s *AvailabilityMonitoringStorageYDB) getAvailabilityMonitoringResultsTx(
	ctx context.Context,
	tx *Transaction,
	component string,
	host string,
) ([]availabilityMonitoringResult, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $component as Utf8;
		declare $host as Utf8;
		declare $created_at as Timestamp;

		select *
		from availability_monitoring
		where component = $component and host = $host
		order by created_at
	`, s.tablesPath),
		ValueParam("$component", UTF8Value(component)),
		ValueParam("$host", UTF8Value(host)),
	)
	if err != nil {
		return []availabilityMonitoringResult{}, err
	}
	defer res.Close()

	return scanAvailabilityMonitoringResults(ctx, res)
}

func (s *AvailabilityMonitoringStorageYDB) getAvailabilityMonitoringResults(
	ctx context.Context,
	session *Session,
	component string,
	host string,
) ([]availabilityMonitoringResult, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return []availabilityMonitoringResult{}, err
	}
	defer tx.Rollback(ctx)

	results, err := s.getAvailabilityMonitoringResultsTx(
		ctx,
		tx,
		component,
		host,
	)
	if err != nil {
		return []availabilityMonitoringResult{}, err
	}

	return results, tx.Commit(ctx)
}

func (s *AvailabilityMonitoringStorageYDB) updateSuccessRate(
	ctx context.Context,
	session *Session,
	component string,
	host string,
	successRate float64,
	ts time.Time,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	results, err := s.getAvailabilityMonitoringResultsTx(
		ctx,
		tx,
		component,
		host,
	)
	if err != nil {
		return err
	}

	if len(results) > availabilityMonitoringResultsCountLimit {
		_, err = tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $component as Utf8;
			declare $host as Utf8;
			declare $created_at as Timestamp;

			delete from availability_monitoring
			where component = $component and host = $host and created_at = $created_at
		`, s.tablesPath),
			ValueParam("$component", UTF8Value(component)),
			ValueParam("$host", UTF8Value(host)),
			ValueParam("$created_at", TimestampValue(results[0].createdAt)),
		)
		if err != nil {
			return err
		}
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $component as Utf8;
		declare $host as Utf8;
		declare $success_rate as Double;
		declare $created_at as Timestamp;

		upsert into availability_monitoring (component, host, success_rate, created_at)
		values ($component, $host, $success_rate, $created_at);
	`, s.tablesPath),
		ValueParam("$component", UTF8Value(component)),
		ValueParam("$host", UTF8Value(host)),
		ValueParam("$success_rate", DoubleValue(successRate)),
		ValueParam("$created_at", TimestampValue(ts)),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func (s *AvailabilityMonitoringStorageYDB) UpdateSuccessRate(
	ctx context.Context,
	component string,
	host string,
	value float64,
	ts time.Time,
) error {

	return s.db.Execute(ctx, func(ctx context.Context, session *Session) error {
		return s.updateSuccessRate(ctx, session, component, host, value, ts)
	})
}

// Used in tests.
func (s *AvailabilityMonitoringStorageYDB) GetAvailabilityMonitoringResults(
	ctx context.Context,
	component string,
	host string,
) ([]availabilityMonitoringResult, error) {

	var results []availabilityMonitoringResult

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *Session) error {
			var err error
			results, err = s.getAvailabilityMonitoringResults(
				ctx,
				session,
				component,
				host,
			)
			return err
		},
	)
	return results, err
}
