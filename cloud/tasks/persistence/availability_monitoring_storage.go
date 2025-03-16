package persistence

import (
	"context"
	"fmt"
	"time"

	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

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

func componentsAvailabilityTableDescription() CreateTableDescription {
	return NewCreateTableDescription(
		WithColumn("host", Optional(TypeUTF8)),
		WithColumn("component", Optional(TypeUTF8)),
		WithColumn("availability", Optional(TypeBool)),
		WithColumn("updated_at", Optional(TypeTimestamp)),
		WithPrimaryKeyColumn("host", "component"),
	)
}

func componentsAvailabilityStructTypeString() string {
	return `Struct<
		host: Utf8,
		component: Utf8,
		availability: Bool,
		updated_at: Timestamp>`
}

func CreateAvailabilityMonitoringYDBTables(
	ctx context.Context,
	storageFolder string,
	db *YDBClient,
	dropUnusedColumns bool,
) error {

	err := db.CreateOrAlterTable(
		ctx,
		storageFolder,
		"availability_monitoring",
		availabilityMonitoringTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}

	err = db.CreateOrAlterTable(
		ctx,
		storageFolder,
		"components_availability",
		componentsAvailabilityTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}

	return nil
}

func DropAvailabilityMonitoringYDBTables(
	ctx context.Context,
	storageFolder string,
	db *YDBClient,
) error {

	err := db.DropTable(ctx, storageFolder, "availability_monitoring")
	if err != nil {
		return err
	}

	err = db.DropTable(ctx, storageFolder, "components_availability")
	if err != nil {
		return err
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

type AvailabilityMonitoringStorageYDB struct {
	db               *YDBClient
	tablesPath       string
	banInterval      time.Duration
	maxBanHostsCount uint64
}

func NewAvailabilityMonitoringStorage(
	config *tasks_config.TasksConfig,
	db *YDBClient,
	banInterval time.Duration,
	maxBanHostsCount uint64,
) *AvailabilityMonitoringStorageYDB {

	return &AvailabilityMonitoringStorageYDB{
		db:         db,
		tablesPath: db.AbsolutePath(config.GetStorageFolder()),

		banInterval:      banInterval,
		maxBanHostsCount: maxBanHostsCount,
	}
}

////////////////////////////////////////////////////////////////////////////////

// This is mapped into a DB row. If you change this struct, make sure to update
// the mapping code.
type AvailabilityMonitoringResult struct {
	Component   string
	Host        string
	CreatedAt   time.Time
	SuccessRate float64
}

// This is mapped into a DB row. If you change this struct, make sure to update
// the mapping code.
type ComponentsAvailabilityResult struct {
	Host         string
	Component    string
	Availability bool
	UpdatedAt    time.Time
}

////////////////////////////////////////////////////////////////////////////////

func scanAvailabilityMonitoringResult(
	res Result,
) (AvailabilityMonitoringResult, error) {

	var result AvailabilityMonitoringResult
	err := res.ScanNamed(
		OptionalWithDefault("component", &result.Component),
		OptionalWithDefault("host", &result.Host),
		OptionalWithDefault("created_at", &result.CreatedAt),
		OptionalWithDefault("success_rate", &result.SuccessRate),
	)

	return result, err
}

func scanAvailabilityMonitoringResults(
	ctx context.Context,
	res Result,
) ([]AvailabilityMonitoringResult, error) {

	var results []AvailabilityMonitoringResult
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

func scanComponentsAvailabilityResult(
	res Result,
) (ComponentsAvailabilityResult, error) {

	var result ComponentsAvailabilityResult
	err := res.ScanNamed(
		OptionalWithDefault("host", &result.Host),
		OptionalWithDefault("component", &result.Component),
		OptionalWithDefault("availability", &result.Availability),
		OptionalWithDefault("updated_at", &result.UpdatedAt),
	)

	return result, err
}

func (r *ComponentsAvailabilityResult) structValue() Value {
	return StructValue(
		StructFieldValue("host", UTF8Value(r.Host)),
		StructFieldValue("component", UTF8Value(r.Component)),
		StructFieldValue("availability", BoolValue(r.Availability)),
		StructFieldValue("updated_at", TimestampValue(r.UpdatedAt)),
	)
}

func scanComponentsAvailabilityResults(
	ctx context.Context,
	res Result,
) ([]ComponentsAvailabilityResult, error) {

	var results []ComponentsAvailabilityResult
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			result, err := scanComponentsAvailabilityResult(res)
			if err != nil {
				return nil, err
			}

			results = append(results, result)
		}
	}

	return results, nil
}

////////////////////////////////////////////////////////////////////////////////

func (s *AvailabilityMonitoringStorageYDB) getAvailabilityMonitoringResultsTx(
	ctx context.Context,
	tx *Transaction,
	host string,
	component string,
	lastSignificantResultTS time.Time,
) ([]AvailabilityMonitoringResult, error) {

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $host as Utf8;
		declare $component as Utf8;
		declare $lastSignificantResultTS as Timestamp;

		select *
		from availability_monitoring
		where host = $host
		and component = $component
		and created_at >= $lastSignificantResultTS
		order by created_at
	`, s.tablesPath),
		ValueParam("$host", UTF8Value(host)),
		ValueParam("$component", UTF8Value(component)),
		ValueParam("$lastSignificantResultTS", TimestampValue(lastSignificantResultTS)),
	)
	if err != nil {
		return []AvailabilityMonitoringResult{}, err
	}
	defer res.Close()

	results, err := scanAvailabilityMonitoringResults(ctx, res)
	if err != nil {
		return []AvailabilityMonitoringResult{}, err
	}

	return results, nil
}

func (s *AvailabilityMonitoringStorageYDB) getAvailabilityMonitoringResults(
	ctx context.Context,
	session *Session,
	host string,
	component string,
	lastSignificantResultTS time.Time,
) ([]AvailabilityMonitoringResult, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return []AvailabilityMonitoringResult{}, err
	}
	defer tx.Rollback(ctx)

	results, err := s.getAvailabilityMonitoringResultsTx(
		ctx,
		tx,
		host,
		component,
		lastSignificantResultTS,
	)
	if err != nil {
		return []AvailabilityMonitoringResult{}, err
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

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $host as Utf8;
		declare $component as Utf8;

		select *
		from availability_monitoring
		where host = $host and component = $component
		order by created_at
	`, s.tablesPath),
		ValueParam("$host", UTF8Value(host)),
		ValueParam("$component", UTF8Value(component)),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	results, err := scanAvailabilityMonitoringResults(ctx, res)
	if err != nil {
		return err
	}

	if len(results) > 100 {
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
			ValueParam("$created_at", TimestampValue(results[0].CreatedAt)),
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

func (s *AvailabilityMonitoringStorageYDB) getComponentsAvailabilityResults(
	ctx context.Context,
	session *Session,
	host string,
) ([]ComponentsAvailabilityResult, error) {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return []ComponentsAvailabilityResult{}, err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $host as Utf8;

		select *
		from components_availability
		where host = $host
	`, s.tablesPath),
		ValueParam("$host", UTF8Value(host)),
	)
	if err != nil {
		return []ComponentsAvailabilityResult{}, err
	}
	defer res.Close()

	results, err := scanComponentsAvailabilityResults(ctx, res)
	if err != nil {
		return []ComponentsAvailabilityResult{}, err
	}

	return results, tx.Commit(ctx)
}

func (s *AvailabilityMonitoringStorageYDB) updateComponentAvailability(
	ctx context.Context,
	session *Session,
	host string,
	component string,
	availability bool,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if !availability {
		res, err := tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $host as Utf8;
			declare $component as Utf8;
			declare $availability as Bool;

			select *
			from components_availability
			where host = $host
			and component = $component
			and availability = $availability
		`, s.tablesPath),
			ValueParam("$host", UTF8Value(host)),
			ValueParam("$component", UTF8Value(component)),
			ValueParam("$availability", BoolValue(availability)))
		if err != nil {
			return err
		}
		defer res.Close()

		results, err := scanComponentsAvailabilityResults(ctx, res)
		if err != nil {
			return err
		}

		logging.Info(ctx, "pre results is %v", results)
		if len(results) > 0 {
			return tx.Commit(ctx)
		}

		res, err = tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $host as Utf8;
			declare $component as Utf8;
			declare $availability as Bool;

			select *
			from components_availability
			where component = $component
			and availability = $availability
		`, s.tablesPath),
			ValueParam("$component", UTF8Value(component)),
			ValueParam("$availability", BoolValue(availability)))
		if err != nil {
			return err
		}
		defer res.Close()

		results, err = scanComponentsAvailabilityResults(ctx, res)
		if err != nil {
			return err
		}

		if len(results) >= int(s.maxBanHostsCount) {
			// nothing to do
			return tx.Commit(ctx)
		}
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $host as Utf8;
		declare $component as Utf8;
		declare $availability as Bool;
		declare $updated_at as Timestamp;

		upsert into components_availability (host, component, availability, updated_at)
		values ($host, $component, $availability, $updated_at);
	`, s.tablesPath),
		ValueParam("$host", UTF8Value(host)),
		ValueParam("$component", UTF8Value(component)),
		ValueParam("$availability", BoolValue(availability)),
		ValueParam("$updated_at", TimestampValue(time.Now())),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *AvailabilityMonitoringStorageYDB) updateComponentsBackFromUnavailable(
	ctx context.Context,
	session *Session,
	host string,
	component string,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	res, err := tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $host as Utf8;
		declare $component as Utf8;
		declare $updated_at as Timestamp;

		select *
		from components_availability
		where host = $host
		and component = $component
		and availability = false
		and updated_at <= $updated_at
	`, s.tablesPath),
		ValueParam("$host", UTF8Value(host)),
		ValueParam("$component", UTF8Value(component)),
		ValueParam("$updated_at", TimestampValue(time.Now().Add(-s.banInterval))),
	)
	if err != nil {
		return err
	}
	defer res.Close()

	results, err := scanComponentsAvailabilityResults(ctx, res)
	if err != nil {
		return err
	}

	logging.Info(ctx, "updateComponentsBackFromUnavailable results %+v", results)

	if len(results) == 0 {
		return tx.Commit(ctx)
	}

	var values []Value
	for _, result := range results {
		result.Availability = true
		result.UpdatedAt = time.Now()
		values = append(values, result.structValue())
	}

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $componentsAvailability as List<%v>;

		upsert into components_availability
		select *
		from AS_TABLE($componentsAvailability)
	`, s.tablesPath, componentsAvailabilityStructTypeString()),
		ValueParam("$componentsAvailability", ListValue(values...)),
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

func (s *AvailabilityMonitoringStorageYDB) GetAvailabilityMonitoringResults(
	ctx context.Context,
	host string,
	component string,
	lastSignificantResultTS time.Time,
) ([]AvailabilityMonitoringResult, error) {

	var results []AvailabilityMonitoringResult

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *Session) error {
			var err error
			results, err = s.getAvailabilityMonitoringResults(
				ctx,
				session,
				host,
				component,
				lastSignificantResultTS,
			)
			return err
		},
	)
	return results, err
}

func (s *AvailabilityMonitoringStorageYDB) UpdateComponentAvailability(
	ctx context.Context,
	host string,
	component string,
	availability bool,
) error {

	return s.db.Execute(ctx, func(ctx context.Context, session *Session) error {
		return s.updateComponentAvailability(ctx, session, host, component, availability)
	})
}

func (s *AvailabilityMonitoringStorageYDB) GetComponentsAvailabilityResults(
	ctx context.Context,
	host string,
) ([]ComponentsAvailabilityResult, error) {

	var results []ComponentsAvailabilityResult

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *Session) error {
			var err error
			results, err = s.getComponentsAvailabilityResults(
				ctx,
				session,
				host,
			)
			return err
		},
	)
	return results, err
}

func (s *AvailabilityMonitoringStorageYDB) UpdateComponentsBackFromUnavailable(
	ctx context.Context,
	host string,
	component string,
) error {

	return s.db.Execute(ctx, func(ctx context.Context, session *Session) error {
		return s.updateComponentsBackFromUnavailable(ctx, session, host, component)
	})
}

// tbd not ban component on more than X (from config) hosts
