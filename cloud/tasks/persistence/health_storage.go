package persistence

import (
	"context"
	"fmt"
	"time"
)

////////////////////////////////////////////////////////////////////////////////

func availabilityMonitoringTableDescription() CreateTableDescription {
	return NewCreateTableDescription(
		WithColumn("component", Optional(TypeUTF8)),
		WithColumn("host", Optional(TypeUTF8)),
		WithColumn("update_at", Optional(TypeTimestamp)),
		WithPrimaryKeyColumn("component", "host", "update_at"),
	)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) updateAvailabilityRate(
	ctx context.Context,
	session *Session,
	component string,
	host string,
	value float64,
	ts time.Time,
) error {

	_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $component as Utf8;
		declare $host as Utf8;
		declare $value as Double;
		declare $ts as Timestamp;

		upsert into nodes (component, host, value, ts)
		values ($component, $host, $value, $ts);
	`, s.tablesPath),
		ValueParam("$component", UTF8Value(component)),
		ValueParam("$host", UTF8Value(host)),
		ValueParam("$value", DoubleValue(value)),
		ValueParam("$update_at", TimestampValue(ts)),
	)
	return err
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) UpdateAvailabilityRate(
	ctx context.Context,
	component string,
	host string,
	value float64,
	ts time.Time,
) error {

	return s.db.Execute(ctx, func(ctx context.Context, session *Session) error {
		return s.updateAvailabilityRate(ctx, session, component, host, value, ts)
	})
}
