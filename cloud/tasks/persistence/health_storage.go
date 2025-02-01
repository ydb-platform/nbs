package persistence

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

func healthCheckTableDescription() CreateTableDescription {
	return NewCreateTableDescription(
		WithColumn("component", Optional(TypeUTF8)),
		WithColumn("update_at", Optional(TypeTimestamp)),
		WithPrimaryKeyColumn("component", "update_at"),
	)
}

func CreateYDBTables(
	ctx context.Context,
	db *YDBClient,
	dropUnusedColumns bool,
) error {

	err := db.CreateOrAlterTable(
		ctx,
		"kek",
		"health",
		healthCheckTableDescription(),
		dropUnusedColumns,
	)
	if err != nil {
		return err
	}
	logging.Info(ctx, "Created nodes table")

	return nil
}

type healthCheckStorage struct {
	db         *YDBClient
	tablesPath string
}

type HealthStorage interface {
	HeartbeatNode(
		ctx context.Context,
		// host string,
		ts time.Time,
		// value float64,
	) error
}

func NewStorage(db *YDBClient) HealthStorage {
	return &healthCheckStorage{
		db:         db,
		tablesPath: db.AbsolutePath("test"),
	}
}

// type HealthCheck struct {
// 	componentName string
// 	Rate          float64
// 	LastUpdate    time.Time
// }

////////////////////////////////////////////////////////////////////////////////

// Returns ydb entity of the node object.
// func (h *HealthCheck) structValue() persistence.Value {
// 	return persistence.StructValue(
// 		persistence.StructFieldValue("rate", persistence.DoubleValue(h.Rate)),
// 		persistence.StructFieldValue("last_update", persistence.DatetimeValueFromTime(h.LastUpdate)),
// 	)
// }

// // Scans single node from the YDB result set.
// func scanHealthCheck(result persistence.Result) (healthCheck HealthCheck, err error) {
// 	err = result.ScanNamed(
// 		persistence.OptionalWithDefault("rate", &healthCheck.Rate),
// 		persistence.OptionalWithDefault("last_update", &healthCheck.LastUpdate),
// 	)
// 	return
// }

// // Scans all nodes from the YDB result set.
// func scanNodes(ctx context.Context, res persistence.Result) ([]Node, error) {
// 	var nodes []Node
// 	for res.NextResultSet(ctx) {
// 		for res.NextRow() {
// 			node, err := scanNode(res)
// 			if err != nil {
// 				return nil, err
// 			}
// 			nodes = append(nodes, node)
// 		}
// 	}

// 	return nodes, nil
// }

// Returns node struct definition in YQL.
// func nodeStructTypeString() string {
// 	return `Struct<
// 		host: Utf8,
// 		last_heartbeat: Timestamp,
// 		inflight_task_count: Uint32>`
// }

// Returns table description for the table that holds nodes.
// func healthCheckTableDescription() persistence.CreateTableDescription {
// 	return persistence.NewCreateTableDescription(
// 		persistence.WithColumn("host", persistence.Optional(persistence.TypeUTF8)),
// 		persistence.WithColumn("last_heartbeat", persistence.Optional(persistence.TypeTimestamp)),
// 		persistence.WithColumn("inflight_task_count", persistence.Optional(persistence.TypeUint32)),
// 		persistence.WithPrimaryKeyColumn("host"),
// 	)
// }

////////////////////////////////////////////////////////////////////////////////

// Updates heartbeat timestamp and the current number of inflight tasks.
func (s *healthCheckStorage) heartbeatNode(
	ctx context.Context,
	session *Session,
	// host string,
	ts time.Time,
	// value float64,
) error {

	logging.Debug(
		ctx,
		"KEK IS",
	)

	_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $component as Utf8;
		declare $update_at as Timestamp;

		upsert into nodes (component, update_at)
		values ($component, $update_at);
	`, s.tablesPath),
		ValueParam("$host", UTF8Value("BORIS_BG")),
		ValueParam("$update_at", TimestampValue(ts)),
	)
	return err
}

////////////////////////////////////////////////////////////////////////////////

func (s *healthCheckStorage) HeartbeatNode(
	ctx context.Context,
	// host string,
	ts time.Time,
	// value float64,
) error {

	return s.db.Execute(ctx, func(ctx context.Context, session *Session) error {
		return s.heartbeatNode(ctx, session, ts)
	})
}
