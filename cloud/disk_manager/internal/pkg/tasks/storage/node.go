package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/logging"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/persistence"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/tasks/errors"
	ydb_table "github.com/ydb-platform/ydb-go-sdk/v3/table"
	ydb_result "github.com/ydb-platform/ydb-go-sdk/v3/table/result"
	ydb_named "github.com/ydb-platform/ydb-go-sdk/v3/table/result/named"
	ydb_types "github.com/ydb-platform/ydb-go-sdk/v3/table/types"
)

////////////////////////////////////////////////////////////////////////////////

// Registered node that sends heartbeats.
type Node struct {
	Host string
	// Timestamp of the last heartbeat.
	LastHeartbeat time.Time
	// Number of inflight tasks reported during the last heartbeat.
	InflightTaskCount uint32
}

////////////////////////////////////////////////////////////////////////////////

// Returns ydb entity of the node object.
func (n *Node) structValue() ydb_types.Value {
	return ydb_types.StructValue(
		ydb_types.StructFieldValue("host", ydb_types.UTF8Value(n.Host)),
		ydb_types.StructFieldValue("last_heartbeat", ydb_types.DatetimeValueFromTime(n.LastHeartbeat)),
		ydb_types.StructFieldValue("inflight_task_count", ydb_types.Uint32Value(n.InflightTaskCount)),
	)
}

// Scans single node from the YDB result set.
func scanNode(result ydb_result.BaseResult) (node Node, err error) {
	err = result.ScanNamed(
		ydb_named.OptionalWithDefault("host", &node.Host),
		ydb_named.OptionalWithDefault("last_heartbeat", &node.LastHeartbeat),
		ydb_named.OptionalWithDefault("inflight_task_count", &node.InflightTaskCount),
	)
	return
}

// Scans all nodes from the YDB result set.
func scanNodes(ctx context.Context, res ydb_result.BaseResult) ([]Node, error) {
	var nodes []Node
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			node, err := scanNode(res)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, node)
		}
	}

	return nodes, nil
}

// Returns node struct definition in YQL.
func nodeStructTypeString() string {
	return `Struct<
		host: Utf8,
		last_heartbeat: Timestamp,
		inflight_task_count: Uint32>`
}

// Returns table description for the table that holds nodes.
func nodeTableDescription() persistence.CreateTableDescription {
	return persistence.NewCreateTableDescription(
		persistence.WithColumn("host", ydb_types.Optional(ydb_types.TypeUTF8)),
		persistence.WithColumn("last_heartbeat", ydb_types.Optional(ydb_types.TypeTimestamp)),
		persistence.WithColumn("inflight_task_count", ydb_types.Optional(ydb_types.TypeUint32)),
		persistence.WithPrimaryKeyColumn("host"),
	)
}

////////////////////////////////////////////////////////////////////////////////

// Updates heartbeat timestamp and the current number of inflight tasks.
func (s *storageYDB) heartbeat(
	ctx context.Context,
	session *persistence.Session,
	host string,
	ts time.Time,
	inflightTaskCount uint32,
) error {

	logging.Debug(
		ctx,
		"Sending node %q heartbeat: inflight tasks: %d",
		host,
		inflightTaskCount,
	)

	_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $host as Utf8;
		declare $last_heartbeat_ts as Timestamp;
		declare $inflight_task_count as Uint32;

		upsert into nodes (host, last_heartbeat, inflight_task_count)
		values ($host, $last_heartbeat_ts, $inflight_task_count);
	`, s.tablesPath), ydb_table.NewQueryParameters(
		ydb_table.ValueParam("$host", ydb_types.UTF8Value(host)),
		ydb_table.ValueParam("$inflight_task_count", ydb_types.Uint32Value(inflightTaskCount)),
		ydb_table.ValueParam("$last_heartbeat_ts", ydb_types.TimestampValueFromTime(ts)),
	))
	return err
}

// Fetches up to a limit of active nodes that have sent hearbeats within the window [now() - interval, now].
func (s *storageYDB) getAliveNodes(
	ctx context.Context,
	session *persistence.Session,
) ([]Node, error) {

	livenessTS := time.Now().Add(-s.livenessWindow)
	res, err := session.StreamExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $liveness_ts as Timestamp;

		select * from nodes
		where last_heartbeat >= $liveness_ts;
	`, s.tablesPath), ydb_table.NewQueryParameters(
		ydb_table.ValueParam("$liveness_ts", ydb_types.TimestampValueFromTime(livenessTS)),
	))
	if err != nil {
		return nil, err
	}
	defer res.Close()

	nodes, err := scanNodes(ctx, res)
	if err != nil {
		return nil, err
	}

	// NOTE: always check stream query result after iteration.
	err = res.Err()
	if err != nil {
		return nil, errors.NewRetriableError(err)
	}

	if len(nodes) < 1 {
		return nil, errors.NewSilentNonRetriableErrorf("no alive nodes found")
	}

	return nodes, nil
}

func (s *storageYDB) getNode(
	ctx context.Context,
	session *persistence.Session,
	host string,
) (Node, error) {

	logging.Debug(ctx, "Fetching the node %q", host)

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $host as Utf8;

		select * from nodes
		where host = $host;
	`, s.tablesPath), ydb_table.NewQueryParameters(
		ydb_table.ValueParam("$host", ydb_types.UTF8Value(host)),
	))
	if err != nil {
		return Node{}, err
	}
	defer res.Close()

	nodes, err := scanNodes(ctx, res)
	if err != nil {
		return Node{}, err
	}

	if len(nodes) == 0 {
		return Node{}, errors.NewSilentNonRetriableErrorf("node %q is not found", host)
	}
	return nodes[0], nil
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) Heartbeat(
	ctx context.Context,
	host string,
	ts time.Time,
	inflightTasks uint32,
) error {

	return s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		return s.heartbeat(ctx, session, host, ts, inflightTasks)
	})
}

func (s *storageYDB) GetAliveNodes(ctx context.Context) (nodes []Node, err error) {
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		nodes, err = s.getAliveNodes(ctx, session)
		return err
	})
	return
}

func (s *storageYDB) GetNode(ctx context.Context, host string) (node Node, err error) {
	err = s.db.Execute(ctx, func(ctx context.Context, session *persistence.Session) error {
		node, err = s.getNode(ctx, session, host)
		return err
	})
	return
}
