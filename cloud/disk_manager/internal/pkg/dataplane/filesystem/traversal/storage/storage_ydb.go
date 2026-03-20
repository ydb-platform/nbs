package storage

import (
	"context"
	"fmt"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

type storageYDB struct {
	db         *persistence.YDBClient
	tablesPath string
}

func NewStorage(
	db *persistence.YDBClient,
	tablesPath string,
) Storage {

	return &storageYDB{
		db:         db,
		tablesPath: db.AbsolutePath(tablesPath),
	}
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) schedulerDirectoryForTraversal(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	nodeID uint64,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Execute(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $node_id as Uint64;
		declare $cookie as String;

		upsert into directory_listing_queue (filesystem_snapshot_id, node_id, cookie)
		values ($snapshot_id, $node_id, $cookie)
	`, s.tablesPath),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$node_id", persistence.Uint64Value(nodeID)),
		persistence.ValueParam("$cookie", persistence.StringValue([]byte(""))),
	)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func (s *storageYDB) selectNodesToList(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	nodesToExclude map[uint64]struct{},
	limit uint64,
) ([]NodeQueueEntry, error) {

	excludeNodeIDs := make([]persistence.Value, 0, len(nodesToExclude))
	for nodeID := range nodesToExclude {
		excludeNodeIDs = append(excludeNodeIDs, persistence.Uint64Value(nodeID))
	}

	// Exclude InvalidNodeID to avoid handling empty excludeNodeIDs slice case.
	// List can't be empty in YDB query.
	excludeNodeIDs = append(
		excludeNodeIDs,
		persistence.Uint64Value(
			uint64(nfs.InvalidNodeID),
		),
	)
	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $limit as Uint64;
		declare $exclude_node_ids as List<Uint64>;

		select node_id, cookie
		from directory_listing_queue
		where filesystem_snapshot_id = $snapshot_id
			and node_id not in $exclude_node_ids
		limit $limit
	`, s.tablesPath),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$limit", persistence.Uint64Value(limit)),
		persistence.ValueParam("$exclude_node_ids", persistence.ListValue(excludeNodeIDs...)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var entries []NodeQueueEntry
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var entry NodeQueueEntry
			var cookie []byte
			err = res.ScanNamed(
				persistence.OptionalWithDefault("node_id", &entry.NodeID),
				persistence.OptionalWithDefault("cookie", &cookie),
			)
			if err != nil {
				return nil, err
			}

			entry.Cookie = string(cookie)
			entries = append(entries, entry)
		}
	}

	return entries, nil
}

func (s *storageYDB) scheduleChildNodesForListing(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	parentNodeID uint64,
	nextCookie string,
	children []nfs.Node,
) error {

	tx, err := session.BeginRWTransaction(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// Empty nextCookie means there are no more directory listing to do for this node.
	// So we delete it from the queue.
	// Update the cookie otherwise.
	if nextCookie == "" {
		_, err = tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $snapshot_id as Utf8;
			declare $node_id as Uint64;

			delete from directory_listing_queue
			where filesystem_snapshot_id = $snapshot_id and node_id = $node_id
		`, s.tablesPath),
			persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
			persistence.ValueParam("$node_id", persistence.Uint64Value(parentNodeID)),
		)
	} else {
		_, err = tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $snapshot_id as Utf8;
			declare $node_id as Uint64;
			declare $cookie as String;

			upsert into directory_listing_queue (filesystem_snapshot_id, node_id, cookie)
			values ($snapshot_id, $node_id, $cookie)
		`, s.tablesPath),
			persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
			persistence.ValueParam("$node_id", persistence.Uint64Value(parentNodeID)),
			persistence.ValueParam("$cookie", persistence.StringValue([]byte(nextCookie))),
		)
	}
	if err != nil {
		return err
	}

	if len(children) > 0 {
		childEntries := make([]persistence.Value, 0, len(children))
		for _, child := range children {
			childEntries = append(childEntries, persistence.StructValue(
				persistence.StructFieldValue("filesystem_snapshot_id", persistence.UTF8Value(snapshotID)),
				persistence.StructFieldValue("node_id", persistence.Uint64Value(child.NodeID)),
				persistence.StructFieldValue("cookie", persistence.StringValue([]byte(""))),
			))
		}

		_, err = tx.Execute(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $entries as List<Struct<
				filesystem_snapshot_id: Utf8,
				node_id: Uint64,
				cookie: String,
			>>;

			upsert into directory_listing_queue
			select * from AS_TABLE($entries)
		`, s.tablesPath),
			persistence.ValueParam("$entries", persistence.ListValue(childEntries...)),
		)
		if err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) clearDirectoryListingQueue(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	deletionLimit uint64,
) (bool, error) {

	res, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $limit as Uint64;

		$to_delete = (
			select filesystem_snapshot_id, node_id
			from directory_listing_queue
			where filesystem_snapshot_id = $snapshot_id
			limit $limit
		);

		delete from directory_listing_queue on
		select * from $to_delete;

		select node_id from directory_listing_queue
		where filesystem_snapshot_id = $snapshot_id
		limit 1;
	`, s.tablesPath),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$limit", persistence.Uint64Value(deletionLimit)),
	)
	if err != nil {
		return false, err
	}
	defer res.Close()

	var remains bool
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			remains = true
		}
	}

	return remains, nil
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) SchedulerDirectoryForTraversal(
	ctx context.Context,
	snapshotID string,
	nodeID uint64,
) (err error) {

	err = s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			err = s.schedulerDirectoryForTraversal(
				ctx,
				session,
				snapshotID,
				nodeID,
			)
			return err
		},
	)
	return err
}

func (s *storageYDB) SelectNodesToList(
	ctx context.Context,
	snapshotID string,
	nodesToExclude map[uint64]struct{},
	limit uint64,
) (entries []NodeQueueEntry, err error) {

	err = s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			entries, err = s.selectNodesToList(
				ctx,
				session,
				snapshotID,
				nodesToExclude,
				limit,
			)
			return err
		},
	)
	return entries, err
}

func (s *storageYDB) ScheduleChildNodesForListing(
	ctx context.Context,
	snapshotID string,
	parentNodeID uint64,
	nextCookie string,
	children []nfs.Node,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.scheduleChildNodesForListing(
				ctx,
				session,
				snapshotID,
				parentNodeID,
				nextCookie,
				children,
			)
		},
	)
}

func (s *storageYDB) ClearDirectoryListingQueue(
	ctx context.Context,
	snapshotID string,
	deletionLimit uint64,
) error {

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			remains := true
			for remains {
				remains, err = s.clearDirectoryListingQueue(
					ctx,
					session,
					snapshotID,
					deletionLimit,
				)
			}
			return err
		},
	)

	return err
}
