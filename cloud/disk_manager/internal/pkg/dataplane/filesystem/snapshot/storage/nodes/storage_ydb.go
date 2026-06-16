package nodes

import (
	"context"
	"fmt"

	nfs_client "github.com/ydb-platform/nbs/cloud/filestore/public/sdk/go/client"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
)

////////////////////////////////////////////////////////////////////////////////

const defaultUpsertBatchSize int = 1000

////////////////////////////////////////////////////////////////////////////////

type storageYDB struct {
	db              *persistence.YDBClient
	tablesPath      string
	deleteLimit     uint64
	upsertBatchSize int
}

func NewStorage(
	db *persistence.YDBClient,
	tablesPath string,
	deleteLimit uint64,
) Storage {

	return &storageYDB{
		db:              db,
		tablesPath:      db.AbsolutePath(tablesPath),
		deleteLimit:     deleteLimit,
		upsertBatchSize: defaultUpsertBatchSize,
	}
}

////////////////////////////////////////////////////////////////////////////////

func nodeRefStructTypeString() string {
	return `Struct<
		filesystem_snapshot_id: Utf8,
		parent_node_id: Uint64,
		name: Utf8,
		child_node_id: Uint64>`
}

func nodeRefStructValue(
	snapshotID string,
	node nfs.Node,
) persistence.Value {

	return persistence.StructValue(
		persistence.StructFieldValue("filesystem_snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.StructFieldValue("parent_node_id", persistence.Uint64Value(node.ParentID)),
		persistence.StructFieldValue("name", persistence.UTF8Value(node.Name)),
		persistence.StructFieldValue("child_node_id", persistence.Uint64Value(node.NodeID)),
	)
}

type nodeRefByShard struct {
	snapshotID        string
	shardFilesystemID string
	parentNodeID      uint64
	name              string
	nodeID            uint64
	storeAsChild      bool
}

func nodeRefByShardStructTypeString() string {
	return `Struct<
		filesystem_snapshot_id: Utf8,
		shard_filesystem_id: Utf8,
		parent_node_id: Uint64,
		name: Utf8,
		node_id: Uint64,
		store_as_child: Bool>`
}

func nodeRefByShardStructValue(
	nodeRef nodeRefByShard,
) persistence.Value {

	return persistence.StructValue(
		persistence.StructFieldValue("filesystem_snapshot_id", persistence.UTF8Value(nodeRef.snapshotID)),
		persistence.StructFieldValue("shard_filesystem_id", persistence.UTF8Value(nodeRef.shardFilesystemID)),
		persistence.StructFieldValue("parent_node_id", persistence.Uint64Value(nodeRef.parentNodeID)),
		persistence.StructFieldValue("name", persistence.UTF8Value(nodeRef.name)),
		persistence.StructFieldValue("node_id", persistence.Uint64Value(nodeRef.nodeID)),
		persistence.StructFieldValue("store_as_child", persistence.BoolValue(nodeRef.storeAsChild)),
	)
}

func scanNodeRefByShard(result persistence.Result) (nodeRefByShard, error) {
	var nodeRef nodeRefByShard
	err := result.ScanNamed(
		persistence.OptionalWithDefault("filesystem_snapshot_id", &nodeRef.snapshotID),
		persistence.OptionalWithDefault("shard_filesystem_id", &nodeRef.shardFilesystemID),
		persistence.OptionalWithDefault("parent_node_id", &nodeRef.parentNodeID),
		persistence.OptionalWithDefault("name", &nodeRef.name),
		persistence.OptionalWithDefault("node_id", &nodeRef.nodeID),
		persistence.OptionalWithDefault("store_as_child", &nodeRef.storeAsChild),
	)
	if err != nil {
		return nodeRefByShard{}, err
	}

	return nodeRef, nil
}

func scanNodeRefsByShard(
	ctx context.Context,
	res persistence.Result,
) ([]nodeRefByShard, error) {

	var nodeRefs []nodeRefByShard
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			nodeRef, err := scanNodeRefByShard(res)
			if err != nil {
				return nil, errors.NewNonRetriableErrorf(
					"listNodeRefsByShard: failed to parse row: %w",
					err,
				)
			}

			nodeRefs = append(nodeRefs, nodeRef)
		}
	}

	// NOTE: always check stream query result after iteration.
	if res.Err() != nil {
		return nil, errors.NewRetriableError(res.Err())
	}

	return nodeRefs, nil
}

func nodeStructTypeString() string {
	return `Struct<
		filesystem_snapshot_id: Utf8,
		node_id: Uint64,
		mode: Uint32,
		uid: Uint32,
		gid: Uint32,
		atime: Uint64,
		mtime: Uint64,
		ctime: Uint64,
		size: Uint64,
		links: Uint32,
		node_type: Uint32,
		symlink_target: Utf8,
		shard_id: Utf8,
		shard_node_name: Utf8,
		dev_id: Uint64>`
}

func nodeStructValue(
	snapshotID string,
	node nfs.Node,
) persistence.Value {

	return persistence.StructValue(
		persistence.StructFieldValue("filesystem_snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.StructFieldValue("node_id", persistence.Uint64Value(node.NodeID)),
		persistence.StructFieldValue("mode", persistence.Uint32Value(node.Mode)),
		persistence.StructFieldValue("uid", persistence.Uint32Value(node.UID)),
		persistence.StructFieldValue("gid", persistence.Uint32Value(node.GID)),
		persistence.StructFieldValue("atime", persistence.Uint64Value(node.Atime)),
		persistence.StructFieldValue("mtime", persistence.Uint64Value(node.Mtime)),
		persistence.StructFieldValue("ctime", persistence.Uint64Value(node.Ctime)),
		persistence.StructFieldValue("size", persistence.Uint64Value(node.Size)),
		persistence.StructFieldValue("links", persistence.Uint32Value(node.Links)),
		persistence.StructFieldValue("node_type", persistence.Uint32Value(uint32(node.Type))),
		persistence.StructFieldValue("symlink_target", persistence.UTF8Value(node.LinkTarget)),
		persistence.StructFieldValue("shard_id", persistence.UTF8Value(node.ShardFileSystemID)),
		persistence.StructFieldValue("shard_node_name", persistence.UTF8Value(node.ShardNodeName)),
		persistence.StructFieldValue("dev_id", persistence.Uint64Value(node.DevID)),
	)
}

func hardlinkStructTypeString() string {
	return `Struct<
		filesystem_snapshot_id: Utf8,
		node_id: Uint64,
		parent_node_id: Uint64,
		name: Utf8>`
}

func hardlinkStructValue(
	snapshotID string,
	node nfs.Node,
) persistence.Value {

	return persistence.StructValue(
		persistence.StructFieldValue("filesystem_snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.StructFieldValue("node_id", persistence.Uint64Value(node.NodeID)),
		persistence.StructFieldValue("parent_node_id", persistence.Uint64Value(node.ParentID)),
		persistence.StructFieldValue("name", persistence.UTF8Value(node.Name)),
	)
}

func restoreMappingStructTypeString() string {
	return `Struct<
		source_snapshot_id: Utf8,
		destination_filesystem_id: Utf8,
		source_node_id: Uint64,
		destination_node_id: Uint64>`
}

func restoreMappingStructValue(
	srcSnapshotID string,
	dstSnapshotID string,
	srcNodeID uint64,
	dstNodeID uint64,
) persistence.Value {

	return persistence.StructValue(
		persistence.StructFieldValue("source_snapshot_id", persistence.UTF8Value(srcSnapshotID)),
		persistence.StructFieldValue("destination_filesystem_id", persistence.UTF8Value(dstSnapshotID)),
		persistence.StructFieldValue("source_node_id", persistence.Uint64Value(srcNodeID)),
		persistence.StructFieldValue("destination_node_id", persistence.Uint64Value(dstNodeID)),
	)
}

func scanNodeRef(result persistence.Result) (nfs.Node, error) {
	var (
		parentID    uint64
		name        string
		childNodeID uint64
	)
	err := result.ScanNamed(
		persistence.OptionalWithDefault("parent_node_id", &parentID),
		persistence.OptionalWithDefault("name", &name),
		persistence.OptionalWithDefault("child_node_id", &childNodeID),
	)
	if err != nil {
		return nfs.Node{}, err
	}

	return nfs.Node{
		ParentID: parentID,
		NodeID:   childNodeID,
		Name:     name,
	}, nil
}

func scanNodeRefs(
	ctx context.Context,
	res persistence.Result,
) ([]nfs.Node, error) {

	var nodes []nfs.Node
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			node, err := scanNodeRef(res)
			if err != nil {
				return nil, err
			}
			nodes = append(nodes, node)
		}
	}

	if res.Err() != nil {
		return nil, errors.NewRetriableError(res.Err())
	}

	return nodes, nil
}

func scanNode(result persistence.Result) (nfs.Node, error) {
	var (
		node     nfs.Node
		nodeType uint32
	)
	err := result.ScanNamed(
		persistence.OptionalWithDefault("node_id", &node.NodeID),
		persistence.OptionalWithDefault("mode", &node.Mode),
		persistence.OptionalWithDefault("uid", &node.UID),
		persistence.OptionalWithDefault("gid", &node.GID),
		persistence.OptionalWithDefault("atime", &node.Atime),
		persistence.OptionalWithDefault("mtime", &node.Mtime),
		persistence.OptionalWithDefault("ctime", &node.Ctime),
		persistence.OptionalWithDefault("size", &node.Size),
		persistence.OptionalWithDefault("links", &node.Links),
		persistence.OptionalWithDefault("node_type", &nodeType),
		persistence.OptionalWithDefault("symlink_target", &node.LinkTarget),
		persistence.OptionalWithDefault("shard_id", &node.ShardFileSystemID),
		persistence.OptionalWithDefault("shard_node_name", &node.ShardNodeName),
		persistence.OptionalWithDefault("dev_id", &node.DevID),
	)
	if err != nil {
		return nfs.Node{}, err
	}

	node.Type = nfs_client.NodeType(nodeType)
	return node, nil
}

func scanNodes(
	ctx context.Context,
	res persistence.Result,
) (map[uint64]nfs.Node, error) {

	attrs := make(map[uint64]nfs.Node)
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			node, err := scanNode(res)
			if err != nil {
				return nil, err
			}

			attrs[node.NodeID] = node
		}
	}

	if res.Err() != nil {
		return nil, errors.NewRetriableError(res.Err())
	}

	return attrs, nil
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) upsertInBatches(
	values []persistence.Value,
	upsert func(batch []persistence.Value) error,
) error {

	for i := 0; i < len(values); i += s.upsertBatchSize {
		end := i + s.upsertBatchSize
		if end > len(values) {
			end = len(values)
		}

		if err := upsert(values[i:end]); err != nil {
			return err
		}
	}

	return nil
}

func (s *storageYDB) saveNodeRefs(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	nodes []nfs.Node,
) error {

	values := make([]persistence.Value, 0, len(nodes))
	for _, node := range nodes {
		values = append(values, nodeRefStructValue(snapshotID, node))
	}

	return s.upsertInBatches(values, func(batch []persistence.Value) error {
		_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $node_refs as List<%v>;

			upsert into node_refs
			select *
			from AS_TABLE($node_refs)
		`, s.tablesPath, nodeRefStructTypeString()),
			persistence.ValueParam("$node_refs", persistence.ListValue(batch...)),
		)
		return err
	})
}

func (s *storageYDB) saveNodes(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	nodes []nfs.Node,
) error {

	values := make([]persistence.Value, 0, len(nodes))
	for _, node := range nodes {
		values = append(values, nodeStructValue(snapshotID, node))
	}

	return s.upsertInBatches(values, func(batch []persistence.Value) error {
		_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $nodes as List<%v>;

			upsert into nodes
			select *
			from AS_TABLE($nodes)
		`, s.tablesPath, nodeStructTypeString()),
			persistence.ValueParam("$nodes", persistence.ListValue(batch...)),
		)
		return err
	})
}

func (s *storageYDB) saveNodesByShard(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	nodes []nfs.Node,
) error {

	parentNodeIDs := make([]uint64, 0, len(nodes))
	seenParentNodeIDs := make(map[uint64]struct{})
	for _, node := range nodes {
		if _, ok := seenParentNodeIDs[node.ParentID]; ok {
			continue
		}

		seenParentNodeIDs[node.ParentID] = struct{}{}
		parentNodeIDs = append(parentNodeIDs, node.ParentID)
	}

	parentAttrs, err := s.fetchNodeAttrs(ctx, session, snapshotID, parentNodeIDs)
	if err != nil {
		return err
	}

	values := make([]persistence.Value, 0, len(nodes)*2)
	for _, node := range nodes {
		if len(node.ShardFileSystemID) == 0 {
			continue
		}
		if len(node.ShardNodeName) == 0 {
			continue
		}

		nodeRef := nodeRefByShard{
			snapshotID:        snapshotID,
			shardFilesystemID: node.ShardFileSystemID,
			parentNodeID:      node.ParentID,
			name:              node.Name,
			nodeID:            node.NodeID,
			storeAsChild:      false,
		}
		values = append(values, nodeRefByShardStructValue(nodeRef))
	}

	for _, node := range nodes {
		parent, ok := parentAttrs[node.ParentID]
		if !ok || len(parent.ShardFileSystemID) == 0 {
			continue
		}

		nodeRef := nodeRefByShard{
			snapshotID:        snapshotID,
			shardFilesystemID: parent.ShardFileSystemID,
			parentNodeID:      node.ParentID,
			name:              node.Name,
			nodeID:            node.NodeID,
			storeAsChild:      true,
		}
		values = append(values, nodeRefByShardStructValue(nodeRef))
	}

	return s.upsertInBatches(values, func(batch []persistence.Value) error {
		_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $node_refs as List<%v>;

			upsert into node_refs_by_shard
			select *
			from AS_TABLE($node_refs)
		`, s.tablesPath, nodeRefByShardStructTypeString()),
			persistence.ValueParam("$node_refs", persistence.ListValue(batch...)),
		)
		return err
	})
}

func (s *storageYDB) saveHardlinks(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	nodes []nfs.Node,
) error {

	values := make([]persistence.Value, 0)
	for _, node := range nodes {
		if node.Links >= 2 {
			values = append(values, hardlinkStructValue(snapshotID, node))
		}
	}

	return s.upsertInBatches(values, func(batch []persistence.Value) error {
		_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $hardlinks as List<%v>;

			upsert into hardlinks
			select *
			from AS_TABLE($hardlinks)
		`, s.tablesPath, hardlinkStructTypeString()),
			persistence.ValueParam("$hardlinks", persistence.ListValue(batch...)),
		)
		return err
	})
}

func (s *storageYDB) listNodeRefsByShard(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	shardFilesystemID string,
	limit uint64,
	offset uint64,
) ([]nodeRefByShard, error) {

	res, err := session.StreamExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $shard_filesystem_id as Utf8;
		declare $limit as Uint64;
		declare $offset as Uint64;

		select *
		from node_refs_by_shard
		where filesystem_snapshot_id = $snapshot_id
			and shard_filesystem_id = $shard_filesystem_id
		order by parent_node_id, name, store_as_child
		limit $limit
		offset $offset
	`, s.tablesPath),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$shard_filesystem_id", persistence.UTF8Value(shardFilesystemID)),
		persistence.ValueParam("$limit", persistence.Uint64Value(limit)),
		persistence.ValueParam("$offset", persistence.Uint64Value(offset)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	return scanNodeRefsByShard(ctx, res)
}

func (s *storageYDB) listNodeRefs(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	parentNodeID uint64,
	cookie string,
	limit int,
) ([]nfs.Node, string, error) {

	var lastChildName string
	if len(cookie) > 0 {
		lastChildName = cookie
	}

	res, err := session.StreamExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $parent_node_id as Uint64;
		declare $last_child_name as Utf8;
		declare $limit as Uint64;

		select *
		from node_refs
		where filesystem_snapshot_id = $snapshot_id
			and parent_node_id = $parent_node_id
			and name >= $last_child_name
		order by name
		limit $limit
	`, s.tablesPath),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$parent_node_id", persistence.Uint64Value(parentNodeID)),
		persistence.ValueParam("$last_child_name", persistence.UTF8Value(lastChildName)),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit+1))),
	)
	if err != nil {
		return nil, "", err
	}
	defer res.Close()

	nodes, err := scanNodeRefs(ctx, res)
	if err != nil {
		return nil, "", errors.NewNonRetriableErrorf(
			"listNodeRefs: failed to parse row: %w",
			err,
		)
	}

	var nextCookie string
	if len(nodes) > limit {
		lastNode := nodes[len(nodes)-1]
		nextCookie = lastNode.Name
		nodes = nodes[:limit]
	}

	return nodes, nextCookie, nil
}

func (s *storageYDB) fetchNodeAttrs(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	nodeIDs []uint64,
) (map[uint64]nfs.Node, error) {

	if len(nodeIDs) == 0 {
		return nil, nil
	}

	nodeIDValues := make([]persistence.Value, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		nodeIDValues = append(nodeIDValues, persistence.Uint64Value(id))
	}

	res, err := session.StreamExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $node_ids as List<Uint64>;

		select *
		from nodes
		where filesystem_snapshot_id = $snapshot_id
			and node_id in $node_ids
	`, s.tablesPath),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$node_ids", persistence.ListValue(nodeIDValues...)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	attrs, err := scanNodes(ctx, res)
	if err != nil {
		return nil, errors.NewNonRetriableErrorf(
			"fetchNodeAttrs: failed to parse row: %w",
			err,
		)
	}

	return attrs, nil
}

func (s *storageYDB) listNodes(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	parentNodeID uint64,
	cookie string,
	limit int,
) ([]nfs.Node, string, error) {

	nodes, nextCookie, err := s.listNodeRefs(
		ctx, session, snapshotID, parentNodeID, cookie, limit,
	)
	if err != nil {
		return nil, "", err
	}

	nodeIDs := make([]uint64, 0, len(nodes))
	for _, node := range nodes {
		nodeIDs = append(nodeIDs, node.NodeID)
	}

	attrs, err := s.fetchNodeAttrs(ctx, session, snapshotID, nodeIDs)
	if err != nil {
		return nil, "", err
	}

	for i, node := range nodes {
		if a, ok := attrs[node.NodeID]; ok {
			node.Mode = a.Mode
			node.UID = a.UID
			node.GID = a.GID
			node.Atime = a.Atime
			node.Mtime = a.Mtime
			node.Ctime = a.Ctime
			node.Size = a.Size
			node.Links = a.Links
			node.Type = a.Type
			node.LinkTarget = a.LinkTarget
			node.ShardFileSystemID = a.ShardFileSystemID
			node.ShardNodeName = a.ShardNodeName
			node.DevID = a.DevID
			nodes[i] = node
		}
	}

	return nodes, nextCookie, nil
}

func (s *storageYDB) listNodesByShard(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	shardFilesystemID string,
	limit uint64,
	offset uint64,
) ([]nfs.Node, error) {

	nodeRefsByShard, err := s.listNodeRefsByShard(
		ctx,
		session,
		snapshotID,
		shardFilesystemID,
		limit,
		offset,
	)
	if err != nil {
		return nil, err
	}

	uniqueNodeIDs := make(map[uint64]struct{}, len(nodeRefsByShard))
	nodeIDs := make([]uint64, 0, len(nodeRefsByShard))
	for _, nodeRefByShard := range nodeRefsByShard {
		if _, ok := uniqueNodeIDs[nodeRefByShard.nodeID]; ok {
			continue
		}

		uniqueNodeIDs[nodeRefByShard.nodeID] = struct{}{}
		nodeIDs = append(nodeIDs, nodeRefByShard.nodeID)
	}

	attrs, err := s.fetchNodeAttrs(ctx, session, snapshotID, nodeIDs)
	if err != nil {
		return nil, err
	}

	nodes := make([]nfs.Node, 0, len(nodeRefsByShard))
	for _, nodeRefByShard := range nodeRefsByShard {
		node, ok := attrs[nodeRefByShard.nodeID]
		if !ok {
			return nil, errors.NewNonRetriableErrorf(
				"node not found: snapshot_id=%v node_id=%v",
				snapshotID,
				nodeRefByShard.nodeID,
			)
		}

		if nodeRefByShard.storeAsChild {
			node.ParentID = nodeRefByShard.parentNodeID
			node.Name = nodeRefByShard.name
			node.NodeID = 0
		} else {
			node.ParentID = uint64(nfs.RootNodeID)
			node.Name = node.ShardNodeName
			node.ShardFileSystemID = ""
			node.ShardNodeName = ""
		}

		nodes = append(nodes, node)
	}

	return nodes, nil
}

func (s *storageYDB) deleteFromTables(
	ctx context.Context,
	snapshotID string,
	tables []string,
) error {

	for {
		var done bool

		err := s.db.Execute(
			ctx,
			func(ctx context.Context, session *persistence.Session) error {
				deletedCount := uint64(0)

				for _, table := range tables {
					deleted, err := s.deleteFromTable(ctx, session, snapshotID, table)
					if err != nil {
						return err
					}

					deletedCount += deleted
				}

				done = deletedCount == 0
				return nil
			},
		)
		if err != nil {
			return err
		}

		if done {
			return nil
		}
	}
}

func (s *storageYDB) deleteFromTable(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	table string,
) (uint64, error) {

	snapshotIDColumn := "filesystem_snapshot_id"
	if table == "restoration_node_ids_mapping" {
		snapshotIDColumn = "source_snapshot_id"
	}

	res, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $limit as Uint64;

		$to_delete = (
			select *
			from %v
			where %v = $snapshot_id
			limit $limit
		);

		select count(*) as deleted_count from $to_delete;

		delete from %v on
		select * from $to_delete;
		`, s.tablesPath, table, snapshotIDColumn, table),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$limit", persistence.Uint64Value(s.deleteLimit)),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, nil
	}

	var count uint64
	err = res.ScanNamed(
		persistence.OptionalWithDefault("deleted_count", &count),
	)
	if err != nil {
		return 0, errors.NewNonRetriableErrorf(
			"deleteFromTable %v: failed to parse count: %w",
			table,
			err,
		)
	}

	return count, nil
}

func (s *storageYDB) deleteFromRestorationNodeIDsMapping(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	destinationFilesystemID string,
) (uint64, error) {

	res, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $destination_filesystem_id as Utf8;
		declare $limit as Uint64;

		$to_delete = (
			select source_snapshot_id, destination_filesystem_id, source_node_id
			from restoration_node_ids_mapping
			where source_snapshot_id = $snapshot_id
				and destination_filesystem_id = $destination_filesystem_id
			limit $limit
		);

		select count(*) as deleted_count from $to_delete;

		delete from restoration_node_ids_mapping on
		select * from $to_delete;
	`, s.tablesPath),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$destination_filesystem_id", persistence.UTF8Value(destinationFilesystemID)),
		persistence.ValueParam("$limit", persistence.Uint64Value(s.deleteLimit)),
	)
	if err != nil {
		return 0, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, nil
	}

	var count uint64
	err = res.ScanNamed(
		persistence.OptionalWithDefault("deleted_count", &count),
	)
	if err != nil {
		return 0, errors.NewNonRetriableErrorf(
			"deleteFromRestorationNodeIDsMapping: failed to parse count: %w",
			err,
		)
	}

	logging.Debug(
		ctx,
		"Deleted %v rows from restoration_node_ids_mapping for snapshot %v and destination filesystem %v",
		count,
		snapshotID,
		destinationFilesystemID,
	)

	return count, nil
}

func (s *storageYDB) updateRestorationNodeIDMapping(
	ctx context.Context,
	session *persistence.Session,
	srcSnapshotID string,
	dstFilesystemID string,
	nodeIDMapping map[uint64]uint64,
) error {

	if len(nodeIDMapping) == 0 {
		return nil
	}

	values := make([]persistence.Value, 0, len(nodeIDMapping))
	for srcNodeID, dstNodeID := range nodeIDMapping {
		values = append(values, restoreMappingStructValue(
			srcSnapshotID,
			dstFilesystemID,
			srcNodeID,
			dstNodeID,
		))
	}

	return s.upsertInBatches(values, func(batch []persistence.Value) error {
		_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
			--!syntax_v1
			pragma TablePathPrefix = "%v";
			declare $mappings as List<%v>;

			upsert into restoration_node_ids_mapping
			select *
			from AS_TABLE($mappings)
		`, s.tablesPath, restoreMappingStructTypeString()),
			persistence.ValueParam("$mappings", persistence.ListValue(batch...)),
		)
		return err
	})
}

func (s *storageYDB) getDestinationNodeIDs(
	ctx context.Context,
	session *persistence.Session,
	srcSnapshotID string,
	dstFilesystemID string,
	srcNodeIDs []uint64,
) (map[uint64]uint64, error) {

	if len(srcNodeIDs) == 0 {
		return map[uint64]uint64{}, nil
	}

	nodeIDValues := make([]persistence.Value, 0, len(srcNodeIDs))
	for _, id := range srcNodeIDs {
		nodeIDValues = append(nodeIDValues, persistence.Uint64Value(id))
	}

	res, err := session.StreamExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $src_snapshot_id as Utf8;
		declare $dst_filesystem_id as Utf8;
		declare $src_node_ids as List<Uint64>;

		select source_node_id, destination_node_id
		from restoration_node_ids_mapping
		where source_snapshot_id = $src_snapshot_id
			and destination_filesystem_id = $dst_filesystem_id
			and source_node_id in $src_node_ids
	`, s.tablesPath),
		persistence.ValueParam("$src_snapshot_id", persistence.UTF8Value(srcSnapshotID)),
		persistence.ValueParam("$dst_filesystem_id", persistence.UTF8Value(dstFilesystemID)),
		persistence.ValueParam("$src_node_ids", persistence.ListValue(nodeIDValues...)),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	result := make(map[uint64]uint64)
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var srcNodeID, dstNodeID uint64
			err = res.ScanNamed(
				persistence.OptionalWithDefault("source_node_id", &srcNodeID),
				persistence.OptionalWithDefault("destination_node_id", &dstNodeID),
			)
			if err != nil {
				return nil, errors.NewNonRetriableErrorf(
					"getDestinationNodeIDs: failed to parse row: %w",
					err,
				)
			}

			result[srcNodeID] = dstNodeID
		}
	}

	if res.Err() != nil {
		return nil, errors.NewRetriableError(res.Err())
	}

	return result, nil
}

////////////////////////////////////////////////////////////////////////////////

func (s *storageYDB) SaveNodes(
	ctx context.Context,
	snapshotID string,
	nodes []nfs.Node,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			err := s.saveNodeRefs(ctx, session, snapshotID, nodes)
			if err != nil {
				return err
			}

			err = s.saveNodes(ctx, session, snapshotID, nodes)
			if err != nil {
				return err
			}

			err = s.saveNodesByShard(ctx, session, snapshotID, nodes)
			if err != nil {
				return err
			}

			return s.saveHardlinks(ctx, session, snapshotID, nodes)
		},
	)
}

func (s *storageYDB) ListNodes(
	ctx context.Context,
	snapshotID string,
	parentNodeID uint64,
	cookie string,
	limit int,
) ([]nfs.Node, string, error) {

	var result []nfs.Node
	var nextCookie string

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			result, nextCookie, err = s.listNodes(
				ctx,
				session,
				snapshotID,
				parentNodeID,
				cookie,
				limit,
			)
			return err
		},
	)
	return result, nextCookie, err
}

func (s *storageYDB) ListNodesByShard(
	ctx context.Context,
	snapshotID string,
	shardFilesystemID string,
	limit uint64,
	offset uint64,
) ([]nfs.Node, error) {

	var result []nfs.Node

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			result, err = s.listNodesByShard(
				ctx,
				session,
				snapshotID,
				shardFilesystemID,
				limit,
				offset,
			)
			return err
		},
	)
	return result, err
}

func (s *storageYDB) CleanupRestorationNodeIDsMapping(
	ctx context.Context,
	snapshotID string,
	destinationFilesystemID string,
) error {

	for {
		deletedCount := uint64(0)

		err := s.db.Execute(
			ctx,
			func(ctx context.Context, session *persistence.Session) error {
				deleted, err := s.deleteFromRestorationNodeIDsMapping(
					ctx,
					session,
					snapshotID,
					destinationFilesystemID,
				)
				if err != nil {
					return err
				}

				deletedCount = deleted
				return nil
			},
		)
		if err != nil {
			return err
		}

		if deletedCount == 0 {
			return nil
		}
	}
}

func (s *storageYDB) DeleteSnapshotData(
	ctx context.Context,
	snapshotID string,
) error {

	return s.deleteFromTables(ctx, snapshotID, []string{
		"node_refs",
		"node_refs_by_shard",
		"nodes",
		"restoration_node_ids_mapping",
		"hardlinks",
	})
}

func (s *storageYDB) UpdateRestorationNodeIDMapping(
	ctx context.Context,
	srcSnapshotID string,
	dstFilesystemID string,
	nodeIDMapping map[uint64]uint64,
) error {

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.updateRestorationNodeIDMapping(
				ctx,
				session,
				srcSnapshotID,
				dstFilesystemID,
				nodeIDMapping,
			)
		},
	)
}

func (s *storageYDB) GetDestinationNodeIDs(
	ctx context.Context,
	srcSnapshotID string,
	dstFilesystemID string,
	srcNodeIDs []uint64,
) (map[uint64]uint64, error) {

	var result map[uint64]uint64

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			result, err = s.getDestinationNodeIDs(
				ctx,
				session,
				srcSnapshotID,
				dstFilesystemID,
				srcNodeIDs,
			)
			return err
		},
	)
	return result, err
}

func (s *storageYDB) listHardLinks(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	limit int,
	offset int,
) ([]nfs.Node, error) {

	res, err := session.StreamExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $limit as Uint64;
		declare $offset as Uint64;

		select *
		from hardlinks
		where filesystem_snapshot_id = $snapshot_id
		order by node_id, parent_node_id, name
		limit $limit
		offset $offset
	`, s.tablesPath),
		persistence.ValueParam("$snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(limit))),
		persistence.ValueParam("$offset", persistence.Uint64Value(uint64(offset))),
	)
	if err != nil {
		return nil, err
	}
	defer res.Close()

	var nodes []nfs.Node
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			var node nfs.Node
			err := res.ScanNamed(
				persistence.OptionalWithDefault("node_id", &node.NodeID),
				persistence.OptionalWithDefault("parent_node_id", &node.ParentID),
				persistence.OptionalWithDefault("name", &node.Name),
			)
			if err != nil {
				return nil, errors.NewNonRetriableErrorf(
					"listHardLinks: failed to parse row: %w",
					err,
				)
			}

			nodes = append(nodes, node)
		}
	}

	if res.Err() != nil {
		return nil, errors.NewRetriableError(res.Err())
	}

	nodeIDs := make([]uint64, 0, len(nodes))
	for _, node := range nodes {
		nodeIDs = append(nodeIDs, node.NodeID)
	}

	attrs, err := s.fetchNodeAttrs(ctx, session, snapshotID, nodeIDs)
	if err != nil {
		return nil, err
	}

	for i, node := range nodes {
		if a, ok := attrs[node.NodeID]; ok {
			node.Mode = a.Mode
			node.UID = a.UID
			node.GID = a.GID
			node.Atime = a.Atime
			node.Mtime = a.Mtime
			node.Ctime = a.Ctime
			node.Size = a.Size
			node.Links = a.Links
			node.Type = a.Type
			node.LinkTarget = a.LinkTarget
			node.DevID = a.DevID
			nodes[i] = node
		}
	}

	return nodes, nil
}

func (s *storageYDB) ListHardLinks(
	ctx context.Context,
	snapshotID string,
	limit int,
	offset int,
) ([]nfs.Node, error) {

	var result []nfs.Node

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			result, err = s.listHardLinks(
				ctx,
				session,
				snapshotID,
				limit,
				offset,
			)
			return err
		},
	)
	return result, err
}
