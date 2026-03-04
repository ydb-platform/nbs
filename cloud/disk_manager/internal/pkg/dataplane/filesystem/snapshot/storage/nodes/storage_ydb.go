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

type storageYDB struct {
	db          *persistence.YDBClient
	tablesPath  string
	deleteLimit int
}

func NewStorage(
	db *persistence.YDBClient,
	tablesPath string,
	deleteLimit int,
) Storage {

	return &storageYDB{
		db:          db,
		tablesPath:  db.AbsolutePath(tablesPath),
		deleteLimit: deleteLimit,
	}
}

////////////////////////////////////////////////////////////////////////////////

func nodeRefStructTypeString() string {
	return `Struct<
		filesystem_snapshot_id: Utf8,
		parent_node_id: Uint64,
		name: Utf8,
		child_node_id: Uint64,
		node_type: Uint32>`
}

func nodeRefStructValue(
	snapshotID string,
	node nfs.Node,
) persistence.Value {

	n := nfs_client.Node(node)
	return persistence.StructValue(
		persistence.StructFieldValue("filesystem_snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.StructFieldValue("parent_node_id", persistence.Uint64Value(n.ParentID)),
		persistence.StructFieldValue("name", persistence.UTF8Value(n.Name)),
		persistence.StructFieldValue("child_node_id", persistence.Uint64Value(n.NodeID)),
		persistence.StructFieldValue("node_type", persistence.Uint32Value(uint32(n.Type))),
	)
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
		symlink_target: Utf8>`
}

func nodeStructValue(
	snapshotID string,
	node nfs.Node,
) persistence.Value {

	n := nfs_client.Node(node)
	return persistence.StructValue(
		persistence.StructFieldValue("filesystem_snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.StructFieldValue("node_id", persistence.Uint64Value(n.NodeID)),
		persistence.StructFieldValue("mode", persistence.Uint32Value(n.Mode)),
		persistence.StructFieldValue("uid", persistence.Uint32Value(uint32(n.UID))),
		persistence.StructFieldValue("gid", persistence.Uint32Value(uint32(n.GID))),
		persistence.StructFieldValue("atime", persistence.Uint64Value(n.Atime)),
		persistence.StructFieldValue("mtime", persistence.Uint64Value(n.Mtime)),
		persistence.StructFieldValue("ctime", persistence.Uint64Value(n.Ctime)),
		persistence.StructFieldValue("size", persistence.Uint64Value(n.Size)),
		persistence.StructFieldValue("links", persistence.Uint32Value(n.Links)),
		persistence.StructFieldValue("symlink_target", persistence.UTF8Value(n.LinkTarget)),
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

	n := nfs_client.Node(node)
	return persistence.StructValue(
		persistence.StructFieldValue("filesystem_snapshot_id", persistence.UTF8Value(snapshotID)),
		persistence.StructFieldValue("node_id", persistence.Uint64Value(n.NodeID)),
		persistence.StructFieldValue("parent_node_id", persistence.Uint64Value(n.ParentID)),
		persistence.StructFieldValue("name", persistence.UTF8Value(n.Name)),
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
		nodeType    uint32
	)
	err := result.ScanNamed(
		persistence.OptionalWithDefault("parent_node_id", &parentID),
		persistence.OptionalWithDefault("name", &name),
		persistence.OptionalWithDefault("child_node_id", &childNodeID),
		persistence.OptionalWithDefault("node_type", &nodeType),
	)
	if err != nil {
		return nfs.Node{}, err
	}

	return nfs.Node(nfs_client.Node{
		ParentID: parentID,
		NodeID:   childNodeID,
		Name:     name,
		Type:     nfs_client.NodeType(nodeType),
	}), nil
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

	return nodes, nil
}

func scanNode(result persistence.Result) (nfs_client.Node, error) {
	var (
		nodeID        uint64
		mode          uint32
		uid           uint32
		gid           uint32
		atime         uint64
		mtime         uint64
		ctime         uint64
		size          uint64
		links         uint32
		symlinkTarget string
	)
	err := result.ScanNamed(
		persistence.OptionalWithDefault("node_id", &nodeID),
		persistence.OptionalWithDefault("mode", &mode),
		persistence.OptionalWithDefault("uid", &uid),
		persistence.OptionalWithDefault("gid", &gid),
		persistence.OptionalWithDefault("atime", &atime),
		persistence.OptionalWithDefault("mtime", &mtime),
		persistence.OptionalWithDefault("ctime", &ctime),
		persistence.OptionalWithDefault("size", &size),
		persistence.OptionalWithDefault("links", &links),
		persistence.OptionalWithDefault("symlink_target", &symlinkTarget),
	)
	if err != nil {
		return nfs_client.Node{}, err
	}

	return nfs_client.Node{
		NodeID:     nodeID,
		Mode:       mode,
		UID:        uint64(uid),
		GID:        uint64(gid),
		Atime:      atime,
		Mtime:      mtime,
		Ctime:      ctime,
		Size:       size,
		Links:      links,
		LinkTarget: symlinkTarget,
	}, nil
}

func scanNodes(
	ctx context.Context,
	res persistence.Result,
) (map[uint64]nfs_client.Node, error) {

	attrs := make(map[uint64]nfs_client.Node)
	for res.NextResultSet(ctx) {
		for res.NextRow() {
			node, err := scanNode(res)
			if err != nil {
				return nil, err
			}
			attrs[node.NodeID] = node
		}
	}

	return attrs, nil
}

////////////////////////////////////////////////////////////////////////////////

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

	_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $node_refs as List<%v>;

		upsert into node_refs
		select *
		from AS_TABLE($node_refs)
	`, s.tablesPath, nodeRefStructTypeString()),
		persistence.ValueParam("$node_refs", persistence.ListValue(values...)),
	)
	return err
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

	_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $nodes as List<%v>;

		upsert into nodes
		select *
		from AS_TABLE($nodes)
	`, s.tablesPath, nodeStructTypeString()),
		persistence.ValueParam("$nodes", persistence.ListValue(values...)),
	)
	return err
}

func (s *storageYDB) saveHardlinks(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
	nodes []nfs.Node,
) error {

	values := make([]persistence.Value, 0)
	for _, node := range nodes {
		n := nfs_client.Node(node)
		if n.Links >= 2 {
			values = append(values, hardlinkStructValue(snapshotID, node))
		}
	}

	if len(values) == 0 {
		return nil
	}

	_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $hardlinks as List<%v>;

		upsert into hardlinks
		select *
		from AS_TABLE($hardlinks)
	`, s.tablesPath, hardlinkStructTypeString()),
		persistence.ValueParam("$hardlinks", persistence.ListValue(values...)),
	)
	return err
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

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $parent_node_id as Uint64;
		declare $last_child_name as Utf8;
		declare $limit as Uint64;

		select
			parent_node_id,
			name,
			child_node_id,
			node_type
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
		lastNode := nfs_client.Node(nodes[len(nodes)-1])
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
) (map[uint64]nfs_client.Node, error) {

	if len(nodeIDs) == 0 {
		return nil, nil
	}

	nodeIDValues := make([]persistence.Value, 0, len(nodeIDs))
	for _, id := range nodeIDs {
		nodeIDValues = append(nodeIDValues, persistence.Uint64Value(id))
	}

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $snapshot_id as Utf8;
		declare $node_ids as List<Uint64>;

		select node_id,
			mode,
			uid,
			gid,
			atime,
			mtime,
			ctime,
			size,
			links,
			symlink_target
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
		nodeIDs = append(nodeIDs, nfs_client.Node(node).NodeID)
	}

	attrs, err := s.fetchNodeAttrs(ctx, session, snapshotID, nodeIDs)
	if err != nil {
		return nil, "", err
	}

	for i := range nodes {
		n := nfs_client.Node(nodes[i])
		if a, ok := attrs[n.NodeID]; ok {
			n.Mode = a.Mode
			n.UID = a.UID
			n.GID = a.GID
			n.Atime = a.Atime
			n.Mtime = a.Mtime
			n.Ctime = a.Ctime
			n.Size = a.Size
			n.Links = a.Links
			n.LinkTarget = a.LinkTarget
			nodes[i] = nfs.Node(n)
		}
	}

	return nodes, nextCookie, nil
}

func (s *storageYDB) deleteSnapshotData(
	ctx context.Context,
	session *persistence.Session,
	snapshotID string,
) (bool, error) {

	tables := []string{
		"node_refs",
		"nodes",
		"restoration_node_ids_mapping",
		"hardlinks",
	}

	totalDeleted := uint64(0)

	for _, table := range tables {
		deleted, err := s.deleteFromTable(ctx, session, snapshotID, table)
		if err != nil {
			return false, err
		}
		totalDeleted += deleted
	}

	logging.Debug(
		ctx,
		"Deleted %v rows for snapshot %v",
		totalDeleted,
		snapshotID,
	)

	return totalDeleted == 0, nil
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
		persistence.ValueParam("$limit", persistence.Uint64Value(uint64(s.deleteLimit))),
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

func (s *storageYDB) updateRestorationNodeIDMapping(
	ctx context.Context,
	session *persistence.Session,
	srcSnapshotID string,
	dstSnapshotID string,
	srcNodeIds []uint64,
	dstNodeIds []uint64,
) error {

	values := make([]persistence.Value, 0, len(srcNodeIds))
	for i := range srcNodeIds {
		values = append(values, restoreMappingStructValue(
			srcSnapshotID,
			dstSnapshotID,
			srcNodeIds[i],
			dstNodeIds[i],
		))
	}

	_, err := session.ExecuteRW(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $mappings as List<%v>;

		upsert into restoration_node_ids_mapping
		select *
		from AS_TABLE($mappings)
	`, s.tablesPath, restoreMappingStructTypeString()),
		persistence.ValueParam("$mappings", persistence.ListValue(values...)),
	)
	return err
}

func (s *storageYDB) getDestinationNodeID(
	ctx context.Context,
	session *persistence.Session,
	srcSnapshotID string,
	dstFilesystemID string,
	srcNodeID uint64,
) (uint64, bool, error) {

	res, err := session.ExecuteRO(ctx, fmt.Sprintf(`
		--!syntax_v1
		pragma TablePathPrefix = "%v";
		declare $src_snapshot_id as Utf8;
		declare $dst_filesystem_id as Utf8;
		declare $src_node_id as Uint64;

		select destination_node_id
		from restoration_node_ids_mapping
		where source_snapshot_id = $src_snapshot_id
			and destination_filesystem_id = $dst_filesystem_id
			and source_node_id = $src_node_id
	`, s.tablesPath),
		persistence.ValueParam("$src_snapshot_id", persistence.UTF8Value(srcSnapshotID)),
		persistence.ValueParam("$dst_filesystem_id", persistence.UTF8Value(dstFilesystemID)),
		persistence.ValueParam("$src_node_id", persistence.Uint64Value(srcNodeID)),
	)
	if err != nil {
		return 0, false, err
	}
	defer res.Close()

	if !res.NextResultSet(ctx) || !res.NextRow() {
		return 0, false, nil
	}

	var dstNodeID uint64
	err = res.ScanNamed(
		persistence.OptionalWithDefault("destination_node_id", &dstNodeID),
	)
	if err != nil {
		return 0, false, errors.NewNonRetriableErrorf(
			"getDestinationNodeID: failed to parse row: %w",
			err,
		)
	}

	return dstNodeID, true, nil
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

func (s *storageYDB) DeleteSnapshotData(
	ctx context.Context,
	snapshotID string,
) (bool, error) {

	var done bool

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			done, err = s.deleteSnapshotData(ctx, session, snapshotID)
			return err
		},
	)
	return done, err
}

func (s *storageYDB) UpdateRestorationNodeIDMapping(
	ctx context.Context,
	srcSnapshotID string,
	dstSnapshotID string,
	srcNodeIds []uint64,
	dstNodeIds []uint64,
) error {

	if len(srcNodeIds) != len(dstNodeIds) {
		return errors.NewNonRetriableErrorf(
			"srcNodeIds and dstNodeIds must have the same length, got %v and %v",
			len(srcNodeIds),
			len(dstNodeIds),
		)
	}

	return s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			return s.updateRestorationNodeIDMapping(
				ctx,
				session,
				srcSnapshotID,
				dstSnapshotID,
				srcNodeIds,
				dstNodeIds,
			)
		},
	)
}

func (s *storageYDB) GetDestinationNodeID(
	ctx context.Context,
	srcSnapshotID string,
	dstFilesystemID string,
	srcNodeID uint64,
) (uint64, bool, error) {

	var dstNodeID uint64
	var ok bool

	err := s.db.Execute(
		ctx,
		func(ctx context.Context, session *persistence.Session) error {
			var err error
			dstNodeID, ok, err = s.getDestinationNodeID(
				ctx,
				session,
				srcSnapshotID,
				dstFilesystemID,
				srcNodeID,
			)
			return err
		},
	)
	return dstNodeID, ok, err
}
