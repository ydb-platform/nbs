package snapshot

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/listers"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/config"
	snapshot_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage"
	nodes_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/nodes"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal"
	traversal_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type transferFromSnapshotToFilesystemTask struct {
	config           *snapshot_config.FilesystemSnapshotConfig
	factory          nfs.Factory
	storage          snapshot_storage.Storage
	traversalStorage traversal_storage.Storage
	nodesStorage     nodes_storage.Storage
	request          *snapshot_protos.TransferFromSnapshotToFilesystemRequest
	state            *snapshot_protos.TransferFromSnapshotToFilesystemTaskState
}

func (t *transferFromSnapshotToFilesystemTask) snapshotID() string {
	return t.request.GetSnapshotId()
}

func (t *transferFromSnapshotToFilesystemTask) filesystemID() string {
	return t.request.GetFilesystem().GetFilesystemId()
}

func (t *transferFromSnapshotToFilesystemTask) checkSourceSnapshotReady(
	ctx context.Context,
) error {

	return t.storage.CheckFilesystemSnapshotReady(ctx, t.snapshotID())
}

func (t *transferFromSnapshotToFilesystemTask) traversalID(
	execCtx tasks.ExecutionContext,
) string {

	return fmt.Sprintf("restore_%s_%s", t.snapshotID(), execCtx.GetTaskID())
}

func (t *transferFromSnapshotToFilesystemTask) getParentNodeIDsInDestinationFs(
	ctx context.Context,
	nodes []nfs.Node,
) (map[uint64]uint64, error) {

	parentNodeIDSet := make(map[uint64]struct{})
	for _, node := range nodes {
		parentNodeIDSet[node.ParentNodeID] = struct{}{}
	}

	srcParentNodeIDs := make([]uint64, 0, len(parentNodeIDSet))
	for id := range parentNodeIDSet {
		srcParentNodeIDs = append(srcParentNodeIDs, id)
	}

	return t.nodesStorage.GetDestinationNodeIDs(
		ctx,
		t.snapshotID(),
		t.filesystemID(),
		srcParentNodeIDs,
	)
}

func (t *transferFromSnapshotToFilesystemTask) groupHardlinksByNodeID(
	nodes []nfs.Node,
) map[uint64][]nfs.Node {

	hardlinksByNodeID := make(map[uint64][]nfs.Node)
	for _, node := range nodes {
		hardlinksByNodeID[node.NodeID] = append(
			hardlinksByNodeID[node.NodeID],
			node,
		)
	}

	return hardlinksByNodeID
}

func (t *transferFromSnapshotToFilesystemTask) getAlreadyCreatedNodes(
	ctx context.Context,
	hardlinksByNodeID map[uint64][]nfs.Node,
) (map[uint64]uint64, error) {

	srcNodeIDs := make([]uint64, 0, len(hardlinksByNodeID))
	for nodeID := range hardlinksByNodeID {
		srcNodeIDs = append(srcNodeIDs, nodeID)
	}

	return t.nodesStorage.GetDestinationNodeIDs(
		ctx,
		t.snapshotID(),
		t.filesystemID(),
		srcNodeIDs,
	)
}

func (t *transferFromSnapshotToFilesystemTask) restoreHardlinksBatch(
	ctx context.Context,
	session nfs.Session,
	offset int,
) (bool, error) {

	err := t.checkSourceSnapshotReady(ctx)
	if err != nil {
		return false, err
	}

	limit := int(t.config.GetRestoreHardlinksBatchSize())

	batch, err := t.nodesStorage.ListHardLinks(
		ctx,
		t.snapshotID(),
		limit,
		offset,
	)
	if err != nil {
		return false, err
	}

	if len(batch) == 0 {
		return false, nil
	}

	hardlinksByNodeID := t.groupHardlinksByNodeID(batch)

	parentMapping, err := t.getParentNodeIDsInDestinationFs(ctx, batch)
	if err != nil {
		return false, err
	}

	alreadyCreatedNodeIDsMapping, err := t.getAlreadyCreatedNodes(
		ctx,
		hardlinksByNodeID,
	)
	if err != nil {
		return false, err
	}

	newMappings := make(map[uint64]uint64)

	for srcNodeID, nodes := range hardlinksByNodeID {
		for i := range nodes {
			if dstParentNodeID, ok := parentMapping[nodes[i].ParentNodeID]; ok {
				nodes[i].ParentNodeID = dstParentNodeID
			}
		}

		dstNodeID, ok := alreadyCreatedNodeIDsMapping[srcNodeID]

		if !ok {
			first := nodes[0]
			dstNodeID, err = session.CreateNodeIdempotent(ctx, first)
			if err != nil {
				return false, err
			}

			logging.Debug(
				ctx,
				"recovered filesystem hardlink source node from snapshot: "+
					"snapshot_id=%v filesystem_id=%v source_node_id=%v "+
					"node=%v dst_node_id=%v",
				t.snapshotID(),
				t.filesystemID(),
				srcNodeID,
				first,
				dstNodeID,
			)

			newMappings[srcNodeID] = dstNodeID
			nodes = nodes[1:]
		}

		for _, node := range nodes {
			node.NodeID = dstNodeID
			node.Type = nfs.NODE_KIND_LINK
			_, err = session.CreateNodeIdempotent(ctx, node)
			if err != nil {
				return false, err
			}

			logging.Debug(
				ctx,
				"recovered filesystem hardlink node from snapshot: "+
					"snapshot_id=%v filesystem_id=%v source_node_id=%v "+
					"node=%v dst_node_id=%v",
				t.snapshotID(),
				t.filesystemID(),
				srcNodeID,
				node,
				dstNodeID,
			)
		}
	}

	if len(newMappings) > 0 {
		err = t.nodesStorage.UpdateRestorationNodeIDMapping(
			ctx,
			t.snapshotID(),
			t.filesystemID(),
			newMappings,
		)
		if err != nil {
			return false, err
		}
	}

	err = t.checkSourceSnapshotReady(ctx)
	if err != nil {
		return false, err
	}

	return len(batch) == limit, nil
}

func (t *transferFromSnapshotToFilesystemTask) restoreHardlinks(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	session nfs.Session,
) error {

	batchSize := int(t.config.GetRestoreHardlinksBatchSize())
	offset := int(t.state.GetHardlinksRestoreOffset())
	for {
		remains, err := t.restoreHardlinksBatch(
			ctx,
			session,
			offset,
		)
		if err != nil {
			return err
		}

		t.state.HardlinksRestoreOffset = int64(offset + batchSize)
		err = execCtx.SaveState(ctx)
		if err != nil {
			return err
		}

		if !remains {
			return nil
		}

		offset += batchSize
	}
}

////////////////////////////////////////////////////////////////////////////////

func (t *transferFromSnapshotToFilesystemTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *transferFromSnapshotToFilesystemTask) Load(request, state []byte) error {
	t.request = &snapshot_protos.TransferFromSnapshotToFilesystemRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &snapshot_protos.TransferFromSnapshotToFilesystemTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *transferFromSnapshotToFilesystemTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	filesystem := t.request.GetFilesystem()

	err := t.checkSourceSnapshotReady(ctx)
	if err != nil {
		return err
	}

	client, err := t.factory.NewClient(ctx, filesystem.GetZoneId())
	if err != nil {
		return err
	}
	defer client.Close()

	session, err := client.CreateSession(
		ctx,
		t.filesystemID(),
		"",    // checkpointID
		false, // readOnly
	)
	if err != nil {
		return err
	}
	defer func() {
		err := session.Close(ctx)
		if err != nil {
			logging.Warn(ctx, "failed to close session: %v", err)
		}
	}()

	filesystemListerFactory := nodes_storage.NewNodeStorageListerFactory(
		t.nodesStorage,
		int(t.config.GetFetchNodesFromStorageLimit()),
	)

	onListedNodes := func(
		ctx context.Context,
		nodes []nfs.Node,
		_ listers.FilesystemLister,
	) error {
		if len(nodes) == 0 {
			return nil
		}

		// Traversal lists one directory at a time, so all nodes in the same
		// listing result have the same parent.
		srcParentNodeID := nodes[0].ParentNodeID

		parentMapping, err := t.nodesStorage.GetDestinationNodeIDs(
			ctx,
			t.snapshotID(),
			t.filesystemID(),
			[]uint64{srcParentNodeID},
		)
		if err != nil {
			return err
		}

		dirMapping := make(map[uint64]uint64)

		for _, node := range nodes {
			if node.Links >= 2 {
				continue
			}

			if dstParentNodeID, ok := parentMapping[node.ParentNodeID]; ok {
				node.ParentNodeID = dstParentNodeID
			}

			dstNodeID, err := session.CreateNodeIdempotent(ctx, node)
			if err != nil {
				return err
			}

			logging.Debug(
				ctx,
				"recovered filesystem node from snapshot: "+
					"snapshot_id=%v filesystem_id=%v "+
					"node=%v dst_node_id=%v",
				t.snapshotID(),
				t.filesystemID(),
				node,
				dstNodeID,
			)

			if node.Type.IsDirectory() {
				dirMapping[node.NodeID] = dstNodeID
			}
		}

		if len(dirMapping) > 0 {
			err = t.nodesStorage.UpdateRestorationNodeIDMapping(
				ctx,
				t.snapshotID(),
				t.filesystemID(),
				dirMapping,
			)
			if err != nil {
				return err
			}
		}

		return nil
	}

	traverser, err := traversal.NewFilesystemTraverser(
		t.traversalID(execCtx),
		t.filesystemID(),
		t.snapshotID(), //  use snapshotID as checkpointID to read nodes from snapshot storage
		filesystemListerFactory,
		t.traversalStorage,
		func(ctx context.Context) error {
			t.state.RootNodeScheduled = true
			return execCtx.SaveState(ctx)
		},
		onListedNodes,
		t.checkSourceSnapshotReady,
		t.config.GetTraversalConfig(),
		t.state.GetRootNodeScheduled(),
		nfs.RootNodeID,
	)
	if err != nil {
		return err
	}

	err = traverser.Traverse(ctx)
	if err != nil {
		return err
	}

	err = t.restoreHardlinks(ctx, execCtx, session)
	if err != nil {
		return err
	}

	return t.nodesStorage.CleanupRestorationNodeIDsMapping(
		ctx,
		t.snapshotID(),
		t.filesystemID(),
	)
}

func (t *transferFromSnapshotToFilesystemTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.traversalStorage.ClearDirectoryListingQueue(
		ctx,
		t.traversalID(execCtx),
	)
	if err != nil {
		return err
	}

	return t.nodesStorage.CleanupRestorationNodeIDsMapping(
		ctx,
		t.snapshotID(),
		t.filesystemID(),
	)
}

func (t *transferFromSnapshotToFilesystemTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *transferFromSnapshotToFilesystemTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
