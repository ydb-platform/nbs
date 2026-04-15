package snapshot

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/listers"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/config"
	snapshot_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/protos"
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
	traversalStorage traversal_storage.Storage
	nodesStorage     nodes_storage.Storage
	request          *snapshot_protos.TransferFromSnapshotToFilesystemRequest
	state            *snapshot_protos.TransferFromSnapshotToFilesystemTaskState
}

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

func (t *transferFromSnapshotToFilesystemTask) snapshotID() string {
	return t.request.GetSnapshotId()
}

func (t *transferFromSnapshotToFilesystemTask) filesystemID() string {
	return t.request.GetFilesystem().GetFilesystemId()
}

func (t *transferFromSnapshotToFilesystemTask) getParentMapping(
	ctx context.Context,
	nodes []nfs.Node,
) (map[uint64]uint64, error) {

	parentIDSet := make(map[uint64]struct{})
	for _, node := range nodes {
		parentIDSet[node.ParentID] = struct{}{}
	}

	srcParentIDs := make([]uint64, 0, len(parentIDSet))
	for id := range parentIDSet {
		srcParentIDs = append(srcParentIDs, id)
	}

	return t.nodesStorage.GetDestinationNodeIDs(
		ctx,
		t.snapshotID(),
		t.filesystemID(),
		srcParentIDs,
	)
}

func (t *transferFromSnapshotToFilesystemTask) getHardlinksByNodeID(
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

func (t *transferFromSnapshotToFilesystemTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	filesystem := t.request.GetFilesystem()

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

	// Number of nodes to fetch from the storage per batch (not the same
	// as fetching from the traversal queue).
	filesystemListerFactory := nodes_storage.NewNodeStorageListerFactory(
		t.nodesStorage,
		int(t.config.GetFetchNodesFromStorageLimit()),
	)

	traverser := traversal.NewFilesystemTraverser(
		t.snapshotID(),
		t.filesystemID(),
		t.snapshotID(), //  use snapshotID as checkpointID to read nodes from snapshot storage
		filesystemListerFactory,
		t.traversalStorage,
		func(ctx context.Context) error {
			t.state.RootNodeScheduled = true
			return execCtx.SaveState(ctx)
		},
		t.config.GetTraversalConfig(),
		t.state.GetRootNodeScheduled(),
		nfs.RootNodeID,
	)

	err = traverser.Traverse(ctx, func(
		ctx context.Context,
		nodes []nfs.Node,
		filesystemLister listers.FilesystemLister,
	) error {
		srcParentID := nodes[0].ParentID

		parentMapping, err := t.nodesStorage.GetDestinationNodeIDs(
			ctx,
			t.snapshotID(),
			t.filesystemID(),
			[]uint64{srcParentID},
		)
		if err != nil {
			return err
		}

		dirMapping := make(map[uint64]uint64)

		for _, node := range nodes {
			if node.Links >= 2 {
				continue
			}

			if dstParentID, ok := parentMapping[node.ParentID]; ok {
				node.ParentID = dstParentID
			}

			dstNodeID, err := session.CreateNodeIdempotent(ctx, node)
			if err != nil {
				return err
			}

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
	})
	if err != nil {
		return err
	}

	return t.restoreHardlinks(ctx, execCtx, session)
}

func (t *transferFromSnapshotToFilesystemTask) restoreHardlinksBatch(
	ctx context.Context,
	session nfs.Session,
	offset int,
) (bool, error) {

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

	hardlinksByNodeID := t.getHardlinksByNodeID(batch)

	parentMapping, err := t.getParentMapping(ctx, batch)
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
			if dstParentID, ok := parentMapping[nodes[i].ParentID]; ok {
				nodes[i].ParentID = dstParentID
			}
		}

		dstNodeID, ok := alreadyCreatedNodeIDsMapping[srcNodeID]

		if !ok {
			first := nodes[0]
			dstNodeID, err = session.CreateNodeIdempotent(ctx, first)
			if err != nil {
				return false, err
			}

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

func (t *transferFromSnapshotToFilesystemTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.snapshotID()

	err := t.traversalStorage.ClearDirectoryListingQueue(
		ctx,
		snapshotID,
		t.config.GetTraversalQueueDeletionLimit(),
	)
	if err != nil {
		return err
	}

	for {
		done, err := t.nodesStorage.DeleteSnapshotData(
			ctx,
			snapshotID,
		)
		if err != nil {
			return err
		}

		if done {
			return nil
		}
	}
}

func (t *transferFromSnapshotToFilesystemTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *transferFromSnapshotToFilesystemTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
