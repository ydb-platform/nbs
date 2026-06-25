package snapshot

import (
	"context"

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

type createSnapshotFromFilesystemTask struct {
	config           *snapshot_config.FilesystemSnapshotConfig
	factory          nfs.Factory
	storage          snapshot_storage.Storage
	traversalStorage traversal_storage.Storage
	nodesStorage     nodes_storage.Storage
	request          *snapshot_protos.CreateFilesystemSnapshotRequest
	state            *snapshot_protos.CreateSnapshotFromFilesystemTaskState
}

func (t *createSnapshotFromFilesystemTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createSnapshotFromFilesystemTask) Load(request, state []byte) error {
	t.request = &snapshot_protos.CreateFilesystemSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &snapshot_protos.CreateSnapshotFromFilesystemTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createSnapshotFromFilesystemTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	filesystem := t.request.GetFilesystem()
	snapshotID := t.request.GetSnapshotId()

	snapshotMeta, err := t.storage.CreateFilesystemSnapshot(
		ctx,
		snapshot_storage.FilesystemSnapshotMeta{
			ID:           snapshotID,
			Filesystem:   filesystem,
			CreateTaskID: execCtx.GetTaskID(),
		},
	)
	if err != nil {
		return err
	}

	if snapshotMeta.Ready {
		return nil
	}

	client, err := t.factory.NewClient(ctx, filesystem.GetZoneId())
	if err != nil {
		return err
	}
	defer client.Close()

	filesystemListerFactory := listers.NewFilestoreListerFactory(
		client,
		t.config.GetListNodesMaxBytes(),
		true,  // readOnly
		true,  // unsafe
		false, // ignoreNotFound
	)

	onListedNodes := func(
		ctx context.Context,
		nodes []nfs.Node,
		filesystemLister listers.FilesystemLister,
	) error {

		session := filesystemLister.(*listers.FilestoreLister).Session
		nodesToSave := make([]nfs.Node, 0, len(nodes))

		for i := range nodes {
			if nodes[i].NodeID == nfs.InvalidNodeID {
				logging.Warn(
					ctx,
					"skipping node with invalid id during filesystem backup: filesystem_id=%v snapshot_id=%v node=%+v",
					filesystem.GetFilesystemId(),
					snapshotID,
					nodes[i],
				)
				continue
			}

			if nodes[i].Type.IsSymlink() {
				linkTarget, err := session.ReadLink(ctx, nodes[i].NodeID)
				if err != nil {
					return err
				}

				nodes[i].LinkTarget = string(linkTarget)
			}

			nodesToSave = append(nodesToSave, nodes[i])
		}

		err := t.nodesStorage.SaveNodes(ctx, snapshotID, nodesToSave)
		if err != nil {
			return err
		}

		logging.Debug(
			ctx,
			"saved filesystem nodes to snapshot: "+
				"filesystem_id=%v snapshot_id=%v nodes=%v",
			filesystem.GetFilesystemId(),
			snapshotID,
			nodesToSave,
		)

		return nil
	}

	traverser, err := traversal.NewFilesystemTraverser(
		snapshotID,
		filesystem.GetFilesystemId(),
		t.request.GetCheckpointId(),
		filesystemListerFactory,
		t.traversalStorage,
		func(ctx context.Context) error {
			t.state.RootNodeScheduled = true
			return execCtx.SaveState(ctx)
		},
		onListedNodes,
		func(ctx context.Context) error {
			return t.storage.CheckFilesystemSnapshotAlive(ctx, snapshotID)
		},
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

	err = t.storage.FilesystemSnapshotCreated(
		ctx,
		snapshotID,
		0,
		0,
		0,
	)
	if err != nil {
		return err
	}

	return nil
}

func (t *createSnapshotFromFilesystemTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// TODO (jkuradobery): Clean up directory listing queue on snapshot data deletion
	_, err := t.storage.DeletingFilesystemSnapshot(
		ctx,
		t.request.GetSnapshotId(),
		execCtx.GetTaskID(),
	)
	return err
}

func (t *createSnapshotFromFilesystemTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *createSnapshotFromFilesystemTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
