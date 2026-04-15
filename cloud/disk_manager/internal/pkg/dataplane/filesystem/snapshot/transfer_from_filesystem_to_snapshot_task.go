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
)

////////////////////////////////////////////////////////////////////////////////

type transferFromFilesystemToSnapshotTask struct {
	config           *snapshot_config.FilesystemSnapshotConfig
	factory          nfs.Factory
	traversalStorage traversal_storage.Storage
	nodesStorage     nodes_storage.Storage
	request          *snapshot_protos.CreateFilesystemSnapshotRequest
	state            *snapshot_protos.TransferFromFilesystemToSnapshotTaskState
}

func (t *transferFromFilesystemToSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *transferFromFilesystemToSnapshotTask) Load(request, state []byte) error {
	t.request = &snapshot_protos.CreateFilesystemSnapshotRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &snapshot_protos.TransferFromFilesystemToSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *transferFromFilesystemToSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	filesystem := t.request.GetFilesystem()

	client, err := t.factory.NewClient(ctx, filesystem.GetZoneId())
	if err != nil {
		return err
	}
	defer client.Close()

	snapshotID := t.request.GetSnapshotId()

	filesystemListerFactory := listers.NewFilestoreListerFactory(
		client,
		t.config.GetListNodesMaxBytes(),
		true,  // readOnly
		true, // unsafe
		false, // ignoreNotFound
	)

	traverser := traversal.NewFilesystemTraverser(
		snapshotID,
		filesystem.GetFilesystemId(),
		t.request.GetCheckpointId(),
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

	return traverser.Traverse(ctx, func(
		ctx context.Context,
		nodes []nfs.Node,
		filesystemLister listers.FilesystemLister,
	) error {

		session := filesystemLister.(*listers.FilestoreLister).Session

		for i := range nodes {
			if nodes[i].Type.IsSymlink() {
				linkTarget, err := session.ReadLink(ctx, nodes[i].NodeID)
				if err != nil {
					return err
				}

				nodes[i].LinkTarget = string(linkTarget)
			}
		}

		return t.nodesStorage.SaveNodes(ctx, snapshotID, nodes)
	})
}

func (t *transferFromFilesystemToSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	snapshotID := t.request.GetSnapshotId()

	err := t.traversalStorage.ClearDirectoryListingQueue(
		ctx,
		snapshotID,
		t.config.GetTraversalQueueDeletionLimit(),
	)
	if err != nil {
		return err
	}

	return nil
}

func (t *transferFromFilesystemToSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *transferFromFilesystemToSnapshotTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
