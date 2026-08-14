package snapshot

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"

	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	snapshot_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/config"
	snapshot_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/protos"
	snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage"
	nodes_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/storage/nodes"
	"github.com/ydb-platform/nbs/cloud/tasks"
	task_errors "github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type restoreFilesystemShardTask struct {
	config       *snapshot_config.FilesystemSnapshotConfig
	factory      nfs.Factory
	storage      snapshot_storage.Storage
	nodesStorage nodes_storage.Storage
	request      *snapshot_protos.RestoreFilesystemShardRequest
	state        *snapshot_protos.RestoreFilesystemShardTaskState
}

func (t *restoreFilesystemShardTask) snapshotID() string {
	return t.request.GetSnapshotId()
}

func (t *restoreFilesystemShardTask) shardID() string {
	return t.request.GetShard().GetFilesystemId()
}

func (t *restoreFilesystemShardTask) validateRequest() error {
	if t.request == nil {
		return task_errors.NewSilentNonRetriableErrorf(
			"restore filesystem shard request is missing",
		)
	}

	if len(t.snapshotID()) == 0 {
		return task_errors.NewSilentNonRetriableErrorf(
			"filesystem snapshot id is missing",
		)
	}

	shard := t.request.GetShard()
	if shard == nil {
		return task_errors.NewSilentNonRetriableErrorf(
			"filesystem shard is missing",
		)
	}

	if len(shard.GetFilesystemId()) == 0 {
		return task_errors.NewSilentNonRetriableErrorf(
			"filesystem shard id is missing",
		)
	}

	if len(shard.GetZoneId()) == 0 {
		return task_errors.NewSilentNonRetriableErrorf(
			"filesystem shard zone id is missing",
		)
	}

	return nil
}

func (t *restoreFilesystemShardTask) page() *nodes_storage.NodeRefsByShardCookie {
	if len(t.state.GetName()) == 0 {
		return nil
	}

	return &nodes_storage.NodeRefsByShardCookie{
		ParentNodeID: t.state.GetParentNodeId(),
		Name:         t.state.GetName(),
		StoreAsChild: t.state.GetStoreAsChild(),
	}
}

// restoreNode recreates a node stored in the shard and its reference from the
// shard root.
func (t *restoreFilesystemShardTask) restoreNode(
	ctx context.Context,
	client nfs.Client,
	node nfs.Node,
) error {

	err := client.UnsafeCreateNode(ctx, t.shardID(), node)
	if err != nil {
		return err
	}

	return client.UnsafeCreateNodeRef(
		ctx,
		t.shardID(),
		nfs.RootNodeID,
		node.Name,
		node.NodeID,
		"", // shardID
		"", // shardNodeName
	)
}

// restoreChildRef recreates a child reference owned by a directory in the
// shard.
func (t *restoreFilesystemShardTask) restoreChildRef(
	ctx context.Context,
	client nfs.Client,
	node nfs.Node,
) error {

	return client.UnsafeCreateNodeRef(
		ctx,
		t.shardID(),
		node.ParentNodeID,
		node.Name,
		node.NodeID,
		node.ShardFileSystemID,
		node.ShardNodeName,
	)
}

func (t *restoreFilesystemShardTask) restorePage(
	ctx context.Context,
	client nfs.Client,
) (*nodes_storage.NodeRefsByShardCookie, error) {

	nodes, nextPage, err := t.nodesStorage.ListNodesByShard(
		ctx,
		t.snapshotID(),
		t.shardID(),
		uint64(t.config.GetFetchNodesFromStorageLimit()),
		t.page(),
	)
	if err != nil {
		return nil, err
	}

	for _, node := range nodes {
		if node.NodeID == 0 {
			err = t.restoreChildRef(ctx, client, node)
		} else {
			err = t.restoreNode(ctx, client, node)
		}

		if err != nil {
			return nil, err
		}
	}

	return nextPage, nil
}

func (t *restoreFilesystemShardTask) savePage(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	nextPage *nodes_storage.NodeRefsByShardCookie,
) error {

	t.state.ParentNodeId = nextPage.ParentNodeID
	t.state.Name = nextPage.Name
	t.state.StoreAsChild = nextPage.StoreAsChild
	return execCtx.SaveState(ctx)
}

func (t *restoreFilesystemShardTask) restore(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	client nfs.Client,
) error {

	for {
		nextPage, err := t.restorePage(ctx, client)
		if err != nil {
			return err
		}

		if nextPage == nil {
			return nil
		}

		err = t.savePage(ctx, execCtx, nextPage)
		if err != nil {
			return err
		}
	}
}

func (t *restoreFilesystemShardTask) getShardFileSystemIDs(
	ctx context.Context,
	client nfs.Client,
) ([]string, error) {

	shardTopology, err := client.GetFileSystemTopology(ctx, t.shardID())
	if err != nil {
		return nil, err
	}

	mainFileSystemID := shardTopology.MainFileSystemID
	if len(mainFileSystemID) == 0 {
		return nil, task_errors.NewNonRetriableErrorf(
			"main filesystem id is missing for shard %q",
			t.shardID(),
		)
	}

	mainFileSystemTopology, err := client.GetFileSystemTopology(
		ctx,
		mainFileSystemID,
	)
	if err != nil {
		return nil, err
	}

	shardFileSystemIDs := mainFileSystemTopology.ShardFileSystemIDs
	if len(shardFileSystemIDs) == 0 {
		return nil, task_errors.NewNonRetriableErrorf(
			"shards are missing for main filesystem %q",
			mainFileSystemID,
		)
	}

	return shardFileSystemIDs, nil
}

func (t *restoreFilesystemShardTask) freezeShards(
	ctx context.Context,
	client nfs.Client,
) error {

	shardFileSystemIDs, err := t.getShardFileSystemIDs(ctx, client)
	if err != nil {
		return err
	}

	for _, shardFileSystemID := range shardFileSystemIDs {
		err = client.FreezeTablet(ctx, shardFileSystemID)
		if err != nil {
			return err
		}
	}

	return nil
}

func (t *restoreFilesystemShardTask) unfreezeShards(
	ctx context.Context,
	client nfs.Client,
) error {

	err := client.UnfreezeTablet(ctx, t.shardID())
	if err != nil {
		return err
	}

	shardFileSystemIDs, err := t.getShardFileSystemIDs(ctx, client)
	if err != nil {
		return err
	}

	for _, shardFileSystemID := range shardFileSystemIDs {
		if shardFileSystemID == t.shardID() {
			continue
		}

		err = client.UnfreezeTablet(ctx, shardFileSystemID)
		if err != nil {
			return err
		}
	}

	return nil
}

////////////////////////////////////////////////////////////////////////////////

func (t *restoreFilesystemShardTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *restoreFilesystemShardTask) Load(request, state []byte) error {
	t.request = &snapshot_protos.RestoreFilesystemShardRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &snapshot_protos.RestoreFilesystemShardTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *restoreFilesystemShardTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	err := t.validateRequest()
	if err != nil {
		return err
	}

	err = t.storage.CheckFilesystemSnapshotReady(ctx, t.snapshotID())
	if err != nil {
		return err
	}

	client, err := t.factory.NewClient(ctx, t.request.GetShard().GetZoneId())
	if err != nil {
		return err
	}
	defer client.Close()

	err = t.freezeShards(ctx, client)
	if err != nil {
		return err
	}

	err = t.restore(ctx, execCtx, client)
	if err != nil {
		return err
	}

	err = t.unfreezeShards(ctx, client)
	if err != nil {
		return err
	}

	return nil
}

func (t *restoreFilesystemShardTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.validateRequest() != nil {
		return nil
	}

	client, err := t.factory.NewClient(ctx, t.request.GetShard().GetZoneId())
	if err != nil {
		return err
	}
	defer client.Close()

	err = t.unfreezeShards(ctx, client)
	if err != nil {
		return err
	}

	return nil
}

func (t *restoreFilesystemShardTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *restoreFilesystemShardTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
