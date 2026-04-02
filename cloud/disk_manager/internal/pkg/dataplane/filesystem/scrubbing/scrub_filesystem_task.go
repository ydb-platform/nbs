package scrubbing

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/listers"
	scrubbing_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/config"
	scrubbing_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal"
	filesystem_snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type OnScrubbedCallback func([]nfs.Node)

////////////////////////////////////////////////////////////////////////////////

type scrubFilesystemTask struct {
	config   *scrubbing_config.FilesystemScrubbingConfig
	factory  nfs.Factory
	storage  filesystem_snapshot_storage.Storage
	request  *scrubbing_protos.ScrubFilesystemRequest
	state    *scrubbing_protos.ScrubFilesystemTaskState
	callback OnScrubbedCallback
}

func (t *scrubFilesystemTask) getSnapshotID(
	execCtx tasks.ExecutionContext,
) string {

	return fmt.Sprintf("scrubbing_%s", execCtx.GetTaskID())
}

func (t *scrubFilesystemTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *scrubFilesystemTask) Load(request, state []byte) error {
	t.request = &scrubbing_protos.ScrubFilesystemRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &scrubbing_protos.ScrubFilesystemTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *scrubFilesystemTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	filesystem := t.request.GetFilesystem()
	client, err := t.factory.NewClient(ctx, filesystem.GetZoneId())
	if err != nil {
		return err
	}
	defer client.Close()

	filesystemListerFactory := listers.NewFilestoreListerFactory(
		client,
		t.config.GetListNodesMaxBytes(),
		true, // readOnly
		true, // unsafe
		true, // ignoreNotFound
	)

	rootNodeAlreadyScheduled := t.state.GetRootNodeScheduled()
	traverser := traversal.NewFilesystemTraverser(
		t.getSnapshotID(execCtx),
		filesystem.GetFilesystemId(),
		t.request.GetFilesystemCheckpointId(),
		filesystemListerFactory,
		t.storage,
		func(ctx context.Context) error {
			t.state.RootNodeScheduled = true
			return execCtx.SaveState(ctx)
		},
		t.config.GetTraversalConfig(),
		rootNodeAlreadyScheduled,
		nfs.RootNodeID,
	)

	return traverser.Traverse(ctx, func(
		ctx context.Context,
		nodes []nfs.Node,
		_ listers.FilesystemLister,
	) error {

		t.callback(nodes)
		for _, node := range nodes {
			logging.Debug(ctx, "scrubbing returned inode %v", node)
		}

		return nil
	})
}

func (t *scrubFilesystemTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.storage.ClearDirectoryListingQueue(
		ctx,
		t.getSnapshotID(execCtx),
		t.config.GetTraversalQueueDeletionLimit(),
	)
}

func (t *scrubFilesystemTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *scrubFilesystemTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
