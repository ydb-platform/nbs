package filesystem_scrubbing

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	scrubbing_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_scrubbing/config"
	scrubbing_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_scrubbing/protos"
	filesystem_traversal "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/filesystem_traversal"
	filesystem_snapshot_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem_snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type OnScrubbedCallback func([]nfs.Node)

////////////////////////////////////////////////////////////////////////////////

type scrubFilesystemTask struct {
	config  *scrubbing_config.FilesystemScrubbingConfig
	factory nfs.Factory
	storage filesystem_snapshot_storage.Storage
	request *scrubbing_protos.ScrubFilesystemRequest
	state   *scrubbing_protos.ScrubFilesystemTaskState
	callback OnScrubbedCallback
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
	if filesystem == nil {
		return errors.NewNonRetriableErrorf("filesystem is not set")
	}

	client, err := t.factory.NewClient(ctx, filesystem.GetZoneId())
	if err != nil {
		return err
	}
	defer client.Close()

	rootNodeAlreadyScheduled := t.state.GetRootNodeScheduled()
	if !rootNodeAlreadyScheduled {
		t.state.RootNodeScheduled = true
	}

	traverser := filesystem_traversal.NewFilesystemTraverser(
		execCtx.GetTaskID(),
		filesystem.GetFilesystemId(),
		t.request.GetFilesystemCheckpointId(),
		client,
		t.storage,
		execCtx.SaveState,
		int(t.config.GetTraversalWorkersCount()),
		t.config.GetSelectNodesToListLimit(),
		rootNodeAlreadyScheduled,
	)

	return traverser.Traverse(ctx, func(
		ctx context.Context,
		nodes []nfs.Node,
		_ nfs.Session,
		_ nfs.Client,
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

	return nil
}

func (t *scrubFilesystemTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *scrubFilesystemTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
