package scrubbing

import (
	"context"
	"fmt"
	"math/bits"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/clients/nfs"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/listers"
	scrubbing_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/config"
	scrubbing_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal"
	filesystem_traversal_storage "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type OnScrubbedCallback func([]nfs.Node)

////////////////////////////////////////////////////////////////////////////////

type scrubFilesystemTask struct {
	config   *scrubbing_config.FilesystemScrubbingConfig
	factory  nfs.Factory
	storage  filesystem_traversal_storage.Storage
	request  *scrubbing_protos.ScrubFilesystemRequest
	state    *scrubbing_protos.ScrubFilesystemTaskState
	callback OnScrubbedCallback

	histogramMutex sync.Mutex
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
	// The histogram is not saved periodically, so a resumed traversal can have
	// missing or duplicate counts. Keep this flag sticky.
	t.state.FileSizeHistogramUnreliable =
		t.state.FileSizeHistogramUnreliable || rootNodeAlreadyScheduled
	onListedNodes := func(
		ctx context.Context,
		nodes []nfs.Node,
		_ listers.FilesystemLister,
	) error {

		t.addToFileSizeHistogram(nodes)
		t.callback(nodes)
		for _, node := range nodes {
			logging.Debug(ctx, "scrubbing returned inode %v", node)
		}

		return nil
	}

	// Called by the traverser once the root node is added to the listing
	// queue, so that a restarted task does not schedule it again.
	onRootNodeScheduled := func(ctx context.Context) error {
		t.state.RootNodeScheduled = true
		return execCtx.SaveState(ctx)
	}

	traverser, err := traversal.NewFilesystemTraverser(
		t.getSnapshotID(execCtx),
		filesystem.GetFilesystemId(),
		t.request.GetFilesystemCheckpointId(),
		filesystemListerFactory,
		t.storage,
		onRootNodeScheduled,
		onListedNodes,
		nil,
		t.config.GetTraversalConfig(),
		rootNodeAlreadyScheduled,
		nfs.RootNodeID,
	)
	if err != nil {
		return err
	}

	return traverser.Traverse(ctx)
}

func (t *scrubFilesystemTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.storage.ClearDirectoryListingQueue(
		ctx,
		t.getSnapshotID(execCtx),
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

////////////////////////////////////////////////////////////////////////////////

func (t *scrubFilesystemTask) addToFileSizeHistogram(nodes []nfs.Node) {
	t.histogramMutex.Lock()
	defer t.histogramMutex.Unlock()

	for _, node := range nodes {
		if node.Type != nfs.NODE_KIND_FILE {
			continue
		}

		// Hard links are weighted 1/Links instead of deduplicating NodeIDs.
		weight := 1.0
		if node.Links > 1 {
			weight /= float64(node.Links)
		}

		bucket := bits.Len64(node.Size)
		for len(t.state.FileCountBySizeLog2) <= bucket {
			t.state.FileCountBySizeLog2 = append(t.state.FileCountBySizeLog2, 0)
			t.state.FileBytesBySizeLog2 = append(t.state.FileBytesBySizeLog2, 0)
		}

		t.state.FileCountBySizeLog2[bucket] += weight
		t.state.FileBytesBySizeLog2[bucket] += weight * float64(node.Size)
	}
}
