package dataplane

import (
	"context"
	"strings"

	"github.com/golang/protobuf/proto"

	api "github.com/ydb-platform/nbs/cloud/disk_manager/api"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

// Describes export operations on nodes without a transfer executor.
// ExportState remains opaque so worker checkpoints can be read unchanged.
type exportSnapshotTask struct {
	request *protos.ExportSnapshotRequest
	state   *protos.ExportSnapshotTaskState
}

func (t *exportSnapshotTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *exportSnapshotTask) Load(request, state []byte) error {
	t.request = &protos.ExportSnapshotRequest{}
	if err := proto.Unmarshal(request, t.request); err != nil {
		return err
	}

	t.state = &protos.ExportSnapshotTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *exportSnapshotTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return errors.NewNonRetriableErrorf(
		"snapshot export execution is not registered on this host",
	)
}

func (t *exportSnapshotTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return errors.NewNonRetriableErrorf(
		"snapshot export execution is not registered on this host",
	)
}

func (t *exportSnapshotTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &api.ExportMetadata{Progress: t.state.Progress}, nil
}

func (t *exportSnapshotTask) GetResponse() proto.Message {
	prefix := strings.Trim(t.request.Destination.GetPrefix(), "/")
	if prefix != "" {
		prefix += "/"
	}

	return &api.ExportResponse{
		Bucket:      t.request.Destination.GetBucket(),
		ManifestKey: prefix + "manifest.json",
	}
}

// Registers the child task for operation metadata, without execution.
func RegisterExportForSerialization(registry *tasks.Registry) error {
	return registry.Register(
		"dataplane.ExportSnapshotToS3",
		func() tasks.Task { return &exportSnapshotTask{} },
	)
}
