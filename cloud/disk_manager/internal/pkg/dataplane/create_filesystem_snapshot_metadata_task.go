package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type createFilesystemSnapshotMetadataTask struct {
	request *protos.CreateFilesystemSnapshotMetadataRequest
	state   *protos.CreateFilesystemSnapshotMetadataTaskState
}

func (t *createFilesystemSnapshotMetadataTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *createFilesystemSnapshotMetadataTask) Load(request, state []byte) error {
	t.request = &protos.CreateFilesystemSnapshotMetadataRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.CreateFilesystemSnapshotMetadataTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *createFilesystemSnapshotMetadataTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	// TODO: implement filesystem metadata backup
	t.state.RootNodeScheduled = true

	return nil
}

func (t *createFilesystemSnapshotMetadataTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *createFilesystemSnapshotMetadataTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.CreateFilesystemSnapshotMetadataTaskState{
		RootNodeScheduled: t.state.RootNodeScheduled,
	}, nil
}

func (t *createFilesystemSnapshotMetadataTask) GetResponse() proto.Message {
	return &protos.CreateFilesystemSnapshotMetadataResponse{}
}
