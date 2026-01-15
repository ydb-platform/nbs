package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/config"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type deleteDiskFromIncrementalTask struct {
	config  *config.DataplaneConfig
	storage storage.Storage
	request *protos.DeleteDiskFromIncrementalRequest
	state   *protos.DeleteDiskFromIncrementalState
}

func (t *deleteDiskFromIncrementalTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *deleteDiskFromIncrementalTask) Load(request, state []byte) error {
	t.request = &protos.DeleteDiskFromIncrementalRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.DeleteDiskFromIncrementalState{}
	return proto.Unmarshal(state, t.state)
}

func (t *deleteDiskFromIncrementalTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return t.storage.DeleteDiskFromIncremental(
		ctx,
		t.request.Disk.ZoneId,
		t.request.Disk.DiskId,
	)
}

func (t *deleteDiskFromIncrementalTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *deleteDiskFromIncrementalTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *deleteDiskFromIncrementalTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
