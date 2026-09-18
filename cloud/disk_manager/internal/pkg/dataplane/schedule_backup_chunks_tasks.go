package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/tasks"
)

////////////////////////////////////////////////////////////////////////////////

type scheduleBackupChunksTasks struct {
	request *protos.ScheduleBackupChunksTasksRequest
	state   *protos.ScheduleBackupChunksTasksState
}

func (t *scheduleBackupChunksTasks) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *scheduleBackupChunksTasks) Load(request, state []byte) error {
	t.request = &protos.ScheduleBackupChunksTasksRequest{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.ScheduleBackupChunksTasksState{}
	return proto.Unmarshal(state, t.state)
}

func (t *scheduleBackupChunksTasks) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *scheduleBackupChunksTasks) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *scheduleBackupChunksTasks) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *scheduleBackupChunksTasks) GetResponse() proto.Message {
	return &empty.Empty{}
}
