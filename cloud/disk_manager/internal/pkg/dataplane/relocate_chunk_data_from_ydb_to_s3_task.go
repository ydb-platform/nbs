package dataplane

import (
	"context"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/logging"
)

////////////////////////////////////////////////////////////////////////////////

type relocateChunkDataFromYDBToS3Task struct {
	storage storage.Storage
	request *protos.RelocateChunkDataFromYDBToS3Request
	state   *protos.RelocateChunkDataFromYDBToS3TaskState
}

func (t *relocateChunkDataFromYDBToS3Task) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *relocateChunkDataFromYDBToS3Task) Load(request, state []byte) error {
	t.request = &protos.RelocateChunkDataFromYDBToS3Request{}
	err := proto.Unmarshal(request, t.request)
	if err != nil {
		return err
	}

	t.state = &protos.RelocateChunkDataFromYDBToS3TaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *relocateChunkDataFromYDBToS3Task) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	total := len(t.request.ChunkIds)
	for index := int(t.state.MilestoneIndex); index < total; index++ {
		chunkID := t.request.ChunkIds[index]
		if len(chunkID) == 0 {
			t.state.MilestoneIndex = uint32(index + 1)
			continue
		}

		err := t.storage.RelocateChunkDataToS3(
			ctx,
			chunkID,
			t.request.KeepYdbData,
		)
		if err != nil {
			return err
		}

		t.state.MilestoneIndex = uint32(index + 1)
		err = t.saveProgress(ctx, execCtx, total)
		if err != nil {
			return err
		}
	}

	t.state.Progress = 1
	return execCtx.SaveState(ctx)
}

func (t *relocateChunkDataFromYDBToS3Task) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *relocateChunkDataFromYDBToS3Task) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &protos.RelocateChunkDataFromYDBToS3Metadata{
		Progress: t.state.Progress,
	}, nil
}

func (t *relocateChunkDataFromYDBToS3Task) GetResponse() proto.Message {
	return &protos.RelocateChunkDataFromYDBToS3Response{}
}

////////////////////////////////////////////////////////////////////////////////

func (t *relocateChunkDataFromYDBToS3Task) saveProgress(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
	total int,
) error {

	if total != 0 {
		t.state.Progress = float64(t.state.MilestoneIndex) / float64(total)
	}

	logging.Debug(ctx, "saving relocate chunk data to s3 state %+v", t.state)
	return execCtx.SaveState(ctx)
}
