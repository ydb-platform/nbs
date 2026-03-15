package scrubbing

import (
	"context"
	"fmt"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/empty"
	scrubbing_config "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/config"
	scrubbing_protos "github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/protos"
	"github.com/ydb-platform/nbs/cloud/disk_manager/internal/pkg/types"
	"github.com/ydb-platform/nbs/cloud/tasks"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/headers"
)

////////////////////////////////////////////////////////////////////////////////

type regularScrubFilesystemsTask struct {
	config    *scrubbing_config.FilesystemScrubbingConfig
	scheduler tasks.Scheduler
	state     *scrubbing_protos.RegularScrubFilesystemsTaskState
}

func (t *regularScrubFilesystemsTask) Save() ([]byte, error) {
	return proto.Marshal(t.state)
}

func (t *regularScrubFilesystemsTask) Load(_, state []byte) error {
	t.state = &scrubbing_protos.RegularScrubFilesystemsTaskState{}
	return proto.Unmarshal(state, t.state)
}

func (t *regularScrubFilesystemsTask) idempotencyKey(
	filesystemID string,
	generation uint64,
) string {

	return fmt.Sprintf("%v_%v", filesystemID, generation)
}

func (t *regularScrubFilesystemsTask) Run(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	if t.state.IdempotencyKeyGenerations == nil {
		t.state.IdempotencyKeyGenerations = make(map[string]uint64)
	}

	filesystems := t.config.GetFilesystemsWithRegularScrubbingEnabled()

	taskIDs := make([]string, 0, len(filesystems))
	taskIDToFilesystemID := make(map[string]string, len(filesystems))

	for _, filesystem := range filesystems {
		fsID := filesystem.GetFilesystemId()
		generation := t.state.IdempotencyKeyGenerations[fsID]

		taskID, err := t.scheduler.ScheduleTask(
			headers.SetIncomingIdempotencyKey(
				ctx,
				t.idempotencyKey(fsID, generation),
			),
			"dataplane.ScrubFilesystem",
			"Scrub filesystem "+fsID,
			&scrubbing_protos.ScrubFilesystemRequest{
				Filesystem: &types.Filesystem{
					ZoneId:       filesystem.GetZoneId(),
					FilesystemId: fsID,
				},
			},
		)
		if err != nil {
			return err
		}

		taskIDs = append(taskIDs, taskID)
		taskIDToFilesystemID[taskID] = fsID
	}

	finishedTaskIDs, err := t.scheduler.WaitAnyTasks(ctx, taskIDs)
	if err != nil {
		return err
	}

	for _, taskID := range finishedTaskIDs {
		fsID := taskIDToFilesystemID[taskID]
		t.state.IdempotencyKeyGenerations[fsID]++
	}

	err = execCtx.SaveState(ctx)
	if err != nil {
		return err
	}

	return errors.NewInterruptExecutionError()
}

func (t *regularScrubFilesystemsTask) Cancel(
	ctx context.Context,
	execCtx tasks.ExecutionContext,
) error {

	return nil
}

func (t *regularScrubFilesystemsTask) GetMetadata(
	ctx context.Context,
) (proto.Message, error) {

	return &empty.Empty{}, nil
}

func (t *regularScrubFilesystemsTask) GetResponse() proto.Message {
	return &empty.Empty{}
}
