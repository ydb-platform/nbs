package tasks

import (
	"context"
	"encoding/json"
	"time"

	tasks_config "github.com/ydb-platform/nbs/cloud/tasks/config"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/storage"
)

// Each storage folder has its own regular task and cursor. A failure in legacy
// storage must not stop reconciliation of current storage, or vice versa.
type reconcileDelayedTasksTask struct {
	blankTask
	storage storage.Storage
	limit   int
	folder  string
	cursor  storage.DelayedQueueCursor
}

func (t *reconcileDelayedTasksTask) Save() ([]byte, error) {
	return json.Marshal(t.cursor)
}

func (t *reconcileDelayedTasksTask) Load(_, state []byte) error {
	t.cursor = storage.DelayedQueueCursor{StorageFolder: t.folder}
	if len(state) == 0 {
		return nil
	}
	return json.Unmarshal(state, &t.cursor)
}

func (t *reconcileDelayedTasksTask) Run(ctx context.Context, execCtx ExecutionContext) error {
	next, err := t.storage.ReconcileReadyToRunDelayed(ctx, t.limit, t.cursor)
	if err != nil {
		return errors.NewRetriableErrorWithIgnoreRetryLimit(err)
	}
	t.cursor = next
	if err := execCtx.SaveState(ctx); err != nil {
		return err
	}
	if !t.cursor.Done {
		// Yield the runner after one page. The same task is picked up again
		// with its persisted cursor; this is not a failed reconciliation.
		return errors.NewInterruptExecutionError()
	}
	return nil
}

func (s *scheduler) registerAndScheduleDelayedQueueReconciliation(
	ctx context.Context,
	config *tasks_config.TasksConfig,
) error {
	limit := int(config.GetReconcileReadyToRunDelayedLimit())
	if limit <= 0 {
		return errors.NewNonRetriableErrorf("delayed queue reconciliation limit must be positive")
	}
	interval, err := time.ParseDuration(config.GetReconcileReadyToRunDelayedTaskScheduleInterval())
	if err != nil {
		return err
	}
	if interval <= 0 {
		return errors.NewNonRetriableErrorf("delayed queue reconciliation interval must be positive")
	}

	type folderTask struct {
		taskType string
		folder   string
	}
	folders := []folderTask{{"tasks.ReconcileReadyToRunDelayed", config.GetStorageFolder()}}
	if folder := config.GetLegacyStorageFolder(); folder != "" && folder != config.GetStorageFolder() {
		folders = append(folders, folderTask{"tasks.ReconcileLegacyReadyToRunDelayed", folder})
	}
	for _, entry := range folders {
		folder := entry.folder
		err := s.registry.RegisterForExecution(entry.taskType, func() Task {
			return &reconcileDelayedTasksTask{storage: s.storage, limit: limit, folder: folder}
		})
		if err != nil {
			return err
		}
		// Registration is retained when scheduling is disabled so outstanding
		// tasks can finish before the last supporting binary is rolled back.
		if config.GetReconcileReadyToRunDelayedEnabled() {
			s.scheduleRegularTasksInFolder(
				ctx,
				entry.taskType,
				entry.folder,
				TaskSchedule{
					ScheduleInterval: interval,
					MaxTasksInflight: 1,
				},
			)
		}
	}
	return nil
}
