package tasks

import (
	"context"

	"github.com/golang/protobuf/proto"
)

////////////////////////////////////////////////////////////////////////////////

type Task interface {
	// Serialize task state.
	Save() ([]byte, error)

	// Deserialize task state.
	Load(request []byte, state []byte) error

	// Synchronously run the task.
	// At the end it's expected to respond to GetRequest.
	Run(ctx context.Context, execCtx ExecutionContext) error

	// Synchronously cancel the task.
	Cancel(ctx context.Context, execCtx ExecutionContext) error

	GetMetadata(ctx context.Context) (proto.Message, error)

	// It only makes sense after Run has completed successfully.
	// But in that case it must not return nil.
	GetResponse() proto.Message
}

////////////////////////////////////////////////////////////////////////////////

// TaskWithRetryPolicy controls retry limits after Run returns RetriableError.
type TaskWithRetryPolicy interface {
	Task

	// True ignores the limit; false uses RetriableError.IgnoreRetryLimit.
	IgnoreRetryLimit() bool
}

////////////////////////////////////////////////////////////////////////////////

type StepFunc func(done *bool) error

func RunSteps(
	startStep uint32,
	steps []StepFunc,
	saveState func(uint32) error,
) error {

	for idx := startStep; idx < uint32(len(steps)); idx++ {
		stop := false
		err := steps[idx](&stop)
		if err != nil {
			return err
		}

		if stop {
			return nil
		}

		err = saveState(idx + 1)
		if err != nil {
			return err
		}
	}
	return nil
}
