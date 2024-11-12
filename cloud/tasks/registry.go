package tasks

import (
	"sync"

	"github.com/ydb-platform/nbs/cloud/tasks/errors"
)

////////////////////////////////////////////////////////////////////////////////

type taskFactory struct {
	newTask       func() Task
	canBeExecuted bool
}

type Registry struct {
	taskFactories      map[string]taskFactory
	taskFactoriesMutex sync.RWMutex
}

func (r *Registry) getTaskTypes(forExecutionOnly bool) []string {
	r.taskFactoriesMutex.RLock()
	defer r.taskFactoriesMutex.RUnlock()

	var taskTypes []string
	for taskType, taskFactory := range r.taskFactories {
		if !forExecutionOnly || taskFactory.canBeExecuted {
			taskTypes = append(taskTypes, taskType)
		}
	}

	return taskTypes
}

func (r *Registry) TaskTypes() []string {
	return r.getTaskTypes(false /* forExecutionOnly */)
}

func (r *Registry) TaskTypesForExecution() []string {
	return r.getTaskTypes(true /* forExecutionOnly */)
}

func (r *Registry) Register(
	taskType string,
	newTask func() Task,
) error {

	return r.register(taskType, newTask, false /* canBeExecuted */)
}

func (r *Registry) RegisterForExecution(
	taskType string,
	newTask func() Task,
) error {

	return r.register(taskType, newTask, true /* canBeExecuted */)
}

func (r *Registry) NewTask(taskType string) (Task, error) {
	r.taskFactoriesMutex.RLock()
	defer r.taskFactoriesMutex.RUnlock()

	factory, ok := r.taskFactories[taskType]
	if !ok {
		return nil, errors.NewNonRetriableErrorf(
			"Task factory with type %v can not be found",
			taskType,
		)
	}

	return factory.newTask(), nil
}

////////////////////////////////////////////////////////////////////////////////

func NewRegistry() *Registry {
	return &Registry{
		taskFactories: make(map[string]taskFactory),
	}
}

////////////////////////////////////////////////////////////////////////////////

func (r *Registry) register(
	taskType string,
	newTask func() Task,
	canBeExecuted bool,
) error {

	r.taskFactoriesMutex.Lock()
	defer r.taskFactoriesMutex.Unlock()

	_, ok := r.taskFactories[taskType]
	if ok {
		return errors.NewNonRetriableErrorf(
			"Task factory with type %v already exists",
			taskType,
		)
	}

	r.taskFactories[taskType] = taskFactory{
		newTask:       newTask,
		canBeExecuted: canBeExecuted,
	}
	return nil
}
