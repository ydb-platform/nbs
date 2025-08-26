package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/ydb-platform/nbs/cloud/tasks/common"
	"github.com/ydb-platform/nbs/cloud/tasks/errors"
	"github.com/ydb-platform/nbs/cloud/tasks/persistence"
	grpc_codes "google.golang.org/grpc/codes"
	grpc_status "google.golang.org/grpc/status"
)

////////////////////////////////////////////////////////////////////////////////

type TaskStatus uint32

func (s *TaskStatus) UnmarshalYDB(res persistence.RawValue) error {
	*s = TaskStatus(res.Int64())
	return nil
}

// NOTE: These values are stored in DB, do not shuffle them around.
const (
	TaskStatusReadyToRun      TaskStatus = iota
	TaskStatusWaitingToRun    TaskStatus = iota
	TaskStatusRunning         TaskStatus = iota
	TaskStatusFinished        TaskStatus = iota
	TaskStatusReadyToCancel   TaskStatus = iota
	TaskStatusWaitingToCancel TaskStatus = iota
	TaskStatusCancelling      TaskStatus = iota
	TaskStatusCancelled       TaskStatus = iota
)

func TaskStatusToString(status TaskStatus) string {
	switch status {
	case TaskStatusReadyToRun:
		return "ready_to_run"
	case TaskStatusWaitingToRun:
		return "waiting_to_run"
	case TaskStatusRunning:
		return "running"
	case TaskStatusFinished:
		return "finished"
	case TaskStatusReadyToCancel:
		return "ready_to_cancel"
	case TaskStatusWaitingToCancel:
		return "waiting_to_cancel"
	case TaskStatusCancelling:
		return "cancelling"
	case TaskStatusCancelled:
		return "cancelled"
	}
	return fmt.Sprintf("unknown_%v", status)
}

func HasResult(status TaskStatus) bool {
	return status > TaskStatusRunning
}

func IsEnded(status TaskStatus) bool {
	return status == TaskStatusFinished || status == TaskStatusCancelled
}

func IsCancellingOrCancelled(status TaskStatus) bool {
	return status >= TaskStatusReadyToCancel
}

////////////////////////////////////////////////////////////////////////////////

type Metadata struct {
	vals map[string]string
}

func (m *Metadata) Vals() map[string]string {
	return m.vals
}

func (m *Metadata) Put(key string, val string) {
	if m.vals == nil {
		m.vals = make(map[string]string)
	}

	m.vals[key] = val
}

func (m *Metadata) Remove(key string) {
	if m.vals != nil {
		delete(m.vals, key)
	}
}

func (m *Metadata) DeepCopy() Metadata {
	return NewMetadata(m.vals)
}

func NewMetadata(vals map[string]string) Metadata {
	if len(vals) == 0 {
		return Metadata{}
	}

	m := Metadata{
		vals: make(map[string]string),
	}

	for key, val := range vals {
		m.Put(key, val)
	}

	return m
}

////////////////////////////////////////////////////////////////////////////////

// This is mapped into a DB row. If you change this struct, make sure to update
// the mapping code.
type TaskState struct {
	ID                  string
	IdempotencyKey      string
	AccountID           string
	TaskType            string
	Regular             bool
	Description         string
	StorageFolder       string
	CreatedAt           time.Time
	CreatedBy           string
	ModifiedAt          time.Time
	GenerationID        uint64
	Status              TaskStatus
	ErrorCode           grpc_codes.Code
	ErrorMessage        string
	ErrorDetails        *errors.ErrorDetails
	ErrorSilent         bool
	RetriableErrorCount uint64
	Request             []byte
	State               []byte
	Metadata            Metadata
	Dependencies        common.StringSet
	ChangedStateAt      time.Time
	EndedAt             time.Time
	LastHost            string
	LastRunner          string
	ZoneID              string
	EstimatedDuration   time.Duration
	InflightDuration    time.Duration
	StallingDuration    time.Duration
	WaitingDuration     time.Duration
	PanicCount          uint64
	Events              []int64

	// Internal part of the state. Fully managed by DB and can't be overwritten
	// by client.
	// TODO: Should be extracted from TaskState.
	dependants common.StringSet
}

func (s *TaskState) DeepCopy() TaskState {
	copied := *s

	if s.ErrorDetails != nil {
		copied.ErrorDetails = proto.Clone(s.ErrorDetails).(*errors.ErrorDetails)
	}

	copied.Metadata = s.Metadata.DeepCopy()
	copied.Dependencies = s.Dependencies.DeepCopy()
	// Unnecessary. We do it only for convenience.
	copied.dependants = s.dependants.DeepCopy()
	return copied
}

func (s *TaskState) SetError(e error) {
	status := grpc_status.Convert(e)
	s.ErrorCode = status.Code()
	s.ErrorMessage = status.Message()
	s.ErrorSilent = errors.IsSilent(e)

	detailedError := errors.NewEmptyDetailedError()
	if errors.As(e, &detailedError) {
		s.ErrorDetails = detailedError.Details
	}
}

////////////////////////////////////////////////////////////////////////////////

type TaskInfo struct {
	ID           string
	GenerationID uint64
	TaskType     string
}

type TaskSchedule struct {
	ScheduleInterval time.Duration
	MaxTasksInflight int

	// Crontab params.
	// Schedules task every day - only 'hour' and 'min' are supported.
	UseCrontab bool // If set, ScheduleInterval is ignored.
	Hour       int  // (0 - 23)
	Min        int  // (0 - 59)
}

////////////////////////////////////////////////////////////////////////////////

type Storage interface {
	// Attempt to register new task in the storage. TaskState.ID is ignored.
	// Returns generated task id for newly created task.
	// Returns existing task id if the task with the same idempotency key and
	// the same account id already exists.
	CreateTask(ctx context.Context, state TaskState) (string, error)

	// Creates periodically scheduled task.
	CreateRegularTasks(
		ctx context.Context,
		state TaskState,
		schedule TaskSchedule,
	) error

	GetTask(ctx context.Context, taskID string) (TaskState, error)

	GetTaskByIdempotencyKey(
		ctx context.Context,
		idempotencyKey string,
		accountID string,
	) (TaskState, error)

	// Used in task execution workflow.
	ListTasksReadyToRun(
		ctx context.Context,
		limit uint64,
		taskTypeWhitelist []string,
	) ([]TaskInfo, error)

	// Used in task execution workflow.
	ListTasksReadyToCancel(
		ctx context.Context,
		limit uint64,
		taskTypeWhitelist []string,
	) ([]TaskInfo, error)

	// Lists tasks that are currently running but don't make any progress for
	// some time.
	ListTasksStallingWhileRunning(
		ctx context.Context,
		excludingHostname string,
		limit uint64,
		taskTypeWhitelist []string,
	) ([]TaskInfo, error)

	// Lists tasks that are currently cancelling but don't make any progress for
	// some time.
	ListTasksStallingWhileCancelling(
		ctx context.Context,
		excludingHostname string,
		limit uint64,
		taskTypeWhitelist []string,
	) ([]TaskInfo, error)

	// Used for SRE tools and metrics collection.
	ListTasksRunning(ctx context.Context, limit uint64) ([]TaskInfo, error)
	ListTasksCancelling(ctx context.Context, limit uint64) ([]TaskInfo, error)

	// Used for metrics collection.
	// Wraps some of the ListTasks* methods above.
	ListTasksWithStatus(
		ctx context.Context,
		status string,
	) ([]TaskInfo, error)

	// Used for SRE tools.
	ListHangingTasks(ctx context.Context, limit uint64) ([]TaskInfo, error)
	ListFailedTasks(ctx context.Context, since time.Time) ([]string, error)
	ListSlowTasks(ctx context.Context, since time.Time, estimateMiss time.Duration) ([]string, error)

	// Fails with WrongGenerationError, if generationID does not match.
	LockTaskToRun(
		ctx context.Context,
		taskInfo TaskInfo,
		at time.Time,
		hostname string,
		runner string,
	) (TaskState, error)

	// Fails with WrongGenerationError, if generationID does not match.
	LockTaskToCancel(
		ctx context.Context,
		taskInfo TaskInfo,
		at time.Time,
		hostname string,
		runner string,
	) (TaskState, error)

	// Mark task for cancellation.
	// Returns true if it's already cancelling (or cancelled)
	// Returns false if it has successfully finished.
	MarkForCancellation(ctx context.Context, taskID string, at time.Time) (bool, error)

	// This fails with WrongGenerationError, if generationID does not match.
	// In callback you could perform custom transaction and it will be coupled
	// with current task's updating.
	UpdateTaskWithPreparation(
		ctx context.Context,
		state TaskState,
		preparation func(context.Context, *persistence.Transaction) error,
	) (TaskState, error)

	// This fails with WrongGenerationError, if generationID does not match.
	UpdateTask(ctx context.Context, state TaskState) (TaskState, error)

	SendEvent(ctx context.Context, taskID string, event int64) error

	// Used for garbage collecting of ended and outdated tasks.
	ClearEndedTasks(ctx context.Context, endedBefore time.Time, limit int) error

	// NOTE: used for SRE operations only.
	// Forcefully finishes task by setting "finished" status.
	ForceFinishTask(ctx context.Context, taskID string) error

	// NOTE: used for SRE operations only.
	// Pauses task execution until ResumeTask is called.
	PauseTask(ctx context.Context, taskID string) error

	// NOTE: used for SRE operations only.
	ResumeTask(ctx context.Context, taskID string) error

	// Update last heartbeat column.
	HeartbeatNode(ctx context.Context, host string, ts time.Time, inflightTaskCount uint32) error

	// Fetch the nodes that have recently send heartbeats.
	GetAliveNodes(ctx context.Context) ([]Node, error)

	// Fetch the node.
	// This method is used in testing, but can be safely used if needed.
	GetNode(ctx context.Context, host string) (Node, error)
}
