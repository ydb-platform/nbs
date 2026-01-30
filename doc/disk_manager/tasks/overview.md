# cloud/tasks
`cloud/tasks` provides the asynchronous task engine that powers long-running control-plane workflows across the `cloud` monorepo. It is a self-contained subsystem that offers reliable scheduling, persistence, execution, cancellation, dependency tracking, and observability for tasks that may span minutes or hours and must survive process or host restarts.
## Core building blocks
### Task interface (`task.go`)
```go
type Task interface {
	Save() ([]byte, error)
	Load(request []byte, state []byte) error
	Run(ctx context.Context, execCtx ExecutionContext) error
	Cancel(ctx context.Context, execCtx ExecutionContext) error
	GetMetadata(ctx context.Context) (proto.Message, error)
	GetResponse() proto.Message
}
```
Each task owns its protobuf request and serialized state. `Run` is executed on a worker; `Cancel` is executed when the scheduler decides to abort work. `GetMetadata` feeds operation polling APIs, while `GetResponse` is surfaced when the task finishes.
### ExecutionContext (`execution_context.go`)
`ExecutionContext` is passed into every `Run`/`Cancel` call and encapsulates persistence concerns:
- `SaveState`/`SaveStateWithPreparation` persist state blobs, optionally in the same DB transaction as custom logic (`persistence.Transaction` hook).
- `FinishWithPreparation` and `finish` transition tasks to `Finished`.
- `AddTaskDependency` and `HasEvent` expose dependency graphs and integer event flags stored alongside the task row.
- `SetEstimatedInflightDuration` / `SetEstimatedStallingDuration` supply expected durations that are later used for hang detection.
- `IsHanging` reports whether the current task broke timing guarantees (global timeouts live in `tasks/config`).
- `SetEstimated*` overrides only if the DB has not populated the estimate (helpful for dynamic overrides).
### Registry (`registry.go`)
The registry is the factory for all tasks. Services register:
- `Register` – task can be scheduled but should never be executed on this binary (used by clients).
- `RegisterForExecution` – task may be picked up by runners on this binary.
Registrations happen during service bootstrap before the scheduler or runners are started.
### Scheduler (`scheduler.go`, `scheduler_impl.go`)
The scheduler is the API surface exposed to services. It is responsible for:
- Persisting tasks into storage via `CreateTask` (idempotent per account and idempotency key).
- Scheduling zonal tasks (`ScheduleZonalTask`) so that only runners configured for the zone (`TasksConfig.ZoneIds`) will grab them.
- Registering cron-like regular tasks via `ScheduleRegularTasks` (`TaskSchedule` supports fixed intervals or simple daily crontab).
- Canceling tasks (`CancelTask`), waiting for completions (`WaitTask`, `WaitAnyTasks`, `WaitTaskEnded`), propagating events (`SendEvent`), and exposing metadata/operation protos (`GetTaskMetadata`, `GetOperation`).
- Bootstrapping built-in system tasks (e.g. `tasks.ClearEndedTasks`, `tasks.CollectListerMetrics`) when `RegularSystemTasksEnabled` is true.
### Storage layer (`storage` package)
`Storage` abstracts the durable backend. The default implementation (`storage/storage_ydb.go`) talks to YDB through the `persistence` package. A `compoundStorage` wrapper optionally fronts both legacy and new tables to simplify migrations (`storage/compound_storage.go`).
`TaskState` is the canonical schema that lives in storage. Notable fields:
- Status (`TaskStatusReadyToRun`, `Running`, `Finished`, `ReadyToCancel`, `Cancelling`, `Cancelled`).
- Request/state blobs, protobuf error payloads, retriable error counters, panic counters.
- Dependency tracking (`Dependencies`/`dependants`) and event integers.
- Accounting metadata (`AccountID`, `IdempotencyKey`, `StorageFolder`) pulled from gRPC headers via `headers`.
- Zone affinity (`ZoneID`) and tracing metadata.
- Duration bookkeeping (`InflightDuration`, `StallingDuration`, `WaitingDuration`, estimates).
The storage interface additionally exposes operational helpers (`ListTasksRunning`, `ListHangingTasks`, `ForceFinishTask`, `PauseTask`, `ResumeTask`, `HeartbeatNode`, `GetAliveNodes`) that SRE tools rely on.
### Controller, listers, and runners (`controller.go`, `lister.go`, `runner.go`)
- `Controller` wires runners to health checks. When `NodeEvictionEnabled` is on, it stops runners on unhealthy nodes and restarts them once healthy again.
- Listers continuously query storage for tasks that are `ready_to_run`, `ready_to_cancel`, or stuck in `running/cancelling`. They add jitter, enforce per-type inflight limits, and feed tasks into buffered channels.
- Regular runners execute new work; stalking runners retry tasks that stalled on other hosts. Both rely on `taskPinger` to periodically update `ModifiedAt` and durations to avoid spurious hang detection.
- Every runner locks a task (`LockTaskToRun` / `LockTaskToCancel`), reconstructs it via the registry, attaches tracing metadata, and drives either `Run` or `Cancel`.
## Task lifecycle (happy path)
1. Client code calls `Scheduler.ScheduleTask` with a task type and protobuf request. The scheduler persists the task and returns the task ID.
2. A lister notices the task in `ready_to_run` state and pushes it to a runner.
3. The runner locks the task (generation-checked), loads the request/state into a concrete `Task`, and spawns a goroutine that pings storage while execution is in progress.
4. The task logic calls `execCtx.SaveState` whenever it wants to persist partial progress and uses `execCtx.WaitTask` to block on other tasks, which automatically records dependencies.
5. On success, the runner finalizes the task via `execCtx.finish`, allowing dependants to transition to runnable. On failure, specialized error handling kicks in (see below).
6. Once `TaskStatusFinished` is persisted, clients polling via `WaitTask`, `GetOperation`, or service-specific facades receive the final response, and TTL cleaners eventually purge the row.
## Scheduler API surface
- `ScheduleTask` / `ScheduleZonalTask` – enqueue tasks with optional zone affinity.
- `ScheduleRegularTasks` – continuously enqueue copies of a task respecting `TaskSchedule` (either a period or daily crontab; per-task inflight limits avoid piling up).
- `CancelTask` – mark a task for cancellation; returns whether cancellation was already in progress.
- `WaitTask` – used by task code to wait for another task; automatically records dependencies so storage will block dependants until prerequisites finish.
- `WaitAnyTasks`, `WaitTaskEnded`, `WaitTaskSync` – helper APIs for orchestration and tests.
- `GetTaskMetadata` / `GetOperation` – fetch metadata or wrap results into `operation.Operation` protos suitable for public APIs.
- `SendEvent` – emit integer events that tasks can observe through `ExecutionContext.HasEvent`.
- `ScheduleBlankTask` – schedules a no-op task, mostly for tests and health checks.
## Execution semantics and helpers
- **State management:** Use `SaveState` early and often inside `Run` so that retries resume from the last checkpoint. `RunSteps` is a convenience helper for linear state machines that want to checkpoint between steps.
- **Dependencies:** `Scheduler.WaitTask` appends the awaited task ID to the dependant’s `Dependencies` set; storage keeps reciprocal `dependants` so it can transition blocked tasks when prerequisites reach a terminal status.
- **Events:** `SendEvent`/`HasEvent` lets external actors poke a running task (e.g. to unblock after waiting on external confirmation) without rescheduling.
- **Storage folders:** `storage_folder.go` propagates a hidden gRPC metadata key so that child tasks reuse the same storage shard (`TasksConfig.StorageFolder` vs `LegacyStorageFolder`) during migrations.
- **Metadata propagation:** The scheduler captures tracing headers, account IDs, idempotency keys, and request IDs through the `headers` package. These headers are re-attached when runners execute the task so downstream RPCs continue the same trace.
## Error handling and retries (`errors/errors.go`, `runner.go`)
Runners interpret errors according to their type:
- `errors.RetriableError` – increments the retriable counter and reschedules unless the per-task or per-type limit (configurable via `MaxRetriableErrorCount` and the per-type override map) is exceeded.
- `errors.NonRetriableError` – persisted into the task row and finishes the task with failure.
- `errors.NonCancellableError` – used when cancellation cannot proceed; the task transitions straight to `Cancelled`.
- `errors.AbortedError` – clears state so the task restarts from the beginning (useful when an upstream dependency changed in a way that invalidates saved state).
- `errors.PanicError` – produced from panics; a per-task `PanicCount` prevents endless crash loops before failing the task.
- `errors.WrongGenerationError` / `errors.InterruptExecutionError` – treated as benign race conditions (e.g. another worker already advanced the state or the waiter timed out).
There is also `errors.NonRetriableError{Silent: true}` for user-visible operations that should not spam logs, `errors.DetailedError` for attaching protobuf details, and `errors.NotFoundError` surfaced by storage.
## Storage model and persistence
- Tasks live in YDB tables (see `storage/storage_ydb_impl.go` for the SQL used).
- Listings (`ListTasksReadyToRun`, etc.) respect the node’s zone allow-list so that zonal affinity is enforced. Tasks without a zone can land anywhere.
- Heartbeats (`HeartbeatNode`) update a node registry table that `ListTasksStalling*` uses to avoid stealing work that is still progressing elsewhere.
- `SendEvent`, dependency maintenance, and transitions are updated in a single transaction to keep the graph consistent (`prepareUnfinishedDependencies`, `prepareDependenciesToClear`).
- Compound storage falls back to legacy tables when the incoming task doesn’t specify a folder, enabling gradual migrations.
## Node topology, listers, and runners
- Two families of listers run per node: “ready” (feed new work) and “stalling” (hunt for tasks that exceeded stalling/inflight thresholds).
- Each family produces tasks for both run and cancel channels so specialized runner goroutines can focus on execution or cancellation paths.
- Config knobs (`TasksConfig.PollForTasksPeriod*`, `TasksToListLimit`, `InflightTaskPerNodeLimits`) govern how aggressively listers pull work.
- `startHeartbeats` sends periodic node heartbeats (`HearbeatInterval`) with inflight counts so SRE dashboards can spot stuck nodes and so node eviction logic has data.
- Stalking runners are safe to run with `ExceptHangingTaskTypes` to ignore noisy task types when producing hanging metrics.
## Regular and system tasks
- `Scheduler.ScheduleRegularTasks` is available to services; the scheduler itself uses it to register `tasks.ClearEndedTasks` (garbage collect completed rows older than `EndedTaskExpirationTimeout`) and `tasks.CollectListerMetrics` (push per-task-type gauges) when `RegularSystemTasksEnabled` is true.
- System tasks are registered inside `scheduler.registerAndScheduleRegularSystemTasks` so that any binary embedding the scheduler automatically benefits from cleanup/metrics duty.
## Configuration (`config/config.proto`)
Important knobs exposed through `TasksConfig`:
- Polling cadence: `PollForTaskUpdatesPeriod`, `PollForTasksPeriodMin/Max`, `PollForStallingTasksPeriodMin/Max`, `TaskPingPeriod`, `TaskWaitingTimeout`.
- Concurrency: `RunnersCount`, `StalkingRunnersCount`, per-type inflight limits (`InflightTaskPerNodeLimits`), `MaxRetriableErrorCount`, `MaxPanicCount`.
- Hang detection: `HangingTaskTimeout`, `InflightHangingTaskTimeout`, `StallingHangingTaskTimeout`, `MissedEstimatesUntilTaskIsHanging`, override maps for estimated durations.
- Storage layout: `StorageFolder`, `LegacyStorageFolder`, `EndedTaskExpirationTimeout`, limit for cleanup batch size.
- Zonal awareness & heartbeats: `ZoneIds`, `HearbeatInterval`, `LivenessWindow`, `NodeEvictionEnabled`.
- Observability: `CollectListerMetricsTaskScheduleInterval`, `ListerMetricsCollectionInterval`, `MaxSampledTaskGeneration`, `AvailabilityMonitoringSuccessRateReportingPeriod`.
The config proto is compiled into the `tasks_config` Go package; see `pkg/app/controlplane.go` and service registries for real-world construction.
## Observability and tooling
- Logs include task IDs, runner IDs, and hostnames via the `logging` package (`logging.WithComponent` marks whether events originate from the scheduler, runner, or task).
- Structured metrics flow through `metrics.Registry` with helpers in `runner_metrics.go` and per-task instrumentation in `metrics` subpackages.
- Tracing: task execution spans are created via `tracing.StartSpanWithSampling` and respect `MaxSampledTaskGeneration` to avoid overwhelming tracing backends.
- YDB availability monitoring has its own helpers under `persistence/availability_monitoring*.go`.
- Public-facing APIs expose status via `operation.Operation` protos built in `Scheduler.GetOperation`.
- Events and dependency graph data can be inspected through storage helpers and tests (`storage/storage_ydb_test.go` contains numerous scenarios).
## Extending the system
1. **Define the protobuf request/response** in the API package and regenerate Go code.
2. **Implement the Task** (conventionally under `internal/pkg/services/<domain>/tasks`): embed request/response structs, implement `Save`/`Load`, and write `Run`/`Cancel` using `ExecutionContext` helpers.
3. **Register the task** inside the relevant service `register.go` via `registry.RegisterForExecution`.
4. **Expose a facade** method that calls `Scheduler.ScheduleTask` (or `.ScheduleZonalTask` / `.ScheduleRegularTasks`) with the new task type.
5. **Write tests** using `tasks_tests` integration harness, `runner_test.go` helpers, or service-level mocks in `mocks/`.
6. **Add observability**: emit metrics/traces inside the task, set estimated durations when they are known, and add metadata for UI surfaces via `GetMetadata`.
## Testing
- `tasks_tests` contains stress and regression tests that spin up in-memory storage implementations.
- `runner_test.go` and `scheduler_test.go` cover the orchestration logic and serve as good references for expected state transitions.
- `acceptance_tests/recipe` runs a miniature cluster that exercises the full runner + storage stack.
- `mocks/` packages provide gomock-style mocks for injecting fake storages, registries, or schedulers in service tests.
## Related packages
- `headers`, `logging`, `metrics`, and `tracing` glue cloud-wide infrastructure into tasks.
- `storage` depends on `persistence` (YDB/S3 clients) and exports helper structs/metrics.
- `operation` defines the protobuf returned by `Scheduler.GetOperation`.
- `common` holds low-level helpers such as `StringSet` and random jitter utilities.
Use this README as the entry point when navigating the task system; the referenced files provide the implementation details.
