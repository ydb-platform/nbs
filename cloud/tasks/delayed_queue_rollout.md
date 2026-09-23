# Delayed task queue rollout and rollback

Delayed tasks are initially stored only in `ready_to_run_delayed`. Binaries from
before delayed execution read only `ready_to_run`: reaching a deadline does not
make an outstanding delayed entry visible to them. Rollback therefore requires
draining this queue before removing the last supporting runner.

## Rollout

1. Create/alter the task tables in every configured `StorageFolder` and
   `LegacyStorageFolder`. Preserve additional columns when applying an older
   schema; do not drop `available_at`, `first_run_started_at`,
   `cancel_requested_at`, or `received_at` during rollback.
2. Keep `CreateSnapshotStaggeringWindow = "0s"` while upgrading every task writer:
   control-plane processes, runners, and administrative tools that cancel or
   update tasks. Update the schema before deploying the new readers/writers.
3. Keep regular system tasks enabled. Reconciliation has its own scheduling
   settings: `ReconcileReadyToRunDelayedEnabled` (default true),
   `ReconcileReadyToRunDelayedTaskScheduleInterval` (default `1h`), and
   `ReconcileReadyToRunDelayedLimit` (default 5000 rows per attempt).
4. Enable a nonzero staggering window after all writers support delayed tasks.

GC is independent of reconciliation. The current and legacy storage folders
have separate regular tasks (`tasks.ReconcileReadyToRunDelayed` and
`tasks.ReconcileLegacyReadyToRunDelayed`), so one unavailable folder does not
prevent progress in the other. Each attempt processes one bounded page and
saves its cursor in the same task's state. Database retries for a page are
bounded by `UpdateTaskTimeout`. The saved upper key bounds a pass; subsequent
insertions and changes behind the cursor are handled on a later pass.

## Rollback to a version without delayed execution

1. Set `CreateSnapshotStaggeringWindow = "0s"` on **all** task creators and verify
   that every process has applied it. Stop any other caller of `ScheduleTaskAt`
   that creates tasks with a future initial execution time. Changing the window
   does not change the deadlines of existing tasks.
2. Keep supporting runners online until existing delayed tasks have started or
   been cancelled. An outstanding task with a future deadline must be allowed
   to reach that deadline, or explicitly cancelled through the normal API.
3. Let reconciliation finish in every storage folder to remove stale entries.
   With regular system tasks disabled, drive `ReconcileReadyToRunDelayed`
   explicitly, persisting each returned cursor until `Done`. Use a separate
   cursor with `StorageFolder` set for each folder. Retry a failed page with its
   previous cursor; never skip forward after an error.
4. Check the **physical** delayed table in every folder. A zero
   `GetDelayedTaskStats().Total` or zero delayed gauge is insufficient: these
   exclude stale/orphaned rows. Run the following read-only checks with the
   actual task table path substituted for `<task-tables-path>`:

   ```sql
   --!syntax_v1
   PRAGMA TablePathPrefix = "<task-tables-path>";
   SELECT COUNT(*) AS remaining FROM ready_to_run_delayed;
   ```

   Every result must be zero. A failed query is not a zero result. If a task
   that has never run can wait on dependencies or be paused, also wait for or
   cancel it: waking it up on the new version could recreate a delayed entry.
   This conservative check must also return zero:

   ```sql
   --!syntax_v1
   PRAGMA TablePathPrefix = "<task-tables-path>";
   SELECT COUNT(*) AS unstarted
   FROM tasks
   WHERE available_at IS NOT NULL AND first_run_started_at IS NULL
       AND status NOT IN (3, 7); -- Finished, Cancelled
   ```

5. Disable `ReconcileReadyToRunDelayedEnabled` on all schedulers, while retaining
   `RegularSystemTasksEnabled = true` and the supporting binaries. Registration
   remains enabled so outstanding maintenance tasks can finish. Wait until no
   unfinished instances of either new maintenance type remain in any folder:

   ```sql
   --!syntax_v1
   PRAGMA TablePathPrefix = "<task-tables-path>";
   SELECT COUNT(*) AS maintenance_tasks
   FROM tasks
   WHERE task_type IN (
       'tasks.ReconcileReadyToRunDelayed',
       'tasks.ReconcileLegacyReadyToRunDelayed'
   ) AND status NOT IN (3, 7);
   ```

6. Recheck the drain conditions after the settings have reached every process,
   then roll back binaries. Keep the extra table and columns. Tasks which have
   already started use the ordinary execution/retry queues; the old version
   can continue processing them.

An immediate downgrade with outstanding delayed tasks is unsupported. If the
new binaries cannot drain them, restore a supporting version or use a separately
reviewed migration. Do not copy future tasks into the ordinary queue as a
shortcut: old workers would execute them before their promised deadline.

## Cancellation timestamps from intermediate versions

New cancellation requests store `cancel_requested_at` once. Repeated requests,
heartbeats, retries, dependency waits and worker restarts preserve it. For a
delayed task that never started, both hanging detectors measure cancellation
age from this timestamp, independently of the original execution deadline.

Intermediate delayed-execution builds without this column cannot provide the
exact historical cancellation time. For such a row, readers use its last
`changed_state_at` (or `created_at` when missing), and the next state write
persists this approximation before changing the transition time. This can
underestimate historical cancellation age, but subsequent new-version locks
do not reset it. Upgrade all writers before relying on this behavior. Ordinary
tasks and delayed tasks that already started retain their existing age rules.
