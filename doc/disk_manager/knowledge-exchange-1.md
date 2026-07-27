---
marp: true
title: Knowledge Exchange 1
description: Disk Manager architecture and code walkthrough
theme: default
paginate: true
size: 16:9
style: |
  section {
    font-size: 27px;
    line-height: 1.25;
    padding: 48px 64px;
  }
  section.compact { font-size: 23px; }
  section.tight { font-size: 20px; }
  section.lead h1 { font-size: 54px; }
  h1, h2 { color: #203864; }
  a { color: #0067b8; }
  code { background: #eef3f8; color: #17365d; }
  blockquote {
    border-left-color: #0067b8;
    color: #334;
    font-size: 0.92em;
  }
  .muted { color: #65727e; }
---

<!-- _class: lead -->

# Knowledge Exchange 1

## Disk Manager: from API call to durable data movement

Code links are pinned to [`2e72f64`](https://github.com/ydb-platform/nbs/tree/2e72f64c8f067a4e3de292b87f1008dd6876bce3).

---

<!-- _class: compact -->

# Why Disk Manager?

> Disk Manager is the durable lifecycle and orchestration layer above NBS and NFS. It owns metadata and asynchronous workflows; the storage services own the actual disks and filesystems.

First-class resources — defined together in [`resources.Storage`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/resources/storage.go#L14-L322):

- [Disks](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/api/disk_service.proto#L12-L65): empty, image/overlay-backed, snapshot-backed; resize, alter, assign, migrate
- [Images](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/api/image_service.proto#L11-L44): from URL, image, snapshot, or disk
- [Snapshots](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/api/snapshot_service.proto#L11-L35): full/incremental disk state
- [Filesystems](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/api/filesystem_service.proto#L10-L65) and [filesystem snapshots](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/api/filesystem_snapshot_service.proto#L11-L28)
- [Placement groups](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/api/placement_group_service.proto#L11-L53) and membership

Supporting entities:

- [Operations](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/api/operation_service.proto#L10-L21) expose durable tasks; [adapter](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/facade/operation_service.go#L62-L120)
- [Pools, base disks, and overlay slots](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/pools/storage/storage.go#L13-L190)
- [Cells and capacity](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/cells/interface.go#L14-L60), NBS/NFS checkpoints, snapshot chunks/maps, and transfer milestones

---

<!-- _class: compact -->

# Control plane & data plane

> The control plane decides **what lifecycle transition must happen**. The data plane moves **bytes or filesystem nodes**. Both execute through the same durable task layer.

Initialization:

1. [`run`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/app/run.go#L68-L126) creates shared clients.
2. Create [YDB task storage, registry, scheduler](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/app/run.go#L127-L166).
3. Build snapshot stores and [register executable dataplane tasks](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/app/dataplane.go#L25-L95); a non-dataplane node registers [types only](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/app/run.go#L185-L204).
4. [`initControlplane`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/app/controlplane.go#L391-L603) creates pool/resource storage, registers tasks, wires facades, and serves gRPC.
5. Start the shared [controller and runners](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/app/run.go#L371-L400).

Clickable `CreateDisk` execution flow:

> [API](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/api/disk_service.proto#L12-L16)
> → [facade → operation](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/facade/disk_service.go#L20-L31)
> → [service selects task](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/disks/service.go#L418-L476)
> → [task performs transition](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/disks/create_empty_disk_task.go#L45-L109)
> → [resource storage](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/resources/disks.go#L299-L400)
> → NBS/NFS

[All control-plane task groups](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/app/controlplane.go#L232-L389) · [`Register` vs `RegisterForExecution`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/registry.go#L33-L60)

---

<!-- _class: compact -->

# Task processor: durable contract & execution

> For Disk Manager to work correctly, an operation must survive process restarts and worker loss. `cloud/tasks` is that durable execution layer.

- Every task implements [`tasks.Task`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/task.go#L11-L30): `Save`, `Load`, `Run`, `Cancel`, metadata, response.
- [`ExecutionContext`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/execution_context.go#L18-L50) checkpoints state, attaches dependencies/events, records estimates, and can finish atomically.
- [`SaveStateWithPreparation`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/execution_context.go#L72-L96) serializes task state in YDB; transactional preparation lets a resource transition and task checkpoint commit together.
- [`ScheduleTask`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/scheduler_impl.go#L573-L635) persists request + state as `ready_to_run`.

Execution path:

> [lister polls and fans out](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/lister.go#L34-L161)
> → [runner loop](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/runner.go#L687-L709)
> → generation-fenced lock
> → [registry creates task](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/registry.go#L33-L75)
> → `Load`
> → execution context + pinger
> → [`Run` / `Cancel`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/runner.go#L569-L683)

Example: [`CreateSnapshotFromDisk` registration](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/register.go#L32-L42) → [Task methods](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/create_snapshot_from_disk_task.go#L23-L110) → [resumable run](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/create_snapshot_from_disk_task.go#L227-L415) → [milestone `SaveState`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/create_snapshot_from_disk_task.go#L344-L371).

---

<!-- _class: tight -->

# Task processor: stalled ≠ hanging

Stalled-task recovery:

> [pinger](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/runner.go#L518-L565)
> → `modified_at` becomes stale
> → [stalking lister](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/runner.go#L1104-L1159)
> → [select running/cancelling task](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/storage/storage_ydb_impl.go#L1019-L1056)
> → [increment generation + take ownership](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/storage/storage_ydb_impl.go#L1225-L1275)
> → `Load` last saved state

- Default ping is **2 s**; `modified_at < now − 10 s` makes an executing task eligible for takeover. The old owner is [generation-fenced](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/storage/storage_ydb_impl.go#L1650-L1654).
- A healthy pinger means a slow `Run` is **not stalled**. It can still be **hanging**, which is observability—not cancellation or takeover.
- [`IsHanging`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/execution_context.go#L135-L152) when any is true:

```text
total age > 24h
OR inflight > max(2 × estimated inflight, 1h)
OR stalling > max(2 × estimated stalling, 30m)
```

[`Defaults`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/config/config.proto#L11-L88) · [`runner metrics`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/runner_metrics.go#L83-L120) · [`hanging-task query`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/storage/storage_ydb_impl.go#L931-L1017)

Background/system tasks:

- Framework: [`ClearEndedTasks`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/scheduler_impl.go#L643-L677), [`CollectListerMetrics`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/scheduler_impl.go#L679-L717), [node heartbeat](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/tasks/runner.go#L1164-L1183)
- Disk Manager: [snapshot collection/metrics](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/register.go#L194-L267), [resource GC groups](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/app/controlplane.go#L232-L389), [pool maintenance](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/pools/register.go#L175-L276), [filesystem snapshot GC](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/register.go#L188-L210), [regular scrub](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/register.go#L90-L116)

---

<!-- _class: compact -->

# Where all dataplane tasks are registered

> `RegisterForExecution` injects real NBS/NFS/storage dependencies. `Register` only makes a task type deserializable on a node that must not execute it.

- Block/snapshot task registry — [entire executable list](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/register.go#L17-L290):
  - create snapshot from disk / snapshot / URL / legacy snapshot
  - optional snapshot and snapshot-database migrations
  - snapshot → disk, legacy snapshot → disk, disk → disk, disk replication
  - delete snapshot / data, collect snapshots / metrics
  - delete disk from incremental index, create DR-based disk checkpoint
- The [serialization-only map](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/register.go#L293-L320) mirrors non-regular block task types.
- Filesystem dataplane:
  - [delete/create/restore/collect filesystem snapshots](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/register.go#L52-L212)
  - [scrub one filesystem / schedule regular scrubs](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/register.go#L17-L116)
- Startup connects the registries to their storage/client dependencies in [`initDataplane`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/app/dataplane.go#L25-L95) and [`initFilesystemDataplane`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/app/dataplane.go#L98-L222).

The registry is the quickest answer to: “Which process can pick this task up, and what concrete dependencies will it receive?”

---

<!-- _class: tight -->

# Data transfer: source → Transferer → target

> Transfer is one reusable chunk engine. A [`Source`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/common/transfer.go#L51-L67) emits sorted chunk indices and reads data; a [`Target`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/common/transfer.go#L68-L71) writes it.

- [`Transferer`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/common/transfer.go#L79-L254): reader workers → bounded chunk channel → writer workers → completion acknowledgements.
- Sources: [NBS disk](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/nbs/source.go#L22-L42), [snapshot storage](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/source.go#L16-L142), [URL](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/url/source.go#L30-L169) ([RAW/QCOW2/VMDK/VHD](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/url/formats.go#L12-L79), [ETag-safe task](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/create_snapshot_from_url_task.go#L54-L188)).
- Targets: [NBS disk](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/nbs/target.go#L14-L49) or [snapshot storage](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/target.go#L13-L69). Examples: [snapshot → disk](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/transfer_from_snapshot_to_disk_task.go#L47-L112), [disk → disk](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/transfer_from_disk_to_disk_task.go#L44-L131).

**Checkpoint vs milestone**

- NBS `(base checkpoint, current checkpoint)` defines a stable changed-block interval: [`GetChangedBlocks`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/nbs/source.go#L77-L130), then [read current checkpoint](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/nbs/source.go#L197-L221). No base means full transfer. URL/snapshot sources do not use NBS checkpoints.
- A [`Milestone`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/common/transfer.go#L15-L18) is restart progress: first not-yet-safe chunk + transferred count.
- [`ChannelWithInflightQueue`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/common/channel_with_inflight_queue.go#L9-L77) provides backpressure. Writers finish out of order, but the [milestone advances only over the contiguous completed prefix](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/common/inflight_queue.go#L138-L188); [`ProgressSaver`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/common/progress_saver.go#L12-L68) persists it.

---

<!-- _class: compact -->

# Snapshots: checkpoints & incrementality

> latest snapshot/checkpoint → NBS changed chunks → read disk → write new blobs <br>
> latest snapshot → unchanged chunks → shallow-copy references

- The control-plane task creates a stable NBS checkpoint, then schedules dataplane work: [`CreateSnapshotFromDisk`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/snapshots/create_snapshot_from_disk_task.go#L88-L124).
- YDB’s [`incremental` table](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/schema/schema.go#L53-L75) stores the latest ready `(snapshot, checkpoint)` for a disk.
- Creating the next snapshot [loads and locks that base](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/storage_ydb_impl.go#L107-L204).
- The disk source selects changed chunks; the [shallow source](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/shallow_source.go#L13-L60) reuses base chunk references. Changed indices become “holes,” so they are not copied twice: [`Transferer` routing](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/common/transfer.go#L109-L131).
- Changed zero chunks are explicit, otherwise old base data would reappear: [incremental source/zero/shallow setup](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/create_snapshot_from_disk_task.go#L274-L342).
- On success, storage [atomically moves the incremental head](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/storage_ydb_impl.go#L239-L298) to the new snapshot/checkpoint.

Replication uses lightweight checkpoints as change boundaries but reads current data: [range/read](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/replicate_disk_task.go#L260-L354) → [checkpoint rotation](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/replicate_disk_task.go#L359-L420).

---

<!-- _class: tight -->

# Snapshot storage: YDB, S3, refs & deletion

- The [storage factory](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/factory.go#L13-L53) always builds YDB metadata/chunk-map storage and optionally an S3 byte backend.
- [`chunk_map`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/schema/schema.go#L100-L118): `(snapshot_id, chunk_index) → (chunk_id, stored_in_s3)`.
- [`chunk_blobs`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/schema/schema.go#L77-L98): canonical row has `referer=""`, data metadata, and `refcnt`; one marker row per referring snapshot makes retries exactly once.
- YDB backend keeps compressed bytes in [`chunk_blobs`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks/storage_ydb.go#L18-L165). S3 backend keeps [bytes in S3 and ref metadata in YDB](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks/storage_s3.go#L19-L148).
- Direct write uses deterministic `taskID.snapshotID.chunkIndex`, inserts the [map before the blob](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/storage_ydb_impl.go#L813-L873), and starts at refcount 1. Zero chunks have an empty chunk ID and [no blob](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/storage_ydb_impl.go#L875-L909).
- Shallow copy inserts a destination map entry then [`RefChunk`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks/common.go#L63-L118). [`UnrefChunk`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks/common.go#L120-L201) removes one marker and decrements/deletes the canonical row; S3 deletes its object only [at zero refs](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/chunks/storage_s3.go#L129-L148).

Deletion is deliberately asynchronous:

> [`DeleteSnapshot`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/delete_snapshot_task.go#L40-L95)
> → mark deleting + remove checkpoint/index
> → default 30 min
> → [`CollectSnapshots`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/collect_snapshots_task.go#L52-L118)
> → [`DeleteSnapshotData`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/delete_snapshot_data_task.go#L75-L100)
> → [unref blobs, delete map](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/storage_ydb_impl.go#L580-L674)

---

<!-- _class: tight -->

# Pools: acquire, release, rebase, delete image

Pools pre-create image-backed base disks; each overlay occupies a slot. [Design/reference](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/doc/disk_manager/pools/README.md) · [task registry](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/pools/register.go#L82-L276)

**Acquire**

`CreateDisk(image)` → [`disks.CreateOverlayDisk`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/disks/create_overlay_disk_task.go#L77-L150) → [`pools.AcquireBaseDisk`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/pools/acquire_base_disk_task.go#L39-L101) → reserve slot / wait for base → create NBS overlay from base checkpoint.

**Release**

`DeleteDisk(overlay)` → delete NBS overlay → [`pools.ReleaseBaseDisk`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/pools/release_base_disk_task.go#L38-L72) → released-slot tombstone + restored capacity. [Trigger](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/disks/delete_disk_task.go#L114-L152)

**Rebase / retire**

[`RetireBaseDisk`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/pools/retire_base_disk_task.go#L42-L105) → one `RebaseInfo` per overlay → [`RebaseOverlayDisk`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/pools/rebase_overlay_disk_task.go#L40-L102) → reserve target → NBS `Rebase` → commit slot move → regular [`DeleteBaseDisks`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/pools/delete_base_disks_task.go#L42-L60).

**Delete image**

`images.DeleteImage` → [`pools.ImageDeleting`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/images/common.go#L19-L61) closes pools → mark image deleting → `RetireBaseDisks`/rebase live overlays → [`dataplane.DeleteSnapshot`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/services/images/common.go#L62-L95) → resource GC.

---

<!-- _class: compact -->

# Filesystem snapshot, transfer & scrubbing

> Snapshot, restore, and scrub share a durable parallel filesystem traverser. Its queue and pagination cookies live in YDB, so a new worker can resume the walk.

- Traversal: [design comment + interface](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/traversal.go#L30-L77) → [root scheduling/workers](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/traversal.go#L110-L158) → [durable completion condition](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/traversal.go#L198-L250) → [paginated child scheduling](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/traversal/traversal.go#L298-L352).
- Snapshot: [Filestore checkpoint session](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/listers/filestore_lister.go#L60-L99) → [traverse and store nodes/symlinks](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/create_snapshot_from_filesystem_task.go#L48-L174).
- Restore/transfer: [create ordinary nodes idempotently + old→new ID mapping](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/transfer_from_snapshot_to_filesystem_task.go#L280-L424), then [restore hardlinks in durable batches](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/transfer_from_snapshot_to_filesystem_task.go#L113-L260).
- Scrub: [`ScrubFilesystem`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/scrub_filesystem_task.go#L56-L123) traverses live NFS in unsafe/read-only mode and tolerates missing entries.
- Background scrub: [`RegularScrubFilesystems`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/regular_scrub_filesystems_task.go#L50-L130) schedules one child task per configured filesystem.

Registries: [snapshot/create/restore/delete/collect](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/snapshot/register.go#L52-L212) · [scrubbing](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/internal/pkg/dataplane/filesystem/scrubbing/register.go#L17-L116)

---

<!-- _class: tight -->

# `disk-manager-admin`: operator capabilities

Entrypoint: [`cmd/disk-manager-admin`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/cmd/disk-manager-admin/main.go#L9-L14) · Complete [top-level command tree](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/admin/run.go#L35-L132)

- [`operations`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/admin/operations.go#L118-L130): get, cancel
- [`disks`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/admin/disks.go#L662-L684): get/list/create/delete, resize/alter, assign/unassign
- [`images`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/admin/images.go#L281-L298): get/list/create/delete
- [`snapshots`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/admin/snapshots.go#L498-L529): get/list/create/delete; legacy copy; snapshot/storage migration scheduling
- [`filesystems`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/admin/filesystem.go#L418-L438): get/list/create/delete/resize/scrub
- [`placement-groups`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/admin/placement_group.go#L332-L350): get/list/create/delete
- [`tasks`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/admin/tasks.go#L752-L773): get/cancel; list ready/running/failed/slow/[hanging](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/admin/tasks.go#L517-L536); pause/resume; schedule blank; [dangerous force-finish](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/admin/tasks.go#L572-L615)
- [`pools`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/admin/pools.go#L146-L163): global/base-disk/pool consistency checks
- [`private`](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/admin/private.go#L1043-L1069): acquire/release/rebase/retire/optimize base disks; configure/delete pools; alive nodes; checkpoint size; finish external-FS transitions; migrate disk between cells

Operator caveat: it mixes [gRPC lifecycle mutations](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/admin/disks.go#L213-L265) with [direct YDB inspection/recovery](https://github.com/ydb-platform/nbs/blob/2e72f64c8f067a4e3de292b87f1008dd6876bce3/cloud/disk_manager/pkg/admin/common.go#L86-L172).
