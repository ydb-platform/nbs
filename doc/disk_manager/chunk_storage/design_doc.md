# Multiple storage backends for Disk Manager chunks

Select chunk storage per image or snapshot and migrate chunks between YDB and configured S3 backends. The first version assigns one requested backend per resource and copies data during creation and migration. A shared chunk can remain on several backends while different snapshots require those copies. A snapshot with unspecified backend can retain its existing per-chunk placement.

## Purpose and scope

Use different S3 backends for images and snapshots. Task and metadata storage stay in the existing YDB databases.

Images are disk templates; snapshots are backups. Separate chunk storage lets us store images in the disks' region, put snapshots in slower storage or another region, and move data to new capacity before retiring an old backend.

```mermaid
flowchart LR
    subgraph DiskRegion[Disk region]
        DM[Disk Manager dataplane]
        NBS[Disks in NBS]
        Images[Image S3 backend]
        DM -->|Read image chunks| Images
        DM -->|Create disks| NBS
    end
    Snapshots[Snapshot S3 backend]
    YDB[Existing YDB]
    DM -->|Read and write snapshot chunks| Snapshots
    DM -->|Metadata and optional chunk payloads| YDB
```

Support YDB to S3, S3 to YDB, and S3 to S3 migration. Moving a chunk preserves its ID, references, and reference count.

Migration accepts only dataplane snapshots in the **Ready** state. Images use the same dataplane snapshot records. Disk Manager continues serving reads during migration. A lock on the migrating snapshot protects it from deletion.

The controlplane selects a backend for each new image or snapshot and passes its ID to the dataplane. Creating a reference reuses a completed copy on that backend or copies the same chunk there. Other snapshots keep their own backend requirements.

Migration proceeds forward. Detaching the last requirement for a backend records a tombstone before physical deletion and retains it for the remaining lifetime of the chunk. Reject subsequent attempts to attach that chunk to the detached backend, including references from another snapshot, so an earlier S3 DELETE can finish safely. Final chunk deletion removes the root and its tombstones.

## Existing code

[SnapshotConfig](../../../cloud/disk_manager/internal/pkg/dataplane/snapshot/config/config.proto) and [NewStorage](../../../cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/factory.go) currently support YDB and one S3 backend. Image and snapshot services select newly written chunk storage using `UseS3`.

The [schema](../../../cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/schema/schema.go) has snapshot records with status, base snapshot, sizes, encryption information, and `lock_task_id`. `chunk_map` maps a snapshot and chunk index to `chunk_id` and currently includes `stored_in_s3`. In `chunk_blobs`, the row with `referer = ""` holds `refcnt` and the optional YDB payload, checksum, and compression format. Rows with `referer = snapshot_id` represent references. S3 objects hold compressed data and checksum/compression metadata.

[Snapshot sources](../../../cloud/disk_manager/internal/pkg/dataplane/snapshot/source.go) cache chunk-map entries. Cache chunk IDs and resolve their placement from `chunk_blobs` on each read to follow migrations and select among configured backends.

[ShallowCopyChunk](../../../cloud/disk_manager/internal/pkg/dataplane/snapshot/storage/storage_ydb_impl.go) already shares a chunk by adding a reference to its ID. Extend it to register backend requirements and copy the payload when the requested backend has no reusable copy.

[backupChunksTask](../../../cloud/disk_manager/internal/pkg/dataplane/backup_chunks_task.go) copies compressed chunks to separate backup S3 storage. [backupSnapshotDataTask](../../../cloud/disk_manager/internal/pkg/dataplane/backup_snapshot_data_task.go) saves chunk maps and coordinates chunk backups. Their reads must also resolve current placement.

The existing [snapshot migration](../../../cloud/disk_manager/internal/pkg/dataplane/migrate_snapshot.go) and [database migration](../../../cloud/disk_manager/internal/pkg/dataplane/migrate_snapshot_database_task.go) copy data into another database. Reuse their resumable transfer and child-task scheduling patterns within the existing snapshot database. Reference transactions support migration in any chunk-creator order.

## Configuration and stored fields

### Backend selection

`S3Backends` is a repeated configuration field containing a stable unsigned 32-bit ID, S3 connection settings, bucket, and key prefix. ID `0` denotes the existing singleton S3 backend; `0x7fffffff` denotes YDB chunk data. Other IDs identify additional S3 backends.

Each backend ID MUST have a permanent binding to one storage namespace. Distinct backend IDs identify distinct physical object namespaces. Changing the storage location requires a new ID. Reject duplicate IDs, the reserved YDB ID in S3 configuration, and incomplete settings. Accept YDB-only configurations. When an explicit backend list is active, default S3 backend settings can be omitted. 

The controlplane selects the image or snapshot service default, optionally overridden by an individual creation request. Use a scalar `uint32` backend ID in dataplane creation requests. A zero value selects the existing `UseS3` and inheritance rules; a nonzero value selects that backend directly.

For a creation request with a nonzero backend ID, that ID takes precedence over `UseS3` and applies to both newly written and referenced chunks. The reserved YDB ID also uses this path. Persist the selection before starting chunk work. Retries use the persisted destination across configuration changes.

Snapshot creation tasks in progress must continue across version upgrades. For a snapshot with unspecified backend, infer YDB or S3 `0` from the stored chunk metadata.

Migration requests use their destination ID directly, including ID `0` for the existing S3 backend. Backend IDs in stored placement sets also identify physical backends directly.

Validate each selected backend before creating records, including all-zero inputs. Reject invalid or unknown selections before writes begin. If a validated backend later becomes unavailable, preserve recorded work and retry the operation using the same ID.

### References, payloads, and detach tombstones

Store placement and tombstones in the existing `snapshots`, `chunk_map`, and `chunk_blobs` tables.

Add `s3_backend_ids` and `desired_backend_ids` to each `chunk_blobs` reference row:

- **Actual** (`s3_backend_ids`) records completed copies available to this reference. During detachment, an actual entry remains until physical deletion is acknowledged; a read can therefore encounter NotFound and refresh its locations.
- **Desired** (`desired_backend_ids`) records this reference's requirements, including unfinished writes. Readers use completed copies recorded in actual. During cleanup of an unfinished write, a desired-only entry can remain solely to identify outstanding deletion work.

State examples use **desired → actual**. To add backend A to a reference on backend `0`, first record `{0,A} → {0}`, then copy the data and publish `{0,A} → {0,A}`. The first state reserves A before its payload is written.

Add **`detached_backend_ids`** to the shared row with `referer = ""`. This set is append-only during the chunk's lifetime and contains chunk-wide tombstones. Write a tombstone when the last reference starts physically detaching a backend, before any S3 DELETE. Reject every subsequent requirement, reuse, or publication of that `(chunk_id, backend_id)`, including requests from a different snapshot.

A per-reference release keeps the physical copy live while another reference still needs that backend. Record a detach tombstone only for the final release that authorizes physical deletion.

All three sets can contain the YDB ID. Store sets as sorted, unique sequences of four-byte, big-endian IDs in YDB `String` columns. NULL and zero-length bytes decode as empty sets. Encode ID `0` as four bytes. Reject invalid byte lengths and require configured IDs in active actual or desired sets. Read historical detached IDs directly from metadata after their backend configuration is retired.

`refcnt` counts reference rows, including those retained for unfinished cleanup. Increment it once when inserting a reference; decrement it once when removing the reference after cleanup. Update it in the same transaction as that insertion or removal. Preserve the count when changing a reference's backends.

Keep detach tombstones while the chunk has any references, including references retained for unfinished cleanup. Once physical cleanup finishes and the last reference is removed, delete the root and its tombstones in that same transaction. A chunk remains live while any backend copy is still retained. Allocate a fresh identity for each new chunk.

For an existing reference, both actual and desired empty identifies unspecified placement. Empty actual with non-empty desired means an unfinished write. Non-empty actual with empty desired means retained cleanup work. Return NotFound immediately for a missing reference.

Every newly inserted reference has non-empty desired in the insertion transaction. When cleanup removes its last actual or desired entry, delete that reference and decrement `refcnt` in the same transaction. Preserve root tombstones while other references remain; remove the root when the last reference is removed.

```mermaid
flowchart LR
    Map["chunk_map: snapshot and index to chunk K"] --> Ref["Reference S1: actual B, desired B"]
    Ref --- Root["Root K: refcnt 1, detached A"]
    Ref --> B["Backend B: chunk K"]
    Root -.-> Denied["Reject attaching K to A"]
```

All mutations compute their result from the main chunk table and mirror it to the configured shadow table in the same transaction. This includes reference counts, placement sets, payloads, and tombstones. Carry existing tombstones into root updates during retries and when populating a previously missing shadow row.

### Resolve a snapshot with unspecified backend

When both placement fields are empty on an existing reference, read that reference and the root row in one consistent database read. Infer YDB presence from the root's payload: non-empty `data` identifies YDB; an empty payload identifies existing S3 backend `0`. An S3 copy stores its payload and checksum/compression metadata in the S3 object. Use payload presence for inference, since uncompressed YDB chunks have empty compression. Identify zero chunks from `chunk_map` and store only their applicable map representation. Report a missing root for an existing reference as a metadata error.

Resolve an existing reference with both fields empty as having actual and desired both equal to the inferred backend. For an S3-backed chunk, interpret both empty fields as `{0}`. Apply this interpretation to reads, reuse, and every detach decision, including checks of other references. The chunk's stored state determines this interpretation.

Populate explicit sets only on the reference being changed, using its inferred placement in the same transaction as the update. For an S3-backed reference, requesting A changes `"" → ""` to `{0,A} → {0}` in desired → actual notation. During S3-to-S3 migration, other empty references continue to retain backend `0` through inference.

When S3-to-YDB copying atomically writes a complete root payload and publishes the copying reference's actual YDB entry, other references with both fields empty begin resolving to YDB. Their unspecified placement follows the new root payload. Detach decisions count them as retaining YDB and keep the payload while any such reference remains. Explicit requirements for S3 `0` continue to retain that copy independently. When moving a reference from YDB to S3, populate its inferred YDB entries before changing them; other empty references still retain YDB.

If a placement transaction fails before commit, its explicit sets and YDB payload remain at their last committed state. If its response is lost after commit, retry from the stored state and preserve populated sets. Concurrent placement and detach transactions resolve references against the same root state or retry. A reader that selected S3 `0` before a YDB copy appeared can discover YDB through the all-NotFound refresh described below if S3 `0` is subsequently deleted. New writers always create explicit sets; enable placement changes only after all dataplane writers understand inference and honor detach tombstones.

## Snapshot lifetime and migration ownership

Creation uses the existing lifecycle checks and worker shutdown. Deletion marks the destination Deleting; creation observes that state through its progress checks and stops its workers. The collector begins physical cleanup after `SnapshotCollectionTimeout`, which defaults to 30 minutes. This ordering relies on creation workers stopping within that interval. A worker stalled beyond the collection delay remains an existing lifecycle edge case.

Reuse the existing `snapshots.lock_task_id` for the migrating snapshot. In the acquisition transaction, require Ready status and claim that lock. Hold it through copying and detachment so deletion tasks wait for migration to release the snapshot. Serve concurrent reads throughout the lock's lifetime.

Reference transactions and chunk-wide detach tombstones coordinate physical copies. Children can migrate concurrently, including after their creator's snapshot record has been deleted. Creation locks the source snapshot against deletion while reading its map and registering references. Reusing the existing exclusive source lock can make creation retry while that source is itself being migrated.

The target's existing exclusive lock admits one migration task at a time. Each task stores its destination in its request and uses that destination on retries. After cancellation releases the lock, a later task acquires it and migrates toward the destination in its own request, starting from the recorded chunk placement.

Store a migration execution generation in `snapshots.operation_generation` alongside `lock_task_id`. Advance the generation when a migration execution is admitted or its ownership is revoked. Each migration transaction that updates placement, payloads, detach state, or completion checks Ready status and the current owner/generation. Cancellation releases the lock while preserving the Ready snapshot for continuation, so these checks reject further updates from the cancelled execution as soon as ownership is revoked. Release the lock only for the matching owner/generation. Integrate admission and cancellation with the task framework's execution ownership, and reject ownership acquisition by a cancelled execution.

If deletion marks the target Deleting before migration admission, reject admission. If migration acquires its lock first, deletion waits. Start chunk operations after admission commits successfully. Resolve a lost admission response by retrying idempotently from the same execution, preserving the recorded destination and lock ownership.

## Chunk operations

### Write a new chunk

Preserve the existing ordering: create the `chunk_map` entry before registering or writing the blob. The destination remains Creating while the mapping identifies unfinished work for retry or creation cleanup. Chunk IDs remain deterministic for the creating task and chunk index.

In one serializable transaction, reject a tombstoned backend, insert the reference with empty actual and the backend in desired, and increment `refcnt` only if the reference is new. Resume an existing reference from its recorded sets. Preserve the root's tombstones when initializing its other fields.

For S3, compress the data and PUT it with checksum/compression metadata. After success, publish the backend in actual in a transaction that checks reference existence, desired membership, and absence of a tombstone. For YDB, write the payload and publish actual in the same transaction. Zero chunks need only their applicable map representation.

If the map write fails, retry before creating any blob. If only the map commits, cleanup sees the mapping and treats a missing reference as already cleaned. If reference registration commits but PUT fails or times out, keep desired as pending work and require successful publication before serving reads through that reference. Retry the same immutable payload and ID. If PUT succeeds but publication fails, repeat PUT or verify completion through the normal retry path. Use actual membership to establish completion.

Creation cancellation leaves recorded actual and desired entries for the collector to clean through the detach protocol. If another reference retains a backend, its requirement keeps the object live; otherwise final cleanup deletes that location. An S3 PUT with an unknown outcome may finish remotely after the worker has stopped. A PUT that finishes after cleanup can leave an unreferenced object, as described under snapshot deletion.

```mermaid
sequenceDiagram
    participant Writer
    participant DB as YDB metadata
    participant S3
    Writer->>DB: Write map, then register desired B and count reference
    Writer->>S3: PUT immutable chunk and metadata
    alt PUT acknowledged
        S3-->>Writer: Success
        Writer->>DB: Check reference and desired B, reject tombstoned B, publish actual B
    else PUT outcome unknown
        Note over Writer,DB: Keep desired B, retry can repeat the same write
    end
```

### Read a chunk

Get the chunk ID from `chunk_map` and pass the snapshot referer through the read API. Read the reference's actual and desired sets from `chunk_blobs`. If both placement fields are empty on an existing reference, resolve it from the root `chunk_blobs` row in the same consistent read, using the compatibility rules above. A missing reference is NotFound. Invalid metadata and unknown backend IDs are errors. An empty actual set with non-empty desired represents an unfinished write and returns NotFound. Read candidates come from the effective actual set.

Keep a set of backend IDs already attempted during this read:

1. Try every actual backend not yet attempted, in random order, until one returns the chunk. Decompress it and verify its checksum. A YDB read uses the root row's payload; a missing payload is NotFound.
2. On NotFound or Unavailable, continue with the remaining candidates. If none succeeds and any candidate was Unavailable, return Unavailable. Propagate other errors and context cancellation.
3. Only when all attempted candidates returned NotFound, reread the reference's placement consistently, including the root row if unspecified-backend resolution is needed. Try only actual backend IDs that were not attempted before.
4. Repeat this process across successive forward moves. If refreshed metadata contains no new actual backend, return NotFound. All metadata reads and backend requests use the caller's context; stop when it is canceled or its deadline expires.

An initially empty actual set returns NotFound immediately. Each metadata refresh supplies newly discovered actual backends, and each backend is attempted once per read. This allows a reader that captured A before migration to discover B after A is deleted, and then C if another move overtakes its read of B.

```mermaid
sequenceDiagram
    participant Reader
    participant DB as YDB metadata
    participant Migration
    participant A as Backend A
    participant B as Backend B
    Reader->>DB: Read reference placement
    DB-->>Reader: Actual A
    Migration->>B: Write chunk
    B-->>Migration: Success
    Migration->>DB: Publish B in actual
    Migration->>DB: Tombstone final source A, keep actual A for cleanup
    Migration->>A: Delete obsolete copy
    A-->>Migration: Success
    Migration->>DB: Remove actual A, retain tombstone A
    Reader->>A: Read chunk
    A-->>Reader: NotFound
    Reader->>DB: Refresh reference placement
    DB-->>Reader: Actual B
    Reader->>B: Read newly discovered backend
    B-->>Reader: Data and metadata
    Reader->>Reader: Decompress and verify checksum
```

If the refreshed set still contains only A, the read has exhausted its candidates and returns NotFound. If A is NotFound and B is Unavailable, the read returns Unavailable after exhausting the candidates. Propagate metadata-read failures with their error classification.

### Reference an existing chunk

Protect the source from deletion while reading its map and registering the new reference. Write the destination map before or in the same transaction as reference registration, so every counted reference has a discoverable mapping after a crash.

Resolve unspecified source reference fields using the shared root and preserve the source references' stored fields. In one serializable transaction, require an existing source reference, inspect the root tombstones and source references, register the destination reference with explicit sets, and increment `refcnt` exactly once. Reject registration if the source reference or root is missing, including when the caller supplies a cached chunk-map entry. Reuse requires membership in both effective actual and desired on a reference, together with a tombstone check. Reject tombstoned copies. Actual-only copies may already be undergoing deletion; desired-only copies are unfinished.

For a creation request selecting a nonzero destination B, reuse B from any qualifying reference by setting both sets on the new reference to exactly `{B}`. If no reusable B exists, register completed, retained source locations in actual and `{B}` in desired. Those actual entries pin the source data while copying B. When creating a snapshot with unspecified backend, inherit completed, retained locations according to the request compatibility rules.

If B is tombstoned for this chunk, reject the request before adding a reference or issuing PUT. Apply this check to every requesting snapshot, including one requesting B for the first time, because a stale DELETE is tied to the shared chunk key. The tombstone takes precedence over a source's recorded presence on B. Requests to share a chunk preserve its ID and return the tombstone error when that destination has been detached.

If registration and another reference's release race, serializable transactions determine the outcome. Registration first makes the new requirement visible, so the releaser keeps the physical object. Final detachment first writes a tombstone, so registration fails. A retry after an acknowledged or ambiguous commit resumes the destination reference's recorded progress and count. A source disappearing before registration causes an error; after registration, the destination's own actual/desired entries protect its copies.

After registering temporary source locations, use the copy operation below. Once every destination chunk is available, release temporary locations using detach. A failed copy keeps those locations readable and counted. Creation cancellation uses the explicit reference-cleanup procedure. Migration cancellation preserves recorded placement for continuation.

### Copy to another backend

`replicateChunk(snapshot_id, chunk_id, destination)` preserves the chunk ID, reference, and reference count. Migration invokes it for an existing reference and applies its owner/generation checks to each placement transaction. Creation invokes it after registering the destination reference, using the existing creation lifecycle.

In a transaction, check the reference and root tombstones, resolve and populate this reference's fields if unspecified, then add the destination to desired. Reject a tombstoned destination before any write. If this reference already has a completed, retained destination, the copy is complete. Reuse a completed, retained destination on another reference by registering it transactionally, using its effective sets if its fields are unspecified. Otherwise read compressed data and checksum/compression metadata from an actual source using the read-and-refresh algorithm, then write through the selected destination client.

After a successful S3 PUT, publish the destination in actual only if the reference still exists, the backend remains desired, and the root has no tombstone for it. For YDB, write the complete payload and publish actual in the same transaction with those same checks; references whose fields are still empty can then infer YDB presence. S3-to-S3 transfer uses separate clients, allowing different endpoints, buckets, credentials, and regions.

Begin payload writes after the desired update commits. If that update fails, retry the transaction. If source reading fails, keep desired and all existing actual locations. A PUT timeout leaves an unknown physical outcome, so retry writes the same immutable data. If publication fails after successful PUT, the existing desired entry retains the obligation to complete or clean that write. If publication is rejected because the reference was removed, the backend is tombstoned, or migration ownership was revoked, preserve the current metadata state. Another reference's requirement can keep the object live; otherwise final cleanup deletes the location using the detach protocol. A PUT that finishes after cleanup can leave an unreferenced object, as described under snapshot deletion.

Two references may concurrently copy the same chunk to B. Both register desired B before writing and publish completion only on their own references. Duplicate PUTs contain the same immutable payload and metadata. When the faster task releases A, the slower reference's requirement keeps the physical copy on A. Desired-only B likewise retains B while a copy is unfinished.

```mermaid
sequenceDiagram
    participant S1 as Snapshot 1 task
    participant S2 as Snapshot 2 task
    participant DB as YDB metadata
    participant A as Source A
    participant B as Destination B
    S1->>DB: Register desired B on S1
    S2->>DB: Register desired B on S2
    par Copy for S1
        S1->>B: PUT chunk K
        B-->>S1: Success
        S1->>DB: Publish B on S1
    and Copy for S2
        S2->>B: PUT the same immutable chunk K
        B-->>S2: Success
        S2->>DB: Publish B on S2
    end
    S1->>DB: Release S1's A, S2 still retains A
    S2->>DB: Final release of A, persist tombstone A
    S2->>A: DELETE chunk K
    A-->>S2: Success
    S2->>DB: Remove S2's A entry, keep tombstone A
```

### Detach one backend

`detachChunkBackend(snapshot_id, chunk_id, backend_id)` releases one reference's requirement for a backend. Migration and creation's temporary-source cleanup require a completed destination in both actual and desired and reject a tombstoned destination. Snapshot deletion uses the same final-release protocol to release every location, including the last readable copy.

In a serializable transaction, read this reference, the root, and all other references for the chunk, including references whose snapshots are being deleted. Resolve unspecified fields for every reference when computing its effective actual and desired sets, and populate this reference's fields before changing them. If this reference no longer contains the backend in either effective set, its release is already complete. If another reference retains the backend in effective actual or desired, remove it from both sets on this reference and commit. Keep the physical object and preserve the root's current tombstone set. Every entry-removal transaction, including this shared-copy branch, deletes an emptied reference and decrements `refcnt` in that same transaction.

If no other reference retains the backend, append its ID to the root's `detached_backend_ids` in that transaction. This is the irreversible detachment decision. For an actual entry, remove desired and retain actual as pending cleanup. For a desired-only entry, retain desired as pending cleanup. The tombstone distinguishes this entry from a new write requirement and prevents a late writer from publishing it.

Begin S3 DELETE after the tombstone transaction commits. After success or NotFound, remove the retained backend entry transactionally. If that empties a reference, remove the reference and decrement `refcnt` exactly once. Keep the root and its tombstones while any reference remains. If this removes the last reference, delete the root, including its tombstones, in the same transaction. For YDB, complete physical deletion and metadata cleanup in one transaction: write the tombstone, clear unneeded payload/checksum/compression columns, and release the entry. The same final-reference rule applies to the YDB transaction.

The transaction that registers any new requirement reads the same root and checks for a tombstone. Commit order determines the outcome: if the new requirement commits first, detach observes it and leaves the object; if detach commits first, registration rejects the destination. Concurrent last releases also serialize: one releases its entry while another still retains the backend, and the final releaser retains responsibility for physical deletion.

If the tombstone transaction fails, retry it before issuing DELETE. If it commits but its response is lost, a retry observes the tombstone and resumes the retained cleanup entry. If DELETE fails, preserve both the tombstone and pending entry and retry. If DELETE succeeds but metadata cleanup fails, repeating DELETE is safe: a surviving chunk retains the tombstone, and a fully deleted chunk ID stays retired. If migration is cancelled at any point, its cancellation handler preserves the tombstone and pending entry; a later continuation completes the forward operation.

The original DELETE may still arrive after its retry has succeeded. Keeping the tombstone rejects later attachment to that backend, so the delayed request targets the retired placement.

```mermaid
sequenceDiagram
    participant Forward as Migration A to B
    participant DB as YDB metadata
    participant A as S3 backend A
    participant Reverse as Later request for A
    Forward->>DB: B is actual, tombstone A and retain pending A cleanup
    Forward->>A: DELETE attempt 1
    Note over Forward,A: Response times out, request may still be outstanding
    Forward->>A: DELETE retry
    A-->>Forward: Success
    Forward->>DB: Remove actual A, keep tombstone A
    Reverse->>DB: Request A for the same chunk K
    DB-->>Reverse: Reject detached backend A before PUT
    Note over DB,Reverse: A to AB to B advances placement, reject B to AB to A
    Note over A: Delayed DELETE attempt 1 targets the retired A placement
```

Keep tombstones through timeouts and individual backend deletions. Remove them with the root during final chunk cleanup. Until snapshot deletion finishes, its lifecycle rejects recreation under the same snapshot ID; a later creation uses a new chunk identity. Sharing the old chunk requires a surviving source reference, checked transactionally. If another snapshot still references the chunk, the root and tombstones remain. A delayed DELETE for a fully deleted chunk targets its retired identity, while newly created chunks use fresh identities.

### Delete a snapshot reference

Snapshot deletion and creation cancellation mark the destination Deleting. Existing creation lifecycle checks stop workers, and the collector cleans references after the collection delay. Migration cancellation preserves recorded placement for continuation. Keep each chunk-map entry until reference cleanup finishes so a retry can find all remaining work.

For each backend in this reference's effective actual union desired, including inferred placement when both fields are empty, run the detach protocol. When another reference retains a backend, release only this reference's entries. For the final requirement, keep the reference counted until physical deletion succeeds or returns NotFound. Then remove the entry; remove an empty reference and decrement its count in the same transaction. If this removes the last reference, also delete the root and its tombstones. A retry of a missing reference returns success and preserves the current metadata. After reference cleanup, remove the chunk-map entry.

A crash before reference cleanup leaves the map and counted reference. A crash after reference cleanup but before map removal leaves a discoverable mapping whose missing reference is already cleaned. Reference removal decrements the count exactly once. When two snapshots delete the same shared chunk, their checks and releases conflict, and the last one records its tombstone before DELETE.

If a PUT was outstanding when deletion began, it may complete after DELETE and recreate an unreferenced object. Reject its actual publication. Reference validation keeps the deleted chunk inaccessible, and fresh chunk identities isolate new data from a stale DELETE. Final chunk cleanup removes the root and tombstones. An acknowledged DELETE establishes completion of that request; an earlier PUT can still finish afterward and leave an unreferenced object requiring separate orphan-object cleanup.

```mermaid
sequenceDiagram
    participant Writer as Earlier writer
    participant DB as YDB metadata
    participant Cleanup
    participant S3
    Writer->>S3: PUT chunk K with unknown outcome
    Cleanup->>DB: Final release, persist tombstone B
    Cleanup->>S3: DELETE chunk K
    S3-->>Cleanup: Success
    Cleanup->>DB: Atomically remove last reference, root and tombstones
    Cleanup->>DB: Remove chunk-map entry
    Note over Writer,S3: Earlier PUT may finish after DELETE
    Writer->>DB: Attempt to publish actual B
    DB-->>Writer: Reject deleted reference
    Note over Writer,S3: Any recreated object is unreferenced, chunk K stays retired
```

## Snapshot migration

### Copy, then detach

The request stores the dataplane snapshot ID and destination. State stores the phase, a chunk-index milestone for that phase, operation ownership, and a completion flag. Every new migration task starts with the copy phase and scans the current placement of all chunks toward its requested destination.

After Ready-only admission, scan the snapshot map and copy every nonzero chunk to the destination. Each chunk publishes its completed destination before its milestone can advance. Retain source requirements throughout this phase. A source read or destination write failure leaves the original data available and the destination's desired entry available for retry.

Once every chunk has its destination completed, save the transition to the detach phase with a fresh phase milestone. Then release every other backend in each reference's actual or desired sets, including unfinished writes and pending cleanup from earlier migrations. Shared copies stay while other snapshots need them; final releases create tombstones and delete the payload. This two-phase ordering preserves existing source copies during copying. Detach permanently releases a source backend, and continuation advances toward the requested destination.

If a worker stops before saving a chunk milestone, replay that chunk: completed copies are reused, pending copies repeat immutable writes, and detach resumes from its tombstone and retained entry. If execution stops with completed copies and an unsaved phase transition, rerun the copy scan and skip completed destinations. Begin detachment after that transition is durably recorded. A new task after cancellation starts the copy scan from the beginning, reuses completed copies on its requested destination, and copies the remaining chunks there before entering its detach phase.

When every reference has actual and desired equal to the destination, transactionally set `snapshots.s3_backend_ids` to that destination. Determine completion from this snapshot's reference sets while retaining the source copies required by other snapshots and the detached IDs in root tombstones. Save task completion, then release the owned lock. If completion was saved before unlocking finished, a retry resumes at the unlock and returns success with the completed placement preserved.

```mermaid
sequenceDiagram
    participant Task
    participant DB as YDB metadata
    participant Storage
    Task->>DB: Admit Ready target and claim lock
    Task->>Storage: Copy chunks and publish completed destination entries
    Task->>DB: Save transition to detach phase
    Task->>Storage: Release source requirements, tombstone final releases before DELETE
    Task->>DB: Set completed destination
    Task->>DB: Save task completion
    Task->>DB: Release owned lock
```

### Migration cancellation and continuation

Migration cancellation stops new work, invalidates the execution's ownership, stops and joins workers belonging to that execution where possible, and releases its owned lock. The cancellation handler **retains objects, references, and backend placement for forward continuation**. Preserve actual, desired, root tombstones, and pending deletion entries exactly as recorded by operations that committed before cancellation.

An earlier PUT or DELETE may still finish remotely after cancellation. After ownership is revoked, storage ownership checks reject metadata publication and attempts to start another chunk by the stale execution. A completed PUT whose publication was rejected remains represented by desired and can be repeated on continuation. A DELETE already authorized by a tombstone may finish; the retained cleanup entry makes it resumable, and the tombstone rejects reattachment.

Cancellation can run in a different task instance. Joining local workers covers that instance, while storage mutations enforce the ownership checks above against previous executions. Retry a failed cancellation transaction or unlock idempotently, releasing only the cancelled execution's owned lock and preserving recorded placement and any newer owner's lock.

A later migration chooses its destination from its own request and starts a full copy scan. For example, after cancelling A-to-B, a task targeting C first establishes C for every chunk, then releases the remaining A and B entries through the detach protocol. A task targeting B reuses the B copies already completed. A tombstoned destination fails that chunk's copy before PUT; completed work remains recorded for a later task. Copy-phase cancellation leaves sources intact. Detach-phase cancellation can leave a mixture of completed releases and pending deletions, with every chunk already present at that task's destination. Later migration completes pending releases during its detach phase and retains tombstones for sources already detached. An explicit snapshot deletion can subsequently clean the resource through the separate deletion procedure.

```mermaid
flowchart TD
    Running[Migration is copying or detaching] --> Cancel[Cancel requested]
    Cancel --> Stop[Revoke execution ownership and stop scheduling work]
    Stop --> Keep[Keep actual, desired, tombstones and pending cleanup]
    Keep --> Unlock[Release owned snapshot lock]
    Unlock --> Next[Later task acquires lock with its requested destination]
    Next --> Copy[Scan all chunks and reuse or copy to destination]
    Copy --> Ready{Every destination copy completed?}
    Ready -->|Yes| Detach[Save detach phase and release remaining placements]
    Ready -->|Failure| Retry[Retain recorded placement for retry or cancellation]
```

This cancellation rule applies to migration. Cancellation of snapshot or image creation retains its existing behavior of deleting the partial destination and cleaning its references through the tombstone-aware deletion protocol. It preserves source snapshots and their backend requirements.

## Bulk migration

The bulk request contains a dataplane snapshot ID prefix and destination backend ID. Validate the destination and save a cutoff once. Wait for creation tasks started before the backend change before capturing that cutoff, because `created_at` records creation completion.

Select Ready snapshots before the cutoff whose completion field is unset:

```sql
SELECT id, created_at
FROM snapshots
WHERE status = Ready
  AND created_at < $cutoff
  AND StartsWith(id, $dataplane_id_prefix)
  AND s3_backend_ids IS NULL
ORDER BY created_at, id
LIMIT $batch_size;
```

`Ready` represents the stored status. Run up to ten children by default, with a configurable limit. Save selected snapshot IDs before scheduling. Use a stable bulk-task/snapshot scheduling key, then save child task IDs before waiting. Keep the cutoff and only active snapshot/task pairs in task state. Remove finished pairs, save, and select again. Schedule snapshots sharing a chunk concurrently within the requested prefix, independently of parent or creator migration order.

If the parent stops before scheduling, the saved snapshot ID remains to schedule. If scheduling succeeds and saving the child ID fails, repeat scheduling with the same key to recover that child. If a child sets the completion field before finishing its unlock, the query excludes the snapshot and the parent continues waiting for the saved child. Retain completed child-task records for the supported restart interval so a restarted parent can resolve those IDs.

A child skips a snapshot that is absent or Deleting when it attempts admission; a direct single-snapshot request reports an error. A lock held by another operation causes retry. An unknown backend or tombstoned destination is an error. Transient source unavailability follows normal retry behavior. Report a permanently failed child as a migration error.

After filtering active or unavailable candidates, continue waiting or selecting while the underlying query has candidates or a saved child remains. Complete the bulk task when both the underlying query result and the saved child set are empty. The NULL-only selector targets snapshots awaiting their first completed migration; use explicit single-snapshot requests or a separate source/target selector for later moves.

Bulk cancellation stops scheduling, recovers any scheduled child with an unsaved ID using its original key, cancels all active children, and waits for cancellation to finish. Child cancellation preserves chunk placement and pending work as described above. If cancelling a child or saving the parent state fails, retry using saved IDs and stable keys while retaining the objects, references, placement, and pending work recorded by the children.

```mermaid
flowchart TD
    Select[Select with saved cutoff] --> Done{Query and saved child set empty?}
    Done -->|Yes| Finish[Finish]
    Done -->|No| Save[Save new snapshot IDs]
    Save --> Schedule[Schedule or recover children with stable keys]
    Schedule --> IDs[Save child IDs]
    IDs --> Wait[Wait for children or retry admission]
    Wait --> Update[Record outcomes and remaining active pairs]
    Update --> Select
```

## Creation and incrementality

Keep existing incremental base lookup and checkpoint handling. A base on a different backend remains usable: register references to unchanged chunk IDs and reuse or copy their payloads to the selected backend. Preserve those IDs and use snapshot storage for any payload transfer when backends differ. Write new or changed chunks directly to the selected backend.

For a nonzero backend ID, complete creation when every nonzero chunk has that destination in actual and temporary-source detachment has finished. Reusable copies register only the requested backend. For a snapshot with unspecified backend, apply the existing per-chunk selection and inheritance rules; completion accepts mixed placement and the recorded or inferred placement of previously processed chunks.

If a chunk copy fails, preserve its map, reference count, desired destination, and actual source locations for retry. If all copies finish and readiness publication fails, reuse the existing references and completed payloads while retrying readiness publication. If a requested backend was previously detached for a shared chunk, reject creation before issuing PUT for that chunk. Creation cancellation deletes its partial destination through the reference-cleanup protocol while preserving the source's references.

## Backup and existing database migration

Backup reads resolve actual backends from current reference rows, including the all-NotFound refresh behavior. Pass the source snapshot referer through the backup read API. A chunk-ID-only operation resolves surviving references consistently and treats their completed actual locations as its read candidates. Tombstones and desired-only entries retain their cleanup and write-tracking roles. Resolve unspecified placement from the shared root in the same database read.

If the creator disappeared and another reference survives, use the surviving reference. If all references disappear, return NotFound; final chunk cleanup removes the root as well. Source protection must cover any backup that requires the snapshot to remain available for the operation. A backend failure leaves the backup task retryable according to the read result. Report a map complete once every chunk ID it contains has a backup payload.

Preserve compressed data, checksums, and compression metadata. Backup objects remain separate from actual chunk backends, and each chunk retains its existing backup object key across moves. Update the source and destination wrappers used by existing database-migration tasks to resolve the new fields. When the destination uses an unrelated chunk-ID namespace, initialize its placement history independently and retain source detach tombstones in the source namespace.

## Capacity and deployment

Configure new backends in the dataplane before selecting them for new writes. Plan for source and destination payloads to coexist during the copy phase. Shared source copies remain until every reference releases them. Count physical copies by `(chunk_id, backend_id)` when estimating capacity. Configure simultaneous-snapshot, chunk-operation, and byte limits, with separate request and bandwidth limits per backend.

Deploy schema and compatible dataplane reads/writes first. Enable placement changes once all writers register explicit actual/desired state and honor tombstones. Existing controlplane requests and saved tasks continue using the unspecified-backend rules. After dataplane rollout, enable controlplane backend selection and migrations.

Before retiring a backend, check active references using both explicit sets and unspecified-backend resolution, outstanding creation/migration destinations, and pending physical cleanup. Finish or explicitly resolve those operations, including ambiguous S3 requests. Preserve tombstones for the lifetime of each surviving chunk, including after active configuration is removed. They continue to reject attachment to that backend ID for those chunks. Final chunk deletion removes them with the root.

## Implementation steps

1. Add stable multi-S3 configuration, zero-value request compatibility, and client selection.
2. Add reference placement sets, root detach tombstones, and migration execution ownership checks, including transactional shadow-table updates.
3. Add consistent unspecified-backend resolution and backend refetching after all attempted locations return NotFound.
4. Extend writes, reference reuse, copying, detach, and deletion with the transaction and retry rules above.
5. For migration, require Ready status when acquiring `snapshots.lock_task_id`, save copy/detach phases, and preserve placement on cancellation.
6. Add bounded bulk scheduling with stable child keys and cancellation that waits for children.
7. Integrate backend selection into image/snapshot creation and update backup/database-migration reads.

## Details to settle

- Whether copying preserves the source storage class or uses the destination's configured class.
- Default bandwidth/concurrency limits and completed child-task retention for bulk restarts.
- The access path for bounded bulk selection.
