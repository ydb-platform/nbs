# Cross-shard linked disk copy

This extends the existing NBS linked-volume copy workflow to storage shards in one availability zone and one YDB cluster. It reuses background copying, mirrored writes, automatic leadership transfer and exact deletion of the old physical volume. The initial supported cross-shard configuration is replicated network SSD/HDD, with unchanged creation parameters except an optional media kind change.

## Configuration

Every participating NBS node must use the same allowlisted shard identifiers and directory mapping. For example, on the source node:

~~~protobuf
SchemeShardDir: "/Root/nbs-source"
ShardDirectories {
    key: "source"
    value: "/Root/nbs-source"
}
ShardDirectories {
    key: "target"
    value: "/Root/nbs-target"
}
~~~

On the destination node, use the same mapping and set `SchemeShardDir` to `"/Root/nbs-target"`. Directories must be absolute canonical YDB paths. Trailing slashes are normalized. Unknown identifiers and paths containing relative components are rejected.

An empty shard identifier selects the receiving node's local shard. A cross-shard link therefore requires both `LeaderShardId` and `FollowerShardId`. `Headers.ShardId` addresses the destination shard of a particular request; `Headers.CellId` is not a destination selector.

## Prepare the destination

Resolve the active source's physical identifier and use the existing `CreateVolume` workflow to create its alternate physical name in the target shard. Preserve the source creation configuration; the postfix alone does not copy resource metadata. Select a different media kind only when requested.

For an active `disk`, the destination name is `disk-copy`. For an active `disk-copy`, the next name is `disk`. Do not invent a new postfix when retrying an existing operation.

The addressing portion of a destination creation request is:

~~~protobuf
Headers {
    ShardId: "target"
}
DiskId: "disk-copy"
~~~

Include the source's remaining creation parameters in that request. Cross-shard creation through a source node rejects DiskRegistry-based media kinds before allocation. Cross-shard links validate both endpoints as replicated SSD/HDD before persisting a relationship. This admission check also applies when recovering a persisted `Created` link and when the destination receives its creation request. Creation records the destination tablet identifier and checks it on the destination owner, so a delayed `CREATE` cannot attach to a volume deleted and recreated under the same physical name. Remote deletion of DiskRegistry-based volumes is rejected before registry or schema changes; a remote not-found synchronous delete never deallocates a local disk. Existing local DiskRegistry workflows remain supported.

## Start and inspect the copy

The existing command accepts optional shard arguments:

~~~bash
blockstore-client createvolumelink \
    --leader-disk-id disk \
    --follower-disk-id disk-copy \
    --leader-shard-id source \
    --follower-shard-id target
~~~

The command is sent to an NBS endpoint that has the configured mapping. Both physical names are resolved exactly in their respective shards.

Inspect the same link, including after a component restart:

~~~bash
blockstore-client executeaction \
    --action GetLinkStatus \
    --input-bytes '{"LeaderDiskId":"disk","LeaderShardId":"source","FollowerDiskId":"disk-copy","FollowerShardId":"target"}'
~~~

A repeated create request for the same active link is idempotent. Query the existing link before starting another operation after an ambiguous response.

`LINK_STATUS_PREPARING` means copying is in progress. `LINK_STATUS_LEADERSHIP_TRANSFERRED` means the destination is authoritative, but old-source cleanup is still pending. `LINK_STATUS_COMPLETED` confirms that the old source has been deleted and the destination recorded that result. Cleanup is restored from the persisted destination link after a tablet restart, even without mounts, I/O or partition GC. Deletion retries are idempotent.

Each operation has at most one scheduled or in-flight cleanup request. The delay belongs to the current destination volume actor, which checks that the UUID still requires cleanup before sending a delete. If an internal unlink removes that UUID, its cleanup reservation is released and another pending operation can proceed, including when an already queued completion transaction is rejected.

Cross-shard cleanup requests carry `ExpectedVolumeTabletId` and use exact physical names. Schema deletion also checks the resolved path incarnation atomically, so an old request cannot delete a volume recreated under the same name. Conditional deletion supports only replicated SSD/HDD and rejects `DestroyIfBroken`. The service checks the media kind both before and after `StatVolume`, before DiskRegistry cleanup or graceful shutdown. If `StatVolume` reports not-found after the guarded describe, conditional deletion returns `S_ALREADY` without DiskRegistry deallocation, including with `Sync=true`. Ordinary local DiskRegistry deletion without `ExpectedVolumeTabletId` remains unchanged.

Before deletion, cleanup verifies the operation UUID, tablet identifier and media kind on the current source owner. It does not infer ownership from the physical name alone. A different current tablet identifier means that the recorded source incarnation is already gone. An incomplete response without verifiable UUID/tablet information leaves cleanup pending for retry; it does not mark the operation completed.

Existing same-shard DiskRegistry copies retain their legacy cleanup path without `ExpectedVolumeTabletId` after source-link verification. Cross-shard cleanup never falls back to unconditional deletion for unsupported or unknown media kinds.

Disconnected disks copy without a user mount: source partitions are retained in a copy-only mode while data is needed. Cancellation releases that retention and resets stopped partition state so subsequent mounts and I/O can start the partitions again.

If a recovered `Created` link fails because the destination is missing, recreated or unsupported, copy-only retention is released after the terminal `Error` state commits. Partitions either stop or return to the normal GC lifecycle if needed. This release does not stop partitions serving a mounted source.

## Cancellation

Cancel before leadership transfer using the existing command:

~~~bash
blockstore-client destroyvolumelink \
    --leader-disk-id disk \
    --follower-disk-id disk-copy \
    --leader-shard-id source \
    --follower-shard-id target
~~~

The service checks the cancellation boundary inside the owning volume tablet's deleting transaction and propagates cancellation only after a successful commit. Once transfer starts, cancellation returns `E_INVALID_STATE` and leaves the authoritative relationship intact. The same protection applies when the source has already disappeared and the request is checked on the destination.

Late progress updates cannot recreate a cancelled source link or modify a newer operation with a different UUID. Cancellation retains the original UUID even if creation has not yet persisted a follower. The source persists a pending cancellation with the original `RequireCancellable` value until the destination acknowledgment is committed. A source restart or repeated cancellation resumes delivery; a lost message or acknowledgment cannot discard the cancellation obligation. Pending cancellations do not block a new copy generation on the source; the destination must process cancellation before accepting the replacement link.

The destination stores a durable cancellation fence for that UUID, rejecting delayed `CREATE` messages after cancellation or reboot. The destination tablet identifier also rejects those messages after the destination volume itself is deleted and recreated. A repeated cancellation that changes no link state does not restart partitions.

Public cancellation retains the cutover check. Internal diagnostic unlink preserves the caller's `RequireCancellable` value on both sides. Link lookup treats aliases of the same configured directory as equivalent, including persisted empty local selectors.

Removing a link does not delete the partially filled destination volume. The internal caller must dispose of that destination separately when cancelling. Address its shard and use an exact physical name for that cleanup:

~~~protobuf
Headers {
    ShardId: "target"
    ExactDiskIdMatch: true
}
DiskId: "disk-copy"
~~~

This is the addressing portion of a `DestroyVolume` request.

## Routing after transfer

After transfer, use an endpoint for the destination shard for mounts, session-bound I/O and other service operations. Through the storage service, a nonlocal `Headers.ShardId` is supported only by `CreateVolume`, `DescribeVolume`, `DestroyVolume` and `StatVolume`. Other service requests with a nonlocal selector return `E_NOT_IMPLEMENTED` before local session lookup or side effects. Unknown selectors return `E_ARGUMENT`. Local aliases remain supported. A remote DiskRegistry volume description is rejected before local device lookup; it cannot combine foreign schema metadata with local allocations.

The logical name remains accepted by the destination shard's existing alternate-name lookup. For example, an explicitly routed describe request is:

~~~protobuf
Headers {
    ShardId: "target"
}
DiskId: "disk"
~~~

This is a complete `DescribeVolume` request. Exact-name matching must remain disabled when resolving the logical name.

This operation does not add cluster-wide disk discovery or update Disk Manager or Compute placement metadata. The internal caller remains responsible for routing and any integration-level resource metadata changes. Normal SDK session recovery/remount is still required after a tablet restart.

## Verification

The test coverage includes:

- existing requests without shard fields;
- known, unknown and invalid shard mappings, including local aliases;
- same physical names in different shards and independent proxy caches;
- schema operations with and without SchemeCache, including backup reads;
- SSD-to-SSD, HDD-to-HDD, SSD-to-HDD and HDD-to-SSD copies;
- detached source volumes and foreground writes/zeroes during copy;
- repeated link creation, cancellation and late-cancellation rejection;
- source, destination and simultaneous tablet restarts with copy I/O pending;
- actual old-source deletion and the committed final link state;
- repeat copying back to the original shard with alternate names;
- preservation of data and destination configuration;
- unsupported remote service requests rejected before local session lookup;
- remote stats isolated from cached local sessions;
- remote DiskRegistry deletion and synchronous not-found cleanup isolation;
- both link endpoint media kinds validated before persistence;
- alias-equivalent and legacy-local link lookup and cancellation;
- queued cutover/cancellation requests on both owning tablets;
- late progress rejected after cancellation and operation recreation;
- remote mount/I/O after cancellation of a rebooted copy-only source;
- idle destination cleanup restored after reboot with GC disabled;
- duplicate cleanup timers and conditional schema deletion against a recreated source;
- original UUID retained when cancelling pending creation;
- cancelled destination UUIDs remain fenced across reboot;
- idempotent cancellation preserves mounted volume I/O;
- unrestricted diagnostic unlink preserves its original flag;
- recovered creation revalidates a replaced destination's media kind;
- wrapper state follows authoritative errors of the same operation UUID;
- cancellation checked with a real executor queue held before execution;
- foreign DiskRegistry descriptions rejected before device lookup;
- lost cancellation messages and acknowledgments recovered after source restart;
- delayed creation rejected after destination deletion and recreation;
- legacy `Created` links acquire and persist the destination tablet identifier on recovery;
- cleanup handed off after cancellation and a rejected queued completion transaction;
- conditional local DiskRegistry deletion rejected before cleanup or shutdown;
- a DiskRegistry replacement created between guarded describe and stat left untouched;
- existing same-shard DiskRegistry copy cleanup completes without conditional deletion;
- conditional Sync deletion handles a late not-found stat without deallocating a replacement;
- failed `Created` recovery releases copy-only partitions after the Error commit;
- delayed Error transactions retain partitions until commit and preserve mounted source I/O.

Build the server and administrative client:

~~~bash
./ya make -j16 cloud/blockstore/apps/server cloud/blockstore/apps/client
~~~

Run the related regression suites:

~~~bash
./ya make -tA -j16 \
    cloud/blockstore/libs/storage/core/ut \
    cloud/blockstore/libs/storage/ss_proxy/ut \
    cloud/blockstore/libs/storage/volume_proxy/ut \
    cloud/blockstore/libs/storage/service/ut \
    cloud/blockstore/libs/storage/volume/ut \
    cloud/blockstore/libs/storage/volume/ut_linked \
    cloud/blockstore/libs/storage/volume/actors/ut \
    cloud/blockstore/libs/storage/volume/model/ut \
    cloud/blockstore/apps/client/lib/ut
~~~
